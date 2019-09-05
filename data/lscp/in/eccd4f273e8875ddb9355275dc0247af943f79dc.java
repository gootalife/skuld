hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirTruncateOp.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirTruncateOp.java
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstructionContiguous;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.RecoverLeaseOp;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;

import com.google.common.annotations.VisibleForTesting;

final class FSDirTruncateOp {

  private FSDirTruncateOp() {}

  static TruncateResult truncate(final FSNamesystem fsn, final String srcArg,
      final long newLength, final String clientName,
      final String clientMachine, final long mtime,
      final BlocksMapUpdateInfo toRemoveBlocks, final FSPermissionChecker pc)
      throws IOException, UnresolvedLinkException {
    assert fsn.hasWriteLock();

    FSDirectory fsd = fsn.getFSDirectory();
    byte[][] pathComponents = FSDirectory
        .getPathComponentsForReservedPath(srcArg);
    final String src;
    final INodesInPath iip;
    final boolean onBlockBoundary;
    Block truncateBlock = null;
    fsd.writeLock();
    try {
      src = fsd.resolvePath(pc, srcArg, pathComponents);
      iip = fsd.getINodesInPath4Write(src, true);
      if (fsn.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }
      INodeFile file = INodeFile.valueOf(iip.getLastINode(), src);
      final BlockStoragePolicy lpPolicy = fsn.getBlockManager()
          .getStoragePolicy("LAZY_PERSIST");

      if (lpPolicy != null && lpPolicy.getId() == file.getStoragePolicyID()) {
        throw new UnsupportedOperationException(
            "Cannot truncate lazy persist file " + src);
      }

      final BlockInfo last = file.getLastBlock();
      if (last != null && last.getBlockUCState()
          == BlockUCState.UNDER_RECOVERY) {
        final Block truncatedBlock = ((BlockInfoUnderConstruction) last)
            .getTruncateBlock();
        if (truncatedBlock != null) {
          final long truncateLength = file.computeFileSize(false, false)
              + truncatedBlock.getNumBytes();
          if (newLength == truncateLength) {
            return new TruncateResult(false, fsd.getAuditFileInfo(iip));
          }
        }
      }

      fsn.recoverLeaseInternal(RecoverLeaseOp.TRUNCATE_FILE, iip, src,
          clientName, clientMachine, false);
      long oldLength = file.computeFileSize();
      if (oldLength == newLength) {
        return new TruncateResult(true, fsd.getAuditFileInfo(iip));
      }
      if (oldLength < newLength) {
        throw new HadoopIllegalArgumentException(
            "Cannot truncate to a larger file size. Current size: " + oldLength
                + ", truncate size: " + newLength + ".");
      }
      final QuotaCounts delta = new QuotaCounts.Builder().build();
      onBlockBoundary = unprotectedTruncate(fsn, iip, newLength,
          toRemoveBlocks, mtime, delta);
      if (!onBlockBoundary) {
        long lastBlockDelta = file.computeFileSize() - newLength;
        assert lastBlockDelta > 0 : "delta is 0 only if on block bounday";
        truncateBlock = prepareFileForTruncate(fsn, iip, clientName,
            clientMachine, lastBlockDelta, null);
      }

      fsd.updateCountNoQuotaCheck(iip, iip.length() - 1, delta);
    } finally {
      fsd.writeUnlock();
    }

    fsn.getEditLog().logTruncate(src, clientName, clientMachine, newLength,
        mtime, truncateBlock);
    return new TruncateResult(onBlockBoundary, fsd.getAuditFileInfo(iip));
  }

  static void unprotectedTruncate(final FSNamesystem fsn, final String src,
      final String clientName, final String clientMachine,
      final long newLength, final long mtime, final Block truncateBlock)
      throws UnresolvedLinkException, QuotaExceededException,
      SnapshotAccessControlException, IOException {
    assert fsn.hasWriteLock();

    FSDirectory fsd = fsn.getFSDirectory();
    INodesInPath iip = fsd.getINodesInPath(src, true);
    INodeFile file = iip.getLastINode().asFile();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    boolean onBlockBoundary = unprotectedTruncate(fsn, iip, newLength,
        collectedBlocks, mtime, null);

    if (!onBlockBoundary) {
      BlockInfo oldBlock = file.getLastBlock();
      Block tBlk = prepareFileForTruncate(fsn, iip, clientName, clientMachine,
          file.computeFileSize() - newLength, truncateBlock);
      assert Block.matchingIdAndGenStamp(tBlk, truncateBlock) &&
          tBlk.getNumBytes() == truncateBlock.getNumBytes() :
          "Should be the same block.";
      if (oldBlock.getBlockId() != tBlk.getBlockId()
          && !file.isBlockInLatestSnapshot(oldBlock)) {
        fsn.getBlockManager().removeBlockFromMap(oldBlock);
      }
    }
    assert onBlockBoundary == (truncateBlock == null) :
      "truncateBlock is null iff on block boundary: " + truncateBlock;
    fsn.removeBlocksAndUpdateSafemodeTotal(collectedBlocks);
  }

  @VisibleForTesting
  static Block prepareFileForTruncate(FSNamesystem fsn, INodesInPath iip,
      String leaseHolder, String clientMachine, long lastBlockDelta,
      Block newBlock) throws IOException {
    assert fsn.hasWriteLock();

    INodeFile file = iip.getLastINode().asFile();
    file.recordModification(iip.getLatestSnapshotId());
    file.toUnderConstruction(leaseHolder, clientMachine);
    assert file.isUnderConstruction() : "inode should be under construction.";
    fsn.getLeaseManager().addLease(
        file.getFileUnderConstructionFeature().getClientName(), file.getId());
    boolean shouldRecoverNow = (newBlock == null);
    BlockInfo oldBlock = file.getLastBlock();
    boolean shouldCopyOnTruncate = shouldCopyOnTruncate(fsn, file, oldBlock);
    if (newBlock == null) {
      newBlock = (shouldCopyOnTruncate) ? fsn.createNewBlock() : new Block(
          oldBlock.getBlockId(), oldBlock.getNumBytes(),
          fsn.nextGenerationStamp(fsn.getBlockIdManager().isLegacyBlock(
              oldBlock)));
    }

    BlockInfoUnderConstruction truncatedBlockUC;
    if (shouldCopyOnTruncate) {
      truncatedBlockUC = new BlockInfoUnderConstructionContiguous(newBlock,
          file.getPreferredBlockReplication());
      truncatedBlockUC.setNumBytes(oldBlock.getNumBytes() - lastBlockDelta);
      truncatedBlockUC.setTruncateBlock(oldBlock);
      file.setLastBlock(truncatedBlockUC,
          fsn.getBlockManager().getStorages(oldBlock));
      fsn.getBlockManager().addBlockCollection(truncatedBlockUC, file);

      NameNode.stateChangeLog.debug(
          "BLOCK* prepareFileForTruncate: Scheduling copy-on-truncate to new"
              + " size {}  new block {} old block {}",
          truncatedBlockUC.getNumBytes(), newBlock,
          truncatedBlockUC.getTruncateBlock());
    } else {
      fsn.getBlockManager().convertLastBlockToUnderConstruction(file,
          lastBlockDelta);
      oldBlock = file.getLastBlock();
      assert !oldBlock.isComplete() : "oldBlock should be under construction";
      truncatedBlockUC = (BlockInfoUnderConstruction) oldBlock;
      truncatedBlockUC.setTruncateBlock(new Block(oldBlock));
      truncatedBlockUC.getTruncateBlock().setNumBytes(
          oldBlock.getNumBytes() - lastBlockDelta);
      truncatedBlockUC.getTruncateBlock().setGenerationStamp(
          newBlock.getGenerationStamp());

      NameNode.stateChangeLog.debug(
          "BLOCK* prepareFileForTruncate: {} Scheduling in-place block "
              + "truncate to new size {}", truncatedBlockUC.getTruncateBlock()
              .getNumBytes(), truncatedBlockUC);
    }
    if (shouldRecoverNow) {
      truncatedBlockUC.initializeBlockRecovery(newBlock.getGenerationStamp());
    }

    return newBlock;
  }

  private static boolean unprotectedTruncate(FSNamesystem fsn,
      INodesInPath iip, long newLength, BlocksMapUpdateInfo collectedBlocks,
      long mtime, QuotaCounts delta) throws IOException {
    assert fsn.hasWriteLock();

    INodeFile file = iip.getLastINode().asFile();
    int latestSnapshot = iip.getLatestSnapshotId();
    file.recordModification(latestSnapshot, true);

    verifyQuotaForTruncate(fsn, iip, file, newLength, delta);

    long remainingLength =
        file.collectBlocksBeyondMax(newLength, collectedBlocks);
    file.excludeSnapshotBlocks(latestSnapshot, collectedBlocks);
    file.setModificationTime(mtime);
    return (remainingLength - newLength) == 0;
  }

  private static void verifyQuotaForTruncate(FSNamesystem fsn,
      INodesInPath iip, INodeFile file, long newLength, QuotaCounts delta)
      throws QuotaExceededException {
    FSDirectory fsd = fsn.getFSDirectory();
    if (!fsn.isImageLoaded() || fsd.shouldSkipQuotaChecks()) {
      return;
    }
    final BlockStoragePolicy policy = fsd.getBlockStoragePolicySuite()
        .getPolicy(file.getStoragePolicyID());
    file.computeQuotaDeltaForTruncate(newLength, policy, delta);
    fsd.readLock();
    try {
      FSDirectory.verifyQuota(iip, iip.length() - 1, delta, null);
    } finally {
      fsd.readUnlock();
    }
  }

  private static boolean shouldCopyOnTruncate(FSNamesystem fsn, INodeFile file,
      BlockInfo blk) {
    if (!fsn.isUpgradeFinalized()) {
      return true;
    }
    if (fsn.isRollingUpgrade()) {
      return true;
    }
    return file.isBlockInLatestSnapshot(blk);
  }

  static class TruncateResult {
    private final boolean result;
    private final HdfsFileStatus stat;

    public TruncateResult(boolean result, HdfsFileStatus stat) {
      this.result = result;
      this.stat = stat;
    }

    boolean getResult() {
      return result;
    }

    HdfsFileStatus getFileStatus() {
      return stat;
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirectory.java
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.security.AccessControlException;
    return inodeMap;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogLoader.java
    }
    case OP_TRUNCATE: {
      TruncateOp truncateOp = (TruncateOp) op;
      FSDirTruncateOp.unprotectedTruncate(fsNamesys, truncateOp.src,
          truncateOp.clientName, truncateOp.clientMachine,
          truncateOp.newLength, truncateOp.timestamp, truncateOp.truncateBlock);
      break;
    }
    case OP_SET_STORAGE_POLICY: {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
  boolean truncate(String src, long newLength, String clientName,
      String clientMachine, long mtime) throws IOException,
      UnresolvedLinkException {

    requireEffectiveLayoutVersionForFeature(Feature.TRUNCATE);
    final FSDirTruncateOp.TruncateResult r;
    try {
      NameNode.stateChangeLog.debug(
          "DIR* NameSystem.truncate: src={} newLength={}", src, newLength);
      if (newLength < 0) {
        throw new HadoopIllegalArgumentException(
            "Cannot truncate to a negative file size: " + newLength + ".");
      }
      final FSPermissionChecker pc = getPermissionChecker();
      checkOperation(OperationCategory.WRITE);
      writeLock();
      BlocksMapUpdateInfo toRemoveBlocks = new BlocksMapUpdateInfo();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot truncate for " + src);
        r = FSDirTruncateOp.truncate(this, src, newLength, clientName,
            clientMachine, mtime, toRemoveBlocks, pc);
      } finally {
        writeUnlock();
      }
        removeBlocks(toRemoveBlocks);
        toRemoveBlocks.clear();
      }
      logAuditEvent(true, "truncate", src, null, r.getFileStatus());
    } catch (AccessControlException e) {
      logAuditEvent(false, "truncate", src);
      throw e;
    }
    return r.getResult();
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFileTruncate.java
    fsn.writeLock();
    try {
      Block oldBlock = file.getLastBlock();
      Block truncateBlock = FSDirTruncateOp.prepareFileForTruncate(fsn, iip,
          client, clientMachine, 1, null);
      assertThat(truncateBlock.getBlockId(),
          is(equalTo(oldBlock.getBlockId())));
    fsn.writeLock();
    try {
      Block oldBlock = file.getLastBlock();
      Block truncateBlock = FSDirTruncateOp.prepareFileForTruncate(fsn, iip,
          client, clientMachine, 1, null);
      assertThat(truncateBlock.getBlockId(),
          is(not(equalTo(oldBlock.getBlockId()))));

