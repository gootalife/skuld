hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirAppendOp.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirAppendOp.java
package org.apache.hadoop.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.RecoverLeaseOp;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion.Feature;

import com.google.common.base.Preconditions;

final class FSDirAppendOp {

  private FSDirAppendOp() {}

  static LastBlockWithStatus appendFile(final FSNamesystem fsn,
      final String srcArg, final FSPermissionChecker pc, final String holder,
      final String clientMachine, final boolean newBlock,
      final boolean logRetryCache) throws IOException {
    assert fsn.hasWriteLock();

    final byte[][] pathComponents = FSDirectory
        .getPathComponentsForReservedPath(srcArg);
    final LocatedBlock lb;
    final FSDirectory fsd = fsn.getFSDirectory();
    final String src;
    fsd.writeLock();
    try {
      src = fsd.resolvePath(pc, srcArg, pathComponents);
      final INodesInPath iip = fsd.getINodesInPath4Write(src);
      final INode inode = iip.getLastINode();
      final String path = iip.getPath();
      if (inode != null && inode.isDirectory()) {
        throw new FileAlreadyExistsException("Cannot append to directory "
            + path + "; already exists as a directory.");
      }
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }

      if (inode == null) {
        throw new FileNotFoundException(
            "Failed to append to non-existent file " + path + " for client "
                + clientMachine);
      }
      final INodeFile file = INodeFile.valueOf(inode, path, true);
      BlockManager blockManager = fsd.getBlockManager();
      final BlockStoragePolicy lpPolicy = blockManager
          .getStoragePolicy("LAZY_PERSIST");
      if (lpPolicy != null && lpPolicy.getId() == file.getStoragePolicyID()) {
        throw new UnsupportedOperationException(
            "Cannot append to lazy persist file " + path);
      }
      fsn.recoverLeaseInternal(RecoverLeaseOp.APPEND_FILE, iip, path, holder,
          clientMachine, false);

      final BlockInfo lastBlock = file.getLastBlock();
      if (lastBlock != null && lastBlock.isComplete()
          && !blockManager.isSufficientlyReplicated(lastBlock)) {
        throw new IOException("append: lastBlock=" + lastBlock + " of src="
            + path + " is not sufficiently replicated yet.");
      }
      lb = prepareFileForAppend(fsn, iip, holder, clientMachine, newBlock,
          true, logRetryCache);
    } catch (IOException ie) {
      NameNode.stateChangeLog
          .warn("DIR* NameSystem.append: " + ie.getMessage());
      throw ie;
    } finally {
      fsd.writeUnlock();
    }

    HdfsFileStatus stat = FSDirStatAndListingOp.getFileInfo(fsd, src, false,
        FSDirectory.isReservedRawName(srcArg), true);
    if (lb != null) {
      NameNode.stateChangeLog.debug(
          "DIR* NameSystem.appendFile: file {} for {} at {} block {} block"
              + " size {}", srcArg, holder, clientMachine, lb.getBlock(), lb
              .getBlock().getNumBytes());
    }
    return new LastBlockWithStatus(lb, stat);
  }

  static LocatedBlock prepareFileForAppend(final FSNamesystem fsn,
      final INodesInPath iip, final String leaseHolder,
      final String clientMachine, final boolean newBlock,
      final boolean writeToEditLog, final boolean logRetryCache)
      throws IOException {
    assert fsn.hasWriteLock();

    final INodeFile file = iip.getLastINode().asFile();
    final QuotaCounts delta = verifyQuotaForUCBlock(fsn, file, iip);

    file.recordModification(iip.getLatestSnapshotId());
    file.toUnderConstruction(leaseHolder, clientMachine);

    fsn.getLeaseManager().addLease(
        file.getFileUnderConstructionFeature().getClientName(), file.getId());

    LocatedBlock ret = null;
    if (!newBlock) {
      FSDirectory fsd = fsn.getFSDirectory();
      ret = fsd.getBlockManager().convertLastBlockToUnderConstruction(file, 0);
      if (ret != null && delta != null) {
        Preconditions.checkState(delta.getStorageSpace() >= 0, "appending to"
            + " a block with size larger than the preferred block size");
        fsd.writeLock();
        try {
          fsd.updateCountNoQuotaCheck(iip, iip.length() - 1, delta);
        } finally {
          fsd.writeUnlock();
        }
      }
    } else {
      BlockInfo lastBlock = file.getLastBlock();
      if (lastBlock != null) {
        ExtendedBlock blk = new ExtendedBlock(fsn.getBlockPoolId(), lastBlock);
        ret = new LocatedBlock(blk, new DatanodeInfo[0]);
      }
    }

    if (writeToEditLog) {
      final String path = iip.getPath();
      if (NameNodeLayoutVersion.supports(Feature.APPEND_NEW_BLOCK,
          fsn.getEffectiveLayoutVersion())) {
        fsn.getEditLog().logAppendFile(path, file, newBlock, logRetryCache);
      } else {
        fsn.getEditLog().logOpenFile(path, file, false, logRetryCache);
      }
    }
    return ret;
  }

  private static QuotaCounts verifyQuotaForUCBlock(FSNamesystem fsn,
      INodeFile file, INodesInPath iip) throws QuotaExceededException {
    FSDirectory fsd = fsn.getFSDirectory();
    if (!fsn.isImageLoaded() || fsd.shouldSkipQuotaChecks()) {
      return null;
    }
    if (file.getLastBlock() != null) {
      final QuotaCounts delta = computeQuotaDeltaForUCBlock(fsn, file);
      fsd.readLock();
      try {
        FSDirectory.verifyQuota(iip, iip.length() - 1, delta, null);
        return delta;
      } finally {
        fsd.readUnlock();
      }
    }
    return null;
  }

  private static QuotaCounts computeQuotaDeltaForUCBlock(FSNamesystem fsn,
      INodeFile file) {
    final QuotaCounts delta = new QuotaCounts.Builder().build();
    final BlockInfo lastBlock = file.getLastBlock();
    if (lastBlock != null) {
      final long diff = file.getPreferredBlockSize() - lastBlock.getNumBytes();
      final short repl = file.getPreferredBlockReplication();
      delta.addStorageSpace(diff * repl);
      final BlockStoragePolicy policy = fsn.getFSDirectory()
          .getBlockStoragePolicySuite().getPolicy(file.getStoragePolicyID());
      List<StorageType> types = policy.chooseStorageTypes(repl);
      for (StorageType t : types) {
        if (t.supportTypeQuota()) {
          delta.addTypeSpace(t, diff);
        }
      }
    }
    return delta;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirStatAndListingOp.java
      final long fileSize = !inSnapshot && isUc ?
          fileNode.computeFileSizeNotIncludingLastUcBlock() : size;

      loc = fsd.getBlockManager().createLocatedBlocks(
          fileNode.getBlocks(snapshot), fileSize, isUc, 0L, size, false,
          inSnapshot, feInfo);
      if (loc == null) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirTruncateOp.java
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstructionContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.RecoverLeaseOp;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
    try {
      src = fsd.resolvePath(pc, srcArg, pathComponents);
      iip = fsd.getINodesInPath4Write(src, true);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }
      INodeFile file = INodeFile.valueOf(iip.getLastINode(), src);
      final BlockStoragePolicy lpPolicy = fsd.getBlockManager()
          .getStoragePolicy("LAZY_PERSIST");

      if (lpPolicy != null && lpPolicy.getId() == file.getStoragePolicyID()) {
          "Should be the same block.";
      if (oldBlock.getBlockId() != tBlk.getBlockId()
          && !file.isBlockInLatestSnapshot(oldBlock)) {
        fsd.getBlockManager().removeBlockFromMap(oldBlock);
      }
    }
    assert onBlockBoundary == (truncateBlock == null) :
    }

    BlockInfoUnderConstruction truncatedBlockUC;
    BlockManager blockManager = fsn.getFSDirectory().getBlockManager();
    if (shouldCopyOnTruncate) {
          file.getPreferredBlockReplication());
      truncatedBlockUC.setNumBytes(oldBlock.getNumBytes() - lastBlockDelta);
      truncatedBlockUC.setTruncateBlock(oldBlock);
      file.setLastBlock(truncatedBlockUC, blockManager.getStorages(oldBlock));
      blockManager.addBlockCollection(truncatedBlockUC, file);

      NameNode.stateChangeLog.debug(
          "BLOCK* prepareFileForTruncate: Scheduling copy-on-truncate to new"
          truncatedBlockUC.getTruncateBlock());
    } else {
      blockManager.convertLastBlockToUnderConstruction(file, lastBlockDelta);
      oldBlock = file.getLastBlock();
      assert !oldBlock.isComplete() : "oldBlock should be under construction";
      truncatedBlockUC = (BlockInfoUnderConstruction) oldBlock;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirWriteFileOp.java
      DatanodeStorageInfo[] locs, long offset) throws IOException {
    LocatedBlock lBlk = BlockManager.newLocatedBlock(fsn.getExtendedBlock(blk),
                                                     locs, offset, false);
    fsn.getFSDirectory().getBlockManager()
        .setBlockToken(lBlk, BlockTokenIdentifier.AccessMode.WRITE);
    return lBlk;
  }

      fsd.setFileEncryptionInfo(src, feInfo);
      newNode = fsd.getInode(newNode.getId()).asFile();
    }
    setNewINodeStoragePolicy(fsd.getBlockManager(), newNode, iip,
                             isLazyPersist);
    fsd.getEditLog().logOpenFile(src, newNode, overwrite, logRetryEntry);
    if (NameNode.stateChangeLog.isDebugEnabled()) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogLoader.java
            FSNamesystem.LOG.debug("Reopening an already-closed file " +
                "for append");
          }
          LocatedBlock lb = FSDirAppendOp.prepareFileForAppend(fsNamesys, iip,
              addCloseOp.clientName, addCloseOp.clientMachine, false, false,
              false);
      INodesInPath iip = fsDir.getINodesInPath4Write(path);
      INodeFile file = INodeFile.valueOf(iip.getLastINode(), path);
      if (!file.isUnderConstruction()) {
        LocatedBlock lb = FSDirAppendOp.prepareFileForAppend(fsNamesys, iip,
            appendOp.clientName, appendOp.clientMachine, appendOp.newBlock,
            false, false);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeException;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RetriableException;
    return stat;
  }

  LastBlockWithStatus appendFile(String srcArg, String holder,
      String clientMachine, EnumSet<CreateFlag> flag, boolean logRetryCache)
      throws IOException {
    boolean newBlock = flag.contains(CreateFlag.NEW_BLOCK);
    if (newBlock) {
      requireEffectiveLayoutVersionForFeature(Feature.APPEND_NEW_BLOCK);
    }

    if (!supportAppends) {
      throw new UnsupportedOperationException(
          "Append is not enabled on this NameNode. Use the " +
          DFS_SUPPORT_APPEND_KEY + " configuration option to enable it.");
    }

    NameNode.stateChangeLog.debug(
        "DIR* NameSystem.appendFile: src={}, holder={}, clientMachine={}",
        srcArg, holder, clientMachine);
    try {
      boolean skipSync = false;
      LastBlockWithStatus lbs = null;
      final FSPermissionChecker pc = getPermissionChecker();
      checkOperation(OperationCategory.WRITE);
      writeLock();
      try {
        checkOperation(OperationCategory.WRITE);
        checkNameNodeSafeMode("Cannot append to file" + srcArg);
        lbs = FSDirAppendOp.appendFile(this, srcArg, pc, holder, clientMachine,
            newBlock, logRetryCache);
      } catch (StandbyException se) {
        skipSync = true;
        throw se;
      } finally {
        writeUnlock();
        if (!skipSync) {
          getEditLog().logSync();
        }
      }
      logAuditEvent(true, "append", srcArg);
      return lbs;
    } catch (AccessControlException e) {
      logAuditEvent(false, "append", srcArg);
      throw e;
    }
  }

  ExtendedBlock getExtendedBlock(Block blk) {

