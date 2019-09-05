hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirWriteFileOp.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirWriteFileOp.java
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class FSDirWriteFileOp {
  private FSDirWriteFileOp() {}
  static boolean unprotectedRemoveBlock(
      FSDirectory fsd, String path, INodesInPath iip, INodeFile fileNode,
      Block block) throws IOException {
    BlockInfoContiguousUnderConstruction uc = fileNode.removeLastBlock(block);
    if (uc == null) {
      return false;
    }
    fsd.getBlockManager().removeBlockFromMap(block);

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.removeBlock: "
          +path+" with "+block
          +" block is removed from the file system");
    }

    fsd.updateCount(iip, 0, -fileNode.getPreferredBlockSize(),
                    fileNode.getPreferredBlockReplication(), true);
    return true;
  }

  static void persistBlocks(
      FSDirectory fsd, String path, INodeFile file, boolean logRetryCache) {
    assert fsd.getFSNamesystem().hasWriteLock();
    Preconditions.checkArgument(file.isUnderConstruction());
    fsd.getEditLog().logUpdateBlocks(path, file, logRetryCache);
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("persistBlocks: " + path
              + " with " + file.getBlocks().length + " blocks is persisted to" +
              " the file system");
    }
  }

  static void abandonBlock(
      FSDirectory fsd, FSPermissionChecker pc, ExtendedBlock b, long fileId,
      String src, String holder) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = fsd.resolvePath(pc, src, pathComponents);

    final INode inode;
    final INodesInPath iip;
    if (fileId == HdfsConstants.GRANDFATHER_INODE_ID) {
      iip = fsd.getINodesInPath(src, true);
      inode = iip.getLastINode();
    } else {
      inode = fsd.getInode(fileId);
      iip = INodesInPath.fromINode(inode);
      if (inode != null) {
        src = iip.getPath();
      }
    }
    FSNamesystem fsn = fsd.getFSNamesystem();
    final INodeFile file = fsn.checkLease(src, holder, inode, fileId);
    Preconditions.checkState(file.isUnderConstruction());

    Block localBlock = ExtendedBlock.getLocalBlock(b);
    fsd.writeLock();
    try {
      if (!unprotectedRemoveBlock(fsd, src, iip, file, localBlock)) {
        return;
      }
    } finally {
      fsd.writeUnlock();
    }
    persistBlocks(fsd, src, file, false);
  }

  static void checkBlock(FSNamesystem fsn, ExtendedBlock block)
      throws IOException {
    String bpId = fsn.getBlockPoolId();
    if (block != null && !bpId.equals(block.getBlockPoolId())) {
      throw new IOException("Unexpected BlockPoolId " + block.getBlockPoolId()
          + " - expected " + bpId);
    }
  }

  static ValidateAddBlockResult validateAddBlock(
      FSNamesystem fsn, FSPermissionChecker pc,
      String src, long fileId, String clientName,
      ExtendedBlock previous, LocatedBlock[] onRetryBlock) throws IOException {
    final long blockSize;
    final int replication;
    final byte storagePolicyID;
    String clientMachine;

    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = fsn.dir.resolvePath(pc, src, pathComponents);
    FileState fileState = analyzeFileState(fsn, src, fileId, clientName,
                                           previous, onRetryBlock);
    final INodeFile pendingFile = fileState.inode;
    if (!fsn.checkFileProgress(src, pendingFile, false)) {
      throw new NotReplicatedYetException("Not replicated yet: " + src);
    }

    if (onRetryBlock[0] != null && onRetryBlock[0].getLocations().length > 0) {
      return null;
    }
    if (pendingFile.getBlocks().length >= fsn.maxBlocksPerFile) {
      throw new IOException("File has reached the limit on maximum number of"
          + " blocks (" + DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_KEY
          + "): " + pendingFile.getBlocks().length + " >= "
          + fsn.maxBlocksPerFile);
    }
    blockSize = pendingFile.getPreferredBlockSize();
    clientMachine = pendingFile.getFileUnderConstructionFeature()
        .getClientMachine();
    replication = pendingFile.getFileReplication();
    storagePolicyID = pendingFile.getStoragePolicyID();
    return new ValidateAddBlockResult(blockSize, replication, storagePolicyID,
                                    clientMachine);
  }

  static LocatedBlock makeLocatedBlock(FSNamesystem fsn, Block blk,
      DatanodeStorageInfo[] locs, long offset) throws IOException {
    LocatedBlock lBlk = BlockManager.newLocatedBlock(fsn.getExtendedBlock(blk),
                                                     locs, offset, false);
    fsn.getBlockManager().setBlockToken(lBlk,
                                        BlockTokenIdentifier.AccessMode.WRITE);
    return lBlk;
  }

  static LocatedBlock storeAllocatedBlock(FSNamesystem fsn, String src,
      long fileId, String clientName, ExtendedBlock previous,
      DatanodeStorageInfo[] targets) throws IOException {
    long offset;
    LocatedBlock[] onRetryBlock = new LocatedBlock[1];
    FileState fileState = analyzeFileState(fsn, src, fileId, clientName,
                                           previous, onRetryBlock);
    final INodeFile pendingFile = fileState.inode;
    src = fileState.path;

    if (onRetryBlock[0] != null) {
      if (onRetryBlock[0].getLocations().length > 0) {
        return onRetryBlock[0];
      } else {
        BlockInfoContiguous lastBlockInFile = pendingFile.getLastBlock();
        ((BlockInfoContiguousUnderConstruction) lastBlockInFile)
            .setExpectedLocations(targets);
        offset = pendingFile.computeFileSize();
        return makeLocatedBlock(fsn, lastBlockInFile, targets, offset);
      }
    }

    fsn.commitOrCompleteLastBlock(pendingFile, fileState.iip,
                                  ExtendedBlock.getLocalBlock(previous));

    Block newBlock = fsn.createNewBlock();
    INodesInPath inodesInPath = INodesInPath.fromINode(pendingFile);
    saveAllocatedBlock(fsn, src, inodesInPath, newBlock, targets);

    persistNewBlock(fsn, src, pendingFile);
    offset = pendingFile.computeFileSize();

    return makeLocatedBlock(fsn, newBlock, targets, offset);
  }

  static DatanodeStorageInfo[] chooseTargetForNewBlock(
      BlockManager bm, String src, DatanodeInfo[] excludedNodes, String[]
      favoredNodes, ValidateAddBlockResult r) throws IOException {
    Node clientNode = bm.getDatanodeManager()
        .getDatanodeByHost(r.clientMachine);
    if (clientNode == null) {
      clientNode = getClientNode(bm, r.clientMachine);
    }

    Set<Node> excludedNodesSet = null;
    if (excludedNodes != null) {
      excludedNodesSet = new HashSet<>(excludedNodes.length);
      Collections.addAll(excludedNodesSet, excludedNodes);
    }
    List<String> favoredNodesList = (favoredNodes == null) ? null
        : Arrays.asList(favoredNodes);

    return bm.chooseTarget4NewBlock(src, r.replication, clientNode,
                                    excludedNodesSet, r.blockSize,
                                    favoredNodesList, r.storagePolicyID);
  }

  static Node getClientNode(BlockManager bm, String clientMachine) {
    List<String> hosts = new ArrayList<>(1);
    hosts.add(clientMachine);
    List<String> rName = bm.getDatanodeManager()
        .resolveNetworkLocation(hosts);
    Node clientNode = null;
    if (rName != null) {
      clientNode = new NodeBase(rName.get(0) + NodeBase.PATH_SEPARATOR_STR
          + clientMachine);
    }
    return clientNode;
  }

  private static BlockInfoContiguous addBlock(
      FSDirectory fsd, String path, INodesInPath inodesInPath, Block block,
      DatanodeStorageInfo[] targets) throws IOException {
    fsd.writeLock();
    try {
      final INodeFile fileINode = inodesInPath.getLastINode().asFile();
      Preconditions.checkState(fileINode.isUnderConstruction());

      fsd.updateCount(inodesInPath, 0, fileINode.getPreferredBlockSize(),
          fileINode.getPreferredBlockReplication(), true);

      BlockInfoContiguousUnderConstruction blockInfo =
        new BlockInfoContiguousUnderConstruction(
            block,
            fileINode.getFileReplication(),
            HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION,
            targets);
      fsd.getBlockManager().addBlockCollection(blockInfo, fileINode);
      fileINode.addBlock(blockInfo);

      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.addBlock: "
            + path + " with " + block
            + " block is added to the in-memory "
            + "file system");
      }
      return blockInfo;
    } finally {
      fsd.writeUnlock();
    }
  }

  private static FileState analyzeFileState(
      FSNamesystem fsn, String src, long fileId, String clientName,
      ExtendedBlock previous, LocatedBlock[] onRetryBlock)
          throws IOException  {
    assert fsn.hasReadLock();

    checkBlock(fsn, previous);
    onRetryBlock[0] = null;
    fsn.checkNameNodeSafeMode("Cannot add block to " + src);

    fsn.checkFsObjectLimit();

    Block previousBlock = ExtendedBlock.getLocalBlock(previous);
    final INode inode;
    final INodesInPath iip;
    if (fileId == HdfsConstants.GRANDFATHER_INODE_ID) {
      iip = fsn.dir.getINodesInPath4Write(src);
      inode = iip.getLastINode();
    } else {
      inode = fsn.dir.getInode(fileId);
      iip = INodesInPath.fromINode(inode);
      if (inode != null) {
        src = iip.getPath();
      }
    }
    final INodeFile file = fsn.checkLease(src, clientName,
                                                 inode, fileId);
    BlockInfoContiguous lastBlockInFile = file.getLastBlock();
    if (!Block.matchingIdAndGenStamp(previousBlock, lastBlockInFile)) {

      BlockInfoContiguous penultimateBlock = file.getPenultimateBlock();
      if (previous == null &&
          lastBlockInFile != null &&
          lastBlockInFile.getNumBytes() >= file.getPreferredBlockSize() &&
          lastBlockInFile.isComplete()) {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
           NameNode.stateChangeLog.debug(
               "BLOCK* NameSystem.allocateBlock: handling block allocation" +
               " writing to a file with a complete previous block: src=" +
               src + " lastBlock=" + lastBlockInFile);
        }
      } else if (Block.matchingIdAndGenStamp(penultimateBlock, previousBlock)) {
        if (lastBlockInFile.getNumBytes() != 0) {
          throw new IOException(
              "Request looked like a retry to allocate block " +
              lastBlockInFile + " but it already contains " +
              lastBlockInFile.getNumBytes() + " bytes");
        }

        NameNode.stateChangeLog.info("BLOCK* allocateBlock: caught retry for " +
            "allocation of a new block in " + src + ". Returning previously" +
            " allocated block " + lastBlockInFile);
        long offset = file.computeFileSize();
        BlockInfoContiguousUnderConstruction lastBlockUC =
            (BlockInfoContiguousUnderConstruction) lastBlockInFile;
        onRetryBlock[0] = makeLocatedBlock(fsn, lastBlockInFile,
            lastBlockUC.getExpectedStorageLocations(), offset);
        return new FileState(file, src, iip);
      } else {
        throw new IOException("Cannot allocate block in " + src + ": " +
            "passed 'previous' block " + previous + " does not match actual " +
            "last block in file " + lastBlockInFile);
      }
    }
    return new FileState(file, src, iip);
  }

  static boolean completeFile(FSNamesystem fsn, FSPermissionChecker pc,
      final String srcArg, String holder, ExtendedBlock last, long fileId)
      throws IOException {
    String src = srcArg;
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.completeFile: " +
                                        src + " for " + holder);
    }
    checkBlock(fsn, last);
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = fsn.dir.resolvePath(pc, src, pathComponents);
    boolean success = completeFileInternal(fsn, src, holder,
                                           ExtendedBlock.getLocalBlock(last),
                                           fileId);
    if (success) {
      NameNode.stateChangeLog.info("DIR* completeFile: " + srcArg
                                       + " is closed by " + holder);
    }
    return success;
  }

  private static boolean completeFileInternal(
      FSNamesystem fsn, String src, String holder, Block last, long fileId)
      throws IOException {
    assert fsn.hasWriteLock();
    final INodeFile pendingFile;
    final INodesInPath iip;
    INode inode = null;
    try {
      if (fileId == HdfsConstants.GRANDFATHER_INODE_ID) {
        iip = fsn.dir.getINodesInPath(src, true);
        inode = iip.getLastINode();
      } else {
        inode = fsn.dir.getInode(fileId);
        iip = INodesInPath.fromINode(inode);
        if (inode != null) {
          src = iip.getPath();
        }
      }
      pendingFile = fsn.checkLease(src, holder, inode, fileId);
    } catch (LeaseExpiredException lee) {
      if (inode != null && inode.isFile() &&
          !inode.asFile().isUnderConstruction()) {
        final Block realLastBlock = inode.asFile().getLastBlock();
        if (Block.matchingIdAndGenStamp(last, realLastBlock)) {
          NameNode.stateChangeLog.info("DIR* completeFile: " +
              "request from " + holder + " to complete inode " + fileId +
              "(" + src + ") which is already closed. But, it appears to be " +
              "an RPC retry. Returning success");
          return true;
        }
      }
      throw lee;
    }
    if (!fsn.checkFileProgress(src, pendingFile, false)) {
      return false;
    }

    fsn.commitOrCompleteLastBlock(pendingFile, iip, last);

    if (!fsn.checkFileProgress(src, pendingFile, true)) {
      return false;
    }

    fsn.finalizeINodeFileUnderConstruction(src, pendingFile,
        Snapshot.CURRENT_STATE_ID);
    return true;
  }

  private static void persistNewBlock(
      FSNamesystem fsn, String path, INodeFile file) {
    Preconditions.checkArgument(file.isUnderConstruction());
    fsn.getEditLog().logAddBlock(path, file);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("persistNewBlock: "
              + path + " with new block " + file.getLastBlock().toString()
              + ", current total block count is " + file.getBlocks().length);
    }
  }

  private static void saveAllocatedBlock(
      FSNamesystem fsn, String src, INodesInPath inodesInPath, Block newBlock,
      DatanodeStorageInfo[] targets)
      throws IOException {
    assert fsn.hasWriteLock();
    BlockInfoContiguous b = addBlock(fsn.dir, src, inodesInPath, newBlock,
                                     targets);
    NameNode.stateChangeLog.info("BLOCK* allocate " + b + " for " + src);
    DatanodeStorageInfo.incrementBlocksScheduled(targets);
  }

  private static class FileState {
    final INodeFile inode;
    final String path;
    final INodesInPath iip;

    FileState(INodeFile inode, String fullPath, INodesInPath iip) {
      this.inode = inode;
      this.path = fullPath;
      this.iip = iip;
    }
  }

  static class ValidateAddBlockResult {
    final long blockSize;
    final int replication;
    final byte storagePolicyID;
    final String clientMachine;

    ValidateAddBlockResult(
        long blockSize, int replication, byte storagePolicyID,
        String clientMachine) {
      this.blockSize = blockSize;
      this.replication = replication;
      this.storagePolicyID = storagePolicyID;
      this.clientMachine = clientMachine;
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirectory.java
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.hdfs.util.EnumCounters;
    return namesystem;
  }

  BlockManager getBlockManager() {
    return getFSNamesystem().getBlockManager();
  }

    return null;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogLoader.java
            + path);
      }
      Block oldBlock = oldBlocks[oldBlocks.length - 1];
      boolean removed = FSDirWriteFileOp.unprotectedRemoveBlock(
          fsDir, path, iip, file, oldBlock);
      if (!removed && !(op instanceof UpdateBlocksOp)) {
        throw new IOException("Trying to delete non-existant block " + oldBlock);
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
  private final long maxFsObjects;          // maximum number of fs objects

  private final long minBlockSize;         // minimum block size
  final long maxBlocksPerFile;     // maximum # of blocks per file

  private final long accessTimePrecision;
            : dir.getFileEncryptionInfo(inode, iip.getPathSnapshotId(), iip);

    final LocatedBlocks blocks = blockManager.createLocatedBlocks(
        inode.getBlocks(iip.getPathSnapshotId()), fileSize, isUc, offset,
        length, needBlockToken, iip.isSnapshot(), feInfo);

    for (LocatedBlock lb : blocks.getLocatedBlocks()) {
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot set storage policy for " + src);
      auditStat = FSDirAttrOp.setStoragePolicy(dir, blockManager, src,
                                               policyName);
    } catch (AccessControlException e) {
      logAuditEvent(false, "setStoragePolicy", src);
      throw e;
            "Cannot append to lazy persist file " + src);
      }
      recoverLeaseInternal(RecoverLeaseOp.APPEND_FILE, iip, src, holder,
                           clientMachine, false);
      
      final BlockInfoContiguous lastBlock = myFile.getLastBlock();
  LocatedBlock getAdditionalBlock(
      String src, long fileId, String clientName, ExtendedBlock previous,
      DatanodeInfo[] excludedNodes, String[] favoredNodes) throws IOException {
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* getAdditionalBlock: "
          + src + " inodeId " +  fileId  + " for " + clientName);
    }

    waitForLoadingFSImage();
    LocatedBlock[] onRetryBlock = new LocatedBlock[1];
    FSDirWriteFileOp.ValidateAddBlockResult r;
    FSPermissionChecker pc = getPermissionChecker();
    checkOperation(OperationCategory.READ);
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      r = FSDirWriteFileOp.validateAddBlock(this, pc, src, fileId, clientName,
                                            previous, onRetryBlock);
    } finally {
      readUnlock();
    }

    if (r == null) {
      assert onRetryBlock[0] != null : "Retry block is null";
      return onRetryBlock[0];
    }

    DatanodeStorageInfo[] targets = FSDirWriteFileOp.chooseTargetForNewBlock(
        blockManager, src, excludedNodes, favoredNodes, r);

    checkOperation(OperationCategory.WRITE);
    writeLock();
    LocatedBlock lb;
    try {
      checkOperation(OperationCategory.WRITE);
      lb = FSDirWriteFileOp.storeAllocatedBlock(
          this, src, fileId, clientName, previous, targets);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    return lb;
  }

    }

    if (clientnode == null) {
      clientnode = FSDirWriteFileOp.getClientNode(blockManager, clientMachine);
    }

  void abandonBlock(ExtendedBlock b, long fileId, String src, String holder)
      throws IOException {
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: " + b
          + "of file " + src);
    }
    waitForLoadingFSImage();
    checkOperation(OperationCategory.WRITE);
    FSPermissionChecker pc = getPermissionChecker();
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot abandon block " + b + " for file" + src);
      FSDirWriteFileOp.abandonBlock(dir, pc, b, fileId, src, holder);
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: "
                                      + b + " is removed from pendingCreates");
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
  }

  INodeFile checkLease(
      String src, String holder, INode inode, long fileId) throws LeaseExpiredException, FileNotFoundException {
    assert hasReadLock();
    final String ident = src + " (inode " + fileId + ")";
    if (inode == null) {
  boolean completeFile(final String src, String holder,
                       ExtendedBlock last, long fileId)
    throws IOException {
    boolean success = false;
    checkOperation(OperationCategory.WRITE);
    waitForLoadingFSImage();
    FSPermissionChecker pc = getPermissionChecker();
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot complete file " + src);
      success = FSDirWriteFileOp.completeFile(this, pc, src, holder, last,
                                              fileId);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    return success;
  }

  Block createNewBlock() throws IOException {
    assert hasWriteLock();
    Block b = new Block(nextBlockId(), 0, 0);
        pendingFile.getFileUnderConstructionFeature().updateLengthOfLastBlock(
            pendingFile, lastBlockLength);
      }
      FSDirWriteFileOp.persistBlocks(dir, src, pendingFile, false);
    } finally {
      writeUnlock();
    }
    return leaseManager.reassignLease(lease, pendingFile, newHolder);
  }

  void commitOrCompleteLastBlock(
      final INodeFile fileINode, final INodesInPath iip,
      final Block commitBlock) throws IOException {
    assert hasWriteLock();
    Preconditions.checkArgument(fileINode.isUnderConstruction());
    if (!blockManager.commitOrCompleteLastBlock(fileINode, commitBlock)) {
    }
  }

  void finalizeINodeFileUnderConstruction(
      String src, INodeFile pendingFile, int latestSnapshot) throws IOException {
    assert hasWriteLock();

    FileUnderConstructionFeature uc = pendingFile.getFileUnderConstructionFeature();
      } else {
        src = iFile.getFullPathName();
        FSDirWriteFileOp.persistBlocks(dir, src, iFile, false);
      }
    } finally {
      writeUnlock();
    hasResourcesAvailable = nnResourceChecker.hasAvailableDiskSpace();
  }

    return getFSImage().getEditLog();
  }

  @Metric({"MissingBlocks", "Number of missing blocks"})
  public long getMissingBlocksCount() {
    getBlockManager().getDatanodeManager().setBalancerBandwidth(bandwidth);
  }

    blockinfo.setExpectedLocations(storages);

    String src = pendingFile.getFullPathName();
    FSDirWriteFileOp.persistBlocks(dir, src, pendingFile, logRetryCache);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNodeRpcServer.java
      String[] favoredNodes)
      throws IOException {
    checkNNStartup();
    LocatedBlock locatedBlock = namesystem.getAdditionalBlock(src, fileId,
        clientName, previous, excludedNodes, favoredNodes);
    if (locatedBlock != null) {
      metrics.incrAddBlockOps();
    }
    return locatedBlock;
  }

  public void abandonBlock(ExtendedBlock b, long fileId, String src,
        String holder) throws IOException {
    checkNNStartup();
    namesystem.abandonBlock(b, fileId, src, holder);
  }

  @Override // ClientProtocol
                          ExtendedBlock last,  long fileId)
      throws IOException {
    checkNNStartup();
    return namesystem.completeFile(src, clientName, last, fileId);
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestAddBlockRetry.java
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

    LOG.info("Starting first addBlock for " + src);
    LocatedBlock[] onRetryBlock = new LocatedBlock[1];
    ns.readLock();
    FSDirWriteFileOp.ValidateAddBlockResult r;
    FSPermissionChecker pc = Mockito.mock(FSPermissionChecker.class);
    try {
      r = FSDirWriteFileOp.validateAddBlock(ns, pc, src,
                                            HdfsConstants.GRANDFATHER_INODE_ID,
                                            "clientName", null, onRetryBlock);
    } finally {
      ns.readUnlock();;
    }
    DatanodeStorageInfo targets[] = FSDirWriteFileOp.chooseTargetForNewBlock(
        ns.getBlockManager(), src, null, null, r);
    assertNotNull("Targets must be generated", targets);

    assertEquals("Wrong replication", REPLICATION, lb2.getLocations().length);

    ns.writeLock();
    LocatedBlock newBlock;
    try {
      newBlock = FSDirWriteFileOp.storeAllocatedBlock(ns, src,
          HdfsConstants.GRANDFATHER_INODE_ID, "clientName", null, targets);
    } finally {
      ns.writeUnlock();
    }
    assertEquals("Blocks are not equal", lb2.getBlock(), newBlock.getBlock());


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestCommitBlockSynchronization.java
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.IOException;

  private FSNamesystem makeNameSystemSpy(Block block, INodeFile file)
      throws IOException {
    Configuration conf = new Configuration();
    FSEditLog editlog = mock(FSEditLog.class);
    FSImage image = new FSImage(conf);
    Whitebox.setInternalState(image, "editLog", editlog);
    final DatanodeStorageInfo[] targets = {};

    FSNamesystem namesystem = new FSNamesystem(conf, image);

