hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockCollection.java
  public BlockInfoContiguousUnderConstruction setLastBlock(BlockInfo lastBlock,
      DatanodeStorageInfo[] targets) throws IOException;


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfo.java
  protected Object[] triplets;

  public BlockUCState getBlockUCState() {
  }

  public BlockInfoContiguousUnderConstruction convertToBlockUnderConstruction(
      BlockUCState s, DatanodeStorageInfo[] targets) {
    if(isComplete()) {
      BlockInfoContiguousUnderConstruction ucBlock =
          new BlockInfoContiguousUnderConstruction(this,
          getBlockCollection().getPreferredBlockReplication(), s, targets);
      ucBlock.setBlockCollection(getBlockCollection());
      return ucBlock;
    }
    BlockInfoContiguousUnderConstruction ucBlock =
        (BlockInfoContiguousUnderConstruction)this;
    ucBlock.setBlockUCState(s);
    ucBlock.setExpectedLocations(targets);
    ucBlock.setBlockCollection(getBlockCollection());
    return ucBlock;
  }

  @Override
  public int hashCode() {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoContiguous.java

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;

@InterfaceAudience.Private
public class BlockInfoContiguous extends BlockInfo {
  public static final BlockInfoContiguous[] EMPTY_ARRAY = {};

  public BlockInfoContiguous(short size) {
    super(size);
  protected BlockInfoContiguous(BlockInfoContiguous from) {
    super(from);
  }

  private int ensureCapacity(int num) {
    assert this.triplets != null : "BlockInfo is not initialized";
    int last = numNodes();
    if (triplets.length >= (last+num)*3) {
      return last;
    }
    Object[] old = triplets;
    triplets = new Object[(last+num)*3];
    System.arraycopy(old, 0, triplets, 0, last * 3);
    return last;
  }

  @Override
  boolean addStorage(DatanodeStorageInfo storage) {
    int lastNode = ensureCapacity(1);
    setStorageInfo(lastNode, storage);
    setNext(lastNode, null);
    setPrevious(lastNode, null);
    return true;
  }

  @Override
  boolean removeStorage(DatanodeStorageInfo storage) {
    int dnIndex = findStorageInfo(storage);
    if (dnIndex < 0) { // the node is not found
      return false;
    }
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null :
        "Block is still in the list and must be removed first.";
    int lastNode = numNodes()-1;
    setStorageInfo(dnIndex, getStorageInfo(lastNode));
    setNext(dnIndex, getNext(lastNode));
    setPrevious(dnIndex, getPrevious(lastNode));
    setStorageInfo(lastNode, null);
    setNext(lastNode, null);
    setPrevious(lastNode, null);
    return true;
  }

  @Override
  public int numNodes() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";

    for (int idx = getCapacity()-1; idx >= 0; idx--) {
      if (getDatanode(idx) != null) {
        return idx + 1;
      }
    }
    return 0;
  }

  @Override
  void replaceBlock(BlockInfo newBlock) {
    assert newBlock instanceof BlockInfoContiguous;
    for (int i = this.numNodes() - 1; i >= 0; i--) {
      final DatanodeStorageInfo storage = this.getStorageInfo(i);
      final boolean removed = storage.removeBlock(this);
      assert removed : "currentBlock not found.";

      final DatanodeStorageInfo.AddBlockResult result = storage.addBlock(
          newBlock);
      assert result == DatanodeStorageInfo.AddBlockResult.ADDED :
          "newBlock already exists.";
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoContiguousUnderConstruction.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoContiguousUnderConstruction.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

public class BlockInfoContiguousUnderConstruction extends BlockInfoContiguous {
  private BlockUCState blockUCState;

  private List<ReplicaUnderConstruction> replicas;

  private int primaryNodeIndex = -1;

  private long blockRecoveryId = 0;

  private Block truncateBlock;

  static class ReplicaUnderConstruction extends Block {
    private final DatanodeStorageInfo expectedLocation;
    private ReplicaState state;
    private boolean chosenAsPrimary;

    ReplicaUnderConstruction(Block block,
                             DatanodeStorageInfo target,
                             ReplicaState state) {
      super(block);
      this.expectedLocation = target;
      this.state = state;
      this.chosenAsPrimary = false;
    }

    private DatanodeStorageInfo getExpectedStorageLocation() {
      return expectedLocation;
    }

    ReplicaState getState() {
      return state;
    }

    boolean getChosenAsPrimary() {
      return chosenAsPrimary;
    }

    void setState(ReplicaState s) {
      state = s;
    }

    void setChosenAsPrimary(boolean chosenAsPrimary) {
      this.chosenAsPrimary = chosenAsPrimary;
    }

    boolean isAlive() {
      return expectedLocation.getDatanodeDescriptor().isAlive;
    }

    @Override // Block
    public int hashCode() {
      return super.hashCode();
    }

    @Override // Block
    public boolean equals(Object obj) {
      return (this == obj) || super.equals(obj);
    }

    @Override
    public String toString() {
      final StringBuilder b = new StringBuilder(50);
      appendStringTo(b);
      return b.toString();
    }
    
    @Override
    public void appendStringTo(StringBuilder sb) {
      sb.append("ReplicaUC[")
        .append(expectedLocation)
        .append("|")
        .append(state)
        .append("]");
    }
  }

  public BlockInfoContiguousUnderConstruction(Block blk, short replication) {
    this(blk, replication, BlockUCState.UNDER_CONSTRUCTION, null);
  }

  public BlockInfoContiguousUnderConstruction(Block blk, short replication,
      BlockUCState state, DatanodeStorageInfo[] targets) {
    super(blk, replication);
    assert getBlockUCState() != BlockUCState.COMPLETE :
      "BlockInfoUnderConstruction cannot be in COMPLETE state";
    this.blockUCState = state;
    setExpectedLocations(targets);
  }

  BlockInfo convertToCompleteBlock() throws IOException {
    assert getBlockUCState() != BlockUCState.COMPLETE :
      "Trying to convert a COMPLETE block";
    return new BlockInfoContiguous(this);
  }

  public void setExpectedLocations(DatanodeStorageInfo[] targets) {
    int numLocations = targets == null ? 0 : targets.length;
    this.replicas = new ArrayList<ReplicaUnderConstruction>(numLocations);
    for(int i = 0; i < numLocations; i++)
      replicas.add(
        new ReplicaUnderConstruction(this, targets[i], ReplicaState.RBW));
  }

  public DatanodeStorageInfo[] getExpectedStorageLocations() {
    int numLocations = replicas == null ? 0 : replicas.size();
    DatanodeStorageInfo[] storages = new DatanodeStorageInfo[numLocations];
    for(int i = 0; i < numLocations; i++)
      storages[i] = replicas.get(i).getExpectedStorageLocation();
    return storages;
  }

  public int getNumExpectedLocations() {
    return replicas == null ? 0 : replicas.size();
  }

  @Override // BlockInfo
  public BlockUCState getBlockUCState() {
    return blockUCState;
  }

  void setBlockUCState(BlockUCState s) {
    blockUCState = s;
  }

  public long getBlockRecoveryId() {
    return blockRecoveryId;
  }

  public Block getTruncateBlock() {
    return truncateBlock;
  }

  public void setTruncateBlock(Block recoveryBlock) {
    this.truncateBlock = recoveryBlock;
  }

  public void setGenerationStampAndVerifyReplicas(long genStamp) {
    setGenerationStamp(genStamp);
    if (replicas == null)
      return;

    for (ReplicaUnderConstruction r : replicas) {
      if (genStamp != r.getGenerationStamp()) {
        r.getExpectedStorageLocation().removeBlock(this);
        NameNode.blockStateChangeLog.debug("BLOCK* Removing stale replica "
            + "from location: {}", r.getExpectedStorageLocation());
      }
    }
  }

  void commitBlock(Block block) throws IOException {
    if(getBlockId() != block.getBlockId())
      throw new IOException("Trying to commit inconsistent block: id = "
          + block.getBlockId() + ", expected id = " + getBlockId());
    blockUCState = BlockUCState.COMMITTED;
    this.set(getBlockId(), block.getNumBytes(), block.getGenerationStamp());
    setGenerationStampAndVerifyReplicas(block.getGenerationStamp());
  }

  public void initializeBlockRecovery(long recoveryId) {
    setBlockUCState(BlockUCState.UNDER_RECOVERY);
    blockRecoveryId = recoveryId;
    if (replicas.size() == 0) {
      NameNode.blockStateChangeLog.warn("BLOCK*"
        + " BlockInfoUnderConstruction.initLeaseRecovery:"
        + " No blocks found, lease removed.");
    }
    boolean allLiveReplicasTriedAsPrimary = true;
    for (int i = 0; i < replicas.size(); i++) {
      if (replicas.get(i).isAlive()) {
        allLiveReplicasTriedAsPrimary =
            (allLiveReplicasTriedAsPrimary && replicas.get(i).getChosenAsPrimary());
      }
    }
    if (allLiveReplicasTriedAsPrimary) {
      for (int i = 0; i < replicas.size(); i++) {
        replicas.get(i).setChosenAsPrimary(false);
      }
    }
    long mostRecentLastUpdate = 0;
    ReplicaUnderConstruction primary = null;
    primaryNodeIndex = -1;
    for(int i = 0; i < replicas.size(); i++) {
      if (!(replicas.get(i).isAlive() && !replicas.get(i).getChosenAsPrimary())) {
        continue;
      }
      final ReplicaUnderConstruction ruc = replicas.get(i);
      final long lastUpdate = ruc.getExpectedStorageLocation()
          .getDatanodeDescriptor().getLastUpdateMonotonic();
      if (lastUpdate > mostRecentLastUpdate) {
        primaryNodeIndex = i;
        primary = ruc;
        mostRecentLastUpdate = lastUpdate;
      }
    }
    if (primary != null) {
      primary.getExpectedStorageLocation().getDatanodeDescriptor().addBlockToBeRecovered(this);
      primary.setChosenAsPrimary(true);
      NameNode.blockStateChangeLog.debug(
          "BLOCK* {} recovery started, primary={}", this, primary);
    }
  }

  void addReplicaIfNotPresent(DatanodeStorageInfo storage,
                     Block block,
                     ReplicaState rState) {
    Iterator<ReplicaUnderConstruction> it = replicas.iterator();
    while (it.hasNext()) {
      ReplicaUnderConstruction r = it.next();
      DatanodeStorageInfo expectedLocation = r.getExpectedStorageLocation();
      if(expectedLocation == storage) {
        r.setGenerationStamp(block.getGenerationStamp());
        return;
      } else if (expectedLocation != null &&
                 expectedLocation.getDatanodeDescriptor() ==
                     storage.getDatanodeDescriptor()) {

        it.remove();
        break;
      }
    }
    replicas.add(new ReplicaUnderConstruction(block, storage, rState));
  }

  @Override // BlockInfo
  public int hashCode() {
    return super.hashCode();
  }

  @Override // BlockInfo
  public boolean equals(Object obj) {
    return (this == obj) || super.equals(obj);
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(100);
    appendStringTo(b);
    return b.toString();
  }

  @Override
  public void appendStringTo(StringBuilder sb) {
    super.appendStringTo(sb);
    appendUCParts(sb);
  }

  private void appendUCParts(StringBuilder sb) {
    sb.append("{UCState=").append(blockUCState)
      .append(", truncateBlock=" + truncateBlock)
      .append(", primaryNodeIndex=").append(primaryNodeIndex)
      .append(", replicas=[");
    if (replicas != null) {
      Iterator<ReplicaUnderConstruction> iter = replicas.iterator();
      if (iter.hasNext()) {
        iter.next().appendStringTo(sb);
        while (iter.hasNext()) {
          sb.append(", ");
          iter.next().appendStringTo(sb);
        }
      }
    }
    sb.append("]}");
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
  private static boolean commitBlock(
      final BlockInfoContiguousUnderConstruction block, final Block commitBlock)
      throws IOException {
    if (block.getBlockUCState() == BlockUCState.COMMITTED)
      return false;
      return false; // already completed (e.g. by syncBlock)
    
    final boolean b = commitBlock(
        (BlockInfoContiguousUnderConstruction) lastBlock, commitBlock);
    if(countNodes(lastBlock).liveReplicas() >= minReplication)
      completeBlock(bc, bc.numBlocks()-1, false);
    return b;
    BlockInfo curBlock = bc.getBlocks()[blkIndex];
    if(curBlock.isComplete())
      return curBlock;
    BlockInfoContiguousUnderConstruction ucBlock =
        (BlockInfoContiguousUnderConstruction) curBlock;
    int numNodes = ucBlock.numNodes();
    if (!force && numNodes < minReplication)
      throw new IOException("Cannot complete block: " +
  public BlockInfo forceCompleteBlock(final BlockCollection bc,
      final BlockInfoContiguousUnderConstruction block) throws IOException {
    block.commitBlock(block);
    return completeBlock(bc, block, true);
  }

    DatanodeStorageInfo[] targets = getStorages(oldBlock);

    BlockInfoContiguousUnderConstruction ucBlock =
        bc.setLastBlock(oldBlock, targets);
    blocksMap.replaceBlock(ucBlock);

  private LocatedBlock createLocatedBlock(final BlockInfo blk, final long pos
      ) throws IOException {
    if (blk instanceof BlockInfoContiguousUnderConstruction) {
      if (blk.isComplete()) {
        throw new IOException(
            "blk instanceof BlockInfoUnderConstruction && blk.isComplete()"
            + ", blk=" + blk);
      }
      final BlockInfoContiguousUnderConstruction uc =
          (BlockInfoContiguousUnderConstruction) blk;
      final DatanodeStorageInfo[] storages = uc.getExpectedStorageLocations();
      final ExtendedBlock eb = new ExtendedBlock(namesystem.getBlockPoolId(), blk);
      return newLocatedBlock(eb, storages, pos, false);
  static class StatefulBlockInfo {
    final BlockInfoContiguousUnderConstruction storedBlock;
    final Block reportedBlock;
    final ReplicaState reportedState;
    
    StatefulBlockInfo(BlockInfoContiguousUnderConstruction storedBlock,
        Block reportedBlock, ReplicaState reportedState) {
      this.storedBlock = storedBlock;
      this.reportedBlock = reportedBlock;

    BlockToMarkCorrupt(BlockInfo stored, long gs, String reason,
        Reason reasonCode) {
      this(new BlockInfoContiguous((BlockInfoContiguous)stored), stored,
          reason, reasonCode);
      corrupted.setGenerationStamp(gs);
      
      if (isBlockUnderConstruction(storedBlock, ucState, reportedState)) {
        ((BlockInfoContiguousUnderConstruction)storedBlock)
            .addReplicaIfNotPresent(storageInfo, iblk, reportedState);
        BlockInfoContiguousUnderConstruction blockUC =
            (BlockInfoContiguousUnderConstruction) storedBlock;
        if (namesystem.isInSnapshot(blockUC)) {
          int numOfReplicas = blockUC.getNumExpectedLocations();
          namesystem.incrementSafeBlockCount(numOfReplicas);

    if (isBlockUnderConstruction(storedBlock, ucState, reportedState)) {
      toUC.add(new StatefulBlockInfo(
          (BlockInfoContiguousUnderConstruction) storedBlock,
          new Block(block), reportedState));
      return storedBlock;
    }

  void addStoredBlockUnderConstruction(StatefulBlockInfo ucBlock,
      DatanodeStorageInfo storageInfo) throws IOException {
    BlockInfoContiguousUnderConstruction block = ucBlock.storedBlock;
    block.addReplicaIfNotPresent(
        storageInfo, ucBlock.reportedBlock, ucBlock.reportedState);

    assert block != null && namesystem.hasWriteLock();
    BlockInfo storedBlock;
    DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();
    if (block instanceof BlockInfoContiguousUnderConstruction) {
      storedBlock = blocksMap.getStoredBlock(block);
    } else {
      String src, BlockInfo[] blocks) {
    for (BlockInfo b: blocks) {
      if (!b.isComplete()) {
        final BlockInfoContiguousUnderConstruction uc =
            (BlockInfoContiguousUnderConstruction)b;
        final int numNodes = b.numNodes();
        LOG.info("BLOCK* " + b + " is not COMPLETE (ucState = "
          + uc.getBlockUCState() + ", replication# = " + numNodes

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DatanodeDescriptor.java
  private final BlockQueue<BlockTargetPair> replicateBlocks =
      new BlockQueue<>();
  private final BlockQueue<BlockInfoContiguousUnderConstruction> recoverBlocks =
                                new BlockQueue<BlockInfoContiguousUnderConstruction>();
  private final LightWeightHashSet<Block> invalidateBlocks =
      new LightWeightHashSet<>();
  void addBlockToBeRecovered(BlockInfoContiguousUnderConstruction block) {
    if(recoverBlocks.contains(block)) {
      BlockManager.LOG.info(block + " is already in the recovery queue");
    return replicateBlocks.poll(maxTransfers);
  }

  public BlockInfoContiguousUnderConstruction[] getLeaseRecoveryCommand(int maxTransfers) {
    List<BlockInfoContiguousUnderConstruction> blocks = recoverBlocks.poll(maxTransfers);
    if(blocks == null)
      return null;
    return blocks.toArray(new BlockInfoContiguousUnderConstruction[blocks.size()]);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DatanodeManager.java
        }

        BlockInfoContiguousUnderConstruction[] blocks = nodeinfo
            .getLeaseRecoveryCommand(Integer.MAX_VALUE);
        if (blocks != null) {
          BlockRecoveryCommand brCommand = new BlockRecoveryCommand(
              blocks.length);
          for (BlockInfoContiguousUnderConstruction b : blocks) {
            final DatanodeStorageInfo[] storages = b.getExpectedStorageLocations();
            final List<DatanodeStorageInfo> recoveryLocations =

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirTruncateOp.java
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.RecoverLeaseOp;
      final BlockInfo last = file.getLastBlock();
      if (last != null && last.getBlockUCState()
          == BlockUCState.UNDER_RECOVERY) {
        final Block truncatedBlock = ((BlockInfoContiguousUnderConstruction) last)
            .getTruncateBlock();
        if (truncatedBlock != null) {
          final long truncateLength = file.computeFileSize(false, false)
              oldBlock)));
    }

    BlockInfoContiguousUnderConstruction truncatedBlockUC;
    BlockManager blockManager = fsn.getFSDirectory().getBlockManager();
    if (shouldCopyOnTruncate) {
      truncatedBlockUC = new BlockInfoContiguousUnderConstruction(newBlock,
          file.getPreferredBlockReplication());
      truncatedBlockUC.setNumBytes(oldBlock.getNumBytes() - lastBlockDelta);
      truncatedBlockUC.setTruncateBlock(oldBlock);
      blockManager.convertLastBlockToUnderConstruction(file, lastBlockDelta);
      oldBlock = file.getLastBlock();
      assert !oldBlock.isComplete() : "oldBlock should be under construction";
      truncatedBlockUC = (BlockInfoContiguousUnderConstruction) oldBlock;
      truncatedBlockUC.setTruncateBlock(new Block(oldBlock));
      truncatedBlockUC.getTruncateBlock().setNumBytes(
          oldBlock.getNumBytes() - lastBlockDelta);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirWriteFileOp.java
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
      Block block) throws IOException {
    BlockInfoContiguousUnderConstruction uc = fileNode.removeLastBlock(block);
    if (uc == null) {
      return false;
    }
      } else {
        BlockInfo lastBlockInFile = pendingFile.getLastBlock();
        ((BlockInfoContiguousUnderConstruction) lastBlockInFile)
            .setExpectedLocations(targets);
        offset = pendingFile.computeFileSize();
        return makeLocatedBlock(fsn, lastBlockInFile, targets, offset);
          fileINode.getPreferredBlockReplication(), true);

      BlockInfoContiguousUnderConstruction blockInfo =
        new BlockInfoContiguousUnderConstruction(
            block,
            fileINode.getFileReplication(),
            HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION,
            "allocation of a new block in " + src + ". Returning previously" +
            " allocated block " + lastBlockInFile);
        long offset = file.computeFileSize();
        BlockInfoContiguousUnderConstruction lastBlockUC =
            (BlockInfoContiguousUnderConstruction) lastBlockInFile;
        onRetryBlock[0] = makeLocatedBlock(fsn, lastBlockInFile,
            lastBlockUC.getExpectedStorageLocations(), offset);
        return new FileState(file, src, iip);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogLoader.java
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
      }
      
      oldLastBlock.setNumBytes(pBlock.getNumBytes());
      if (oldLastBlock instanceof BlockInfoContiguousUnderConstruction) {
        fsNamesys.getBlockManager().forceCompleteBlock(file,
            (BlockInfoContiguousUnderConstruction) oldLastBlock);
        fsNamesys.getBlockManager().processQueuedMessagesForBlock(pBlock);
      }
    } else { // the penultimate block is null
      Preconditions.checkState(oldBlocks == null || oldBlocks.length == 0);
    }
    BlockInfo newBI = new BlockInfoContiguousUnderConstruction(
          newBlock, file.getPreferredBlockReplication());
    fsNamesys.getBlockManager().addBlockCollection(newBI, file);
    file.addBlock(newBI);
        oldBlock.getGenerationStamp() != newBlock.getGenerationStamp();
      oldBlock.setGenerationStamp(newBlock.getGenerationStamp());
      
      if (oldBlock instanceof BlockInfoContiguousUnderConstruction &&
          (!isLastBlock || op.shouldCompleteLastBlock())) {
        changeMade = true;
        fsNamesys.getBlockManager().forceCompleteBlock(file,
            (BlockInfoContiguousUnderConstruction) oldBlock);
      }
      if (changeMade) {
          newBI = new BlockInfoContiguousUnderConstruction(
              newBlock, file.getPreferredBlockReplication());
        } else {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageFormat.java
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
            if (blocks.length > 0) {
              BlockInfo lastBlk = blocks[blocks.length - 1];
              blocks[blocks.length - 1] = new BlockInfoContiguousUnderConstruction(
                  lastBlk, replication);
            }
          }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageFormatPBINode.java
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.LoaderContext;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SaverContext;
        if (blocks.length > 0) {
          BlockInfo lastBlk = file.getLastBlock();
          file.setBlock(file.numBlocks() - 1, new BlockInfoContiguousUnderConstruction(
              lastBlk, replication));
        }
      }
      return file;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageSerialization.java
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat.ReferenceMap;
    if(numBlocks > 0) {
      blk.readFields(in);
      blocks[i] = new BlockInfoContiguousUnderConstruction(
        blk, blockReplication, BlockUCState.UNDER_CONSTRUCTION, null);
    }
    PermissionStatus perm = PermissionStatus.read(in);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
      throw new AlreadyBeingCreatedException(message);
    case UNDER_CONSTRUCTION:
    case UNDER_RECOVERY:
      final BlockInfoContiguousUnderConstruction uc = (BlockInfoContiguousUnderConstruction)lastBlock;
      Block recoveryBlock = uc.getTruncateBlock();
      boolean truncateRecovery = recoveryBlock != null;
  }
  
  @Override
  public boolean isInSnapshot(BlockInfoContiguousUnderConstruction blockUC) {
    assert hasReadLock();
    final BlockCollection bc = blockUC.getBlockCollection();
    if (bc == null || !(bc instanceof INodeFile)
    waitForLoadingFSImage();
    writeLock();
    boolean copyTruncate = false;
    BlockInfoContiguousUnderConstruction truncatedBlock = null;
    try {
      checkOperation(OperationCategory.WRITE);
        return;
      }

      truncatedBlock = (BlockInfoContiguousUnderConstruction) iFile
          .getLastBlock();
      long recoveryId = truncatedBlock.getBlockRecoveryId();
      copyTruncate = truncatedBlock.getBlockId() != storedBlock.getBlockId();
    assert hasWriteLock();
    final INodeFile pendingFile = checkUCBlock(oldBlock, clientName);
    final BlockInfoContiguousUnderConstruction blockinfo
        = (BlockInfoContiguousUnderConstruction)pendingFile.getLastBlock();

    if (newBlock.getGenerationStamp() <= blockinfo.getGenerationStamp() ||

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FileUnderConstructionFeature.java

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;

    BlockInfo lastBlock = f.getLastBlock();
    assert (lastBlock != null) : "The last block for path "
        + f.getFullPathName() + " is null when updating its length";
    assert (lastBlock instanceof BlockInfoContiguousUnderConstruction)
        : "The last block for path " + f.getFullPathName()
            + " is not a BlockInfoUnderConstruction when updating its length";
    lastBlock.setNumBytes(lastBlockLength);
      final BlocksMapUpdateInfo collectedBlocks) {
    final BlockInfo[] blocks = f.getBlocks();
    if (blocks != null && blocks.length > 0
        && blocks[blocks.length - 1] instanceof BlockInfoContiguousUnderConstruction) {
      BlockInfoContiguousUnderConstruction lastUC =
          (BlockInfoContiguousUnderConstruction) blocks[blocks.length - 1];
      if (lastUC.getNumBytes() == 0) {
        collectedBlocks.addDeleteBlock(lastUC);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeFile.java
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
  }

  @Override // BlockCollection, the file should be under construction
  public BlockInfoContiguousUnderConstruction setLastBlock(
      BlockInfo lastBlock, DatanodeStorageInfo[] locations)
      throws IOException {
    Preconditions.checkState(isUnderConstruction(),
    if (numBlocks() == 0) {
      throw new IOException("Failed to set last block: File is empty.");
    }
    BlockInfoContiguousUnderConstruction ucBlock =
      lastBlock.convertToBlockUnderConstruction(
          BlockUCState.UNDER_CONSTRUCTION, locations);
    setBlock(numBlocks() - 1, ucBlock);
  BlockInfoContiguousUnderConstruction removeLastBlock(Block oldblock) {
    Preconditions.checkState(isUnderConstruction(),
        "file is no longer under construction");
    if (blocks == null || blocks.length == 0) {
      return null;
    }

    BlockInfoContiguousUnderConstruction uc =
        (BlockInfoContiguousUnderConstruction)blocks[size_1];
    BlockInfo[] newlist = new BlockInfo[size_1];
    System.arraycopy(blocks, 0, newlist, 0, size_1);
    final int last = blocks.length - 1;
    long size = blocks[last].getNumBytes();
    if (blocks[last] instanceof BlockInfoContiguousUnderConstruction) {
       if (!includesLastUcBlock) {
         size = 0;
       } else if (usePreferredBlockSize4LastUcBlock) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/Namesystem.java

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.util.RwLock;
import org.apache.hadoop.ipc.StandbyException;

  void checkOperation(OperationCategory read) throws StandbyException;

  boolean isInSnapshot(BlockInfoContiguousUnderConstruction blockUC);

  CacheManager getCacheManager();
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FileDiffList.java

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
    Block dontRemoveBlock = null;
    if (lastBlock != null && lastBlock.getBlockUCState().equals(
        HdfsServerConstants.BlockUCState.UNDER_RECOVERY)) {
      dontRemoveBlock = ((BlockInfoContiguousUnderConstruction) lastBlock)
          .getTruncateBlock();
    }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/DFSTestUtil.java
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
    BlockInfo storedBlock = bm0.getStoredBlock(blk.getLocalBlock());
    assertTrue("Block " + blk + " should be under construction, " +
        "got: " + storedBlock,
        storedBlock instanceof BlockInfoContiguousUnderConstruction);
    BlockInfoContiguousUnderConstruction ucBlock =
      (BlockInfoContiguousUnderConstruction)storedBlock;
    final DatanodeStorageInfo[] storages = ucBlock.getExpectedStorageLocations();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockInfoUnderConstruction.java
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.util.Time;
import org.junit.Test;

    DatanodeDescriptor dd3 = s3.getDatanodeDescriptor();

    dd1.isAlive = dd2.isAlive = dd3.isAlive = true;
    BlockInfoContiguousUnderConstruction blockInfo = new BlockInfoContiguousUnderConstruction(
        new Block(0, 0, GenerationStamp.LAST_RESERVED_STAMP),
        (short) 3,
        BlockUCState.UNDER_CONSTRUCTION,
    DFSTestUtil.resetLastUpdatesWithOffset(dd2, -1 * 1000);
    DFSTestUtil.resetLastUpdatesWithOffset(dd3, -2 * 1000);
    blockInfo.initializeBlockRecovery(1);
    BlockInfoContiguousUnderConstruction[] blockInfoRecovery = dd2.getLeaseRecoveryCommand(1);
    assertEquals(blockInfoRecovery[0], blockInfo);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockManager.java
    assertTrue(bm.getStoredBlock(new Block(receivedBlockId)).findStorageInfo
        (ds) >= 0);
    assertTrue(((BlockInfoContiguousUnderConstruction) bm.
        getStoredBlock(new Block(receivingBlockId))).getNumExpectedLocations() > 0);
    assertTrue(bm.getStoredBlock(new Block(receivingReceivedBlockId))
        .findStorageInfo(ds) >= 0);

  private BlockInfo addUcBlockToBM(long blkId) {
    Block block = new Block(blkId);
    BlockInfoContiguousUnderConstruction blockInfo =
        new BlockInfoContiguousUnderConstruction(block, (short) 3);
    BlockCollection bc = Mockito.mock(BlockCollection.class);
    Mockito.doReturn((short) 3).when(bc).getPreferredBlockReplication();
    bm.blocksMap.addBlockCollection(blockInfo, bc);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestHeartbeatHandling.java
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.util.Time;
import org.junit.Test;

              dd1.getStorageInfos()[0],
              dd2.getStorageInfos()[0],
              dd3.getStorageInfos()[0]};
          BlockInfoContiguousUnderConstruction blockInfo = new BlockInfoContiguousUnderConstruction(
              new Block(0, 0, GenerationStamp.LAST_RESERVED_STAMP), (short) 3,
              BlockUCState.UNDER_RECOVERY, storages);
          dd1.addBlockToBeRecovered(blockInfo);
          DFSTestUtil.resetLastUpdatesWithOffset(dd2, -40 * 1000);
          DFSTestUtil.resetLastUpdatesWithOffset(dd3, 0);
          blockInfo = new BlockInfoContiguousUnderConstruction(
              new Block(0, 0, GenerationStamp.LAST_RESERVED_STAMP), (short) 3,
              BlockUCState.UNDER_RECOVERY, storages);
          dd1.addBlockToBeRecovered(blockInfo);
          DFSTestUtil.resetLastUpdatesWithOffset(dd2, - 40 * 1000);
          DFSTestUtil.resetLastUpdatesWithOffset(dd3, - 80 * 1000);
          blockInfo = new BlockInfoContiguousUnderConstruction(
              new Block(0, 0, GenerationStamp.LAST_RESERVED_STAMP), (short) 3,
              BlockUCState.UNDER_RECOVERY, storages);
          dd1.addBlockToBeRecovered(blockInfo);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestReplicationPolicy.java
    BlockInfoContiguousUnderConstruction info = new BlockInfoContiguousUnderConstruction(block1, (short) 1);
    BlockCollection bc = mock(BlockCollection.class);
    when(bc.getPreferredBlockReplication()).thenReturn((short)1);
    bm.addBlockCollection(info, bc);

    DatanodeStorageInfo[] storageAry = {new DatanodeStorageInfo(
        dataNodes[0], new DatanodeStorage("s1"))};
    final BlockInfoContiguousUnderConstruction ucBlock =
        info.convertToBlockUnderConstruction(BlockUCState.UNDER_CONSTRUCTION,
            storageAry);
    DatanodeStorageInfo storage = mock(DatanodeStorageInfo.class);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestBlockUnderConstruction.java
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.junit.AfterClass;
      final List<LocatedBlock> blocks = lb.getLocatedBlocks();
      assertEquals(i, blocks.size());
      final Block b = blocks.get(blocks.size() - 1).getBlock().getLocalBlock();
      assertTrue(b instanceof BlockInfoContiguousUnderConstruction);

      if (++i < NUM_BLOCKS) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestCommitBlockSynchronization.java
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.junit.Test;
    namesystem.dir.getINodeMap().put(file);

    FSNamesystem namesystemSpy = spy(namesystem);
    BlockInfoContiguousUnderConstruction blockInfo = new BlockInfoContiguousUnderConstruction(
        block, (short) 1, HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION, targets);
    blockInfo.setBlockCollection(file);
    blockInfo.setGenerationStamp(genStamp);
    blockInfo.initializeBlockRecovery(genStamp);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFileTruncate.java
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.security.UserGroupInformation;
          is(fsn.getBlockIdManager().getGenerationStampV2()));
      assertThat(file.getLastBlock().getBlockUCState(),
          is(HdfsServerConstants.BlockUCState.UNDER_RECOVERY));
      long blockRecoveryId = ((BlockInfoContiguousUnderConstruction) file.getLastBlock())
          .getBlockRecoveryId();
      assertThat(blockRecoveryId, is(initialGenStamp + 1));
      fsn.getEditLog().logTruncate(
          is(fsn.getBlockIdManager().getGenerationStampV2()));
      assertThat(file.getLastBlock().getBlockUCState(),
          is(HdfsServerConstants.BlockUCState.UNDER_RECOVERY));
      long blockRecoveryId = ((BlockInfoContiguousUnderConstruction) file.getLastBlock())
          .getBlockRecoveryId();
      assertThat(blockRecoveryId, is(initialGenStamp + 1));
      fsn.getEditLog().logTruncate(

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestRetryCacheWithHA.java
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
    boolean checkNamenodeBeforeReturn() throws Exception {
      INodeFile fileNode = cluster.getNamesystem(0).getFSDirectory()
          .getINode4Write(file).asFile();
      BlockInfoContiguousUnderConstruction blkUC =
          (BlockInfoContiguousUnderConstruction) (fileNode.getBlocks())[1];
      int datanodeNum = blkUC.getExpectedStorageLocations().length;
      for (int i = 0; i < CHECKTIMES && datanodeNum != 2; i++) {
        Thread.sleep(1000);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/SnapshotTestHelper.java
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.datanode.BlockPoolSliceStorage;
import org.apache.hadoop.hdfs.server.datanode.BlockScanner;

