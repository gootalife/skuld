hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfo.java
  abstract void addStorage(DatanodeStorageInfo storage, Block reportedBlock);

  abstract void replaceBlock(BlockInfo newBlock);

  abstract boolean hasEmptyStorage();


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoContiguous.java
  }

  @Override
  void addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    ContiguousBlockStorageOp.addStorage(this, storage);
  }

  @Override
    ucBlock.setBlockCollection(getBlockCollection());
    return ucBlock;
  }

  @Override
  boolean hasEmptyStorage() {
    return ContiguousBlockStorageOp.hasEmptyStorage(this);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoUnderConstruction.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

          "No blocks found, lease removed.");
    }
    boolean allLiveReplicasTriedAsPrimary = true;
    for (ReplicaUnderConstruction replica : replicas) {
      if (replica.isAlive()) {
        allLiveReplicasTriedAsPrimary = allLiveReplicasTriedAsPrimary
            && replica.getChosenAsPrimary();
      }
    }
    if (allLiveReplicasTriedAsPrimary) {
      for (ReplicaUnderConstruction replica : replicas) {
        replica.setChosenAsPrimary(false);
      }
    }
    long mostRecentLastUpdate = 0;
  public abstract BlockInfo convertToCompleteBlock();

  }

  private void appendUCParts(StringBuilder sb) {
    sb.append("{UCState=").append(blockUCState).append(", truncateBlock=")
      .append(truncateBlock)
      .append(", primaryNodeIndex=").append(primaryNodeIndex)
      .append(", replicas=[");
    if (replicas != null) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoUnderConstructionContiguous.java
  @Override
  public BlockInfoContiguous convertToCompleteBlock() {
  }

  @Override
  void addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    ContiguousBlockStorageOp.addStorage(this, storage);
  }

  @Override
    ContiguousBlockStorageOp.replaceBlock(this, newBlock);
  }

  @Override
  boolean hasEmptyStorage() {
    return ContiguousBlockStorageOp.hasEmptyStorage(this);
  }

  @Override
  public void setExpectedLocations(DatanodeStorageInfo[] targets) {
    int numLocations = targets == null ? 0 : targets.length;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
  private final Set<BlockInfo> postponedMisreplicatedBlocks = Sets.newHashSet();

            DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_DEFAULT);
    this.shouldCheckForEnoughRacks =
        conf.get(DFSConfigKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY) != null;

    this.blocksInvalidateWorkPct = DFSUtil.getInvalidateWorkPctPerIteration(conf);
    this.blocksReplWorkMultiplier = DFSUtil.getReplWorkMultiplier(conf);

  boolean shouldUpdateBlockKey(final long updateTime) throws IOException {
    return isBlockTokenEnabled() && blockTokenSecretManager.updateKeys(updateTime);
  }

  public void activate(Configuration conf) {
    synchronized (neededReplications) {
      out.println("Metasave: Blocks waiting for replication: " + 
                  neededReplications.size());
      for (BlockInfo block : neededReplications) {
        dumpBlockMeta(block, out);
      }
    }
    
    out.println("Mis-replicated blocks that have been postponed:");
    for (BlockInfo block : postponedMisreplicatedBlocks) {
      dumpBlockMeta(block, out);
    }

  private void dumpBlockMeta(BlockInfo block, PrintWriter out) {
    List<DatanodeDescriptor> containingNodes = new ArrayList<>();
    List<DatanodeStorageInfo> containingLiveReplicasNodes = new ArrayList<>();

    NumberReplicas numReplicas = new NumberReplicas();
        containingLiveReplicasNodes, numReplicas,
        UnderReplicatedBlocks.LEVEL);
    
    assert containingLiveReplicasNodes.size() >= numReplicas.liveReplicas();
    int usableReplicas = numReplicas.liveReplicas() +
                         numReplicas.decommissionedAndDecommissioning();

    BlockCollection bc = block.getBlockCollection();
    String fileName = (bc == null) ? "[orphaned]" : bc.getName();
    out.print(fileName + ": ");

    out.print(block + ((usableReplicas > 0)? "" : " MISSING") + 
              " (replicas:" +
      final long offset, final long length, final int nrBlocksToReturn,
      final AccessMode mode) throws IOException {
    int curBlk;
    long curPos = 0;
    long blkSize;
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
      blkSize = blocks[curBlk].getNumBytes();
  }

  private void markBlockAsCorrupt(BlockToMarkCorrupt b,
      DatanodeStorageInfo storageInfo,
    }

    corruptReplicas.addToCorruptReplicasMap(b.stored, node, b.reason,
        b.reasonCode);

    NumberReplicas numberOfReplicas = countNodes(b.stored);
    if (hasEnoughLiveReplicas || hasMoreCorruptReplicas
        || corruptedDuringWrite) {
      invalidateBlock(b, node, numberOfReplicas);
    } else if (namesystem.isPopulatingReplQueues()) {
      updateNeededReplications(b.stored, -1, 0);
  }

  private boolean invalidateBlock(BlockToMarkCorrupt b, DatanodeInfo dn,
      NumberReplicas nr) throws IOException {
    blockLog.info("BLOCK* invalidateBlock: {} on {}", b, dn);
    DatanodeDescriptor node = getDatanodeManager().getDatanode(dn);
    if (node == null) {
    }

    if (nr.replicasOnStaleNodes() > 0) {
      blockLog.info("BLOCK* invalidateBlocks: postponing " +
          "invalidation of {} on {} because {} replica(s) are located on " +
          "nodes with potentially out-of-date block reports", b, dn,
          nr.replicasOnStaleNodes());
      postponeBlock(b.stored);
      return false;
    } else {
      addToInvalidates(b.corrupted, dn);
      removeStoredBlock(b.stored, node);
      blockLog.debug("BLOCK* invalidateBlocks: {} on {} listed for deletion.",
          b, dn);
      return true;
    }
  }

    this.shouldPostponeBlocksFromFuture  = postpone;
  }

  private void postponeBlock(BlockInfo blk) {
    if (postponedMisreplicatedBlocks.add(blk)) {
      postponedMisreplicatedBlocksCount.incrementAndGet();
    }
    int requiredReplication, numEffectiveReplicas;
    List<DatanodeDescriptor> containingNodes;
    DatanodeDescriptor srcNode;
    BlockCollection bc;
    int additionalReplRequired;

    int scheduledWork = 0;
        DatanodeStorageInfo[] targets = rw.targets;
        if (targets != null && targets.length != 0) {
          StringBuilder targetList = new StringBuilder("datanode(s)");
          for (DatanodeStorageInfo target : targets) {
            targetList.append(' ');
            targetList.append(target.getDatanodeDescriptor());
          }
          blockLog.info("BLOCK* ask {} to replicate {} to {}", rw.srcNode,
              rw.block, targetList);
    List<DatanodeDescriptor> datanodeDescriptors = null;
    if (nodes != null) {
      datanodeDescriptors = new ArrayList<>(nodes.size());
      for (String nodeStr : nodes) {
        DatanodeDescriptor node = datanodeManager.getDatanodeDescriptor(nodeStr);
        if (node != null) {
          datanodeDescriptors.add(node);
        }
   @VisibleForTesting
   DatanodeDescriptor chooseSourceDatanode(BlockInfo block,
       List<DatanodeDescriptor> containingNodes,
       List<DatanodeStorageInfo>  nodesContainingLiveReplicas,
       NumberReplicas numReplicas,
    if (timedOutItems != null) {
      namesystem.writeLock();
      try {
        for (BlockInfo timedOutItem : timedOutItems) {
          BlockInfo bi = getStoredBlock(timedOutItem);
          if (bi == null) {
            continue;
          }
          NumberReplicas num = countNodes(timedOutItem);
          if (isNeededReplication(bi, getReplication(bi), num.liveReplicas())) {
            neededReplications.add(bi, num.liveReplicas(),
                num.decommissionedAndDecommissioning(), getReplication(bi));

  public long requestBlockReportLeaseId(DatanodeRegistration nodeReg) {
    assert namesystem.hasReadLock();
    DatanodeDescriptor node;
    try {
      node = datanodeManager.getDatanode(nodeReg);
    } catch (UnregisteredNodeException e) {
          startIndex += (base+1);
        }
      }
      Iterator<BlockInfo> it = postponedMisreplicatedBlocks.iterator();
      for (int tmp = 0; tmp < startIndex; tmp++) {
        it.next();
      }
      long oldGenerationStamp, long oldNumBytes,
      DatanodeStorageInfo[] newStorages) throws IOException {
    assert namesystem.hasWriteLock();
    BlockToMarkCorrupt b;
    if (block.getGenerationStamp() != oldGenerationStamp) {
      b = new BlockToMarkCorrupt(oldBlock, block, oldGenerationStamp,
          "genstamp does not match " + oldGenerationStamp
          " but corrupt replicas map has " + corruptReplicasCount);
    }
    if ((corruptReplicasCount > 0) && (numLiveReplicas >= fileReplication)) {
      invalidateCorruptReplicas(storedBlock, reportedBlock, num);
    }
    return storedBlock;
  }
  private void invalidateCorruptReplicas(BlockInfo blk, Block reported,
      NumberReplicas numberReplicas) {
    Collection<DatanodeDescriptor> nodes = corruptReplicas.getNodes(blk);
    boolean removedFromBlocksMap = true;
    if (nodes == null)
      return;
    DatanodeDescriptor[] nodesCopy = nodes.toArray(
        new DatanodeDescriptor[nodes.size()]);
    for (DatanodeDescriptor node : nodesCopy) {
      try {
        if (!invalidateBlock(new BlockToMarkCorrupt(reported, blk, null,
            Reason.ANY), node, numberReplicas)) {
          removedFromBlocksMap = false;
        }
      } catch (IOException e) {
        replicationQueuesInitializer.join();
      } catch (final InterruptedException e) {
        LOG.warn("Interrupted while waiting for replicationQueueInitializer. Returning..");
      } finally {
        replicationQueuesInitializer = null;
      }
      CachedBlock cblock = namesystem.getCacheManager().getCachedBlocks()
          .get(new CachedBlock(storedBlock.getBlockId(), (short) 0, false));
      if (cblock != null) {
        boolean removed = node.getPendingCached().remove(cblock);
        removed |= node.getCached().remove(cblock);
        removed |= node.getPendingUncached().remove(cblock);
        if (removed) {
        stale++;
      }
    }
    return new NumberReplicas(live, decommissioned, decommissioning, corrupt,
        excess, stale);
  }

      String src, BlockInfo[] blocks) {
    for (BlockInfo b: blocks) {
      if (!b.isComplete()) {
        final int numNodes = b.numNodes();
        final int min = getMinStorageNum(b);
        final BlockUCState state = b.getBlockUCState();
    return blocksMap.getBlockCollection(b);
  }

  public void removeBlockFromMap(BlockInfo block) {
    removeFromExcessReplicateMap(block);
    blocksMap.removeBlock(block);
  private void removeFromExcessReplicateMap(BlockInfo block) {
    for (DatanodeStorageInfo info : getStorages(block)) {
      String uuid = info.getDatanodeDescriptor().getDatanodeUuid();
      LightWeightLinkedSet<BlockInfo> excessReplicas =
  public Collection<DatanodeDescriptor> getCorruptReplicas(BlockInfo block) {
    return corruptReplicas.getNodes(block);
  }

 public String getCorruptReason(BlockInfo block, DatanodeDescriptor node) {
   return corruptReplicas.getCorruptReason(block, node);
 }

    datanodeManager.clearPendingQueues();
    postponedMisreplicatedBlocks.clear();
    postponedMisreplicatedBlocksCount.set(0);
  }

  public static LocatedBlock newLocatedBlock(
      ExtendedBlock b, DatanodeStorageInfo[] storages,

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlocksMap.java
  void removeBlock(BlockInfo block) {
    BlockInfo blockInfo = blocks.remove(block);
    if (blockInfo == null)
      return;
    boolean removed = node.removeBlock(info);

    if (info.hasEmptyStorage()     // no datanodes left
              && info.isDeleted()) {  // does not belong to a file
      blocks.remove(b);  // remove block from the map
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/ContiguousBlockStorageOp.java
    return last;
  }

  static void addStorage(BlockInfo b, DatanodeStorageInfo storage) {
    int lastNode = ensureCapacity(b, 1);
    b.setStorageInfo(lastNode, storage);
    b.setNext(lastNode, null);
    b.setPrevious(lastNode, null);
  }

  static boolean removeStorage(BlockInfo b,
          "newBlock already exists.");
    }
  }

  static boolean hasEmptyStorage(BlockInfo b) {
    return b.getStorageInfo(0) == null;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/CorruptReplicasMap.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.Server;

    CORRUPTION_REPORTED  // client or datanode reported the corruption
  }

  private final SortedMap<BlockInfo, Map<DatanodeDescriptor, Reason>> corruptReplicasMap = new TreeMap<>();

  void addToCorruptReplicasMap(BlockInfo blk, DatanodeDescriptor dn,
      String reason, Reason reasonCode) {
    Map <DatanodeDescriptor, Reason> nodes = corruptReplicasMap.get(blk);
    if (nodes == null) {
      nodes = new HashMap<>();
      corruptReplicasMap.put(blk, nodes);
    }

  void removeFromCorruptReplicasMap(BlockInfo blk) {
    if (corruptReplicasMap != null) {
      corruptReplicasMap.remove(blk);
    }
             false if the replica is not in the map
  boolean removeFromCorruptReplicasMap(BlockInfo blk,
      DatanodeDescriptor datanode) {
    return removeFromCorruptReplicasMap(blk, datanode, Reason.ANY);
  }

  boolean removeFromCorruptReplicasMap(BlockInfo blk,
      DatanodeDescriptor datanode, Reason reason) {
    Map <DatanodeDescriptor, Reason> datanodes = corruptReplicasMap.get(blk);
    if (datanodes==null)
      return false;
  Collection<DatanodeDescriptor> getNodes(BlockInfo blk) {
    Map<DatanodeDescriptor, Reason> nodes = corruptReplicasMap.get(blk);
    return nodes != null ? nodes.keySet() : null;
  }

  boolean isReplicaCorrupt(BlockInfo blk, DatanodeDescriptor node) {
    Collection<DatanodeDescriptor> nodes = getNodes(blk);
    return ((nodes != null) && (nodes.contains(node)));
  }

  int numCorruptReplicas(BlockInfo blk) {
    Collection<DatanodeDescriptor> nodes = getNodes(blk);
    return (nodes == null) ? 0 : nodes.size();
  }
  @VisibleForTesting
  long[] getCorruptReplicaBlockIds(int numExpectedBlocks,
                                   Long startingBlockId) {
    if (numExpectedBlocks < 0 || numExpectedBlocks > 100) {
      return null;
    }
    Iterator<BlockInfo> blockIt = corruptReplicasMap.keySet().iterator();
    if (startingBlockId != null) {
      boolean isBlockFound = false;
      while (blockIt.hasNext()) {
        BlockInfo b = blockIt.next();
        if (b.getBlockId() == startingBlockId) {
          isBlockFound = true;
          break;
      }
    }

    ArrayList<Long> corruptReplicaBlockIds = new ArrayList<>();
    for(int i=0; i<numExpectedBlocks && blockIt.hasNext(); i++) {
      corruptReplicaBlockIds.add(blockIt.next().getBlockId());
  String getCorruptReason(BlockInfo block, DatanodeDescriptor node) {
    Reason reason = null;
    if(corruptReplicasMap.containsKey(block)) {
      if (corruptReplicasMap.get(block).containsKey(node)) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirWriteFileOp.java
  private FSDirWriteFileOp() {}
  static boolean unprotectedRemoveBlock(
      FSDirectory fsd, String path, INodesInPath iip, INodeFile fileNode,
      BlockInfo block) throws IOException {
    BlockInfoUnderConstruction uc = fileNode.removeLastBlock(block);
    fsd.writeLock();
    try {
      BlockInfo storedBlock = fsd.getBlockManager().getStoredBlock(localBlock);
      if (storedBlock != null &&
          !unprotectedRemoveBlock(fsd, src, iip, file, storedBlock)) {
        return;
      }
    } finally {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogLoader.java
        throw new IOException("Trying to remove more than one block from file "
            + path);
      }
      BlockInfo oldBlock = oldBlocks[oldBlocks.length - 1];
      boolean removed = FSDirWriteFileOp.unprotectedRemoveBlock(
          fsDir, path, iip, file, oldBlock);
      if (!removed && !(op instanceof UpdateBlocksOp)) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NamenodeFsck.java
      out.println("No. of corrupted Replica: " +
          numberReplicas.corruptReplicas());
      Collection<DatanodeDescriptor> corruptionRecord =
          bm.getCorruptReplicas(blockInfo);

      for(int idx = (blockInfo.numNodes()-1); idx >= 0; idx--) {
            dn.getNetworkLocation() + " ");
        if (corruptionRecord != null && corruptionRecord.contains(dn)) {
          out.print(CORRUPT_STATUS+"\t ReasonCode: "+
            bm.getCorruptReason(blockInfo, dn));
        } else if (dn.isDecommissioned() ){
          out.print(DECOMMISSIONED_STATUS);
        } else if (dn.isDecommissionInProgress()) {
              LightWeightLinkedSet<BlockInfo> blocksExcess =
                  bm.excessReplicateMap.get(dnDesc.getDatanodeUuid());
              Collection<DatanodeDescriptor> corruptReplicas =
                  bm.getCorruptReplicas(storedBlock);
              sb.append("(");
              if (dnDesc.isDecommissioned()) {
                sb.append("DECOMMISSIONED)");
                sb.append("DECOMMISSIONING)");
              } else if (corruptReplicas != null && corruptReplicas.contains(dnDesc)) {
                sb.append("CORRUPT)");
              } else if (blocksExcess != null && blocksExcess.contains(storedBlock)) {
                sb.append("EXCESS)");
              } else if (dnDesc.isStale(this.staleInterval)) {
                sb.append("STALE_NODE)");

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/DFSTestUtil.java
      throws TimeoutException, InterruptedException {
    int count = 0;
    final int ATTEMPTS = 50;
    int repls = BlockManagerTestUtil.numCorruptReplicas(ns.getBlockManager(),
        b.getLocalBlock());
    while (repls != corruptRepls && count < ATTEMPTS) {
      try {
        IOUtils.copyBytes(fs.open(file), new IOUtils.NullOutputStream(),
      count++;
      for (int i = 0; i < 10; i++) {
        repls = BlockManagerTestUtil.numCorruptReplicas(ns.getBlockManager(),
            b.getLocalBlock());
        Thread.sleep(100);
        if (repls == corruptRepls) {
          break;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManagerTestUtil.java
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
      final Block b) {
    final Set<String> rackSet = new HashSet<String>(0);
    final Collection<DatanodeDescriptor> corruptNodes = 
       getCorruptReplicas(blockManager).getNodes(blockManager.getStoredBlock(b));
    for(DatanodeStorageInfo storage : blockManager.blocksMap.getStorages(b)) {
      final DatanodeDescriptor cur = storage.getDatanodeDescriptor();
      if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
      throws ExecutionException, InterruptedException {
    dm.getDecomManager().runMonitor();
  }

  public static int numCorruptReplicas(BlockManager bm, Block block) {
    return bm.corruptReplicas.numCorruptReplicas(bm.getStoredBlock(block));
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockInfo.java

    final DatanodeStorageInfo storage = DFSTestUtil.createDatanodeStorageInfo("storageID", "127.0.0.1");

    blockInfo.addStorage(storage, blockInfo);
    Assert.assertEquals(storage, blockInfo.getStorageInfo(0));
  }

  public void testCopyConstructor() {
    BlockInfo old = new BlockInfoContiguous((short) 3);
    try {
      BlockInfo copy = new BlockInfoContiguous(old);
      assertEquals(old.getBlockCollection(), copy.getBlockCollection());
      assertEquals(old.getCapacity(), copy.getCapacity());
    } catch (Exception e) {
    final int MAX_BLOCKS = 10;

    DatanodeStorageInfo dd = DFSTestUtil.createDatanodeStorageInfo("s1", "1.1.1.1");
    ArrayList<Block> blockList = new ArrayList<>(MAX_BLOCKS);
    ArrayList<BlockInfo> blockInfoList = new ArrayList<>();
    int headIndex;
    int curIndex;


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockManager.java
        + " even if all available source nodes have reached their replication"
        + " limits below the hard limit.",
        bm.chooseSourceDatanode(
            bm.getStoredBlock(aBlock),
            cntNodes,
            liveNodes,
            new NumberReplicas(),
        + " replication since all available source nodes have reached"
        + " their replication limits.",
        bm.chooseSourceDatanode(
            bm.getStoredBlock(aBlock),
            cntNodes,
            liveNodes,
            new NumberReplicas(),
    assertNull("Does not choose a source node for a highest-priority"
        + " replication when all available nodes exceed the hard limit.",
        bm.chooseSourceDatanode(
            bm.getStoredBlock(aBlock),
            cntNodes,
            liveNodes,
            new NumberReplicas(),
        + " if all available source nodes have reached their replication"
        + " limits below the hard limit.",
        bm.chooseSourceDatanode(
            bm.getStoredBlock(aBlock),
            cntNodes,
            liveNodes,
            new NumberReplicas(),
    assertNull("Does not choose a source decommissioning node for a normal"
        + " replication when all available nodes exceed the hard limit.",
        bm.chooseSourceDatanode(
            bm.getStoredBlock(aBlock),
            cntNodes,
            liveNodes,
            new NumberReplicas(),

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestCorruptReplicaInfo.java
  private static final Log LOG = 
                           LogFactory.getLog(TestCorruptReplicaInfo.class);
  
  private final Map<Long, BlockInfo> block_map = new HashMap<>();
    
  private BlockInfo getBlock(Long block_id) {
    if (!block_map.containsKey(block_id)) {
      block_map.put(block_id,
          new BlockInfoContiguous(new Block(block_id, 0, 0), (short) 1));
    }
    return block_map.get(block_id);
  }
  
  private BlockInfo getBlock(int block_id) {
    return getBlock((long)block_id);
  }
  
      int NUM_BLOCK_IDS = 140;
      List<Long> block_ids = new LinkedList<>();
      for (int i=0;i<NUM_BLOCK_IDS;i++) {
        block_ids.add((long)i);
      }
  }
  
  private static void addToCorruptReplicasMap(CorruptReplicasMap crm,
      BlockInfo blk, DatanodeDescriptor dn) {
    crm.addToCorruptReplicasMap(blk, dn, "TEST", Reason.NONE);
  }
}

