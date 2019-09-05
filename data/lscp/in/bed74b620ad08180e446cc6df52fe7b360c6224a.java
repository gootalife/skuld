hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfo.java
  abstract boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock);

  abstract void replaceBlock(BlockInfo newBlock);


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoContiguous.java
  }

  @Override
  boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    return ContiguousBlockStorageOp.addStorage(this, storage);
  }

  @Override
    ucBlock.setBlockCollection(getBlockCollection());
    return ucBlock;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoUnderConstruction.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

          "No blocks found, lease removed.");
    }
    boolean allLiveReplicasTriedAsPrimary = true;
    for (int i = 0; i < replicas.size(); i++) {
      if (replicas.get(i).isAlive()) {
        allLiveReplicasTriedAsPrimary =
            (allLiveReplicasTriedAsPrimary &&
                replicas.get(i).getChosenAsPrimary());
      }
    }
    if (allLiveReplicasTriedAsPrimary) {
      for (int i = 0; i < replicas.size(); i++) {
        replicas.get(i).setChosenAsPrimary(false);
      }
    }
    long mostRecentLastUpdate = 0;
  public abstract BlockInfo convertToCompleteBlock();

  }

  private void appendUCParts(StringBuilder sb) {
    sb.append("{UCState=").append(blockUCState)
      .append(", truncateBlock=" + truncateBlock)
      .append(", primaryNodeIndex=").append(primaryNodeIndex)
      .append(", replicas=[");
    if (replicas != null) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoUnderConstructionContiguous.java
  @Override
  public BlockInfoContiguous convertToCompleteBlock() {
  }

  @Override
  boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    return ContiguousBlockStorageOp.addStorage(this, storage);
  }

  @Override
    ContiguousBlockStorageOp.replaceBlock(this, newBlock);
  }

  @Override
  public void setExpectedLocations(DatanodeStorageInfo[] targets) {
    int numLocations = targets == null ? 0 : targets.length;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.IOException;
  private final Set<Block> postponedMisreplicatedBlocks = Sets.newHashSet();

            DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_DEFAULT);
    this.shouldCheckForEnoughRacks =
        conf.get(DFSConfigKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY) == null
            ? false : true;

    this.blocksInvalidateWorkPct = DFSUtil.getInvalidateWorkPctPerIteration(conf);
    this.blocksReplWorkMultiplier = DFSUtil.getReplWorkMultiplier(conf);

  boolean shouldUpdateBlockKey(final long updateTime) throws IOException {
    return isBlockTokenEnabled()? blockTokenSecretManager.updateKeys(updateTime)
        : false;
  }

  public void activate(Configuration conf) {
    synchronized (neededReplications) {
      out.println("Metasave: Blocks waiting for replication: " + 
                  neededReplications.size());
      for (Block block : neededReplications) {
        dumpBlockMeta(block, out);
      }
    }
    
    out.println("Mis-replicated blocks that have been postponed:");
    for (Block block : postponedMisreplicatedBlocks) {
      dumpBlockMeta(block, out);
    }

  private void dumpBlockMeta(Block block, PrintWriter out) {
    List<DatanodeDescriptor> containingNodes =
                                      new ArrayList<DatanodeDescriptor>();
    List<DatanodeStorageInfo> containingLiveReplicasNodes =
      new ArrayList<>();

    NumberReplicas numReplicas = new NumberReplicas();
        containingLiveReplicasNodes, numReplicas,
        UnderReplicatedBlocks.LEVEL);
    
    assert containingLiveReplicasNodes.size() >= numReplicas.liveReplicas();
    int usableReplicas = numReplicas.liveReplicas() +
                         numReplicas.decommissionedAndDecommissioning();
    
    if (block instanceof BlockInfo) {
      BlockCollection bc = ((BlockInfo) block).getBlockCollection();
      String fileName = (bc == null) ? "[orphaned]" : bc.getName();
      out.print(fileName + ": ");
    }
    out.print(block + ((usableReplicas > 0)? "" : " MISSING") + 
              " (replicas:" +
      final long offset, final long length, final int nrBlocksToReturn,
      final AccessMode mode) throws IOException {
    int curBlk;
    long curPos = 0, blkSize = 0;
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
      blkSize = blocks[curBlk].getNumBytes();
  }

  private void markBlockAsCorrupt(BlockToMarkCorrupt b,
      DatanodeStorageInfo storageInfo,
    }

    corruptReplicas.addToCorruptReplicasMap(b.corrupted, node, b.reason,
        b.reasonCode);

    NumberReplicas numberOfReplicas = countNodes(b.stored);
    if (hasEnoughLiveReplicas || hasMoreCorruptReplicas
        || corruptedDuringWrite) {
      invalidateBlock(b, node);
    } else if (namesystem.isPopulatingReplQueues()) {
      updateNeededReplications(b.stored, -1, 0);
  }

  private boolean invalidateBlock(BlockToMarkCorrupt b, DatanodeInfo dn
      ) throws IOException {
    blockLog.info("BLOCK* invalidateBlock: {} on {}", b, dn);
    DatanodeDescriptor node = getDatanodeManager().getDatanode(dn);
    if (node == null) {
    }

    NumberReplicas nr = countNodes(b.stored);
    if (nr.replicasOnStaleNodes() > 0) {
      blockLog.info("BLOCK* invalidateBlocks: postponing " +
          "invalidation of {} on {} because {} replica(s) are located on " +
          "nodes with potentially out-of-date block reports", b, dn,
          nr.replicasOnStaleNodes());
      postponeBlock(b.corrupted);
      return false;
    } else if (nr.liveReplicas() >= 1) {
      addToInvalidates(b.corrupted, dn);
      removeStoredBlock(b.stored, node);
      blockLog.debug("BLOCK* invalidateBlocks: {} on {} listed for deletion.",
          b, dn);
      return true;
    } else {
      blockLog.info("BLOCK* invalidateBlocks: {} on {} is the only copy and" +
          " was not deleted", b, dn);
      return false;
    }
  }

    this.shouldPostponeBlocksFromFuture  = postpone;
  }


  private void postponeBlock(Block blk) {
    if (postponedMisreplicatedBlocks.add(blk)) {
      postponedMisreplicatedBlocksCount.incrementAndGet();
    }
    int requiredReplication, numEffectiveReplicas;
    List<DatanodeDescriptor> containingNodes;
    DatanodeDescriptor srcNode;
    BlockCollection bc = null;
    int additionalReplRequired;

    int scheduledWork = 0;
        DatanodeStorageInfo[] targets = rw.targets;
        if (targets != null && targets.length != 0) {
          StringBuilder targetList = new StringBuilder("datanode(s)");
          for (int k = 0; k < targets.length; k++) {
            targetList.append(' ');
            targetList.append(targets[k].getDatanodeDescriptor());
          }
          blockLog.info("BLOCK* ask {} to replicate {} to {}", rw.srcNode,
              rw.block, targetList);
    List<DatanodeDescriptor> datanodeDescriptors = null;
    if (nodes != null) {
      datanodeDescriptors = new ArrayList<>(nodes.size());
      for (int i = 0; i < nodes.size(); i++) {
        DatanodeDescriptor node = datanodeManager.getDatanodeDescriptor(nodes.get(i));
        if (node != null) {
          datanodeDescriptors.add(node);
        }
   @VisibleForTesting
   DatanodeDescriptor chooseSourceDatanode(Block block,
       List<DatanodeDescriptor> containingNodes,
       List<DatanodeStorageInfo>  nodesContainingLiveReplicas,
       NumberReplicas numReplicas,
    if (timedOutItems != null) {
      namesystem.writeLock();
      try {
        for (int i = 0; i < timedOutItems.length; i++) {
          BlockInfo bi = getStoredBlock(timedOutItems[i]);
          if (bi == null) {
            continue;
          }
          NumberReplicas num = countNodes(timedOutItems[i]);
          if (isNeededReplication(bi, getReplication(bi), num.liveReplicas())) {
            neededReplications.add(bi, num.liveReplicas(),
                num.decommissionedAndDecommissioning(), getReplication(bi));

  public long requestBlockReportLeaseId(DatanodeRegistration nodeReg) {
    assert namesystem.hasReadLock();
    DatanodeDescriptor node = null;
    try {
      node = datanodeManager.getDatanode(nodeReg);
    } catch (UnregisteredNodeException e) {
          startIndex += (base+1);
        }
      }
      Iterator<Block> it = postponedMisreplicatedBlocks.iterator();
      for (int tmp = 0; tmp < startIndex; tmp++) {
        it.next();
      }
      long oldGenerationStamp, long oldNumBytes,
      DatanodeStorageInfo[] newStorages) throws IOException {
    assert namesystem.hasWriteLock();
    BlockToMarkCorrupt b = null;
    if (block.getGenerationStamp() != oldGenerationStamp) {
      b = new BlockToMarkCorrupt(oldBlock, block, oldGenerationStamp,
          "genstamp does not match " + oldGenerationStamp
          " but corrupt replicas map has " + corruptReplicasCount);
    }
    if ((corruptReplicasCount > 0) && (numLiveReplicas >= fileReplication)) {
      invalidateCorruptReplicas(storedBlock, reportedBlock);
    }
    return storedBlock;
  }
  private void invalidateCorruptReplicas(BlockInfo blk, Block reported) {
    Collection<DatanodeDescriptor> nodes = corruptReplicas.getNodes(blk);
    boolean removedFromBlocksMap = true;
    if (nodes == null)
      return;
    DatanodeDescriptor[] nodesCopy = nodes.toArray(new DatanodeDescriptor[0]);
    for (DatanodeDescriptor node : nodesCopy) {
      try {
        if (!invalidateBlock(new BlockToMarkCorrupt(reported, blk, null,
            Reason.ANY), node)) {
          removedFromBlocksMap = false;
        }
      } catch (IOException e) {
        replicationQueuesInitializer.join();
      } catch (final InterruptedException e) {
        LOG.warn("Interrupted while waiting for replicationQueueInitializer. Returning..");
        return;
      } finally {
        replicationQueuesInitializer = null;
      }
      CachedBlock cblock = namesystem.getCacheManager().getCachedBlocks()
          .get(new CachedBlock(storedBlock.getBlockId(), (short) 0, false));
      if (cblock != null) {
        boolean removed = false;
        removed |= node.getPendingCached().remove(cblock);
        removed |= node.getCached().remove(cblock);
        removed |= node.getPendingUncached().remove(cblock);
        if (removed) {
        stale++;
      }
    }
    return new NumberReplicas(live, decommissioned, decommissioning, corrupt, excess, stale);
  }

      String src, BlockInfo[] blocks) {
    for (BlockInfo b: blocks) {
      if (!b.isComplete()) {
        final BlockInfoUnderConstruction uc =
            (BlockInfoUnderConstruction)b;
        final int numNodes = b.numNodes();
        final int min = getMinStorageNum(b);
        final BlockUCState state = b.getBlockUCState();
    return blocksMap.getBlockCollection(b);
  }

  public int numCorruptReplicas(Block block) {
    return corruptReplicas.numCorruptReplicas(block);
  }

  public void removeBlockFromMap(Block block) {
    removeFromExcessReplicateMap(block);
    blocksMap.removeBlock(block);
  private void removeFromExcessReplicateMap(Block block) {
    for (DatanodeStorageInfo info : getStorages(block)) {
      String uuid = info.getDatanodeDescriptor().getDatanodeUuid();
      LightWeightLinkedSet<BlockInfo> excessReplicas =
  public Collection<DatanodeDescriptor> getCorruptReplicas(Block block) {
    return corruptReplicas.getNodes(block);
  }

 public String getCorruptReason(Block block, DatanodeDescriptor node) {
   return corruptReplicas.getCorruptReason(block, node);
 }

    datanodeManager.clearPendingQueues();
    postponedMisreplicatedBlocks.clear();
    postponedMisreplicatedBlocksCount.set(0);
  };

  public static LocatedBlock newLocatedBlock(
      ExtendedBlock b, DatanodeStorageInfo[] storages,

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlocksMap.java
  void removeBlock(Block block) {
    BlockInfo blockInfo = blocks.remove(block);
    if (blockInfo == null)
      return;
    boolean removed = node.removeBlock(info);

    if (info.getDatanode(0) == null     // no datanodes left
              && info.isDeleted()) {  // does not belong to a file
      blocks.remove(b);  // remove block from the map
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/ContiguousBlockStorageOp.java
    return last;
  }

  static boolean addStorage(BlockInfo b, DatanodeStorageInfo storage) {
    int lastNode = ensureCapacity(b, 1);
    b.setStorageInfo(lastNode, storage);
    b.setNext(lastNode, null);
    b.setPrevious(lastNode, null);
    return true;
  }

  static boolean removeStorage(BlockInfo b,
          "newBlock already exists.");
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/CorruptReplicasMap.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.Server;

    CORRUPTION_REPORTED  // client or datanode reported the corruption
  }

  private final SortedMap<Block, Map<DatanodeDescriptor, Reason>> corruptReplicasMap =
    new TreeMap<Block, Map<DatanodeDescriptor, Reason>>();

  void addToCorruptReplicasMap(Block blk, DatanodeDescriptor dn,
      String reason, Reason reasonCode) {
    Map <DatanodeDescriptor, Reason> nodes = corruptReplicasMap.get(blk);
    if (nodes == null) {
      nodes = new HashMap<DatanodeDescriptor, Reason>();
      corruptReplicasMap.put(blk, nodes);
    }
    
  void removeFromCorruptReplicasMap(Block blk) {
    if (corruptReplicasMap != null) {
      corruptReplicasMap.remove(blk);
    }
             false if the replica is not in the map
  boolean removeFromCorruptReplicasMap(Block blk, DatanodeDescriptor datanode) {
    return removeFromCorruptReplicasMap(blk, datanode, Reason.ANY);
  }

  boolean removeFromCorruptReplicasMap(Block blk, DatanodeDescriptor datanode,
      Reason reason) {
    Map <DatanodeDescriptor, Reason> datanodes = corruptReplicasMap.get(blk);
    if (datanodes==null)
      return false;
  Collection<DatanodeDescriptor> getNodes(Block blk) {
    Map <DatanodeDescriptor, Reason> nodes = corruptReplicasMap.get(blk);
    if (nodes == null)
      return null;
    return nodes.keySet();
  }

  boolean isReplicaCorrupt(Block blk, DatanodeDescriptor node) {
    Collection<DatanodeDescriptor> nodes = getNodes(blk);
    return ((nodes != null) && (nodes.contains(node)));
  }

  int numCorruptReplicas(Block blk) {
    Collection<DatanodeDescriptor> nodes = getNodes(blk);
    return (nodes == null) ? 0 : nodes.size();
  }
  long[] getCorruptReplicaBlockIds(int numExpectedBlocks,
                                   Long startingBlockId) {
    if (numExpectedBlocks < 0 || numExpectedBlocks > 100) {
      return null;
    }
    
    Iterator<Block> blockIt = corruptReplicasMap.keySet().iterator();
    
    if (startingBlockId != null) {
      boolean isBlockFound = false;
      while (blockIt.hasNext()) {
        Block b = blockIt.next();
        if (b.getBlockId() == startingBlockId) {
          isBlockFound = true;
          break; 
      }
    }

    ArrayList<Long> corruptReplicaBlockIds = new ArrayList<Long>();

    for(int i=0; i<numExpectedBlocks && blockIt.hasNext(); i++) {
      corruptReplicaBlockIds.add(blockIt.next().getBlockId());
  String getCorruptReason(Block block, DatanodeDescriptor node) {
    Reason reason = null;
    if(corruptReplicasMap.containsKey(block)) {
      if (corruptReplicasMap.get(block).containsKey(node)) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirWriteFileOp.java
  private FSDirWriteFileOp() {}
  static boolean unprotectedRemoveBlock(
      FSDirectory fsd, String path, INodesInPath iip, INodeFile fileNode,
      Block block) throws IOException {
    BlockInfoUnderConstruction uc = fileNode.removeLastBlock(block);
    fsd.writeLock();
    try {
      if (!unprotectedRemoveBlock(fsd, src, iip, file, localBlock)) {
        return;
      }
    } finally {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogLoader.java
        throw new IOException("Trying to remove more than one block from file "
            + path);
      }
      Block oldBlock = oldBlocks[oldBlocks.length - 1];
      boolean removed = FSDirWriteFileOp.unprotectedRemoveBlock(
          fsDir, path, iip, file, oldBlock);
      if (!removed && !(op instanceof UpdateBlocksOp)) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NamenodeFsck.java
      out.println("No. of corrupted Replica: " +
          numberReplicas.corruptReplicas());
      Collection<DatanodeDescriptor> corruptionRecord = null;
      if (bm.getCorruptReplicas(block) != null) {
        corruptionRecord = bm.getCorruptReplicas(block);
      }

      for(int idx = (blockInfo.numNodes()-1); idx >= 0; idx--) {
            dn.getNetworkLocation() + " ");
        if (corruptionRecord != null && corruptionRecord.contains(dn)) {
          out.print(CORRUPT_STATUS+"\t ReasonCode: "+
            bm.getCorruptReason(block,dn));
        } else if (dn.isDecommissioned() ){
          out.print(DECOMMISSIONED_STATUS);
        } else if (dn.isDecommissionInProgress()) {
              LightWeightLinkedSet<BlockInfo> blocksExcess =
                  bm.excessReplicateMap.get(dnDesc.getDatanodeUuid());
              Collection<DatanodeDescriptor> corruptReplicas =
                  bm.getCorruptReplicas(block.getLocalBlock());
              sb.append("(");
              if (dnDesc.isDecommissioned()) {
                sb.append("DECOMMISSIONED)");
                sb.append("DECOMMISSIONING)");
              } else if (corruptReplicas != null && corruptReplicas.contains(dnDesc)) {
                sb.append("CORRUPT)");
              } else if (blocksExcess != null && blocksExcess.contains(block.getLocalBlock())) {
                sb.append("EXCESS)");
              } else if (dnDesc.isStale(this.staleInterval)) {
                sb.append("STALE_NODE)");

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/DFSTestUtil.java
      throws TimeoutException, InterruptedException {
    int count = 0;
    final int ATTEMPTS = 50;
    int repls = ns.getBlockManager().numCorruptReplicas(b.getLocalBlock());
    while (repls != corruptRepls && count < ATTEMPTS) {
      try {
        IOUtils.copyBytes(fs.open(file), new IOUtils.NullOutputStream(),
      count++;
      for (int i = 0; i < 10; i++) {
        repls = ns.getBlockManager().numCorruptReplicas(b.getLocalBlock());
        Thread.sleep(100);
        if (repls == corruptRepls) {
          break;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManagerTestUtil.java
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
      final Block b) {
    final Set<String> rackSet = new HashSet<String>(0);
    final Collection<DatanodeDescriptor> corruptNodes = 
       getCorruptReplicas(blockManager).getNodes(b);
    for(DatanodeStorageInfo storage : blockManager.blocksMap.getStorages(b)) {
      final DatanodeDescriptor cur = storage.getDatanodeDescriptor();
      if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
      throws ExecutionException, InterruptedException {
    dm.getDecomManager().runMonitor();
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockInfo.java

    final DatanodeStorageInfo storage = DFSTestUtil.createDatanodeStorageInfo("storageID", "127.0.0.1");

    boolean added = blockInfo.addStorage(storage, blockInfo);

    Assert.assertTrue(added);
    Assert.assertEquals(storage, blockInfo.getStorageInfo(0));
  }

  public void testCopyConstructor() {
    BlockInfo old = new BlockInfoContiguous((short) 3);
    try {
      BlockInfo copy = new BlockInfoContiguous((BlockInfoContiguous)old);
      assertEquals(old.getBlockCollection(), copy.getBlockCollection());
      assertEquals(old.getCapacity(), copy.getCapacity());
    } catch (Exception e) {
    final int MAX_BLOCKS = 10;

    DatanodeStorageInfo dd = DFSTestUtil.createDatanodeStorageInfo("s1", "1.1.1.1");
    ArrayList<Block> blockList = new ArrayList<Block>(MAX_BLOCKS);
    ArrayList<BlockInfo> blockInfoList = new ArrayList<BlockInfo>();
    int headIndex;
    int curIndex;


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockManager.java
        + " even if all available source nodes have reached their replication"
        + " limits below the hard limit.",
        bm.chooseSourceDatanode(
            aBlock,
            cntNodes,
            liveNodes,
            new NumberReplicas(),
        + " replication since all available source nodes have reached"
        + " their replication limits.",
        bm.chooseSourceDatanode(
            aBlock,
            cntNodes,
            liveNodes,
            new NumberReplicas(),
    assertNull("Does not choose a source node for a highest-priority"
        + " replication when all available nodes exceed the hard limit.",
        bm.chooseSourceDatanode(
            aBlock,
            cntNodes,
            liveNodes,
            new NumberReplicas(),
        + " if all available source nodes have reached their replication"
        + " limits below the hard limit.",
        bm.chooseSourceDatanode(
            aBlock,
            cntNodes,
            liveNodes,
            new NumberReplicas(),
    assertNull("Does not choose a source decommissioning node for a normal"
        + " replication when all available nodes exceed the hard limit.",
        bm.chooseSourceDatanode(
            aBlock,
            cntNodes,
            liveNodes,
            new NumberReplicas(),

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestCorruptReplicaInfo.java
  private static final Log LOG = 
                           LogFactory.getLog(TestCorruptReplicaInfo.class);
  
  private final Map<Long, Block> block_map =
    new HashMap<Long, Block>();  
    
  private Block getBlock(Long block_id) {
    if (!block_map.containsKey(block_id)) {
      block_map.put(block_id, new Block(block_id,0,0));
    }
    
    return block_map.get(block_id);
  }
  
  private Block getBlock(int block_id) {
    return getBlock((long)block_id);
  }
  
      int NUM_BLOCK_IDS = 140;
      List<Long> block_ids = new LinkedList<Long>();
      for (int i=0;i<NUM_BLOCK_IDS;i++) {
        block_ids.add((long)i);
      }
  }
  
  private static void addToCorruptReplicasMap(CorruptReplicasMap crm,
      Block blk, DatanodeDescriptor dn) {
    crm.addToCorruptReplicasMap(blk, dn, "TEST", Reason.NONE);
  }
}

