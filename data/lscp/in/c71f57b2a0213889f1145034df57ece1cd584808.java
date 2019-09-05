hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
  public boolean checkMinReplication(BlockInfo block) {
    return (countNodes(block).liveReplicas() >= minReplication);
  }

  int computeReplicationWork(int blocksToProcess) {
    List<List<BlockInfo>> blocksToReplicate = null;
    namesystem.writeLock();
    try {
  @VisibleForTesting
  int computeReplicationWorkForBlocks(List<List<BlockInfo>> blocksToReplicate) {
    int requiredReplication, numEffectiveReplicas;
    List<DatanodeDescriptor> containingNodes;
    DatanodeDescriptor srcNode;
    try {
      synchronized (neededReplications) {
        for (int priority = 0; priority < blocksToReplicate.size(); priority++) {
          for (BlockInfo block : blocksToReplicate.get(priority)) {
            bc = blocksMap.getBlockCollection(block);
        }

        synchronized (neededReplications) {
          BlockInfo block = rw.block;
          int priority = rw.priority;
  private void processPendingReplications() {
    BlockInfo[] timedOutItems = pendingReplications.getTimedOutBlocks();
    if (timedOutItems != null) {
      namesystem.writeLock();
      try {
  
  public void setReplication(final short oldRepl, final short newRepl,
      final String src, final BlockInfo... blocks) {
    if (newRepl == oldRepl) {
      return;
    }

    for(BlockInfo b : blocks) {
      updateNeededReplications(b, 0, newRepl-oldRepl);
    }
      
      LOG.info("Decreasing replication from " + oldRepl + " to " + newRepl
          + " for " + src);
      for(BlockInfo b : blocks) {
        processOverReplicatedBlock(b, newRepl, null, null);
      }
    } else { // replication factor is increased
    blockLog.debug("BLOCK* removeStoredBlock: {} from {}", block, node);
    assert (namesystem.hasWriteLock());
    {
      BlockInfo storedBlock = getStoredBlock(block);
      if (storedBlock == null || !blocksMap.removeNode(storedBlock, node)) {
        blockLog.debug("BLOCK* removeStoredBlock: {} has already been" +
            " removed from node {}", block, node);
        return;
      BlockCollection bc = blocksMap.getBlockCollection(block);
      if (bc != null) {
        namesystem.decrementSafeBlockCount(storedBlock);
        updateNeededReplications(storedBlock, -1, 0);
      }

    BlockInfo storedBlock = getStoredBlock(block);
    if (storedBlock != null) {
      pendingReplications.decrement(getStoredBlock(block), node);
    }
    processAndHandleReportedBlock(storageInfo, block, ReplicaState.FINALIZED,
        delHintNode);
  }
  public NumberReplicas countNodes(BlockInfo b) {
    int decommissioned = 0;
    int decommissioning = 0;
    int live = 0;
  }

    if (!namesystem.isPopulatingReplQueues()) {
      return;
    }
    final Iterator<BlockInfo> it = srcNode.getBlockIterator();
    int numOverReplicated = 0;
    while(it.hasNext()) {
      final BlockInfo block = it.next();
      BlockCollection bc = blocksMap.getBlockCollection(block);
      short expectedReplication = bc.getPreferredBlockReplication();
      NumberReplicas num = countNodes(block);
    return blocksMap.size();
  }

  public void removeBlock(BlockInfo block) {
    assert namesystem.hasWriteLock();
  }

  private void updateNeededReplications(final BlockInfo block,
      final int curReplicasDelta, int expectedReplicasDelta) {
    namesystem.writeLock();
    try {
  public void checkReplication(BlockCollection bc) {
    final short expected = bc.getPreferredBlockReplication();
    for (BlockInfo block : bc.getBlocks()) {
      final NumberReplicas n = countNodes(block);
      if (isNeededReplication(block, expected, n.liveReplicas())) { 
        neededReplications.add(block, n.liveReplicas(),
  public Iterator<BlockInfo> getCorruptReplicaBlockIterator() {
    return neededReplications.iterator(
        UnderReplicatedBlocks.QUEUE_WITH_CORRUPT_BLOCKS);
  }

  private static class ReplicationWork {

    private final BlockInfo block;
    private final BlockCollection bc;

    private final DatanodeDescriptor srcNode;
    private DatanodeStorageInfo targets[];
    private final int priority;

    public ReplicationWork(BlockInfo block,
        BlockCollection bc,
        DatanodeDescriptor srcNode,
        List<DatanodeDescriptor> containingNodes,

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/PendingReplicationBlocks.java
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
class PendingReplicationBlocks {
  private static final Logger LOG = BlockManager.LOG;

  private final Map<BlockInfo, PendingBlockInfo> pendingReplications;
  private final ArrayList<BlockInfo> timedOutItems;
  Daemon timerThread = null;
  private volatile boolean fsRunning = true;

    if ( timeoutPeriod > 0 ) {
      this.timeout = timeoutPeriod;
    }
    pendingReplications = new HashMap<>();
    timedOutItems = new ArrayList<>();
  }

  void start() {
  void increment(BlockInfo block, DatanodeDescriptor[] targets) {
    synchronized (pendingReplications) {
      PendingBlockInfo found = pendingReplications.get(block);
      if (found == null) {
  void decrement(BlockInfo block, DatanodeDescriptor dn) {
    synchronized (pendingReplications) {
      PendingBlockInfo found = pendingReplications.get(block);
      if (found != null) {
  void remove(BlockInfo block) {
    synchronized (pendingReplications) {
      pendingReplications.remove(block);
    }
  int getNumReplicas(BlockInfo block) {
    synchronized (pendingReplications) {
      PendingBlockInfo found = pendingReplications.get(block);
      if (found != null) {
  BlockInfo[] getTimedOutBlocks() {
    synchronized (timedOutItems) {
      if (timedOutItems.size() <= 0) {
        return null;
      }
      BlockInfo[] blockList = timedOutItems.toArray(
          new BlockInfo[timedOutItems.size()]);
      timedOutItems.clear();
      return blockList;
    }
    PendingBlockInfo(DatanodeDescriptor[] targets) {
      this.timeStamp = monotonicNow();
      this.targets = targets == null ? new ArrayList<DatanodeDescriptor>()
          : new ArrayList<>(Arrays.asList(targets));
    }

    long getTimeStamp() {

    void incrementReplicas(DatanodeDescriptor... newTargets) {
      if (newTargets != null) {
        Collections.addAll(targets, newTargets);
      }
    }

    void pendingReplicationCheck() {
      synchronized (pendingReplications) {
        Iterator<Map.Entry<BlockInfo, PendingBlockInfo>> iter =
                                    pendingReplications.entrySet().iterator();
        long now = monotonicNow();
        if(LOG.isDebugEnabled()) {
          LOG.debug("PendingReplicationMonitor checking Q");
        }
        while (iter.hasNext()) {
          Map.Entry<BlockInfo, PendingBlockInfo> entry = iter.next();
          PendingBlockInfo pendingBlock = entry.getValue();
          if (now > pendingBlock.getTimeStamp() + timeout) {
            BlockInfo block = entry.getKey();
            synchronized (timedOutItems) {
              timedOutItems.add(block);
            }
    synchronized (pendingReplications) {
      out.println("Metasave: Blocks being replicated: " +
                  pendingReplications.size());
      for (Map.Entry<BlockInfo, PendingBlockInfo> entry :
          pendingReplications.entrySet()) {
        PendingBlockInfo pendingBlock = entry.getValue();
        Block block = entry.getKey();
        out.println(block +

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/UnderReplicatedBlocks.java
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

class UnderReplicatedBlocks implements Iterable<BlockInfo> {
  static final int LEVEL = 5;
  static final int QUEUE_WITH_CORRUPT_BLOCKS = 4;
  private final List<LightWeightLinkedSet<BlockInfo>> priorityQueues
      = new ArrayList<>(LEVEL);

  private int corruptReplOneBlocks = 0;
  UnderReplicatedBlocks() {
    for (int i = 0; i < LEVEL; i++) {
      priorityQueues.add(new LightWeightLinkedSet<BlockInfo>());
    }
  }

  }

  synchronized boolean contains(BlockInfo block) {
    for(LightWeightLinkedSet<BlockInfo> set : priorityQueues) {
      if (set.contains(block)) {
        return true;
      }
  }

  private int getPriority(int curReplicas,
                          int decommissionedReplicas,
                          int expectedReplicas) {
    assert curReplicas >= 0 : "Negative replicas!";
  synchronized boolean add(BlockInfo block,
                           int curReplicas,
                           int decomissionedReplicas,
                           int expectedReplicas) {
    assert curReplicas >= 0 : "Negative replicas!";
    int priLevel = getPriority(curReplicas, decomissionedReplicas,
                               expectedReplicas);
    if(priorityQueues.get(priLevel).add(block)) {
      if (priLevel == QUEUE_WITH_CORRUPT_BLOCKS &&
  }

  synchronized boolean remove(BlockInfo block,
                              int oldReplicas,
                              int decommissionedReplicas,
                              int oldExpectedReplicas) {
    int priLevel = getPriority(oldReplicas,
                               decommissionedReplicas,
                               oldExpectedReplicas);
    boolean removedBlock = remove(block, priLevel);
  boolean remove(BlockInfo block, int priLevel) {
    if(priLevel >= 0 && priLevel < LEVEL
        && priorityQueues.get(priLevel).remove(block)) {
      NameNode.blockStateChangeLog.debug(
  synchronized void update(BlockInfo block, int curReplicas,
                           int decommissionedReplicas,
                           int curExpectedReplicas,
                           int curReplicasDelta, int expectedReplicasDelta) {
    int oldReplicas = curReplicas-curReplicasDelta;
    int oldExpectedReplicas = curExpectedReplicas-expectedReplicasDelta;
    int curPri = getPriority(curReplicas, decommissionedReplicas,
        curExpectedReplicas);
    int oldPri = getPriority(oldReplicas, decommissionedReplicas,
        oldExpectedReplicas);
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("UnderReplicationBlocks.update " + 
        block +
  public synchronized List<List<BlockInfo>> chooseUnderReplicatedBlocks(
      int blocksToProcess) {
    List<List<BlockInfo>> blocksToReplicate = new ArrayList<>(LEVEL);
    for (int i = 0; i < LEVEL; i++) {
      blocksToReplicate.add(new ArrayList<BlockInfo>());
    }

    if (size() == 0) { // There are no blocks to collect.
      while (blockCount < blocksToProcess
          && neededReplicationsIterator.hasNext()) {
        BlockInfo block = neededReplicationsIterator.next();
        blocksToReplicate.get(priority).add(block);
        blockCount++;
      }
  class BlockIterator implements Iterator<BlockInfo> {
    private int level;
    private boolean isIteratorForLevel = false;
    private final List<Iterator<BlockInfo>> iterators = new ArrayList<>();

    }

    @Override
    public BlockInfo next() {
      if (isIteratorForLevel) {
        return iterators.get(0).next();
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirAttrOp.java
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.util.EnumCounters;
      }

      final short[] blockRepls = new short[2]; // 0: old, 1: new
      final BlockInfo[] blocks = unprotectedSetReplication(fsd, src,
          replication, blockRepls);
      isFile = blocks != null;
      if (isFile) {
        fsd.getEditLog().logSetReplication(src, replication);
    }
  }

  static BlockInfo[] unprotectedSetReplication(
      FSDirectory fsd, String src, short replication, short[] blockRepls)
      throws QuotaExceededException, UnresolvedLinkException,
             SnapshotAccessControlException {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
  void removeBlocks(BlocksMapUpdateInfo blocks) {
    List<BlockInfo> toDeleteList = blocks.getToDeleteList();
    Iterator<BlockInfo> iter = toDeleteList.iterator();
    while (iter.hasNext()) {
      writeLock();
      try {
    boolean trackBlockCounts = isSafeModeTrackingBlocks();
    int numRemovedComplete = 0, numRemovedSafe = 0;

    for (BlockInfo b : blocks.getToDeleteList()) {
      if (trackBlockCounts) {
        if (b.isComplete()) {
          numRemovedComplete++;
          if (blockManager.checkMinReplication(b)) {
            numRemovedSafe++;
          }
        }
      boolean changed = false;
      writeLock();
      try {
        final Iterator<BlockInfo> it =
            blockManager.getCorruptReplicaBlockIterator();

        while (it.hasNext()) {
          Block b = it.next();
  }

  @Override
  public void decrementSafeBlockCount(BlockInfo b) {
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) // mostly true
      }

      final Iterator<BlockInfo> blkIterator =
          blockManager.getCorruptReplicaBlockIterator();

      int skip = getIntCookie(cookieTab[0]);
      for (int i = 0; i < skip && blkIterator.hasNext(); i++) {
      }

      while (blkIterator.hasNext()) {
        BlockInfo blk = blkIterator.next();
        final INode inode = (INode)blockManager.getBlockCollection(blk);
        skip++;
        if (inode != null && blockManager.countNodes(blk).liveReplicas() == 0) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INode.java
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.DstReference;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithName;
    private final List<BlockInfo> toDeleteList;

    public BlocksMapUpdateInfo() {
      toDeleteList = new ChunkedArrayList<>();
    public List<BlockInfo> getToDeleteList() {
      return toDeleteList;
    }
    
    public void addDeleteBlock(BlockInfo toDelete) {
      assert toDelete != null : "toDelete is null";
      toDeleteList.add(toDelete);
    }

    public void removeDeleteBlock(BlockInfo block) {
      assert block != null : "block is null";
      toDeleteList.remove(block);
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeFile.java
        getDiffs().findEarlierSnapshotBlocks(snapshotId);
    if(snapshotBlocks == null)
      return;
    List<BlockInfo> toDelete = collectedBlocks.getToDeleteList();
    for(BlockInfo blk : snapshotBlocks) {
      if(toDelete.contains(blk))
        collectedBlocks.removeDeleteBlock(blk);
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NamenodeFsck.java
      }
      BlockCollection bc = bm.getBlockCollection(blockInfo);
      INode iNode = (INode) bc;
      NumberReplicas numberReplicas= bm.countNodes(blockInfo);
      out.println("Block Id: " + blockId);
      out.println("Block belongs to: "+iNode.getFullPathName());
      out.println("No. of Expected Replica: " +
      throws IOException {
    long fileLen = file.getLen();
    LocatedBlocks blocks = null;
    final FSNamesystem fsn = namenode.getNamesystem();
    fsn.readLock();
    try {
      blocks = FSDirStatAndListingOp.getBlockLocations(
      ExtendedBlock block = lBlk.getBlock();
      BlockManager bm = namenode.getNamesystem().getBlockManager();

      final BlockInfo storedBlock = bm.getStoredBlock(
          block.getLocalBlock());
      NumberReplicas numberReplicas = bm.countNodes(storedBlock);
      int decommissionedReplicas = numberReplicas.decommissioned();;
      int decommissioningReplicas = numberReplicas.decommissioning();
      res.decommissionedReplicas +=  decommissionedReplicas;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/SafeMode.java
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;

@InterfaceAudience.Private
  public void incrementSafeBlockCount(int replication);

  public void decrementSafeBlockCount(BlockInfo b);
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManagerTestUtil.java
    final BlockManager bm = namesystem.getBlockManager();
    namesystem.readLock();
    try {
      final BlockInfo storedBlock = bm.getStoredBlock(b);
      return new int[]{getNumberOfRacks(bm, b),
          bm.countNodes(storedBlock).liveReplicas(),
          bm.neededReplications.contains(storedBlock) ? 1 : 0};
    } finally {
      namesystem.readUnlock();
    }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockManager.java
    return blockInfo;
  }

  private DatanodeStorageInfo[] scheduleSingleReplication(BlockInfo block) {
    List<BlockInfo> list_p1 = new ArrayList<>();
    list_p1.add(block);

    List<List<BlockInfo>> list_all = new ArrayList<>();
    list_all.add(new ArrayList<BlockInfo>()); // for priority 0
    list_all.add(list_p1); // for priority 1

    assertEquals("Block not initially pending replication", 0,

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestNodeCount.java

  NumberReplicas countNodes(Block block, FSNamesystem namesystem) {
    BlockManager blockManager = namesystem.getBlockManager();
    namesystem.readLock();
    try {
      lastBlock = block;
      lastNum = blockManager.countNodes(blockManager.getStoredBlock(block));
      return lastNum;
    }
    finally {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestOverReplicatedBlocks.java

          assertEquals(1, bm.countNodes(
              bm.getStoredBlock(block.getLocalBlock())).liveReplicas());
        }
      } finally {
        namesystem.writeUnlock();
      out.close();
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, p);
      assertEquals("Expected only one live replica for the block", 1, bm
          .countNodes(bm.getStoredBlock(block.getLocalBlock())).liveReplicas());
    } finally {
      cluster.shutdown();
    }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestPendingReplication.java
  private static final int DFS_REPLICATION_INTERVAL = 1;
  private static final int DATANODE_COUNT = 5;

  private BlockInfo genBlockInfo(long id, long length, long gs) {
    return new BlockInfoContiguous(new Block(id, length, gs),
        (short) DATANODE_COUNT);
  }

  @Test
  public void testPendingReplication() {
    PendingReplicationBlocks pendingReplications;
    DatanodeStorageInfo[] storages = DFSTestUtil.createDatanodeStorageInfos(10);
    for (int i = 0; i < storages.length; i++) {
      BlockInfo block = genBlockInfo(i, i, 0);
      DatanodeStorageInfo[] targets = new DatanodeStorageInfo[i];
      System.arraycopy(storages, 0, targets, 0, i);
      pendingReplications.increment(block,
    BlockInfo blk = genBlockInfo(8, 8, 0);
    pendingReplications.decrement(blk, storages[7].getDatanodeDescriptor()); // removes one replica
    assertEquals("pendingReplications.getNumReplicas ",
                 7, pendingReplications.getNumReplicas(blk));
    for (int i = 0; i < 10; i++) {
      BlockInfo block = genBlockInfo(i, i, 0);
      int numReplicas = pendingReplications.getNumReplicas(block);
      assertTrue(numReplicas == i);
    }
    }

    for (int i = 10; i < 15; i++) {
      BlockInfo block = genBlockInfo(i, i, 0);
      pendingReplications.increment(block,
          DatanodeStorageInfo.toDatanodeDescriptors(
              DFSTestUtil.createDatanodeStorageInfos(i)));
      block = new Block(1, 1, 0);
      blockInfo = new BlockInfoContiguous(block, (short) 3);

      pendingReplications.increment(blockInfo,
          DatanodeStorageInfo.toDatanodeDescriptors(
              DFSTestUtil.createDatanodeStorageInfos(1)));
      BlockCollection bc = Mockito.mock(BlockCollection.class);
      block = new Block(2, 2, 0);
      blockInfo = new BlockInfoContiguous(block, (short) 3);
      pendingReplications.increment(blockInfo,
          DatanodeStorageInfo.toDatanodeDescriptors(
              DFSTestUtil.createDatanodeStorageInfos(1)));


      assertEquals(1, blkManager.pendingReplications.size());
      INodeFile fileNode = fsn.getFSDirectory().getINode4Write(file).asFile();
      BlockInfo[] blocks = fileNode.getBlocks();
      assertEquals(DATANODE_COUNT - 1,
          blkManager.pendingReplications.getNumReplicas(blocks[0]));

      BlockManagerTestUtil.computeAllPendingWork(bm);
      BlockManagerTestUtil.updateState(bm);
      assertEquals(bm.getPendingReplicationBlocksCount(), 1L);
      BlockInfo storedBlock = bm.getStoredBlock(block.getBlock().getLocalBlock());
      assertEquals(bm.pendingReplications.getNumReplicas(storedBlock), 2);

      fs.delete(filePath, true);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestRBWBlockInvalidation.java
  
  private static NumberReplicas countReplicas(final FSNamesystem namesystem,
      ExtendedBlock block) {
    final BlockManager blockManager = namesystem.getBlockManager();
    return blockManager.countNodes(blockManager.getStoredBlock(
        block.getLocalBlock()));
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestReplicationPolicy.java
    assertTrue(isOnSameRack(targets[0], dataNodes[2]));
  }

  private BlockInfo genBlockInfo(long id) {
    return new BlockInfoContiguous(new Block(id), (short) 3);
  }

          .getNamesystem().getBlockManager().neededReplications;
      for (int i = 0; i < 100; i++) {
        neededReplications.add(genBlockInfo(ThreadLocalRandom.current().
            nextLong()), 2, 0, 3);
      }
      Thread.sleep(DFS_NAMENODE_REPLICATION_INTERVAL);
      
      neededReplications.add(genBlockInfo(ThreadLocalRandom.current().
          nextLong()), 1, 0, 3);

      Thread.sleep(DFS_NAMENODE_REPLICATION_INTERVAL);

    for (int i = 0; i < 5; i++) {
      underReplicatedBlocks.add(genBlockInfo(ThreadLocalRandom.current().
          nextLong()), 1, 0, 3);

      underReplicatedBlocks.add(genBlockInfo(ThreadLocalRandom.current().
          nextLong()), 2, 0, 7);

      underReplicatedBlocks.add(genBlockInfo(ThreadLocalRandom.current().
          nextLong()), 6, 0, 6);

      underReplicatedBlocks.add(genBlockInfo(ThreadLocalRandom.current().
          nextLong()), 5, 0, 6);

      underReplicatedBlocks.add(genBlockInfo(ThreadLocalRandom.current().
          nextLong()), 0, 0, 3);
    }

    List<List<BlockInfo>> chosenBlocks =
        underReplicatedBlocks.chooseUnderReplicatedBlocks(6);
    assertTheChosenBlocks(chosenBlocks, 5, 1, 0, 0, 0);

    assertTheChosenBlocks(chosenBlocks, 0, 4, 5, 1, 0);

    underReplicatedBlocks.add(genBlockInfo(ThreadLocalRandom.current().
        nextLong()), 1, 0, 3);

  
  private void assertTheChosenBlocks(
      List<List<BlockInfo>> chosenBlocks, int firstPrioritySize,
      int secondPrioritySize, int thirdPrioritySize, int fourthPrioritySize,
      int fifthPrioritySize) {
    assertEquals(
  public void testUpdateDoesNotCauseSkippedReplication() {
    UnderReplicatedBlocks underReplicatedBlocks = new UnderReplicatedBlocks();

    BlockInfo block1 = genBlockInfo(ThreadLocalRandom.current().nextLong());
    BlockInfo block2 = genBlockInfo(ThreadLocalRandom.current().nextLong());
    BlockInfo block3 = genBlockInfo(ThreadLocalRandom.current().nextLong());

    final int block1CurReplicas = 2;
    underReplicatedBlocks.add(block3, 2, 0, 6);

    List<List<BlockInfo>> chosenBlocks;

        new BlockManager(mockNS, new HdfsConfiguration());
    UnderReplicatedBlocks underReplicatedBlocks = bm.neededReplications;

    BlockInfo block1 = genBlockInfo(ThreadLocalRandom.current().nextLong());
    BlockInfo block2 = genBlockInfo(ThreadLocalRandom.current().nextLong());

    underReplicatedBlocks.add(block1, 0, 1, 1);
    underReplicatedBlocks.add(block2, 0, 1, 1);

    List<List<BlockInfo>> chosenBlocks;

        new BlockManager(mockNS, new HdfsConfiguration());
    UnderReplicatedBlocks underReplicatedBlocks = bm.neededReplications;

    BlockInfo block1 = genBlockInfo(ThreadLocalRandom.current().nextLong());
    BlockInfo block2 = genBlockInfo(ThreadLocalRandom.current().nextLong());

    underReplicatedBlocks.add(block1, 0, 1, 1);
    underReplicatedBlocks.add(block2, 0, 1, 1);

    List<List<BlockInfo>> chosenBlocks;

        new BlockManager(mockNS, new HdfsConfiguration());
    UnderReplicatedBlocks underReplicatedBlocks = bm.neededReplications;

    BlockInfo block1 = genBlockInfo(ThreadLocalRandom.current().nextLong());
    BlockInfo block2 = genBlockInfo(ThreadLocalRandom.current().nextLong());

    underReplicatedBlocks.add(block1, 0, 1, 1);
    underReplicatedBlocks.add(block2, 0, 1, 1);

    List<List<BlockInfo>> chosenBlocks;


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestUnderReplicatedBlockQueues.java

public class TestUnderReplicatedBlockQueues {

  private BlockInfo genBlockInfo(long id) {
    return new BlockInfoContiguous(new Block(id), (short) 3);
  }

  @Test
  public void testBlockPriorities() throws Throwable {
    UnderReplicatedBlocks queues = new UnderReplicatedBlocks();
    BlockInfo block1 = genBlockInfo(1);
    BlockInfo block2 = genBlockInfo(2);
    BlockInfo block_very_under_replicated = genBlockInfo(3);
    BlockInfo block_corrupt = genBlockInfo(4);
    BlockInfo block_corrupt_repl_one = genBlockInfo(5);

    assertAdded(queues, block1, 1, 0, 3);
  }

  private void assertAdded(UnderReplicatedBlocks queues,
                           BlockInfo block,
                           int curReplicas,
                           int decomissionedReplicas,
                           int expectedReplicas) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestReadOnlySharedStorage.java
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
  private DatanodeInfo readOnlyDataNode;
  
  private Block block;
  private BlockInfo storedBlock;

  private ExtendedBlock extendedBlock;

    LocatedBlock locatedBlock = getLocatedBlock();
    extendedBlock = locatedBlock.getBlock();
    block = extendedBlock.getLocalBlock();
    storedBlock = blockManager.getStoredBlock(block);

    assertThat(locatedBlock.getLocations().length, is(1));
    normalDataNode = locatedBlock.getLocations()[0];
  }
  
  private void validateNumberReplicas(int expectedReplicas) throws IOException {
    NumberReplicas numberReplicas = blockManager.countNodes(storedBlock);
    assertThat(numberReplicas.liveReplicas(), is(expectedReplicas));
    assertThat(numberReplicas.excessReplicas(), is(0));
    assertThat(numberReplicas.corruptReplicas(), is(0));
        cluster.getNameNode(), normalDataNode.getXferAddr());
    
    NumberReplicas numberReplicas = blockManager.countNodes(storedBlock);
    assertThat(numberReplicas.liveReplicas(), is(0));
    
    waitForLocations(1);
    
    NumberReplicas numberReplicas = blockManager.countNodes(storedBlock);
    assertThat(numberReplicas.corruptReplicas(), is(0));
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestProcessCorruptBlocks.java
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.junit.Test;
  }

  private static NumberReplicas countReplicas(final FSNamesystem namesystem, ExtendedBlock block) {
    final BlockManager blockManager = namesystem.getBlockManager();
    return blockManager.countNodes(blockManager.getStoredBlock(
        block.getLocalBlock()));
  }

  private void corruptBlock(MiniDFSCluster cluster, FileSystem fs, final Path fileName,

