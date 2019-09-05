hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
      namesystem.writeLock();
      try {
        for (int i = 0; i < timedOutItems.length; i++) {
          BlockInfoContiguous bi = blocksMap.getStoredBlock(timedOutItems[i]);
          if (bi == null) {
            continue;
          }
          NumberReplicas num = countNodes(timedOutItems[i]);
          if (isNeededReplication(bi, getReplication(bi), num.liveReplicas())) {
            neededReplications.add(bi, num.liveReplicas(),
                num.decommissionedAndDecommissioning(), getReplication(bi));
          }
        }
      } finally {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestPendingReplication.java
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.junit.Test;
import org.mockito.Mockito;

    assertEquals("Size of pendingReplications ", 0, pendingReplications.size());
    Block[] timedOut = pendingReplications.getTimedOutBlocks();
    assertTrue(timedOut != null && timedOut.length == 15);
    for (int i = 0; i < timedOut.length; i++) {
    pendingReplications.stop();
  }

  @Test
  public void testProcessPendingReplications() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setLong(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY, TIMEOUT);
    MiniDFSCluster cluster = null;
    Block block;
    BlockInfoContiguous blockInfo;
    try {
      cluster =
          new MiniDFSCluster.Builder(conf).numDataNodes(DATANODE_COUNT).build();
      cluster.waitActive();

      FSNamesystem fsn = cluster.getNamesystem();
      BlockManager blkManager = fsn.getBlockManager();

      PendingReplicationBlocks pendingReplications =
          blkManager.pendingReplications;
      UnderReplicatedBlocks neededReplications = blkManager.neededReplications;
      BlocksMap blocksMap = blkManager.blocksMap;


      block = new Block(1, 1, 0);
      blockInfo = new BlockInfoContiguous(block, (short) 3);

      pendingReplications.increment(block,
          DatanodeStorageInfo.toDatanodeDescriptors(
              DFSTestUtil.createDatanodeStorageInfos(1)));
      BlockCollection bc = Mockito.mock(BlockCollection.class);
      Mockito.doReturn((short) 3).when(bc).getPreferredBlockReplication();
      blockInfo.setGenerationStamp(1);
      blocksMap.addBlockCollection(blockInfo, bc);

      assertEquals("Size of pendingReplications ", 1,
          pendingReplications.size());

      block = new Block(2, 2, 0);
      pendingReplications.increment(block,
          DatanodeStorageInfo.toDatanodeDescriptors(
              DFSTestUtil.createDatanodeStorageInfos(1)));

      assertEquals("Size of pendingReplications ", 2,
          pendingReplications.size());

      while (pendingReplications.size() > 0) {
        try {
          Thread.sleep(100);
        } catch (Exception e) {
        }
      }

      while (neededReplications.size() == 0) {
        try {
          Thread.sleep(100);
        } catch (Exception e) {
        }
      }

      for (Block b: neededReplications) {
        assertEquals("Generation stamp is 1 ", 1,
            b.getGenerationStamp());
      }

      assertEquals("size of neededReplications is 1 ", 1,
          neededReplications.size());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  

