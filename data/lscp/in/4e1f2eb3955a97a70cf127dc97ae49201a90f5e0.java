hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
        return !node.hasStaleStorages();
      }

      if (storageInfo.getBlockReportCount() == 0) {
        processFirstBlockReport(storageInfo, newReport);
      final BlockListAsLongs report) throws IOException {
    if (report == null) return;
    assert (namesystem.hasWriteLock());
    assert (storageInfo.getBlockReportCount() == 0);

    for (BlockReportReplica iblk : report) {
      ReplicaState reportedState = iblk.getState();
    }

    AddBlockResult result = storageInfo.addBlock(storedBlock);

    int numCurrentReplica = countLiveNodes(storedBlock);
    if (storedBlock.getBlockUCState() == BlockUCState.COMMITTED
        && numCurrentReplica >= minReplication) {
      completeBlock(storedBlock.getBlockCollection(), storedBlock, false);
    } else if (storedBlock.isComplete() && result == AddBlockResult.ADDED) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockManager.java
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetworkTopology;
import org.junit.Assert;
    assertEquals(1, ds.getBlockReportCount());
  }

  @Test
  public void testSafeModeIBRBeforeFirstFullBR() throws Exception {
    doReturn(true).when(fsn).isInStartupSafeMode();

    DatanodeDescriptor node = nodes.get(0);
    DatanodeStorageInfo ds = node.getStorageInfos()[0];
    node.isAlive = true;
    DatanodeRegistration nodeReg =  new DatanodeRegistration(node, null, null, "");

    bm.getDatanodeManager().registerDatanode(nodeReg);
    bm.getDatanodeManager().addDatanode(node);
    assertEquals(node, bm.getDatanodeManager().getDatanode(node));
    assertEquals(0, ds.getBlockReportCount());
    List<ReceivedDeletedBlockInfo> rdbiList = new ArrayList<>();
    BlockListAsLongs.Builder builder = BlockListAsLongs.builder();

    long receivedBlockId = 42;  // arbitrary
    BlockInfoContiguous receivedBlock = addBlockToBM(receivedBlockId);
    rdbiList.add(new ReceivedDeletedBlockInfo(new Block(receivedBlock),
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, null));
    builder.add(new FinalizedReplica(receivedBlock, null, null));

    long receivingBlockId = 43;
    BlockInfoContiguous receivingBlock = addUcBlockToBM(receivingBlockId);
    rdbiList.add(new ReceivedDeletedBlockInfo(new Block(receivingBlock),
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVING_BLOCK, null));
    builder.add(new ReplicaBeingWritten(receivingBlock, null, null, null));

    long receivingReceivedBlockId = 44;
    BlockInfoContiguous receivingReceivedBlock = addBlockToBM(receivingReceivedBlockId);
    rdbiList.add(new ReceivedDeletedBlockInfo(new Block(receivingReceivedBlock),
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVING_BLOCK, null));
    rdbiList.add(new ReceivedDeletedBlockInfo(new Block(receivingReceivedBlock),
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, null));
    builder.add(new FinalizedReplica(receivingReceivedBlock, null, null));

    long ReceivedDeletedBlockId = 45;
    rdbiList.add(new ReceivedDeletedBlockInfo(
        new Block(ReceivedDeletedBlockId),
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, null));
    rdbiList.add(new ReceivedDeletedBlockInfo(
        new Block(ReceivedDeletedBlockId),
        ReceivedDeletedBlockInfo.BlockStatus.DELETED_BLOCK, null));

    long existedBlockId = 46;
    BlockInfoContiguous existedBlock = addBlockToBM(existedBlockId);
    builder.add(new FinalizedReplica(existedBlock, null, null));

    StorageReceivedDeletedBlocks srdb =
        new StorageReceivedDeletedBlocks(new DatanodeStorage(ds.getStorageID()),
            rdbiList.toArray(new ReceivedDeletedBlockInfo[rdbiList.size()]));
    bm.processIncrementalBlockReport(node, srdb);
    assertEquals(0, ds.getBlockReportCount());
    bm.processReport(node, new DatanodeStorage(ds.getStorageID()),
        builder.build(), null, false);
    assertEquals(1, ds.getBlockReportCount());

    assertTrue(bm.getStoredBlock(new Block(receivedBlockId)).findStorageInfo
        (ds) >= 0);
    assertTrue(((BlockInfoContiguousUnderConstruction) bm.
        getStoredBlock(new Block(receivingBlockId))).getNumExpectedLocations() > 0);
    assertTrue(bm.getStoredBlock(new Block(receivingReceivedBlockId))
        .findStorageInfo(ds) >= 0);
    assertNull(bm.getStoredBlock(new Block(ReceivedDeletedBlockId)));
    assertTrue(bm.getStoredBlock(new Block(existedBlock)).findStorageInfo
        (ds) >= 0);
  }

  private BlockInfoContiguous addBlockToBM(long blkId) {
    Block block = new Block(blkId);
    BlockInfoContiguous blockInfo =
        new BlockInfoContiguous(block, (short) 3);
    BlockCollection bc = Mockito.mock(BlockCollection.class);
    Mockito.doReturn((short) 3).when(bc).getBlockReplication();
    bm.blocksMap.addBlockCollection(blockInfo, bc);
    return blockInfo;
  }

  private BlockInfoContiguous addUcBlockToBM(long blkId) {
    Block block = new Block(blkId);
    BlockInfoContiguousUnderConstruction blockInfo =
        new BlockInfoContiguousUnderConstruction(block, (short) 3);
    BlockCollection bc = Mockito.mock(BlockCollection.class);
    Mockito.doReturn((short) 3).when(bc).getBlockReplication();
    bm.blocksMap.addBlockCollection(blockInfo, bc);
    return blockInfo;
  }
  

