hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestReplicationPolicy.java
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager.StatefulBlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
    Namesystem mockNS = mock(Namesystem.class);
    when(mockNS.isPopulatingReplQueues()).thenReturn(true);
    when(mockNS.hasWriteLock()).thenReturn(true);
    BlockManager bm = new BlockManager(mockNS, new HdfsConfiguration());
    UnderReplicatedBlocks underReplicatedBlocks = bm.neededReplications;

    BlockInfo block1 = genBlockInfo(ThreadLocalRandom.current().nextLong());
    BlockInfoUnderConstruction info =
        new BlockInfoUnderConstructionContiguous(block1, (short) 1);
    BlockCollection bc = mock(BlockCollection.class);
    when(bc.getPreferredBlockReplication()).thenReturn((short)1);
    bm.addBlockCollection(info, bc);

    bm.addStoredBlockUnderConstruction(new StatefulBlockInfo(info, info,
              ReplicaState.FINALIZED), TestReplicationPolicy.storages[0]);

          throws IOException {
    Namesystem mockNS = mock(Namesystem.class);
    when(mockNS.isPopulatingReplQueues()).thenReturn(true);
    BlockManager bm = new BlockManager(mockNS, new HdfsConfiguration());
    UnderReplicatedBlocks underReplicatedBlocks = bm.neededReplications;

    BlockInfo block1 = genBlockInfo(ThreadLocalRandom.current().nextLong());
    when(mbc.getLastBlock()).thenReturn(info);
    when(mbc.getPreferredBlockSize()).thenReturn(block1.getNumBytes() + 1);
    when(mbc.getPreferredBlockReplication()).thenReturn((short)1);
    when(mbc.isUnderConstruction()).thenReturn(true);
    ContentSummary cs = mock(ContentSummary.class);
    when(cs.getLength()).thenReturn((long)1);
    when(mbc.computeContentSummary(bm.getStoragePolicySuite())).thenReturn(cs);
    info.setBlockCollection(mbc);
    bm.addBlockCollection(info, mbc);

    DatanodeStorageInfo[] storageAry = {new DatanodeStorageInfo(
        dataNodes[0], new DatanodeStorage("s1"))};
    final BlockInfoUnderConstruction ucBlock =
        info.convertToBlockUnderConstruction(BlockUCState.UNDER_CONSTRUCTION,
            storageAry);
    DatanodeStorageInfo storage = mock(DatanodeStorageInfo.class);
    DatanodeDescriptor dn = mock(DatanodeDescriptor.class);
    when(dn.isDecommissioned()).thenReturn(true);
    when(mbc.setLastBlock((BlockInfo) any(), (DatanodeStorageInfo[]) any()))
    .thenReturn(ucBlock);

    bm.convertLastBlockToUnderConstruction(mbc, 0L);

      throws IOException {
    Namesystem mockNS = mock(Namesystem.class);
    when(mockNS.isPopulatingReplQueues()).thenReturn(true);
    BlockManager bm = new BlockManager(mockNS, new HdfsConfiguration());
    UnderReplicatedBlocks underReplicatedBlocks = bm.neededReplications;

    BlockInfo block1 = genBlockInfo(ThreadLocalRandom.current().nextLong());

