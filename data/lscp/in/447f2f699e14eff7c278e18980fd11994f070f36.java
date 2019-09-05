hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoContiguous.java
    this.bc = bc;
  }

  public boolean isDeleted() {
    return (bc == null);
  }

  public DatanodeDescriptor getDatanode(int index) {
    DatanodeStorageInfo storage = getStorageInfo(index);
    return storage == null ? null : storage.getDatanodeDescriptor();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
      DatanodeStorageInfo storageInfo,
      DatanodeDescriptor node) throws IOException {

    if (b.corrupted.isDeleted()) {
      blockLog.info("BLOCK markBlockAsCorrupt: {} cannot be marked as" +
          " corrupt as it does not belong to any file", b);
      addToInvalidates(b.corrupted, node);
      return;
    } 
    short expectedReplicas =
        b.corrupted.getBlockCollection().getBlockReplication();

    if (storageInfo != null) {
        b.reasonCode);

    NumberReplicas numberOfReplicas = countNodes(b.stored);
    boolean hasEnoughLiveReplicas = numberOfReplicas.liveReplicas() >=
        expectedReplicas;
    boolean minReplicationSatisfied =
        numberOfReplicas.liveReplicas() >= minReplication;
    boolean hasMoreCorruptReplicas = minReplicationSatisfied &&
        (numberOfReplicas.liveReplicas() + numberOfReplicas.corruptReplicas()) >
        expectedReplicas;
    boolean corruptedDuringWrite = minReplicationSatisfied &&
        (b.stored.getGenerationStamp() > b.corrupted.getGenerationStamp());
    } else {
      storedBlock = block;
    }
    if (storedBlock == null || storedBlock.isDeleted()) {
      blockLog.info("BLOCK* addStoredBlock: {} on {} size {} but it does not" +
          " belong to any file", block, node, block.getNumBytes());
  private MisReplicationResult processMisReplicatedBlock(BlockInfoContiguous block) {
    if (block.isDeleted()) {
      addToInvalidates(block);
      return MisReplicationResult.INVALID;
      return MisReplicationResult.UNDER_CONSTRUCTION;
    }
    short expectedReplication =
        block.getBlockCollection().getBlockReplication();
    NumberReplicas num = countNodes(block);
    int numCurrentReplica = num.liveReplicas();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlocksMap.java
    boolean removed = node.removeBlock(info);

    if (info.getDatanode(0) == null     // no datanodes left
              && info.isDeleted()) {  // does not belong to a file
      blocks.remove(b);  // remove block from the map
    }
    return removed;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
      if (storedBlock.isDeleted()) {
        throw new IOException("The blockCollection of " + storedBlock
            + " is null, likely because the file owning this block was"
            + " deleted and the block removal is delayed");
      }
      INodeFile iFile = ((INode)storedBlock.getBlockCollection()).asFile();
      if (isFileDeleted(iFile)) {
        throw new FileNotFoundException("File not found: "
            + iFile.getFullPathName() + ", likely due to delayed block"

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockInfo.java
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.hdfs.TestBlockInfo");

  @Test
  public void testIsDeleted() {
    BlockInfoContiguous blockInfo = new BlockInfoContiguous((short) 3);
    BlockCollection bc = Mockito.mock(BlockCollection.class);
    blockInfo.setBlockCollection(bc);
    Assert.assertFalse(blockInfo.isDeleted());
    blockInfo.setBlockCollection(null);
    Assert.assertTrue(blockInfo.isDeleted());
  }

  @Test
  public void testAddStorage() throws Exception {

