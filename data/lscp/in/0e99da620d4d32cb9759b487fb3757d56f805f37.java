hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
  LocatedBlock getAdditionalBlock(String src, long fileId, String clientName,
      ExtendedBlock previous, Set<Node> excludedNodes, 
      List<String> favoredNodes) throws IOException {
    LocatedBlock[] onRetryBlock = new LocatedBlock[1];
    DatanodeStorageInfo targets[] = getNewBlockTargets(src, fileId,
        clientName, previous, excludedNodes, favoredNodes, onRetryBlock);
    if (targets == null) {
      assert onRetryBlock[0] != null : "Retry block is null";
      return onRetryBlock[0];
    }
    LocatedBlock newBlock = storeAllocatedBlock(
        src, fileId, clientName, previous, targets);
    return newBlock;
  }

  DatanodeStorageInfo[] getNewBlockTargets(String src, long fileId,
      String clientName, ExtendedBlock previous, Set<Node> excludedNodes,
      List<String> favoredNodes, LocatedBlock[] onRetryBlock) throws IOException {
    final long blockSize;
    final int replication;
    final byte storagePolicyID;
          + src + " inodeId " +  fileId  + " for " + clientName);
    }

    checkOperation(OperationCategory.READ);
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    FSPermissionChecker pc = getPermissionChecker();
    try {
      checkOperation(OperationCategory.READ);
      src = dir.resolvePath(pc, src, pathComponents);
      FileState fileState = analyzeFileState(
          src, fileId, clientName, previous, onRetryBlock);
      final INodeFile pendingFile = fileState.inode;
      src = fileState.path;

      if (onRetryBlock[0] != null && onRetryBlock[0].getLocations().length > 0) {
        return null;
      }
      if (pendingFile.getBlocks().length >= maxBlocksPerFile) {
        throw new IOException("File has reached the limit on maximum number of"
    }

    return getBlockManager().chooseTarget4NewBlock( 
        src, replication, clientNode, excludedNodes, blockSize, favoredNodes,
        storagePolicyID);
  }

  LocatedBlock storeAllocatedBlock(String src, long fileId, String clientName,
      ExtendedBlock previous, DatanodeStorageInfo[] targets) throws IOException {
    Block newBlock = null;
    long offset;
    checkOperation(OperationCategory.WRITE);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestAddBlockRetry.java


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.EnumSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.EnumSetWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

  private Configuration conf;
  private MiniDFSCluster cluster;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    final String src = "/testRetryAddBlockWhileInChooseTarget";

    final FSNamesystem ns = cluster.getNamesystem();
    final NamenodeProtocols nn = cluster.getNameNodeRpc();

    nn.create(src, FsPermission.getFileDefault(),
        "clientName",

    LOG.info("Starting first addBlock for " + src);
    LocatedBlock[] onRetryBlock = new LocatedBlock[1];
    DatanodeStorageInfo targets[] = ns.getNewBlockTargets(
        src, INodeId.GRANDFATHER_INODE_ID, "clientName",
        null, null, null, onRetryBlock);
    assertNotNull("Targets must be generated", targets);

    LOG.info("Starting second addBlock for " + src);
    nn.addBlock(src, "clientName", null, null,
        INodeId.GRANDFATHER_INODE_ID, null);
    assertTrue("Penultimate block must be complete",
        checkFileProgress(src, false));
    LocatedBlocks lbs = nn.getBlockLocations(src, 0, Long.MAX_VALUE);
    assertEquals("Must be one block", 1, lbs.getLocatedBlocks().size());
    LocatedBlock lb2 = lbs.get(0);
    assertEquals("Wrong replication", REPLICATION, lb2.getLocations().length);

    LocatedBlock newBlock = ns.storeAllocatedBlock(
        src, INodeId.GRANDFATHER_INODE_ID, "clientName", null, targets);
    assertEquals("Blocks are not equal", lb2.getBlock(), newBlock.getBlock());

    lbs = nn.getBlockLocations(src, 0, Long.MAX_VALUE);
    assertEquals("Must be one block", 1, lbs.getLocatedBlocks().size());
    LocatedBlock lb1 = lbs.get(0);
    assertEquals("Wrong replication", REPLICATION, lb1.getLocations().length);
    assertEquals("Blocks are not equal", lb1.getBlock(), lb2.getBlock());
  }

