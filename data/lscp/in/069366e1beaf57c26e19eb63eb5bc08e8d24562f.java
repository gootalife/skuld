hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
  boolean isNodeHealthyForDecommission(DatanodeDescriptor node) {
    if (!node.checkBlockReportReceived()) {
      LOG.info("Node {} hasn't sent its first block report.", node);
      return false;
    }

    if (node.isAlive) {
      return true;
    }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDecommission.java
    int numNamenodes = 1;
    int numDatanodes = 1;
    int replicas = 1;
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
        DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY, 5);

    startCluster(numNamenodes, numDatanodes, conf);
    Path file1 = new Path("testDecommissionWithNamenodeRestart.dat");
    FileSystem fileSys = cluster.getFileSystem();
    writeFile(fileSys, file1, replicas);
        
    assertEquals("All datanodes must be alive", numDatanodes, 
        client.datanodeReport(DatanodeReportType.LIVE).length);
    assertTrue("Checked if block was replicated after decommission.",
        checkFile(fileSys, file1, replicas, datanodeInfo.getXferAddr(),
        numDatanodes) == null);

    cleanupFile(fileSys, file1);
    cluster.shutdown();

