hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/balancer/Dispatcher.java
  private boolean isGoodBlockCandidate(StorageGroup source, StorageGroup target,
      StorageType targetStorageType, DBlock block) {
    if (source.equals(target)) {
      return false;
    }
    if (target.storageType != targetStorageType) {
      return false;
    }
    if (movedBlocks.contains(block.getBlock())) {
      return false;
    }
    final DatanodeInfo targetDatanode = target.getDatanodeInfo();
    if (source.getDatanodeInfo().equals(targetDatanode)) {
      return true;
    }

    for (StorageGroup blockLocation : block.getLocations()) {
      if (blockLocation.getDatanodeInfo().equals(targetDatanode)) {
        return false;
      }
    }

    if (cluster.isNodeGroupAware()
        && isOnSameNodeGroupWithReplicas(source, target, block)) {
      return false;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/balancer/TestBalancer.java
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
    }
  }

  @Test(timeout=100000)
  public void testTwoReplicaShouldNotInSameDN() throws Exception {
    final Configuration conf = new HdfsConfiguration();

    int blockSize = 5 * 1024 * 1024 ;
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1L);

    int numOfDatanodes =2;
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .racks(new String[]{"/default/rack0", "/default/rack0"})
        .storagesPerDatanode(2)
        .storageTypes(new StorageType[][]{
            {StorageType.SSD, StorageType.DISK},
            {StorageType.SSD, StorageType.DISK}})
        .storageCapacities(new long[][]{
            {100 * blockSize, 20 * blockSize},
            {20 * blockSize, 100 * blockSize}})
        .build();

    try {
      cluster.waitActive();

      DistributedFileSystem fs = cluster.getFileSystem();
      Path barDir = new Path("/bar");
      fs.mkdir(barDir,new FsPermission((short)777));
      fs.setStoragePolicy(barDir, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

      long fileLen  = 30 * blockSize;
      Path fooFile = new Path(barDir, "foo");
      createFile(cluster, fooFile, fileLen, (short) numOfDatanodes, 0);
      cluster.triggerHeartbeats();

      Balancer.Parameters p = Balancer.Parameters.DEFAULT;
      Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
      final int r = Balancer.run(namenodes, p, conf);

      assertEquals(ExitStatus.NO_MOVE_PROGRESS.getExitCode(), r);

    } finally {
      cluster.shutdown();
    }
  }

