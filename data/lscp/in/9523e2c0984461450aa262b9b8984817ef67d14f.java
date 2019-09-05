hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/balancer/Balancer.java
      + "\n\t[-include [-f <hosts-file> | <comma-separated list of hosts>]]"
      + "\tIncludes only the specified datanodes."
      + "\n\t[-idleiterations <idleiterations>]"
      + "\tNumber of consecutive idle iterations (-1 for Infinite) before "
      + "exit."
      + "\n\t[-runDuringUpgrade]"
      + "\tWhether to run the balancer during an ongoing HDFS upgrade."
      + "This is usually not desired since it will not affect used space "
      + "on over-utilized machines.";

  private final Dispatcher dispatcher;
  private final NameNodeConnector nnc;
  private final BalancingPolicy policy;
  private final boolean runDuringUpgrade;
  private final double threshold;

        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT);

    this.nnc = theblockpool;
    this.dispatcher = new Dispatcher(theblockpool, p.nodesToBeIncluded,
        p.nodesToBeExcluded, movedWinWidth, moverThreads, dispatcherThreads,
        maxConcurrentMovesPerNode, conf);
    this.threshold = p.threshold;
    this.policy = p.policy;
    this.runDuringUpgrade = p.runDuringUpgrade;
  }
  
  private static long getCapacity(DatanodeStorageReport report, StorageType t) {
          if (thresholdDiff <= 0) { // within threshold
            aboveAvgUtilized.add(s);
          } else {
            overLoadedBytes += percentage2bytes(thresholdDiff, capacity);
            overUtilized.add(s);
          }
          g = s;
          if (thresholdDiff <= 0) { // within threshold
            belowAvgUtilized.add(g);
          } else {
            underLoadedBytes += percentage2bytes(thresholdDiff, capacity);
            underUtilized.add(g);
          }
        }
  private static long computeMaxSize2Move(final long capacity, final long remaining,
      final double utilizationDiff, final double threshold) {
    final double diff = Math.min(threshold, Math.abs(utilizationDiff));
    long maxSizeToMove = percentage2bytes(diff, capacity);
    if (utilizationDiff < 0) {
      maxSizeToMove = Math.min(remaining, maxSizeToMove);
    }
    return Math.min(MAX_SIZE_TO_MOVE, maxSizeToMove);
  }

  private static long percentage2bytes(double percentage, long capacity) {
    Preconditions.checkArgument(percentage >= 0, "percentage = %s < 0",
        percentage);
    return (long)(percentage * capacity / 100.0);
  }

            + " to make the cluster balanced." );
      }

      if (!runDuringUpgrade && nnc.isUpgrading()) {
        return newResult(ExitStatus.UNFINALIZED_UPGRADE, bytesLeftToMove, -1);
      }

    static final Parameters DEFAULT = new Parameters(
        BalancingPolicy.Node.INSTANCE, 10.0,
        NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS,
        Collections.<String> emptySet(), Collections.<String> emptySet(),
        false);

    final BalancingPolicy policy;
    final double threshold;
    Set<String> nodesToBeExcluded;
    Set<String> nodesToBeIncluded;
    final boolean runDuringUpgrade;

    Parameters(BalancingPolicy policy, double threshold, int maxIdleIteration,
        Set<String> nodesToBeExcluded, Set<String> nodesToBeIncluded,
        boolean runDuringUpgrade) {
      this.policy = policy;
      this.threshold = threshold;
      this.maxIdleIteration = maxIdleIteration;
      this.nodesToBeExcluded = nodesToBeExcluded;
      this.nodesToBeIncluded = nodesToBeIncluded;
      this.runDuringUpgrade = runDuringUpgrade;
    }

    @Override
    public String toString() {
      return String.format("%s.%s [%s,"
              + " threshold = %s,"
              + " max idle iteration = %s, "
              + "number of nodes to be excluded = %s,"
              + " number of nodes to be included = %s,"
              + " run during upgrade = %s]",
          Balancer.class.getSimpleName(), getClass().getSimpleName(),
          policy, threshold, maxIdleIteration,
          nodesToBeExcluded.size(), nodesToBeIncluded.size(),
          runDuringUpgrade);
    }
  }

      int maxIdleIteration = Parameters.DEFAULT.maxIdleIteration;
      Set<String> nodesTobeExcluded = Parameters.DEFAULT.nodesToBeExcluded;
      Set<String> nodesTobeIncluded = Parameters.DEFAULT.nodesToBeIncluded;
      boolean runDuringUpgrade = Parameters.DEFAULT.runDuringUpgrade;

      if (args != null) {
        try {
              }
            } else if ("-idleiterations".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                  "idleiterations value is missing: args = " + Arrays
                      .toString(args));
              maxIdleIteration = Integer.parseInt(args[i]);
              LOG.info("Using a idleiterations of " + maxIdleIteration);
            } else if ("-runDuringUpgrade".equalsIgnoreCase(args[i])) {
              runDuringUpgrade = true;
              LOG.info("Will run the balancer even during an ongoing HDFS "
                  + "upgrade. Most users will not want to run the balancer "
                  + "during an upgrade since it will not affect used space "
                  + "on over-utilized machines.");
            } else {
              throw new IllegalArgumentException("args = "
                  + Arrays.toString(args));
        }
      }
      
      return new Parameters(policy, threshold, maxIdleIteration,
          nodesTobeExcluded, nodesTobeIncluded, runDuringUpgrade);
    }

    private static void printUsage(PrintStream out) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/balancer/ExitStatus.java
  NO_MOVE_PROGRESS(-3),
  IO_EXCEPTION(-4),
  ILLEGAL_ARGUMENTS(-5),
  INTERRUPTED(-6),
  UNFINALIZED_UPGRADE(-7);

  private final int code;


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/balancer/NameNodeConnector.java
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
    return namenode.getBlocks(datanode, size);
  }

  public boolean isUpgrading() throws IOException {
    final boolean isUpgrade = !namenode.isUpgradeFinalized();
    RollingUpgradeInfo info = fs.rollingUpgrade(
        HdfsConstants.RollingUpgradeAction.QUERY);
    final boolean isRollingUpgrade = (info != null && !info.isFinalized());
    return (isUpgrade || isRollingUpgrade);
  }

  public DatanodeStorageReport[] getLiveDatanodeStorageReport()
      throws IOException {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/balancer/TestBalancer.java
import org.apache.hadoop.hdfs.server.balancer.Balancer.Cli;
import org.apache.hadoop.hdfs.server.balancer.Balancer.Parameters;
import org.apache.hadoop.hdfs.server.balancer.Balancer.Result;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.LazyPersistTestCase;
            Balancer.Parameters.DEFAULT.policy,
            Balancer.Parameters.DEFAULT.threshold,
            Balancer.Parameters.DEFAULT.maxIdleIteration,
            nodes.getNodesToBeExcluded(), nodes.getNodesToBeIncluded(),
            false);
      }

      int expectedExcludedNodes = 0;
          Balancer.Parameters.DEFAULT.policy,
          Balancer.Parameters.DEFAULT.threshold,
          Balancer.Parameters.DEFAULT.maxIdleIteration,
          datanodes, Balancer.Parameters.DEFAULT.nodesToBeIncluded,
          false);
      final int r = Balancer.run(namenodes, p, conf);
      assertEquals(ExitStatus.SUCCESS.getExitCode(), r);
    } finally {
      Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);

      final Balancer.Parameters p = Parameters.DEFAULT;
      final int r = Balancer.run(namenodes, p, conf);

    }
  }

  @Test(timeout=300000)
  public void testBalancerDuringUpgrade() throws Exception {
    final int SEED = 0xFADED;
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);

    final int BLOCK_SIZE = 1024*1024;
    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(1)
        .storageCapacities(new long[] { BLOCK_SIZE * 10 })
        .storageTypes(new StorageType[] { DEFAULT })
        .storagesPerDatanode(1)
        .build();

    try {
      cluster.waitActive();
      final String METHOD_NAME = GenericTestUtils.getMethodName();
      final Path path1 = new Path("/" + METHOD_NAME + ".01.dat");

      DistributedFileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, path1, BLOCK_SIZE, BLOCK_SIZE * 2, BLOCK_SIZE,
          (short) 1, SEED);

      cluster.startDataNodes(conf, 1, true, null, null);
      cluster.triggerHeartbeats();
      Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);

      final Balancer.Parameters p = Parameters.DEFAULT;

      fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
      fs.rollingUpgrade(HdfsConstants.RollingUpgradeAction.PREPARE);
      fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);

      assertEquals(ExitStatus.UNFINALIZED_UPGRADE.getExitCode(),
          Balancer.run(namenodes, p, conf));

      final Balancer.Parameters runDuringUpgrade =
          new Balancer.Parameters(Parameters.DEFAULT.policy,
              Parameters.DEFAULT.threshold,
              Parameters.DEFAULT.maxIdleIteration,
              Parameters.DEFAULT.nodesToBeExcluded,
              Parameters.DEFAULT.nodesToBeIncluded,
              true);
      assertEquals(ExitStatus.SUCCESS.getExitCode(),
          Balancer.run(namenodes, runDuringUpgrade, conf));

      fs.rollingUpgrade(HdfsConstants.RollingUpgradeAction.FINALIZE);

      assertEquals(ExitStatus.SUCCESS.getExitCode(),
          Balancer.run(namenodes, p, conf));

    } finally {
      cluster.shutdown();
    }
  }


