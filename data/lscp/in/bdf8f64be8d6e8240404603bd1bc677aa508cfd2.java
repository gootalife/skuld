hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  public static final long    DFS_MOVER_MOVEDWINWIDTH_DEFAULT = 5400*1000L;
  public static final String  DFS_MOVER_MOVERTHREADS_KEY = "dfs.mover.moverThreads";
  public static final int     DFS_MOVER_MOVERTHREADS_DEFAULT = 1000;
  public static final String  DFS_MOVER_RETRY_MAX_ATTEMPTS_KEY = "dfs.mover.retry.max.attempts";
  public static final int     DFS_MOVER_RETRY_MAX_ATTEMPTS_DEFAULT = 10;

  public static final String  DFS_DATANODE_ADDRESS_KEY = "dfs.datanode.address";
  public static final int     DFS_DATANODE_DEFAULT_PORT = 50010;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/mover/Mover.java
import java.net.URI;
import java.text.DateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@InterfaceAudience.Private
public class Mover {
  private final Dispatcher dispatcher;
  private final StorageMap storages;
  private final List<Path> targetPaths;
  private final int retryMaxAttempts;
  private final AtomicInteger retryCount;

  private final BlockStoragePolicy[] blockStoragePolicies;

  Mover(NameNodeConnector nnc, Configuration conf, AtomicInteger retryCount) {
    final long movedWinWidth = conf.getLong(
        DFSConfigKeys.DFS_MOVER_MOVEDWINWIDTH_KEY,
        DFSConfigKeys.DFS_MOVER_MOVEDWINWIDTH_DEFAULT);
    final int maxConcurrentMovesPerNode = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT);
    this.retryMaxAttempts = conf.getInt(
        DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_KEY,
        DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_DEFAULT);
    this.retryCount = retryCount;
    this.dispatcher = new Dispatcher(nnc, Collections.<String> emptySet(),
        Collections.<String> emptySet(), movedWinWidth, moverThreads, 0,
        maxConcurrentMovesPerNode, conf);
    private boolean processNamespace() throws IOException {
      getSnapshottableDirs();
      boolean hasRemaining = false;
      for (Path target : targetPaths) {
        hasRemaining |= processPath(target.toUri().getPath());
      }
      boolean hasFailed = Dispatcher.waitForMoveCompletion(storages.targets
          .values());
      if (hasFailed) {
        if (retryCount.get() == retryMaxAttempts) {
          throw new IOException("Failed to move some block's after "
              + retryMaxAttempts + " retries.");
        } else {
          retryCount.incrementAndGet();
        }
      } else {
        retryCount.set(0);
      }
      hasRemaining |= hasFailed;
      return hasRemaining;
    }

            DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 2000 +
        conf.getLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT) * 1000;
    AtomicInteger retryCount = new AtomicInteger(0);
    LOG.info("namenodes = " + namenodes);
    
    List<NameNodeConnector> connectors = Collections.emptyList();
        Iterator<NameNodeConnector> iter = connectors.iterator();
        while (iter.hasNext()) {
          NameNodeConnector nnc = iter.next();
          final Mover m = new Mover(nnc, conf, retryCount);
          final ExitStatus r = m.run();

          if (r == ExitStatus.SUCCESS) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/mover/TestMover.java
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DBlock;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.mover.Mover.MLocation;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
    final List<NameNodeConnector> nncs = NameNodeConnector.newNameNodeConnectors(
        nnMap, Mover.class.getSimpleName(), Mover.MOVER_ID_PATH, conf,
        NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS);
    return new Mover(nncs.get(0), conf, new AtomicInteger(0));
  }

  @Test
      cluster.shutdown();
    }
  }

  @Test
  public void testMoverFailedRetry() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_KEY, "2");
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .storageTypes(
            new StorageType[][] {{StorageType.DISK, StorageType.ARCHIVE},
                {StorageType.DISK, StorageType.ARCHIVE},
                {StorageType.DISK, StorageType.ARCHIVE}}).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testMoverFailedRetry";
      final FSDataOutputStream out = dfs.create(new Path(file), (short) 2);
      out.writeChars("testMoverFailedRetry");
      out.close();

      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      cluster.corruptBlockOnDataNodesByDeletingBlockFile(lb.getBlock());
      dfs.setStoragePolicy(new Path(file), "COLD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] {"-p", file.toString()});
      Assert.assertEquals("Movement should fail after some retry",
          ExitStatus.IO_EXCEPTION.getExitCode(), rc);
    } finally {
      cluster.shutdown();
    }
  }
}

