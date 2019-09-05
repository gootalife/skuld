hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/retry/RetryPolicies.java
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
  
  public static final Log LOG = LogFactory.getLog(RetryPolicies.class);
  
      }

      final double ratio = ThreadLocalRandom.current().nextDouble() + 0.5;
      final long sleepTime = Math.round(p.sleepMillis * ratio);
      return new RetryAction(RetryAction.RetryDecision.RETRY, sleepTime);
    }
  private static long calculateExponentialTime(long time, int retries,
      long cap) {
    long baseTime = Math.min(time * (1L << retries), cap);
    return (long) (baseTime * (ThreadLocalRandom.current().nextDouble() + 0.5));
  }

  private static long calculateExponentialTime(long time, int retries) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
import java.util.Map;
import java.util.Random;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
    
    this.authority = nameNodeUri == null? "null": nameNodeUri.getAuthority();
    this.clientName = "DFSClient_" + dfsClientConf.getTaskId() + "_" + 
        ThreadLocalRandom.current().nextInt()  + "_" +
        Thread.currentThread().getId();
    int numResponseToDrop = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY,
        DFSConfigKeys.DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_DEFAULT);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSInputStream.java
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

          final int timeWindow = dfsClient.getConf().getTimeWindow();
          double waitTime = timeWindow * failures +       // grace period for the last round of attempt
              timeWindow * (failures + 1) *
              ThreadLocalRandom.current().nextDouble();
          DFSClient.LOG.warn("DFS chooseDataNode: got # " + (failures + 1) + " IOException, will wait for " + waitTime + " msec.");
          Thread.sleep((long)waitTime);
        } catch (InterruptedException iex) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSUtil.java
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import javax.net.SocketFactory;

  public static final Log LOG = LogFactory.getLog(DFSUtil.class.getName());
  
  private DFSUtil() { /* Hidden constructor */ }
  
  private static final ThreadLocal<SecureRandom> SECURE_RANDOM = new ThreadLocal<SecureRandom>() {
    @Override
    }
  };

  public static SecureRandom getSecureRandom() {
    return SECURE_RANDOM.get();
  public static <T> T[] shuffle(final T[] array) {
    if (array != null && array.length > 0) {
      for (int n = array.length; n > 1; ) {
        final int randomIndex = ThreadLocalRandom.current().nextInt(n);
        n--;
        if (n != randomIndex) {
          final T tmp = array[randomIndex];

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.HadoopIllegalArgumentException;
      return new BlocksWithLocations(new BlockWithLocations[0]);
    }
    Iterator<BlockInfoContiguous> iter = node.getBlockIterator();
    int startBlock = ThreadLocalRandom.current().nextInt(numBlocks);
    for(int i=0; i<startBlock; i++) {
      iter.next();
      if(ThreadLocalRandom.current().nextBoolean())
        srcNode = node;
    }
    if(numReplicas != null)
          datanodeManager.getBlocksPerPostponedMisreplicatedBlocksRescan();
      long base = getPostponedMisreplicatedBlocksCount() - blocksPerRescan;
      if (base > 0) {
        startIndex = ThreadLocalRandom.current().nextLong() % (base+1);
        if (startIndex < 0) {
          startIndex += (base+1);
        }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DatanodeManager.java
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

        if (node == null && !rackNodes.isEmpty()) {
          node = (DatanodeDescriptor) (rackNodes
              .get(ThreadLocalRandom.current().nextInt(rackNodes.size())));
        }
      }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/Host2NodesMap.java
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
        return nodes[0];
      }
      return nodes[ThreadLocalRandom.current().nextInt(nodes.length)];
    } finally {
      hostmapLock.readLock().unlock();
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/common/JspHelper.java
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
      throw new IOException("No active nodes contain this block");
    }

    int index = doRandom ? ThreadLocalRandom.current().nextInt(l) : 0;
    return nodes[index];
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BPServiceActor.java
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.base.Joiner;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
    this.dn = bpos.getDataNode();
    this.nnAddr = nnAddr;
    this.dnConf = dn.getDnConf();
    prevBlockReportId = ThreadLocalRandom.current().nextLong();
    scheduler = new Scheduler(dnConf.heartBeatInterval, dnConf.blockReportInterval);
  }

    prevBlockReportId++;
    while (prevBlockReportId == 0) {
      prevBlockReportId = ThreadLocalRandom.current().nextLong();
    }
    return prevBlockReportId;
  }
      if (delay > 0) { // send BR after random delay
        nextBlockReportTime =
            monotonicNow() + ThreadLocalRandom.current().nextInt((int) (delay));
      } else { // send at next heartbeat
        nextBlockReportTime = monotonicNow();
      }
      if (resetBlockReportTime) {
        nextBlockReportTime = monotonicNow() +
            ThreadLocalRandom.current().nextInt((int)(blockReportIntervalMs));
        resetBlockReportTime = false;
      } else {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DirectoryScanner.java
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;

  void start() {
    shouldRun = true;
    long offset = ThreadLocalRandom.current().nextInt(
        (int) (scanPeriodMsecs/1000L)) * 1000L; //msec
    long firstScanTime = Time.now() + offset;
    LOG.info("Periodic Directory Tree Verification scan starting at " 
        + firstScanTime + " with interval " + scanPeriodMsecs);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/metrics/DataNodeMetrics.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.source.JvmMetrics;

import java.util.concurrent.ThreadLocalRandom;

    MetricsSystem ms = DefaultMetricsSystem.instance();
    JvmMetrics jm = JvmMetrics.create("DataNode", sessionId, ms);
    String name = "DataNodeActivity-"+ (dnName.isEmpty()
        ? "UndefinedDataNodeName"+ ThreadLocalRandom.current().nextInt()
            : dnName.replace(':', '-'));


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NNStorage.java
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
  private static int newNamespaceID() {
    int newID = 0;
    while(newID == 0)
      newID = ThreadLocalRandom.current().nextInt(0x7FFFFFFF);  // use 31 bits
    return newID;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NamenodeFsck.java
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.BlockReaderFactory;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.RemotePeerFactory;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.net.Peer;
    }
    DatanodeInfo chosenNode;
    do {
      chosenNode = nodes[ThreadLocalRandom.current().nextInt(nodes.length)];
    } while (deadNodes.contains(chosenNode));
    return chosenNode;
  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestAppendSnapshotTruncate.java
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
    
    @Override
    public String call() throws Exception {
      final int op = ThreadLocalRandom.current().nextInt(6);
      if (op <= 1) {
        pauseAllFiles();
        try {
        if (keys.length == 0) {
          return "NO-OP";
        }
        final String snapshot = keys[ThreadLocalRandom.current()
            .nextInt(keys.length)];
        final String s = checkSnapshot(snapshot);
        
        if (op == 2) {

    @Override
    public String call() throws IOException {
      final int op = ThreadLocalRandom.current().nextInt(9);
      if (op == 0) {
        return checkFullFile();
      } else {
        final int nBlocks = ThreadLocalRandom.current().nextInt(4) + 1;
        final int lastBlockSize = ThreadLocalRandom.current()
            .nextInt(BLOCK_SIZE) + 1;
        final int nBytes = nBlocks*BLOCK_SIZE + lastBlockSize;

        if (op <= 4) {
          .append(n).append(" bytes to ").append(file.getName());

      final byte[] bytes = new byte[n];
      ThreadLocalRandom.current().nextBytes(bytes);

      { // write to local file
        final FileOutputStream out = new FileOutputStream(localFile, true);
        final Thread t = new Thread(null, new Runnable() {
          @Override
          public void run() {
            for(State s; !(s = checkErrorState()).isTerminated;) {
              if (s == State.RUNNING) {
                isCalling.set(true);
                }
                isCalling.set(false);
              }
              sleep(ThreadLocalRandom.current().nextInt(100) + 50);
            }
          }
        }, name);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestRollingUpgrade.java

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

      final Path file = new Path(foo, "file");
      final byte[] data = new byte[1024];
      ThreadLocalRandom.current().nextBytes(data);
      final FSDataOutputStream out = cluster.getFileSystem().create(file);
      out.write(data, 0, data.length);
      out.close();
    Assert.assertTrue(dfs.exists(bar));

    final int newLength = ThreadLocalRandom.current().nextInt(data.length - 1)
        + 1;
    dfs.truncate(file, newLength);
    TestFileTruncate.checkBlockRecovery(file, dfs);
    AppendTestUtil.checkFullFile(dfs, file, newLength, data);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestReplicationPolicy.java
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
    ((Log4JLogger)BlockPlacementPolicy.LOG).getLogger().setLevel(Level.ALL);
  }

  private static final int BLOCK_SIZE = 1024;
  private static final int NUM_OF_DATANODES = 6;
  private static NetworkTopology cluster;
          .getNamesystem().getBlockManager().neededReplications;
      for (int i = 0; i < 100; i++) {
        neededReplications.add(new Block(
              ThreadLocalRandom.current().nextLong()), 2, 0, 3);
      }
      Thread.sleep(DFS_NAMENODE_REPLICATION_INTERVAL);
      
      neededReplications.add(new Block(
              ThreadLocalRandom.current().nextLong()), 1, 0, 3);
      
      Thread.sleep(DFS_NAMENODE_REPLICATION_INTERVAL);

    for (int i = 0; i < 5; i++) {
      underReplicatedBlocks.add(new Block(ThreadLocalRandom.current().
            nextLong()), 1, 0, 3);

      underReplicatedBlocks.add(new Block(ThreadLocalRandom.current().
            nextLong()), 2, 0, 7);

      underReplicatedBlocks.add(new Block(ThreadLocalRandom.current().
            nextLong()), 6, 0, 6);

      underReplicatedBlocks.add(new Block(ThreadLocalRandom.current().
            nextLong()), 5, 0, 6);

      underReplicatedBlocks.add(new Block(ThreadLocalRandom.current().
            nextLong()), 0, 0, 3);
    }

    assertTheChosenBlocks(chosenBlocks, 0, 4, 5, 1, 0);

    underReplicatedBlocks.add(new Block(
          ThreadLocalRandom.current().nextLong()), 1, 0, 3);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFileTruncate.java
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
    fs.mkdirs(dir);
    final Path p = new Path(dir, "file");
    final byte[] data = new byte[100 * BLOCK_SIZE];
    ThreadLocalRandom.current().nextBytes(data);
    writeContents(data, data.length, p);

    for(int n = data.length; n > 0; ) {
      final int newLength = ThreadLocalRandom.current().nextInt(n);
      final boolean isReady = fs.truncate(p, newLength);
      LOG.info("newLength=" + newLength + ", isReady=" + isReady);
      assertEquals("File must be closed for truncating at the block boundary",
    fs.allowSnapshot(dir);
    final Path p = new Path(dir, "file");
    final byte[] data = new byte[BLOCK_SIZE];
    ThreadLocalRandom.current().nextBytes(data);
    writeContents(data, data.length, p);
    final String snapshot = "s0";
    fs.createSnapshot(dir, snapshot);
    final Path p = new Path(dir, "file");
    final byte[] data = new byte[2 * BLOCK_SIZE];

    ThreadLocalRandom.current().nextBytes(data);
    writeContents(data, data.length, p);

    final int newLength = data.length - 1;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestDNFencing.java
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.Block;
      Collection<DatanodeStorageInfo> chooseFrom = !first.isEmpty() ? first : second;

      List<DatanodeStorageInfo> l = Lists.newArrayList(chooseFrom);
      return l.get(ThreadLocalRandom.current().nextInt(l.size()));
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestHAAppend.java
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.TestFileTruncate;
      Path fileToTruncate = new Path("/FileToTruncate");
      
      final byte[] data = new byte[1 << 16];
      ThreadLocalRandom.current().nextBytes(data);
      final int[] appendPos = AppendTestUtil.randomFilePartition(
          data.length, COUNT);
      final int[] truncatePos = AppendTestUtil.randomFilePartition(

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/util/TestByteArrayManager.java
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.util.ByteArrayManager.Counter;
import org.apache.hadoop.hdfs.util.ByteArrayManager.CounterMap;
    final long countResetTimePeriodMs = 200L;
    final Counter c = new Counter(countResetTimePeriodMs);

    final int n = ThreadLocalRandom.current().nextInt(512) + 512;
    final List<Future<Integer>> futures = new ArrayList<Future<Integer>>(n);
    
    final ExecutorService pool = Executors.newFixedThreadPool(32);
      public void run() {
        LOG.info("randomRecycler start");
        for(int i = 0; shouldRun(); i++) {
          final int j = ThreadLocalRandom.current().nextInt(runners.length);
          try {
            runners[j].recycle();
          } catch (Exception e) {
        public byte[] call() throws Exception {
          final int lower = maxArrayLength == ByteArrayManager.MIN_ARRAY_LENGTH?
              0: maxArrayLength >> 1;
          final int arrayLength = ThreadLocalRandom.current().nextInt(
              maxArrayLength - lower) + lower + 1;
          final byte[] array = bam.newByteArray(arrayLength);
          try {
    @Override
    public void run() {
      for(int i = 0; i < n; i++) {
        final boolean isAllocate = ThreadLocalRandom.current()
            .nextInt(NUM_RUNNERS) < p;
        if (isAllocate) {
          submitAllocate();
        } else {
        + ", nAllocations=" + nAllocations
        + ", maxArrays=" + maxArrays);
    
    final ByteArrayManager[] impls = {
        new ByteArrayManager.NewByteArrayWithoutLimit(),
        new NewByteArrayWithLimit(maxArrays),
      for(int j = 0; j < nTrials; j++) {
        final int[] sleepTime = new int[nAllocations];
        for(int k = 0; k < sleepTime.length; k++) {
          sleepTime[k] = ThreadLocalRandom.current().nextInt(100);
        }
      
        final long elapsed = performanceTest(arrayLength, maxArrays, nThreads,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/sharedcache/SharedCacheUploader.java
import java.io.InputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URISyntaxException;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
      new FsPermission((short)00555);

  private static final Log LOG = LogFactory.getLog(SharedCacheUploader.class);

  private final LocalResource resource;
  private final Path localPath;
  }

  private String getTemporaryFileName(Path path) {
    return path.getName() + "-" + ThreadLocalRandom.current().nextLong();
  }

  @VisibleForTesting

