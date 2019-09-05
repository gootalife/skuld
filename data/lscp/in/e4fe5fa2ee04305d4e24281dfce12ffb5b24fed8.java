hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsClientConfigKeys.java

public interface HdfsClientConfigKeys {
  long SECOND = 1000L;
  long MINUTE = 60 * SECOND;

  String  DFS_BLOCK_SIZE_KEY = "dfs.blocksize";
  long    DFS_BLOCK_SIZE_DEFAULT = 128*1024*1024;
  String  DFS_REPLICATION_KEY = "dfs.replication";
    int     CONNECTION_RETRIES_DEFAULT = 0;
    String  CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY = PREFIX + "connection.retries.on.timeouts";
    int     CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT = 0;
  }
  
  interface Write {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "write.";

    String  MAX_PACKETS_IN_FLIGHT_KEY = PREFIX + "max-packets-in-flight";
    int     MAX_PACKETS_IN_FLIGHT_DEFAULT = 80;
    String  EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_KEY = PREFIX + "exclude.nodes.cache.expiry.interval.millis";
    long    EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT = 10*MINUTE;

    interface ByteArrayManager {
      String PREFIX = Write.PREFIX + "byte-array-manager.";

      String  ENABLED_KEY = PREFIX + "enabled";
      boolean ENABLED_DEFAULT = false;
      String  COUNT_THRESHOLD_KEY = PREFIX + "count-threshold";
      int     COUNT_THRESHOLD_DEFAULT = 128;
      String  COUNT_LIMIT_KEY = PREFIX + "count-limit";
      int     COUNT_LIMIT_DEFAULT = 2048;
      String  COUNT_RESET_TIME_PERIOD_MS_KEY = PREFIX + "count-reset-time-period-ms";
      long    COUNT_RESET_TIME_PERIOD_MS_DEFAULT = 10*SECOND;
    }
  }

  interface BlockWrite {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "block.write.";

    String  RETRIES_KEY = PREFIX + "retries";
    int     RETRIES_DEFAULT = 3;
    String  LOCATEFOLLOWINGBLOCK_RETRIES_KEY = PREFIX + "locateFollowingBlock.retries";
    int     LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT = 5;
    String  LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_KEY = PREFIX + "locateFollowingBlock.initial.delay.ms";
    int     LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_DEFAULT = 400;

    interface ReplaceDatanodeOnFailure {
      String PREFIX = BlockWrite.PREFIX + "replace-datanode-on-failure.";

      String  ENABLE_KEY = PREFIX + "enable";
      boolean ENABLE_DEFAULT = true;
      String  POLICY_KEY = PREFIX + "policy";
      String  POLICY_DEFAULT = "DEFAULT";
      String  BEST_EFFORT_KEY = PREFIX + "best-effort";
      boolean BEST_EFFORT_DEFAULT = false;
    }
  }


hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/EnumSetParam.java
  }

  static <E extends Enum<E>> EnumSet<E> toEnumSet(final Class<E> clazz,
      final E[] values) {
    final EnumSet<E> set = EnumSet.noneOf(clazz);
    set.addAll(Arrays.asList(values));
    return set;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  public static final int     DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT
      = HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT;
  
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_MAX_PACKETS_IN_FLIGHT_KEY
      = HdfsClientConfigKeys.Write.MAX_PACKETS_IN_FLIGHT_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_WRITE_MAX_PACKETS_IN_FLIGHT_DEFAULT
      = HdfsClientConfigKeys.Write.MAX_PACKETS_IN_FLIGHT_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL
      = HdfsClientConfigKeys.Write.EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_KEY;
  @Deprecated
  public static final long    DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT
      = HdfsClientConfigKeys.Write.EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT; // 10 minutes, in ms
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_ENABLED_KEY
      = HdfsClientConfigKeys.Write.ByteArrayManager.ENABLED_KEY;
  @Deprecated
  public static final boolean DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_ENABLED_DEFAULT
      = HdfsClientConfigKeys.Write.ByteArrayManager.ENABLED_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_THRESHOLD_KEY
      = HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_THRESHOLD_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_THRESHOLD_DEFAULT
      = HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_THRESHOLD_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_LIMIT_KEY
      = HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_LIMIT_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_LIMIT_DEFAULT
      = HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_LIMIT_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_RESET_TIME_PERIOD_MS_KEY
      = HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_RESET_TIME_PERIOD_MS_KEY;
  @Deprecated
  public static final long    DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_RESET_TIME_PERIOD_MS_DEFAULT
      = HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_RESET_TIME_PERIOD_MS_DEFAULT;

  @Deprecated
  public static final String  DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY
      = HdfsClientConfigKeys.BlockWrite.RETRIES_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_BLOCK_WRITE_RETRIES_DEFAULT
      = HdfsClientConfigKeys.BlockWrite.RETRIES_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY
      = HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT
      = HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_KEY
      = HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_DEFAULT
      = HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY
      = HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY;
  @Deprecated
  public static final boolean DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_DEFAULT
      = HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY
      = HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY;
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_DEFAULT
      = HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_KEY
      = HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY;
  @Deprecated
  public static final boolean DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_DEFAULT
      = HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_DEFAULT;


  
  
  public static final String  DFS_CLIENT_WRITE_PACKET_SIZE_KEY = "dfs.client-write-packet-size";
  public static final int     DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64*1024;

  public static final String  DFS_CLIENT_SOCKET_TIMEOUT_KEY = "dfs.client.socket-timeout";
  public static final String  DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY = "dfs.client.socketcache.capacity";
  public static final String  DFS_CLIENT_HTTPS_NEED_AUTH_KEY = "dfs.client.https.need-auth";
  public static final boolean DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT = false;
  public static final String  DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY = "dfs.client.max.block.acquire.failures";
  public static final int     DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT = 3;


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DataStreamer.java

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
              .append("The current failed datanode replacement policy is ")
              .append(dfsClient.dtpReplaceDatanodeOnFailure).append(", and ")
              .append("a client may configure this via '")
              .append(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY)
              .append("' in its configuration.")
              .toString());
    }
          }
          DFSClient.LOG.warn("Failed to replace datanode."
              + " Continue with the remaining datanodes since "
              + HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY
              + " is set to true.", ioe);
        }
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/client/impl/DfsClientConf.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_DATANODE_RESTART_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
    socketTimeout = conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY,
        HdfsServerConstants.READ_TIMEOUT);
    writePacketSize = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY,
        DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
    writeMaxPackets = conf.getInt(
        HdfsClientConfigKeys.Write.MAX_PACKETS_IN_FLIGHT_KEY,
        HdfsClientConfigKeys.Write.MAX_PACKETS_IN_FLIGHT_DEFAULT);
    
    final boolean byteArrayManagerEnabled = conf.getBoolean(
        HdfsClientConfigKeys.Write.ByteArrayManager.ENABLED_KEY,
        HdfsClientConfigKeys.Write.ByteArrayManager.ENABLED_DEFAULT);
    if (!byteArrayManagerEnabled) {
      writeByteArrayManagerConf = null;
    } else {
      final int countThreshold = conf.getInt(
          HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_THRESHOLD_KEY,
          HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_THRESHOLD_DEFAULT);
      final int countLimit = conf.getInt(
          HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_LIMIT_KEY,
          HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_LIMIT_DEFAULT);
      final long countResetTimePeriodMs = conf.getLong(
          HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_RESET_TIME_PERIOD_MS_KEY,
          HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_RESET_TIME_PERIOD_MS_DEFAULT);
      writeByteArrayManagerConf = new ByteArrayManager.Conf(
          countThreshold, countLimit, countResetTimePeriodMs); 
    }
        DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT);
    taskId = conf.get("mapreduce.task.attempt.id", "NONMAPREDUCE");
    excludedNodesCacheExpiry = conf.getLong(
        HdfsClientConfigKeys.Write.EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_KEY,
        HdfsClientConfigKeys.Write.EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT);
    prefetchSize = conf.getLong(DFS_CLIENT_READ_PREFETCH_SIZE_KEY,
        10 * defaultBlockSize);
    numCachedConnRetry = conf.getInt(DFS_CLIENT_CACHED_CONN_RETRY_KEY,
        DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT);
    numBlockWriteRetry = conf.getInt(
        HdfsClientConfigKeys.BlockWrite.RETRIES_KEY,
        HdfsClientConfigKeys.BlockWrite.RETRIES_DEFAULT);
    numBlockWriteLocateFollowingRetry = conf.getInt(
        HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_KEY,
        HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT);
    blockWriteLocateFollowingInitialDelayMs = conf.getInt(
        HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_KEY,
        HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_DEFAULT);
    uMask = FsPermission.getUMask(conf);
    connectToDnViaHostname = conf.getBoolean(DFS_CLIENT_USE_DN_HOSTNAME,
        DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocol/datatransfer/ReplaceDatanodeOnFailure.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

    if (policy == Policy.DISABLE) {
      throw new UnsupportedOperationException(
          "This feature is disabled.  Please refer to "
          + HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY
          + " configuration property.");
    }
  }
  public static ReplaceDatanodeOnFailure get(final Configuration conf) {
    final Policy policy = getPolicy(conf);
    final boolean bestEffort = conf.getBoolean(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY,
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_DEFAULT);
    
    return new ReplaceDatanodeOnFailure(policy, bestEffort);
  }

  private static Policy getPolicy(final Configuration conf) {
    final boolean enabled = conf.getBoolean(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_DEFAULT);
    if (!enabled) {
      return Policy.DISABLE;
    }

    final String policy = conf.get(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT);
    for(int i = 1; i < Policy.values().length; i++) {
      final Policy p = Policy.values()[i];
      if (p.name().equalsIgnoreCase(policy)) {
      }
    }
    throw new HadoopIllegalArgumentException("Illegal configuration value for "
        + HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY
        + ": " + policy);
  }

  public static void write(final Policy policy,
      final boolean bestEffort, final Configuration conf) {
    conf.setBoolean(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
        policy != Policy.DISABLE);
    conf.set(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
        policy.name());
    conf.setBoolean(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY,
        bestEffort);
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestClientProtocolForPipelineRecovery.java
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

    DFSClientFaultInjector.instance = faultInjector;
    Configuration conf = new HdfsConfiguration();

    conf.setInt(HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_KEY, 3);
    MiniDFSCluster cluster = null;

    try {
      FSDataInputStream in = fileSys.open(file);
      try {
        in.read();
      } catch (org.apache.hadoop.hdfs.BlockMissingException bme) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSClientExcludedNodes.java

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.util.ThreadUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
  public void testExcludedNodesForgiveness() throws IOException {
    conf.setLong(
        HdfsClientConfigKeys.Write.EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_KEY,
        2500);
    conf.setInt("io.bytes.per.checksum", 512);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testForgivingExcludedNodes");


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSClientRetries.java
  { 
    final String exceptionMsg = "Nope, not replicated yet...";
    final int maxRetries = 1; // Allow one retry (total of two calls)
    conf.setInt(HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_KEY, maxRetries);
    
    NamenodeProtocols mockNN = mock(NamenodeProtocols.class);
    Answer<Object> answer = new ThrowsException(new IOException()) {
  @Test
  public void testDFSClientConfigurationLocateFollowingBlockInitialDelay()
      throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      assertEquals(client.getConf().
          getBlockWriteLocateFollowingInitialDelayMs(), 400);

      conf.setInt(
          HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_KEY,
          1000);
      client = new DFSClient(null, nn, conf, null);
      assertEquals(client.getConf().
          getBlockWriteLocateFollowingInitialDelayMs(), 1000);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestNamenodeCapacityReport.java
package org.apache.hadoop.hdfs.server.namenode;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
  public void testXceiverCount() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_KEY, 1);
    MiniDFSCluster cluster = null;

    final int nodes = 8;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/util/TestByteArrayManager.java
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.util.ByteArrayManager.Counter;
import org.apache.hadoop.hdfs.util.ByteArrayManager.CounterMap;
import org.apache.hadoop.hdfs.util.ByteArrayManager.FixedLengthManager;
        new ByteArrayManager.NewByteArrayWithoutLimit(),
        new NewByteArrayWithLimit(maxArrays),
        new ByteArrayManager.Impl(new ByteArrayManager.Conf(
            HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_THRESHOLD_DEFAULT,
            maxArrays,
            HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_RESET_TIME_PERIOD_MS_DEFAULT))
    };
    final double[] avg = new double[impls.length];


