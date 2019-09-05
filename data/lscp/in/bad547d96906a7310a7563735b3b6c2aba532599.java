hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsClientConfigKeys.java

  static final String PREFIX = "dfs.client.";

  interface Retry {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "retry.";

    String  POLICY_ENABLED_KEY = PREFIX + "policy.enabled";
    int     WINDOW_BASE_DEFAULT = 3000;
  }

  interface Failover {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "failover.";

    int     CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT = 0;
  }
  
  interface Write {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "write.";

    }
  }

  interface BlockWrite {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "block.write.";

    }
  }

  interface Read {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "read.";
    
    String  PREFETCH_SIZE_KEY = PREFIX + "prefetch.size"; 

    interface ShortCircuit {
      String PREFIX = Read.PREFIX + "shortcircuit.";

      String  KEY = PREFIX.substring(0, PREFIX.length()-1);
      boolean DEFAULT = false;
      String  SKIP_CHECKSUM_KEY = PREFIX + "skip.checksum";
      boolean SKIP_CHECKSUM_DEFAULT = false;
      String  BUFFER_SIZE_KEY = PREFIX + "buffer.size";
      int     BUFFER_SIZE_DEFAULT = 1024 * 1024;

      String  STREAMS_CACHE_SIZE_KEY = PREFIX + "streams.cache.size";
      int     STREAMS_CACHE_SIZE_DEFAULT = 256;
      String  STREAMS_CACHE_EXPIRY_MS_KEY = PREFIX + "streams.cache.expiry.ms";
      long    STREAMS_CACHE_EXPIRY_MS_DEFAULT = 5*MINUTE;
    }
  }

  interface ShortCircuit {
    String PREFIX = Read.PREFIX + "short.circuit.";

    String  REPLICA_STALE_THRESHOLD_MS_KEY = PREFIX + "replica.stale.threshold.ms";
    long    REPLICA_STALE_THRESHOLD_MS_DEFAULT = 30*MINUTE;
  }

  interface Mmap {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "mmap.";

    String  ENABLED_KEY = PREFIX + "enabled";
    boolean ENABLED_DEFAULT = true;
    String  CACHE_SIZE_KEY = PREFIX + "cache.size";
    int     CACHE_SIZE_DEFAULT = 256;
    String  CACHE_TIMEOUT_MS_KEY = PREFIX + "cache.timeout.ms";
    long    CACHE_TIMEOUT_MS_DEFAULT  = 60*MINUTE;
    String  RETRY_TIMEOUT_MS_KEY = PREFIX + "retry.timeout.ms";
    long    RETRY_TIMEOUT_MS_DEFAULT = 5*MINUTE;
  }

  interface HedgedRead {
    String  THRESHOLD_MILLIS_KEY = PREFIX + "threshold.millis";
    long    THRESHOLD_MILLIS_DEFAULT = 500;
    String  THREADPOOL_SIZE_KEY = PREFIX + "threadpool.size";
    int     THREADPOOL_SIZE_DEFAULT = 0;
  }

  interface HttpClient {
    String  PREFIX = "dfs.http.client.";


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/BlockReaderLocalLegacy.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf.ShortCircuitConf;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
      throw new IllegalArgumentException("Configured BlockReaderLocalLegacy " +
          "buffer size (" + bufferSizeBytes + ") is not large enough to hold " +
          "a single chunk (" + bytesPerChecksum +  "). Please configure " +
          HdfsClientConfigKeys.Read.ShortCircuit.BUFFER_SIZE_KEY +
          " appropriately");
    }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
  private final CachingStrategy defaultReadCachingStrategy;
  private final CachingStrategy defaultWriteCachingStrategy;
  private final ClientContext clientContext;

  private static final DFSHedgedReadMetrics HEDGED_READ_METRIC =
      new DFSHedgedReadMetrics();
  private static ThreadPoolExecutor HEDGED_READ_THREAD_POOL;
    this.clientContext = ClientContext.get(
        conf.get(DFS_CLIENT_CONTEXT, DFS_CLIENT_CONTEXT_DEFAULT),
        dfsClientConf);

    if (dfsClientConf.getHedgedReadThreadpoolSize() > 0) {
      this.initThreadsNumForHedgedReads(dfsClientConf.getHedgedReadThreadpoolSize());
    }
    this.saslClient = new SaslDataTransferClient(
      conf, DataTransferSaslUtil.getSaslPropertiesResolver(conf),
    }
  }

  ThreadPoolExecutor getHedgedReadsThreadPool() {
    return HEDGED_READ_THREAD_POOL;
  }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java

  
  
  @Deprecated
  public static final String  DFS_CLIENT_RETRY_POLICY_ENABLED_KEY
      = HdfsClientConfigKeys.Retry.POLICY_ENABLED_KEY;
  public static final int     DFS_CLIENT_RETRY_WINDOW_BASE_DEFAULT
      = HdfsClientConfigKeys.Retry.WINDOW_BASE_DEFAULT;

  @Deprecated
  public static final String  DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX
      = HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX;
  public static final int     DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT
      = HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT;
  
  @Deprecated
  public static final String  DFS_CLIENT_WRITE_MAX_PACKETS_IN_FLIGHT_KEY
      = HdfsClientConfigKeys.Write.MAX_PACKETS_IN_FLIGHT_KEY;
  public static final long    DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_RESET_TIME_PERIOD_MS_DEFAULT
      = HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_RESET_TIME_PERIOD_MS_DEFAULT;

  @Deprecated
  public static final String  DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY
      = HdfsClientConfigKeys.BlockWrite.RETRIES_KEY;
  public static final boolean DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_DEFAULT
      = HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_DEFAULT;

  @Deprecated
  public static final String  DFS_CLIENT_READ_PREFETCH_SIZE_KEY
      = HdfsClientConfigKeys.Read.PREFETCH_SIZE_KEY; 
  @Deprecated
  public static final String  DFS_CLIENT_READ_SHORTCIRCUIT_KEY
      = HdfsClientConfigKeys.Read.ShortCircuit.KEY; 
  @Deprecated
  public static final boolean DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT
      = HdfsClientConfigKeys.Read.ShortCircuit.DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY
      = HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY;
  @Deprecated
  public static final boolean DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_DEFAULT
      = HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_KEY
      = HdfsClientConfigKeys.Read.ShortCircuit.BUFFER_SIZE_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_DEFAULT
      = HdfsClientConfigKeys.Read.ShortCircuit.BUFFER_SIZE_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_KEY
      = HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_SIZE_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_DEFAULT
      = HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_SIZE_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_KEY
      = HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_EXPIRY_MS_KEY;
  @Deprecated
  public static final long    DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_DEFAULT
      = HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_EXPIRY_MS_DEFAULT;

  @Deprecated
  public static final String  DFS_CLIENT_MMAP_ENABLED
      = HdfsClientConfigKeys.Mmap.ENABLED_KEY;
  @Deprecated
  public static final boolean DFS_CLIENT_MMAP_ENABLED_DEFAULT
      = HdfsClientConfigKeys.Mmap.ENABLED_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_MMAP_CACHE_SIZE
      = HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_MMAP_CACHE_SIZE_DEFAULT
      = HdfsClientConfigKeys.Mmap.CACHE_SIZE_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS
      = HdfsClientConfigKeys.Mmap.CACHE_TIMEOUT_MS_KEY;
  @Deprecated
  public static final long    DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS_DEFAULT
      = HdfsClientConfigKeys.Mmap.CACHE_TIMEOUT_MS_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_MMAP_RETRY_TIMEOUT_MS
      = HdfsClientConfigKeys.Mmap.RETRY_TIMEOUT_MS_KEY;
  @Deprecated
  public static final long    DFS_CLIENT_MMAP_RETRY_TIMEOUT_MS_DEFAULT
      = HdfsClientConfigKeys.Mmap.RETRY_TIMEOUT_MS_DEFAULT;

  @Deprecated
  public static final String  DFS_CLIENT_SHORT_CIRCUIT_REPLICA_STALE_THRESHOLD_MS
      = HdfsClientConfigKeys.ShortCircuit.REPLICA_STALE_THRESHOLD_MS_KEY;
  @Deprecated
  public static final long    DFS_CLIENT_SHORT_CIRCUIT_REPLICA_STALE_THRESHOLD_MS_DEFAULT
      = HdfsClientConfigKeys.ShortCircuit.REPLICA_STALE_THRESHOLD_MS_DEFAULT;

  @Deprecated
  public static final String  DFS_DFSCLIENT_HEDGED_READ_THRESHOLD_MILLIS
      = HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY;
  @Deprecated
  public static final long    DEFAULT_DFSCLIENT_HEDGED_READ_THRESHOLD_MILLIS
      = HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_DEFAULT;
  @Deprecated
  public static final String  DFS_DFSCLIENT_HEDGED_READ_THREADPOOL_SIZE
      = HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY;
  @Deprecated
  public static final int     DEFAULT_DFSCLIENT_HEDGED_READ_THREADPOOL_SIZE
      = HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_DEFAULT;




  public static final String  DFS_CLIENT_LOCAL_INTERFACES = "dfs.client.local.interfaces";


  public static final String  DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC = "dfs.client.domain.socket.data.traffic";
  public static final boolean DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT = false;

      "dfs.client.key.provider.cache.expiry";
  public static final long    DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_DEFAULT =
      TimeUnit.DAYS.toMillis(10); // 10 days
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSInputStream.java
      long end, byte[] buf, int offset,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    final DfsClientConf conf = dfsClient.getConf();
    ArrayList<Future<ByteBuffer>> futures = new ArrayList<Future<ByteBuffer>>();
    CompletionService<ByteBuffer> hedgedService =
        new ExecutorCompletionService<ByteBuffer>(
        futures.add(firstRequest);
        try {
          Future<ByteBuffer> future = hedgedService.poll(
              conf.getHedgedReadThresholdMillis(), TimeUnit.MILLISECONDS);
          if (future != null) {
            future.get();
            return;
          }
          if (DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("Waited " + conf.getHedgedReadThresholdMillis()
                + "ms to read from " + chosenNode.info
                + "; spawning hedged read");
          }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/HdfsConfiguration.java

package org.apache.hadoop.hdfs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;

      new DeprecationDelta("dfs.name.edits.dir",
        DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY),
      new DeprecationDelta("dfs.read.prefetch.size",
        HdfsClientConfigKeys.Read.PREFETCH_SIZE_KEY),
      new DeprecationDelta("dfs.safemode.extension",
        DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY),
      new DeprecationDelta("dfs.safemode.threshold.pct",

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/client/impl/DfsClientConf.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT;

  private final ShortCircuitConf shortCircuitConf;
  
  private final long hedgedReadThresholdMillis;
  private final int hedgedReadThreadpoolSize;

  public DfsClientConf(Configuration conf) {
    hdfsTimeout = Client.getTimeout(conf);
    excludedNodesCacheExpiry = conf.getLong(
        HdfsClientConfigKeys.Write.EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_KEY,
        HdfsClientConfigKeys.Write.EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT);
    prefetchSize = conf.getLong(HdfsClientConfigKeys.Read.PREFETCH_SIZE_KEY,
        10 * defaultBlockSize);
    numCachedConnRetry = conf.getInt(DFS_CLIENT_CACHED_CONN_RETRY_KEY,
        DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT);
        DFSConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT);
    
    shortCircuitConf = new ShortCircuitConf(conf);

    hedgedReadThresholdMillis = conf.getLong(
        HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY,
        HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_DEFAULT);
    hedgedReadThreadpoolSize = conf.getInt(
        HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY,
        HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_DEFAULT);
  }

  private DataChecksum.Type getChecksumType(Configuration conf) {
    return slowIoWarningThresholdMs;
  }

  public long getHedgedReadThresholdMillis() {
    return hedgedReadThresholdMillis;
  }

  public int getHedgedReadThreadpoolSize() {
    return hedgedReadThreadpoolSize;
  }

          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL,
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT);
      shortCircuitLocalReads = conf.getBoolean(
          HdfsClientConfigKeys.Read.ShortCircuit.KEY,
          HdfsClientConfigKeys.Read.ShortCircuit.DEFAULT);
      domainSocketDataTraffic = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC,
          DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT);
      if (LOG.isDebugEnabled()) {
        LOG.debug(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL
            + " = " + useLegacyBlockReaderLocal);
        LOG.debug(HdfsClientConfigKeys.Read.ShortCircuit.KEY
            + " = " + shortCircuitLocalReads);
        LOG.debug(DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC
            + " = " + domainSocketDataTraffic);
      }

      skipShortCircuitChecksums = conf.getBoolean(
          HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
          HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_DEFAULT);
      shortCircuitBufferSize = conf.getInt(
          HdfsClientConfigKeys.Read.ShortCircuit.BUFFER_SIZE_KEY,
          HdfsClientConfigKeys.Read.ShortCircuit.BUFFER_SIZE_DEFAULT);
      shortCircuitStreamsCacheSize = conf.getInt(
          HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_SIZE_KEY,
          HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_SIZE_DEFAULT);
      shortCircuitStreamsCacheExpiryMs = conf.getLong(
          HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_EXPIRY_MS_KEY,
          HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_EXPIRY_MS_DEFAULT);
      shortCircuitMmapEnabled = conf.getBoolean(
          HdfsClientConfigKeys.Mmap.ENABLED_KEY,
          HdfsClientConfigKeys.Mmap.ENABLED_DEFAULT);
      shortCircuitMmapCacheSize = conf.getInt(
          HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY,
          HdfsClientConfigKeys.Mmap.CACHE_SIZE_DEFAULT);
      shortCircuitMmapCacheExpiryMs = conf.getLong(
          HdfsClientConfigKeys.Mmap.CACHE_TIMEOUT_MS_KEY,
          HdfsClientConfigKeys.Mmap.CACHE_TIMEOUT_MS_DEFAULT);
      shortCircuitMmapCacheRetryTimeout = conf.getLong(
          HdfsClientConfigKeys.Mmap.RETRY_TIMEOUT_MS_KEY,
          HdfsClientConfigKeys.Mmap.RETRY_TIMEOUT_MS_DEFAULT);
      shortCircuitCacheStaleThresholdMs = conf.getLong(
          HdfsClientConfigKeys.ShortCircuit.REPLICA_STALE_THRESHOLD_MS_KEY,
          HdfsClientConfigKeys.ShortCircuit.REPLICA_STALE_THRESHOLD_MS_DEFAULT);
      shortCircuitSharedMemoryWatcherInterruptCheckMs = conf.getInt(
          DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS,
          DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PLUGINS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_STARTUP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_MAX_NUM_BLOCKS_TO_LOG_DEFAULT;

import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtil.ConfiguredNNAddress;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.net.DomainPeerServer;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockPoolTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier.AccessMode;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.tracing.SpanReceiverHost;
import org.apache.hadoop.tracing.SpanReceiverInfo;
import org.apache.hadoop.tracing.TraceAdminPB.TraceAdminService;
import org.apache.hadoop.tracing.TraceAdminProtocol;
import org.apache.hadoop.tracing.TraceAdminProtocolPB;
import org.apache.hadoop.tracing.TraceAdminProtocolServerSideTranslatorPB;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;
import org.mortbay.util.ajax.JSON;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.protobuf.BlockingService;

        conf.get("hadoop.hdfs.configuration.version", "UNSPECIFIED");

    if (conf.getBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY,
              HdfsClientConfigKeys.Read.ShortCircuit.DEFAULT)) {
      String reason = DomainSocket.getLoadingFailureReason();
      if (reason != null) {
        LOG.warn("File descriptor passing is disabled because " + reason);
    this.dataXceiverServer = new Daemon(threadGroup, xserver);
    this.threadGroup.setDaemon(true); // auto destroy when empty

    if (conf.getBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY,
              HdfsClientConfigKeys.Read.ShortCircuit.DEFAULT) ||
        conf.getBoolean(DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC,
              DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT)) {
      DomainPeerServer domainPeerServer =
    this.shortCircuitRegistry = new ShortCircuitRegistry(conf);
  }

  private static DomainPeerServer getDomainPeerServer(Configuration conf,
      int port) throws IOException {
    String domainSocketPath =
        conf.getTrimmed(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
            DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_DEFAULT);
    if (domainSocketPath.isEmpty()) {
      if (conf.getBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY,
            HdfsClientConfigKeys.Read.ShortCircuit.DEFAULT) &&
         (!conf.getBoolean(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL,
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT))) {
        LOG.warn("Although short-circuit local reads are configured, " +

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/shortcircuit/ShortCircuitCache.java
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf.ShortCircuitConf;
import org.apache.hadoop.hdfs.net.DomainPeer;
  private final DfsClientShmManager shmManager;

  public static ShortCircuitCache fromConf(ShortCircuitConf conf) {
    return new ShortCircuitCache(
        conf.getShortCircuitStreamsCacheSize(),

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/fs/TestEnhancedByteBufferAccess.java
package org.apache.hadoop.fs;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
    Assume.assumeTrue(NativeIO.isAvailable());
    Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, 3);
    conf.setLong(HdfsClientConfigKeys.Mmap.CACHE_TIMEOUT_MS_KEY, 100);
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        new File(sockDir.getDir(),
          "TestRequestMmapAccess._PORT.sock").getAbsolutePath());
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
        true);
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setLong(DFS_CACHEREPORT_INTERVAL_MSEC_KEY, 1000);
    conf.setLong(DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS, 1000);
    final Path TEST_PATH = new Path("/a");
    final int RANDOM_SEED = 23453;
    HdfsConfiguration conf = initZeroCopyTest();
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
        false);
    final String CONTEXT = "testZeroCopyReadOfCachedData";
    conf.set(DFSConfigKeys.DFS_CLIENT_CONTEXT, CONTEXT);
    conf.setLong(DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
  @Test
  public void testClientMmapDisable() throws Exception {
    HdfsConfiguration conf = initZeroCopyTest();
    conf.setBoolean(HdfsClientConfigKeys.Mmap.ENABLED_KEY, false);
    MiniDFSCluster cluster = null;
    final Path TEST_PATH = new Path("/a");
    final int TEST_FILE_LENGTH = 16385;
    conf.set(DFSConfigKeys.DFS_CLIENT_CONTEXT, CONTEXT);

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
    fs = null;
    cluster = null;
    try {
      conf.setBoolean(HdfsClientConfigKeys.Mmap.ENABLED_KEY, true);
      conf.setInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, 0);
      conf.set(DFSConfigKeys.DFS_CLIENT_CONTEXT, CONTEXT + ".1");
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/fs/TestUnbuffer.java
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.PeerCache;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, false);

    conf.setLong(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY,
  public void testOpenManyFilesViaTcp() throws Exception {
    final int NUM_OPENS = 500;
    Configuration conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, false);
    MiniDFSCluster cluster = null;
    FSDataInputStream[] streams = new FSDataInputStream[NUM_OPENS];
    try {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/DFSTestUtil.java
    
    public Configuration newConfiguration() {
      Configuration conf = new Configuration();
      conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
      conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
          new File(sockDir.getDir(),
            testName + "._PORT.sock").getAbsolutePath());

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockReaderFactory.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CONTEXT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS;
import static org.hamcrest.CoreMatchers.equalTo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.shortcircuit.DfsClientShmManager.PerDatanodeVisitorInfo;
    conf.setLong(DFS_BLOCK_SIZE_KEY, 4096);
    conf.set(DFS_DOMAIN_SOCKET_PATH_KEY, new File(sockDir.getDir(),
        testName + "._PORT").getAbsolutePath());
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
        false);
    conf.setBoolean(DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC, false);
    return conf;
        "testFallbackFromShortCircuitToUnixDomainTraffic_clientContext");
    clientConf.setBoolean(DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC, true);
    Configuration serverConf = new Configuration(clientConf);
    serverConf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, false);

    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(serverConf).numDataNodes(1).build();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockReaderLocal.java
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
    MiniDFSCluster cluster = null;
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
        !checksum);
    conf.setLong(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY,
        BlockReaderLocalTest.BYTES_PER_CHECKSUM);
    conf.set(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, "CRC32C");
      conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        new File(sockDir.getDir(), "TestStatisticsForLocalRead.%d.sock").
          getAbsolutePath());
      conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
      DomainSocket.disableBindPathValidation();
    } else {
      conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, false);
    }
    MiniDFSCluster cluster = null;
    final Path TEST_PATH = new Path("/a");

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockReaderLocalLegacy.java
        new File(socketDir.getDir(), "TestBlockReaderLocalLegacy.%d.sock").
          getAbsolutePath());
    }
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL, true);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
        false);
    conf.set(DFSConfigKeys.DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY,
        UserGroupInformation.getCurrentUser().getShortUserName());

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestParallelRead.java
package org.apache.hadoop.hdfs;

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.AfterClass;
import org.junit.BeforeClass;

    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, false);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC,
                    false);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestParallelShortCircuitLegacyRead.java
package org.apache.hadoop.hdfs;

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY, "");
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL, true);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC, false);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
        false);
    conf.set(DFSConfigKeys.DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY,
        UserGroupInformation.getCurrentUser().getShortUserName());
    DomainSocket.disableBindPathValidation();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestParallelShortCircuitRead.java
package org.apache.hadoop.hdfs;

import static org.hamcrest.CoreMatchers.equalTo;

import java.io.File;

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestParallelShortCircuitRead extends TestParallelReadUtil {
  private static TemporarySocketDirectory sockDir;
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
      new File(sockDir.getDir(), "TestParallelLocalRead.%d.sock").getAbsolutePath());
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
        false);
    DomainSocket.disableBindPathValidation();
    setupCluster(1, conf);
  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestParallelShortCircuitReadNoChecksum.java
package org.apache.hadoop.hdfs;

import static org.hamcrest.CoreMatchers.equalTo;

import java.io.File;

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestParallelShortCircuitReadNoChecksum extends TestParallelReadUtil {
  private static TemporarySocketDirectory sockDir;
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
      new File(sockDir.getDir(), "TestParallelLocalRead.%d.sock").getAbsolutePath());
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
        true);
    DomainSocket.disableBindPathValidation();
    setupCluster(1, conf);
  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestParallelShortCircuitReadUnCached.java
package org.apache.hadoop.hdfs;

import static org.hamcrest.CoreMatchers.equalTo;

import java.io.File;

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
      new File(sockDir.getDir(), 
        "TestParallelShortCircuitReadUnCached._PORT.sock").getAbsolutePath());
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
        false);
    conf.setBoolean(DFSConfigKeys.
        DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC, true);
        5 * 60 * 1000);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY, 32);
    conf.setInt(HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_SIZE_KEY,
        0);
    DomainSocket.disableBindPathValidation();
    DFSInputStream.tcpReadsDisabledForTesting = true;
    setupCluster(1, conf);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestParallelUnixDomainRead.java
package org.apache.hadoop.hdfs;

import static org.hamcrest.CoreMatchers.equalTo;

import java.io.File;

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestParallelUnixDomainRead extends TestParallelReadUtil {
  private static TemporarySocketDirectory sockDir;
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
      new File(sockDir.getDir(), "TestParallelLocalRead.%d.sock").getAbsolutePath());
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, false);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC, true);
    DomainSocket.disableBindPathValidation();
    setupCluster(1, conf);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestPread.java
  public void testHedgedPreadDFSBasic() throws IOException {
    isHedgedRead = true;
    Configuration conf = new Configuration();
    conf.setInt(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY, 5);
    conf.setLong(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY, 1);
    dfsPreadTest(conf, false, true); // normal pread
    dfsPreadTest(conf, true, true); // trigger read code path without
    int numHedgedReadPoolThreads = 5;
    final int hedgedReadTimeoutMillis = 50;

    conf.setInt(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY,
        numHedgedReadPoolThreads);
    conf.setLong(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY,
        hedgedReadTimeoutMillis);
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 0);
    int numHedgedReadPoolThreads = 5;
    final int initialHedgedReadTimeoutMillis = 50000;
    final int fixedSleepIntervalMillis = 50;
    conf.setInt(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY,
        numHedgedReadPoolThreads);
    conf.setLong(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY,
        initialHedgedReadTimeoutMillis);

      {
        Configuration conf2 =  new Configuration(cluster.getConfiguration(0));
        conf2.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf2.setLong(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY, 50);
        fileSys.close();
        fileSys = (DistributedFileSystem)FileSystem.get(cluster.getURI(0), conf2);
        metrics = fileSys.getClient().getHedgedReadMetrics();
      }
      pReadFile(fileSys, file1);
      assertTrue(metrics.getHedgedReadOps() > 0);
  private void dfsPreadTest(Configuration conf, boolean disableTransferTo, boolean verifyChecksum)
      throws IOException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 4096);
    conf.setLong(HdfsClientConfigKeys.Read.PREFETCH_SIZE_KEY, 4096);
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 0);
    if (simulatedStorage) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestFsDatasetCacheRevocation.java
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
    conf.setLong(DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
        TestFsDatasetCache.CACHE_CAPACITY);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
      new File(sockDir.getDir(), "sock").getAbsolutePath());
    return conf;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/LazyPersistTestCase.java
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;
import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CONTEXT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_RAM_DISK_LOW_WATERMARK_BYTES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.junit.Rule;
import org.junit.rules.Timeout;

public abstract class LazyPersistTestCase {
  static final byte LAZY_PERSIST_POLICY_ID = (byte) 15;

                EVICTION_LOW_WATERMARK * BLOCK_SIZE);

    if (useSCR) {
      conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
      conf.set(DFS_CLIENT_CONTEXT, UUID.randomUUID().toString());
      if (useLegacyBlockReaderLocal) {

    if (useSCR)
    {
      conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY,useSCR);
      conf.set(DFS_CLIENT_CONTEXT, UUID.randomUUID().toString());
      sockDir = new TemporarySocketDirectory();
      conf.set(DFS_DOMAIN_SOCKET_PATH_KEY, new File(sockDir.getDir(),

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/shortcircuit/TestShortCircuitCache.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CONTEXT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY;
import static org.hamcrest.CoreMatchers.equalTo;

import java.io.DataOutputStream;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockReaderFactory;
import org.apache.hadoop.hdfs.BlockReaderTestUtil;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.net.DomainPeer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.shortcircuit.DfsClientShmManager.Visitor;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache.CacheVisitor;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache.ShortCircuitReplicaCreator;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.ShmId;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.Slot;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;

public class TestShortCircuitCache {
  static final Log LOG = LogFactory.getLog(TestShortCircuitCache.class);
    conf.setLong(DFS_BLOCK_SIZE_KEY, 4096);
    conf.set(DFS_DOMAIN_SOCKET_PATH_KEY, new File(sockDir.getDir(),
        testName).getAbsolutePath());
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
        false);
    conf.setBoolean(DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC, false);
    DFSInputStream.tcpReadsDisabledForTesting = true;
        "testUnlinkingReplicasInFileDescriptorCache", sockDir);
    conf.setLong(HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_EXPIRY_MS_KEY,
        1000000000L);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    Configuration conf = createShortCircuitConf(
        "testDataXceiverCleansUpSlotsOnFailure", sockDir);
    conf.setLong(
        HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_EXPIRY_MS_KEY,
        1000000000L);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/shortcircuit/TestShortCircuitLocalRead.java
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestBlockReaderLocal;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
      int readOffset, String shortCircuitUser, String readingUser,
      boolean legacyShortCircuitFails) throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
        ignoreChecksum);
  public void testSkipWithVerifyChecksum() throws IOException {
    int size = blockSize;
    Configuration conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY, false);
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        "/tmp/testSkipWithVerifyChecksum._PORT");
    DomainSocket.disableBindPathValidation();
  public void testHandleTruncatedBlockFile() throws IOException {
    MiniDFSCluster cluster = null;
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY, false);
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        "/tmp/testHandleTruncatedBlockFile._PORT");
    conf.set(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, "CRC32C");

    final Configuration conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, shortcircuit);
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        "/tmp/TestShortCircuitLocalRead._PORT");
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
        checksum);
    
                                                          int readOffset, boolean shortCircuitFails) throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADER, true);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
             .format(true).build();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/tracing/TestTracingShortCircuitLocalRead.java

import static org.junit.Assume.assumeTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTracingShortCircuitLocalRead {
  private static Configuration conf;
    conf.set(SpanReceiverHost.SPAN_RECEIVERS_CONF_KEY,
        TestTracing.SetSpanReceiver.class.getName());
    conf.setLong("dfs.blocksize", 100 * 1024);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY, false);
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        "testShortCircuitTraceHooks._PORT");
    conf.set(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, "CRC32C");

