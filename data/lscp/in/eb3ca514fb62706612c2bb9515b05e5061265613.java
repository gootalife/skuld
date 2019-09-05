hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsClientConfigKeys.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsClientConfigKeys.java
package org.apache.hadoop.hdfs.client;

public interface HdfsClientConfigKeys {
  static final String PREFIX = "dfs.client.";

  public interface Retry {
    static final String PREFIX = HdfsClientConfigKeys.PREFIX + "retry.";

    public static final String  POLICY_ENABLED_KEY
        = PREFIX + "policy.enabled";
    public static final boolean POLICY_ENABLED_DEFAULT
        = false; 
    public static final String  POLICY_SPEC_KEY
        = PREFIX + "policy.spec";
    public static final String  POLICY_SPEC_DEFAULT
        = "10000,6,60000,10"; //t1,n1,t2,n2,... 

    public static final String  TIMES_GET_LAST_BLOCK_LENGTH_KEY
        = PREFIX + "times.get-last-block-length";
    public static final int     TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT
        = 3;
    public static final String  INTERVAL_GET_LAST_BLOCK_LENGTH_KEY
        = PREFIX + "interval-ms.get-last-block-length";
    public static final int     INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT
        = 4000;

    public static final String  MAX_ATTEMPTS_KEY
        = PREFIX + "max.attempts";
    public static final int     MAX_ATTEMPTS_DEFAULT
        = 10;

    public static final String  WINDOW_BASE_KEY
        = PREFIX + "window.base";
    public static final int     WINDOW_BASE_DEFAULT
        = 3000;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_RETRIES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumResponseProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
          DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
          DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);
      maxRetryAttempts = conf.getInt(
          HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_KEY,
          HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_DEFAULT);
      failoverSleepBaseMillis = conf.getInt(
          DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY,
          DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT);
          DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT);
      prefetchSize = conf.getLong(DFS_CLIENT_READ_PREFETCH_SIZE_KEY,
          10 * defaultBlockSize);
      timeWindow = conf.getInt(
          HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY,
          HdfsClientConfigKeys.Retry.WINDOW_BASE_DEFAULT);
      nCachedConnRetry = conf.getInt(DFS_CLIENT_CACHED_CONN_RETRY_KEY,
          DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT);
      nBlockWriteRetry = conf.getInt(DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY,
          DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS,
          DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS_DEFAULT);
      retryTimesForGetLastBlockLength = conf.getInt(
          HdfsClientConfigKeys.Retry.TIMES_GET_LAST_BLOCK_LENGTH_KEY,
          HdfsClientConfigKeys.Retry.TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT);
      retryIntervalForGetLastBlockLength = conf.getInt(
          HdfsClientConfigKeys.Retry.INTERVAL_GET_LAST_BLOCK_LENGTH_KEY,
          HdfsClientConfigKeys.Retry.INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT);

      useLegacyBlockReader = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADER,

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.RamDiskReplicaLruTracker;
import org.apache.hadoop.hdfs.web.AuthFilter;
import org.apache.hadoop.http.HttpConfig;

@InterfaceAudience.Private
public class DFSConfigKeys extends CommonConfigurationKeys {
  public static final String  DFS_BLOCK_SIZE_KEY = "dfs.blocksize";
  public static final long    DFS_BLOCK_SIZE_DEFAULT = 128*1024*1024;
  public static final String  DFS_REPLICATION_KEY = "dfs.replication";
  public static final int     DFS_BYTES_PER_CHECKSUM_DEFAULT = 512;
  public static final String  DFS_USER_HOME_DIR_PREFIX_KEY = "dfs.user.home.dir.prefix";
  public static final String  DFS_USER_HOME_DIR_PREFIX_DEFAULT = "/user";
  public static final String  DFS_CHECKSUM_TYPE_KEY = "dfs.checksum.type";
  public static final String  DFS_CHECKSUM_TYPE_DEFAULT = "CRC32C";
  public static final String  DFS_HDFS_BLOCKS_METADATA_ENABLED = "dfs.datanode.hdfs-blocks-metadata.enabled";
  public static final boolean DFS_HDFS_BLOCKS_METADATA_ENABLED_DEFAULT = false;
  public static final String DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT =
      "^(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?(,(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?)*$";

  public static final String  DFS_DATANODE_RESTART_REPLICA_EXPIRY_KEY = "dfs.datanode.restart.replica.expiration";
  public static final long    DFS_DATANODE_RESTART_REPLICA_EXPIRY_DEFAULT = 50;
  public static final String  DFS_NAMENODE_BACKUP_ADDRESS_KEY = "dfs.namenode.backup.address";
  public static final int     DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT = 5*60*1000;
  public static final String  DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_KEY = "dfs.namenode.tolerate.heartbeat.multiplier";
  public static final int     DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT = 4;
  public static final String  DFS_NAMENODE_ACCESSTIME_PRECISION_KEY = "dfs.namenode.accesstime.precision";
  public static final long    DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT = 3600000;
  public static final String  DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY = "dfs.namenode.replication.considerLoad";
  public static final String  DFS_NAMENODE_EDITS_PLUGIN_PREFIX = "dfs.namenode.edits.journal-plugin";
  public static final String  DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY = "dfs.namenode.edits.dir.required";
  public static final String  DFS_NAMENODE_EDITS_DIR_DEFAULT = "file:///tmp/hadoop/dfs/name";
  public static final String  DFS_METRICS_SESSION_ID_KEY = "dfs.metrics.session-id";
  public static final String  DFS_METRICS_PERCENTILES_INTERVALS_KEY = "dfs.metrics.percentiles.intervals";
  public static final String  DFS_DATANODE_HOST_NAME_KEY = "dfs.datanode.hostname";
  public static final String  DFS_NAMENODE_HOSTS_KEY = "dfs.namenode.hosts";
  public static final String  DFS_NAMENODE_HOSTS_EXCLUDE_KEY = "dfs.namenode.hosts.exclude";
  public static final String  DFS_NAMENODE_CHECKPOINT_DIR_KEY = "dfs.namenode.checkpoint.dir";
  public static final String  DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY = "dfs.namenode.checkpoint.edits.dir";
  public static final String  DFS_HOSTS = "dfs.hosts";
  public static final String  DFS_HOSTS_EXCLUDE = "dfs.hosts.exclude";
  public static final String  DFS_NAMENODE_AUDIT_LOGGERS_KEY = "dfs.namenode.audit.loggers";
  public static final String  DFS_NAMENODE_DEFAULT_AUDIT_LOGGER_NAME = "default";
  public static final String  DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_KEY = "dfs.namenode.audit.log.token.tracking.id";
  public static final String  DFS_NAMENODE_AUDIT_LOG_ASYNC_KEY = "dfs.namenode.audit.log.async";
  public static final boolean DFS_NAMENODE_AUDIT_LOG_ASYNC_DEFAULT = false;

  public static final String  DFS_BALANCER_MOVEDWINWIDTH_KEY = "dfs.balancer.movedWinWidth";
  public static final long    DFS_BALANCER_MOVEDWINWIDTH_DEFAULT = 5400*1000L;
  public static final String  DFS_BALANCER_MOVERTHREADS_KEY = "dfs.balancer.moverThreads";
  public static final String  DFS_BLOCK_MISREPLICATION_PROCESSING_LIMIT = "dfs.block.misreplication.processing.limit";
  public static final int     DFS_BLOCK_MISREPLICATION_PROCESSING_LIMIT_DEFAULT = 10000;

  public static final String DFS_IMAGE_COMPRESS_KEY = "dfs.image.compress";
  public static final String DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_KEY = "dfs.namenode.retrycache.heap.percent";
  public static final float DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_DEFAULT = 0.03f;
  
  public static final String DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_KEY = "dfs.datanode.xceiver.stop.timeout.millis";
  public static final boolean DFS_REJECT_UNRESOLVED_DN_TOPOLOGY_MAPPING_DEFAULT =
      false;
  
  public static final String DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY =
    "dfs.datanode.slow.io.warning.threshold.ms";
  public static final long DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT = 300;
  public static final boolean DFS_PIPELINE_ECN_ENABLED_DEFAULT = false;

  public static final String DFS_DATANODE_BLOCK_PINNING_ENABLED = 
    "dfs.datanode.block-pinning.enabled";
  public static final boolean DFS_DATANODE_BLOCK_PINNING_ENABLED_DEFAULT =
    false;


  
  
  @Deprecated
  public static final String  DFS_CLIENT_RETRY_POLICY_ENABLED_KEY
      = HdfsClientConfigKeys.Retry.POLICY_ENABLED_KEY;
  @Deprecated
  public static final boolean DFS_CLIENT_RETRY_POLICY_ENABLED_DEFAULT
      = HdfsClientConfigKeys.Retry.POLICY_ENABLED_DEFAULT; 
  @Deprecated
  public static final String  DFS_CLIENT_RETRY_POLICY_SPEC_KEY
      = HdfsClientConfigKeys.Retry.POLICY_SPEC_KEY;
  @Deprecated
  public static final String  DFS_CLIENT_RETRY_POLICY_SPEC_DEFAULT
      = HdfsClientConfigKeys.Retry.POLICY_SPEC_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_RETRY_TIMES_GET_LAST_BLOCK_LENGTH
      = HdfsClientConfigKeys.Retry.TIMES_GET_LAST_BLOCK_LENGTH_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_RETRY_TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT
      = HdfsClientConfigKeys.Retry.TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_RETRY_INTERVAL_GET_LAST_BLOCK_LENGTH
      = HdfsClientConfigKeys.Retry.INTERVAL_GET_LAST_BLOCK_LENGTH_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_RETRY_INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT
      = HdfsClientConfigKeys.Retry.INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_RETRY_MAX_ATTEMPTS_KEY
      = HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT
      = HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_RETRY_WINDOW_BASE
      = HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_RETRY_WINDOW_BASE_DEFAULT
      = HdfsClientConfigKeys.Retry.WINDOW_BASE_DEFAULT;


  
  
  
  public static final String  DFS_CLIENT_WRITE_MAX_PACKETS_IN_FLIGHT_KEY = "dfs.client.write.max-packets-in-flight";
  public static final int     DFS_CLIENT_WRITE_MAX_PACKETS_IN_FLIGHT_DEFAULT = 80;
  public static final String  DFS_CLIENT_WRITE_PACKET_SIZE_KEY = "dfs.client-write-packet-size";
  public static final int     DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64*1024;
  public static final String  DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_ENABLED_KEY
      = "dfs.client.write.byte-array-manager.enabled";
  public static final boolean DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_ENABLED_DEFAULT
      = false;
  public static final String  DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_THRESHOLD_KEY
      = "dfs.client.write.byte-array-manager.count-threshold";
  public static final int     DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_THRESHOLD_DEFAULT
      = 128;
  public static final String  DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_LIMIT_KEY
      = "dfs.client.write.byte-array-manager.count-limit";
  public static final int     DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_LIMIT_DEFAULT
      = 2048;
  public static final String  DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_RESET_TIME_PERIOD_MS_KEY
      = "dfs.client.write.byte-array-manager.count-reset-time-period-ms";
  public static final long    DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_RESET_TIME_PERIOD_MS_DEFAULT
      = 10L * 1000;
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY = "dfs.client.block.write.replace-datanode-on-failure.enable";
  public static final boolean DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_DEFAULT = true;
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY = "dfs.client.block.write.replace-datanode-on-failure.policy";
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_DEFAULT = "DEFAULT";
  public static final String  DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_KEY = "dfs.client.block.write.replace-datanode-on-failure.best-effort";
  public static final boolean DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_DEFAULT = false;
  public static final String  DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL = "dfs.client.write.exclude.nodes.cache.expiry.interval.millis";
  public static final long    DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT = 10 * 60 * 1000; // 10 minutes, in ms

  public static final String  DFS_CLIENT_SOCKET_TIMEOUT_KEY = "dfs.client.socket-timeout";
  public static final String  DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY = "dfs.client.socketcache.capacity";
  public static final int     DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT = 16;
  public static final String  DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY = "dfs.client.socketcache.expiryMsec";
  public static final long    DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT = 3000;

  public static final String  DFS_CLIENT_USE_DN_HOSTNAME = "dfs.client.use.datanode.hostname";
  public static final boolean DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT = false;
  public static final String  DFS_CLIENT_CACHE_DROP_BEHIND_WRITES = "dfs.client.cache.drop.behind.writes";
  public static final String  DFS_CLIENT_CACHE_DROP_BEHIND_READS = "dfs.client.cache.drop.behind.reads";
  public static final String  DFS_CLIENT_CACHE_READAHEAD = "dfs.client.cache.readahead";
  public static final String  DFS_CLIENT_CACHED_CONN_RETRY_KEY = "dfs.client.cached.conn.retry";
  public static final int     DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT = 3;

  public static final String  DFS_CLIENT_CONTEXT = "dfs.client.context";
  public static final String  DFS_CLIENT_CONTEXT_DEFAULT = "default";
  public static final String  DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_NUM_THREADS = "dfs.client.file-block-storage-locations.num-threads";
  public static final int     DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_NUM_THREADS_DEFAULT = 10;
  public static final String  DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS = "dfs.client.file-block-storage-locations.timeout.millis";
  public static final int     DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS_DEFAULT = 1000;

  public static final String  DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX = "dfs.client.failover.proxy.provider";
  public static final String  DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY = "dfs.client.failover.max.attempts";
  public static final int     DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT = 15;
  public static final String  DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY = "dfs.client.failover.sleep.base.millis";
  public static final int     DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT = 500;
  public static final String  DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY = "dfs.client.failover.sleep.max.millis";
  public static final int     DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT = 15000;
  public static final String  DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_KEY = "dfs.client.failover.connection.retries";
  public static final int     DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_DEFAULT = 0;
  public static final String  DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY = "dfs.client.failover.connection.retries.on.timeouts";
  public static final int     DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT = 0;
  
  public static final String  DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY = "dfs.client.datanode-restart.timeout";
  public static final long    DFS_CLIENT_DATANODE_RESTART_TIMEOUT_DEFAULT = 30;

  public static final String  DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY = "dfs.client.https.keystore.resource";
  public static final String  DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_DEFAULT = "ssl-client.xml";
  public static final String  DFS_CLIENT_HTTPS_NEED_AUTH_KEY = "dfs.client.https.need-auth";
  public static final boolean DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT = false;
  public static final String  DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY = "dfs.client.block.write.locateFollowingBlock.retries";
  public static final int     DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT = 5;
  public static final String  DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_KEY = "dfs.client.block.write.locateFollowingBlock.initial.delay.ms";
  public static final int     DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_DEFAULT = 400;
  public static final String  DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY = "dfs.client.block.write.retries";
  public static final int     DFS_CLIENT_BLOCK_WRITE_RETRIES_DEFAULT = 3;
  public static final String  DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY = "dfs.client.max.block.acquire.failures";
  public static final int     DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT = 3;

  public static final String  DFS_CLIENT_USE_LEGACY_BLOCKREADER = "dfs.client.use.legacy.blockreader";
  public static final boolean DFS_CLIENT_USE_LEGACY_BLOCKREADER_DEFAULT = false;
  public static final String  DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL = "dfs.client.use.legacy.blockreader.local";
  public static final boolean DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT = false;

  public static final String  DFS_CLIENT_LOCAL_INTERFACES = "dfs.client.local.interfaces";

  public static final String  DFS_CLIENT_READ_PREFETCH_SIZE_KEY = "dfs.client.read.prefetch.size"; 
  public static final String  DFS_CLIENT_READ_SHORTCIRCUIT_KEY = "dfs.client.read.shortcircuit";
  public static final boolean DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT = false;
  public static final String  DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY = "dfs.client.read.shortcircuit.skip.checksum";
  public static final boolean DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_DEFAULT = false;
  public static final String  DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_KEY = "dfs.client.read.shortcircuit.buffer.size";
  public static final int     DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_DEFAULT = 1024 * 1024;
  public static final String  DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_KEY = "dfs.client.read.shortcircuit.streams.cache.size";
  public static final int     DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_DEFAULT = 256;
  public static final String  DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_KEY = "dfs.client.read.shortcircuit.streams.cache.expiry.ms";
  public static final long    DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_DEFAULT = 5 * 60 * 1000;

  public static final String  DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC = "dfs.client.domain.socket.data.traffic";
  public static final boolean DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT = false;
  public static final String  DFS_CLIENT_MMAP_ENABLED= "dfs.client.mmap.enabled";
  public static final boolean DFS_CLIENT_MMAP_ENABLED_DEFAULT = true;
  public static final String  DFS_CLIENT_MMAP_CACHE_SIZE = "dfs.client.mmap.cache.size";
  public static final int     DFS_CLIENT_MMAP_CACHE_SIZE_DEFAULT = 256;
  public static final String  DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS = "dfs.client.mmap.cache.timeout.ms";
  public static final long    DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS_DEFAULT  = 60 * 60 * 1000;
  public static final String  DFS_CLIENT_MMAP_RETRY_TIMEOUT_MS = "dfs.client.mmap.retry.timeout.ms";
  public static final long    DFS_CLIENT_MMAP_RETRY_TIMEOUT_MS_DEFAULT = 5 * 60 * 1000;
  public static final String  DFS_CLIENT_SHORT_CIRCUIT_REPLICA_STALE_THRESHOLD_MS = "dfs.client.short.circuit.replica.stale.threshold.ms";
  public static final long    DFS_CLIENT_SHORT_CIRCUIT_REPLICA_STALE_THRESHOLD_MS_DEFAULT = 30 * 60 * 1000;

  public static final String  DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY = "dfs.client.test.drop.namenode.response.number";
  public static final int     DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_DEFAULT = 0;
  public static final String  DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY =
      "dfs.client.slow.io.warning.threshold.ms";
  public static final long    DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT = 30000;
  public static final String  DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_MS =
      "dfs.client.key.provider.cache.expiry";
  public static final long    DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_DEFAULT =
      TimeUnit.DAYS.toMillis(10); // 10 days

  public static final String  DFS_DFSCLIENT_HEDGED_READ_THRESHOLD_MILLIS =
      "dfs.client.hedged.read.threshold.millis";
  public static final long    DEFAULT_DFSCLIENT_HEDGED_READ_THRESHOLD_MILLIS =
      500;

  public static final String  DFS_DFSCLIENT_HEDGED_READ_THREADPOOL_SIZE =
      "dfs.client.hedged.read.threadpool.size";
  public static final int     DEFAULT_DFSCLIENT_HEDGED_READ_THREADPOOL_SIZE = 0;
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/NameNodeProxies.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY;

import java.io.IOException;
import java.lang.reflect.Constructor;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.server.namenode.ha.AbstractNNFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.ha.WrappedFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RefreshCallQueueProtocol;
import org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolClientSideTranslatorPB;
import org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolPB;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;
  public static <T> ProxyAndInfo<T> createProxy(Configuration conf,
      URI nameNodeUri, Class<T> xface) throws IOException {
    return createProxy(conf, nameNodeUri, xface, null);
          DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
          DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);
      int maxRetryAttempts = config.getInt(
          HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_KEY,
          HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_DEFAULT);
      InvocationHandler dummyHandler = new LossyRetryInvocationHandler<T>(
              numResponseToDrop, failoverProxyProvider,
              RetryPolicies.failoverOnNetworkException(
  public static <T> ProxyAndInfo<T> createNonHAProxy(
      Configuration conf, InetSocketAddress nnAddr, Class<T> xface,
      UserGroupInformation ugi, boolean withRetries) throws IOException {
    final RetryPolicy defaultPolicy = 
        RetryUtils.getDefaultRetryPolicy(
            conf, 
            HdfsClientConfigKeys.Retry.POLICY_ENABLED_KEY, 
            HdfsClientConfigKeys.Retry.POLICY_ENABLED_DEFAULT, 
            HdfsClientConfigKeys.Retry.POLICY_SPEC_KEY,
            HdfsClientConfigKeys.Retry.POLICY_SPEC_DEFAULT,
            SafeModeException.class);
    
    final long version = RPC.getProtocolVersion(ClientNamenodeProtocolPB.class);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/client/HdfsUtils.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
    conf.setBoolean(String.format("fs.%s.impl.disable.cache", scheme), true);
    conf.setBoolean(HdfsClientConfigKeys.Retry.POLICY_ENABLED_KEY, false);
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockMissingException.java
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.Test;
    int numBlocks = 4;
    conf = new HdfsConfiguration();
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    try {
      dfs = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES).build();
      dfs.waitActive();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockReaderLocalLegacy.java
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
        UserGroupInformation.getCurrentUser().getShortUserName());
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC, false);
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    return conf;
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestClientReportBadBlock.java
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
    conf.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1); 
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes)
        .build();
    cluster.waitActive();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestCrcCorruption.java
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

  public void testCorruptionDuringWrt() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    MiniDFSCluster cluster = null;

    try {
    short replFactor = 2;
    Random random = new Random();
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
      cluster.waitActive();
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, numDataNodes);
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();

    try {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSClientRetries.java
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyShort;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsUtils;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.ThrowsException;
import org.mockito.invocation.InvocationOnMock;
    Path file = new Path("/testFile");

    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    conf.setInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, 2 * 1000);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

    conf.setInt(DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY, xcievers);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY, 
                retries);
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, timeWin);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_KEY, 0);

    if (isWebHDFS) {
      conf.setBoolean(DFSConfigKeys.DFS_HTTP_CLIENT_RETRY_POLICY_ENABLED_KEY, true);
    } else {
      conf.setBoolean(HdfsClientConfigKeys.Retry.POLICY_ENABLED_KEY, true);
    }
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY, 1);
    conf.setInt(MiniDFSCluster.DFS_NAMENODE_SAFEMODE_EXTENSION_TESTING_KEY, 5000);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSShell.java
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
    final Path remotef = new Path(root, fname);
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    TestGetRunner runner = new TestGetRunner() {
    	private int count = 0;
    	private final FsShell shell = new FsShell(conf);
  @Test (timeout = 30000)
  public void testSetXAttrCaseSensitivity() throws Exception {
    MiniDFSCluster cluster = null;
    PrintStream bak = null;
    try {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestEncryptedTransfer.java
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
    try {
      Configuration conf = new Configuration();
      conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
      cluster = new MiniDFSCluster.Builder(conf).build();
      
      FileSystem fs = getFileSystem(conf);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestMissingBlocksAlert.java
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.junit.Assert;
      Configuration conf = new HdfsConfiguration();
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 0);
      conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
      int fileLen = 10*1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, fileLen/2);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestPread.java
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.io.IOUtils;
        numHedgedReadPoolThreads);
    conf.setLong(DFSConfigKeys.DFS_DFSCLIENT_HEDGED_READ_THRESHOLD_MILLIS,
        hedgedReadTimeoutMillis);
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 0);
    DFSClientFaultInjector.instance = Mockito
        .mock(DFSClientFaultInjector.class);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 4096);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY, 4096);
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 0);
    if (simulatedStorage) {
      SimulatedFSDataset.setFactory(conf);
    }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/protocol/datatransfer/sasl/TestSaslDataTransfer.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.IGNORE_SECURE_PORTS_FOR_TESTING_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.IOUtils;
public class TestSaslDataTransfer extends SaslDataTransferTestCase {

  private static final int BLOCK_SIZE = 4096;
  private static final int NUM_BLOCKS = 3;
  private static final Path PATH  = new Path("/file1");

  private MiniDFSCluster cluster;
  private FileSystem fs;
    HdfsConfiguration clusterConf = createSecureConfig(
      "authentication,integrity,privacy");
    clusterConf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    startCluster(clusterConf);
    HdfsConfiguration clientConf = new HdfsConfiguration(clusterConf);
    clientConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "");

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockTokenWithDFS.java
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RemotePeerFactory;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, numDataNodes);
    conf.setInt("ipc.client.connect.max.retries", 0);
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    return conf;
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFsck.java
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000);
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    FileSystem fs = null;
    DFSClient dfsClient = null;
    LocatedBlocks blocks = null;
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000);
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    short minReplication=2;
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,minReplication);
    short NUM_DN = 1;
    final long blockSize = 512;
    Random random = new Random();
    ExtendedBlock block;
    short repFactor = 1;
    String [] racks = {"/rack1"};
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000);
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestListCorruptFileBlocks.java
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
      conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 1); // datanode scans directories
      conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 3 * 1000); // datanode sends block reports
      conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
      cluster = new MiniDFSCluster.Builder(conf).build();
      FileSystem fs = cluster.getFileSystem();

      conf.setFloat(DFSConfigKeys.DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY,
                    0f);
      conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
      cluster = new MiniDFSCluster.Builder(conf).waitSafeMode(false).build();
      cluster.getNameNodeRpc().setSafeMode(
          HdfsConstants.SafeModeAction.SAFEMODE_LEAVE, false);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestFailoverWithBlockTokensEnabled.java
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(1)

