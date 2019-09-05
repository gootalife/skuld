hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsClientConfigKeys.java
      "^(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?(,(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?)*$";

  static final String PREFIX = "dfs.client.";

  public interface Retry {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "retry.";

    String  POLICY_ENABLED_KEY = PREFIX + "policy.enabled";
    boolean POLICY_ENABLED_DEFAULT = false; 
    String  POLICY_SPEC_KEY = PREFIX + "policy.spec";
    String  POLICY_SPEC_DEFAULT = "10000,6,60000,10"; //t1,n1,t2,n2,... 

    String  TIMES_GET_LAST_BLOCK_LENGTH_KEY = PREFIX + "times.get-last-block-length";
    int     TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT = 3;
    String  INTERVAL_GET_LAST_BLOCK_LENGTH_KEY = PREFIX + "interval-ms.get-last-block-length";
    int     INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT = 4000;

    String  MAX_ATTEMPTS_KEY = PREFIX + "max.attempts";
    int     MAX_ATTEMPTS_DEFAULT = 10;

    String  WINDOW_BASE_KEY = PREFIX + "window.base";
    int     WINDOW_BASE_DEFAULT = 3000;
  }

  interface Failover {
    String PREFIX = HdfsClientConfigKeys.PREFIX + "failover.";

    String  PROXY_PROVIDER_KEY_PREFIX = PREFIX + "proxy.provider";
    String  MAX_ATTEMPTS_KEY = PREFIX + "max.attempts";
    int     MAX_ATTEMPTS_DEFAULT = 15;
    String  SLEEPTIME_BASE_KEY = PREFIX + "sleep.base.millis";
    int     SLEEPTIME_BASE_DEFAULT = 500;
    String  SLEEPTIME_MAX_KEY = PREFIX + "sleep.max.millis";
    int     SLEEPTIME_MAX_DEFAULT = 15000;
    String  CONNECTION_RETRIES_KEY = PREFIX + "connection.retries";
    int     CONNECTION_RETRIES_DEFAULT = 0;
    String  CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY = PREFIX + "connection.retries.on.timeouts";
    int     CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT = 0;
    
  }

  interface HttpClient {
    String  PREFIX = "dfs.http.client.";

    String  RETRY_POLICY_ENABLED_KEY = PREFIX + "retry.policy.enabled";
    boolean RETRY_POLICY_ENABLED_DEFAULT = false;
    String  RETRY_POLICY_SPEC_KEY = PREFIX + "retry.policy.spec";
    String  RETRY_POLICY_SPEC_DEFAULT = "10000,6,60000,10"; //t1,n1,t2,n2,...
    String  RETRY_MAX_ATTEMPTS_KEY = PREFIX + "retry.max.attempts";
    int     RETRY_MAX_ATTEMPTS_DEFAULT = 10;
    
    String  FAILOVER_MAX_ATTEMPTS_KEY = PREFIX + "failover.max.attempts";
    int     FAILOVER_MAX_ATTEMPTS_DEFAULT =  15;
    String  FAILOVER_SLEEPTIME_BASE_KEY = PREFIX + "failover.sleep.base.millis";
    int     FAILOVER_SLEEPTIME_BASE_DEFAULT = 500;
    String  FAILOVER_SLEEPTIME_MAX_KEY = PREFIX + "failover.sleep.max.millis";
    int     FAILOVER_SLEEPTIME_MAX_DEFAULT =  15000;
  }  
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_RETRY_POLICY_ENABLED_KEY =
      HdfsClientConfigKeys.HttpClient.RETRY_POLICY_ENABLED_KEY;
  @Deprecated
  public static final boolean DFS_HTTP_CLIENT_RETRY_POLICY_ENABLED_DEFAULT =
      HdfsClientConfigKeys.HttpClient.RETRY_POLICY_ENABLED_DEFAULT;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_RETRY_POLICY_SPEC_KEY =
      HdfsClientConfigKeys.HttpClient.RETRY_POLICY_SPEC_KEY;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_RETRY_POLICY_SPEC_DEFAULT =
      HdfsClientConfigKeys.HttpClient.RETRY_POLICY_SPEC_DEFAULT;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY =
      HdfsClientConfigKeys.HttpClient.FAILOVER_MAX_ATTEMPTS_KEY;
  @Deprecated
  public static final int     DFS_HTTP_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT =
      HdfsClientConfigKeys.HttpClient.FAILOVER_MAX_ATTEMPTS_DEFAULT;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_RETRY_MAX_ATTEMPTS_KEY =
      HdfsClientConfigKeys.HttpClient.RETRY_MAX_ATTEMPTS_KEY;
  @Deprecated
  public static final int     DFS_HTTP_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT =
      HdfsClientConfigKeys.HttpClient.RETRY_MAX_ATTEMPTS_DEFAULT;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY =
      HdfsClientConfigKeys.HttpClient.FAILOVER_SLEEPTIME_BASE_KEY;
  @Deprecated
  public static final int     DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT =
      HdfsClientConfigKeys.HttpClient.FAILOVER_SLEEPTIME_BASE_DEFAULT;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY =
      HdfsClientConfigKeys.HttpClient.FAILOVER_SLEEPTIME_MAX_KEY;
  @Deprecated
  public static final int     DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT
      = HdfsClientConfigKeys.HttpClient.FAILOVER_SLEEPTIME_MAX_DEFAULT;

  public static final String  DFS_REJECT_UNRESOLVED_DN_TOPOLOGY_MAPPING_KEY = 

  
  
  @Deprecated
  public static final String  DFS_CLIENT_RETRY_POLICY_ENABLED_KEY
      = HdfsClientConfigKeys.Retry.POLICY_ENABLED_KEY;
  public static final int     DFS_CLIENT_RETRY_WINDOW_BASE_DEFAULT
      = HdfsClientConfigKeys.Retry.WINDOW_BASE_DEFAULT;

  @Deprecated
  public static final String  DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX
      = HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX;
  @Deprecated
  public static final String  DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY
      = HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT
      = HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY
      = HdfsClientConfigKeys.Failover.SLEEPTIME_BASE_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT
      = HdfsClientConfigKeys.Failover.SLEEPTIME_BASE_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY
      = HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT
      = HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_KEY
      = HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_DEFAULT
      = HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_DEFAULT;
  @Deprecated
  public static final String  DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY
      = HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY;
  @Deprecated
  public static final int     DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT
      = HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT;

  
  
  public static final String  DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS = "dfs.client.file-block-storage-locations.timeout.millis";
  public static final int     DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS_DEFAULT = 1000;
  
  public static final String  DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY = "dfs.client.datanode-restart.timeout";
  public static final long    DFS_CLIENT_DATANODE_RESTART_TIMEOUT_DEFAULT = 30;


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/HAUtil.java
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.NameNodeProxies.ProxyAndInfo;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
  public static boolean isClientFailoverConfigured(
      Configuration conf, URI nameNodeUri) {
    String host = nameNodeUri.getHost();
    String configKey = HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX
        + "." + host;
    return conf.get(configKey) != null;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/NameNodeProxies.java
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;

    if (failoverProxyProvider != null) { // HA case
      int delay = config.getInt(
          HdfsClientConfigKeys.Failover.SLEEPTIME_BASE_KEY,
          HdfsClientConfigKeys.Failover.SLEEPTIME_BASE_DEFAULT);
      int maxCap = config.getInt(
          HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_KEY,
          HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_DEFAULT);
      int maxFailoverAttempts = config.getInt(
          HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_KEY,
          HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_DEFAULT);
      int maxRetryAttempts = config.getInt(
          HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_KEY,
          HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_DEFAULT);
      return null;
    }
    String host = nameNodeUri.getHost();
    String configKey = HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX
        + "." + host;
    try {
      @SuppressWarnings("unchecked")
      Class<FailoverProxyProvider<T>> ret = (Class<FailoverProxyProvider<T>>) conf

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/client/impl/DfsClientConf.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_DATANODE_RESTART_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY;
    hdfsTimeout = Client.getTimeout(conf);

    maxRetryAttempts = conf.getInt(
        HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_KEY,
        HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_DEFAULT);
    timeWindow = conf.getInt(
        HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY,
        HdfsClientConfigKeys.Retry.WINDOW_BASE_DEFAULT);
    retryTimesForGetLastBlockLength = conf.getInt(
        HdfsClientConfigKeys.Retry.TIMES_GET_LAST_BLOCK_LENGTH_KEY,
        HdfsClientConfigKeys.Retry.TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT);
    retryIntervalForGetLastBlockLength = conf.getInt(
        HdfsClientConfigKeys.Retry.INTERVAL_GET_LAST_BLOCK_LENGTH_KEY,
        HdfsClientConfigKeys.Retry.INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT);

    maxFailoverAttempts = conf.getInt(
        HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_KEY,
        HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_DEFAULT);
    failoverSleepBaseMillis = conf.getInt(
        HdfsClientConfigKeys.Failover.SLEEPTIME_BASE_KEY,
        HdfsClientConfigKeys.Failover.SLEEPTIME_BASE_DEFAULT);
    failoverSleepMaxMillis = conf.getInt(
        HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_KEY,
        HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_DEFAULT);

    maxBlockAcquireFailures = conf.getInt(
        DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY,
        DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT);
    prefetchSize = conf.getLong(DFS_CLIENT_READ_PREFETCH_SIZE_KEY,
        10 * defaultBlockSize);
    numCachedConnRetry = conf.getInt(DFS_CLIENT_CACHED_CONN_RETRY_KEY,
        DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT);
    numBlockWriteRetry = conf.getInt(DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY,
    fileBlockStorageLocationsTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS,
        DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS_DEFAULT);

    datanodeRestartTimeout = conf.getLong(
        DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY,

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/ha/ConfiguredFailoverProxyProvider.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
    
    this.conf = new Configuration(conf);
    int maxRetries = this.conf.getInt(
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_KEY,
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        maxRetries);
    
    int maxRetriesOnSocketTimeouts = this.conf.getInt(
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        maxRetriesOnSocketTimeouts);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/ha/IPFailoverProxyProvider.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;


    this.conf = new Configuration(conf);
    int maxRetries = this.conf.getInt(
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_KEY,
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        maxRetries);
    
    int maxRetriesOnSocketTimeouts = this.conf.getInt(
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        maxRetriesOnSocketTimeouts);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsFileSystem.java
hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/DFSTestUtil.java

package org.apache.hadoop.hdfs;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster.NameNodeInfo;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.junit.Assume;
import org.mockito.internal.util.reflection.Whitebox;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class DFSTestUtil {
    }
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX,
            logicalName), "nn1,nn2");
    conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX +
            "." + logicalName,
            ConfiguredFailoverProxyProvider.class.getName());
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    for (Map.Entry<String, List<String>> entry : nameservices.entrySet()) {
      conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX,
          entry.getKey()), Joiner.on(",").join(entry.getValue()));
      conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "."
          + entry.getKey(), ConfiguredFailoverProxyProvider.class.getName());
    }
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, Joiner.on(",")
        .join(nameservices.keySet()));

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSClientFailover.java
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.IPFailoverProxyProvider;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.net.StandardSocketFactory;
  public void testFailureWithMisconfiguredHaNNs() throws Exception {
    String logicalHost = "misconfigured-ha-uri";
    Configuration conf = new Configuration();
    conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + logicalHost,
        ConfiguredFailoverProxyProvider.class.getName());
    
    URI uri = new URI("hdfs://" + logicalHost + "/test");
    Configuration config = new HdfsConfiguration(conf);
    String logicalName = HATestUtil.getLogicalHostname(cluster);
    HATestUtil.setFailoverConfigurations(cluster, config, logicalName);
    config.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + logicalName,
        DummyLegacyFailoverProxyProvider.class.getName());
    Path p = new Path("hdfs://" + logicalName + "/");

    Configuration config = new HdfsConfiguration(conf);
    URI nnUri = cluster.getURI(0);
    config.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." +
        nnUri.getHost(),
        IPFailoverProxyProvider.class.getName());


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSClientRetries.java
    final Path dir = new Path("/testNamenodeRestart");

    if (isWebHDFS) {
      conf.setBoolean(HdfsClientConfigKeys.HttpClient.RETRY_POLICY_ENABLED_KEY, true);
    } else {
      conf.setBoolean(HdfsClientConfigKeys.Retry.POLICY_ENABLED_KEY, true);
    }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSUtil.java
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_HTTP_ADDRESS_KEY, "ns1", "nn2"), nnaddr2);

    conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + logicalHostName,
        ConfiguredFailoverProxyProvider.class.getName());
    return conf;
  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/qjournal/MiniQJMHACluster.java
package org.apache.hadoop.hdfs.qjournal;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, NAMESERVICE);
    conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX, NAMESERVICE),
        NN1 + "," + NN2);
    conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + NAMESERVICE,
        ConfiguredFailoverProxyProvider.class.getName());
    conf.set("fs.defaultFS", "hdfs://" + NAMESERVICE);
    

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/HATestUtil.java
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, logicalName);
    conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX, logicalName),
        nameNodeId1 + "," + nameNodeId2);
    conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + logicalName,
        ConfiguredFailoverProxyProvider.class.getName());
    conf.set("fs.defaultFS", "hdfs://" + logicalName);
  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestPipelinesFailover.java
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
    harness.conf.setInt(HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_KEY, 1000);

    final MiniDFSCluster cluster = harness.startCluster();
    try {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestRetryCacheWithHA.java
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
        failoverProxyProvider, RetryPolicies
        .failoverOnNetworkException(RetryPolicies.TRY_ONCE_THEN_FAIL,
            Integer.MAX_VALUE,
            HdfsClientConfigKeys.Failover.SLEEPTIME_BASE_DEFAULT,
            HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_DEFAULT));
    ClientProtocol proxy = (ClientProtocol) Proxy.newProxyInstance(
        failoverProxyProvider.getInterface().getClassLoader(),
        new Class[] { ClientProtocol.class }, dummyHandler);

