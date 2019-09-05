hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/tracing/SpanReceiverHost.java
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
@InterfaceAudience.Private
public class SpanReceiverHost implements TraceAdminProtocol {
  public static final String SPAN_RECEIVERS_CONF_SUFFIX =
    "spanreceiver.classes";
  private static final Log LOG = LogFactory.getLog(SpanReceiverHost.class);
  private static final HashMap<String, SpanReceiverHost> hosts =
      new HashMap<String, SpanReceiverHost>(1);
  private final TreeMap<Long, SpanReceiver> receivers =
      new TreeMap<Long, SpanReceiver>();
  private final String confPrefix;
  private Configuration config;
  private boolean closed = false;
  private long highestId = 1;

  private final static String LOCAL_FILE_SPAN_RECEIVER_PATH_SUFFIX =
      "local-file-span-receiver.path";

  public static SpanReceiverHost get(Configuration conf, String confPrefix) {
    synchronized (SpanReceiverHost.class) {
      SpanReceiverHost host = hosts.get(confPrefix);
      if (host != null) {
        return host;
      }
      final SpanReceiverHost newHost = new SpanReceiverHost(confPrefix);
      newHost.loadSpanReceivers(conf);
      ShutdownHookManager.get().addShutdownHook(new Runnable() {
          public void run() {
            newHost.closeReceivers();
          }
        }, 0);
      hosts.put(confPrefix, newHost);
      return newHost;
    }
  }

    return new File(tmp, nonce).getAbsolutePath();
  }

  private SpanReceiverHost(String confPrefix) {
    this.confPrefix = confPrefix;
  }

  public synchronized void loadSpanReceivers(Configuration conf) {
    config = new Configuration(conf);
    String receiverKey = confPrefix + SPAN_RECEIVERS_CONF_SUFFIX;
    String[] receiverNames = config.getTrimmedStrings(receiverKey);
    if (receiverNames == null || receiverNames.length == 0) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No span receiver names found in " + receiverKey + ".");
      }
      return;
    }
    String pathKey = confPrefix + LOCAL_FILE_SPAN_RECEIVER_PATH_SUFFIX;
    if (config.get(pathKey) == null) {
      String uniqueFile = getUniqueLocalTraceFileName();
      config.set(pathKey, uniqueFile);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Set " + pathKey + " to " + uniqueFile);
      }
    }
    for (String className : receiverNames) {
  private synchronized SpanReceiver loadInstance(String className,
      List<ConfigurationPair> extraConfig) throws IOException {
    SpanReceiverBuilder builder =
        new SpanReceiverBuilder(TraceUtils.
            wrapHadoopConf(confPrefix, config, extraConfig));
    SpanReceiver rcvr = builder.spanReceiverClass(className.trim()).build();
    if (rcvr == null) {
      throw new IOException("Failed to load SpanReceiver " + className);

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/tracing/TraceUtils.java
@InterfaceAudience.Private
public class TraceUtils {
  private static List<ConfigurationPair> EMPTY = Collections.emptyList();

  public static HTraceConfiguration wrapHadoopConf(final String prefix,
        final Configuration conf) {
    return wrapHadoopConf(prefix, conf, EMPTY);
  }

  public static HTraceConfiguration wrapHadoopConf(final String prefix,
        final Configuration conf, List<ConfigurationPair> extraConfig) {
    final HashMap<String, String> extraMap = new HashMap<String, String>();
    for (ConfigurationPair pair : extraConfig) {
      extraMap.put(pair.getKey(), pair.getValue());
        if (extraMap.containsKey(key)) {
          return extraMap.get(key);
        }
        return conf.get(prefix + key, "");
      }

      @Override
        if (extraMap.containsKey(key)) {
          return extraMap.get(key);
        }
        return conf.get(prefix + key, defaultValue);
      }
    };
  }

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/tracing/TestTraceUtils.java
import org.junit.Test;

public class TestTraceUtils {
  private static String TEST_PREFIX = "test.prefix.htrace.";

  @Test
  public void testWrappedHadoopConf() {
    String key = "sampler";
    String value = "ProbabilitySampler";
    Configuration conf = new Configuration();
    conf.set(TEST_PREFIX + key, value);
    HTraceConfiguration wrapped = TraceUtils.wrapHadoopConf(TEST_PREFIX, conf);
    assertEquals(value, wrapped.get(key));
  }

    String oldValue = "old value";
    String newValue = "new value";
    Configuration conf = new Configuration();
    conf.set(TEST_PREFIX + key, oldValue);
    LinkedList<ConfigurationPair> extraConfig =
        new LinkedList<ConfigurationPair>();
    extraConfig.add(new ConfigurationPair(key, newValue));
    HTraceConfiguration wrapped = TraceUtils.wrapHadoopConf(TEST_PREFIX, conf, extraConfig);
    assertEquals(newValue, wrapped.get(key));
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
  public DFSClient(URI nameNodeUri, ClientProtocol rpcNamenode,
      Configuration conf, FileSystem.Statistics stats)
    throws IOException {
    SpanReceiverHost.get(conf, DFSConfigKeys.DFS_CLIENT_HTRACE_PREFIX);
    traceSampler = new SamplerBuilder(TraceUtils.
        wrapHadoopConf(DFSConfigKeys.DFS_CLIENT_HTRACE_PREFIX, conf)).build();
    this.dfsClientConf = new DfsClientConf(conf);
    this.conf = conf;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  public static final String DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT =
      HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT;

  public static final String  DFS_SERVER_HTRACE_PREFIX = "dfs.htrace.";

  public static final String  DFS_CLIENT_HTRACE_PREFIX = "dfs.client.htrace.";

  public static final String  DFS_DATANODE_RESTART_REPLICA_EXPIRY_KEY = "dfs.datanode.restart.replica.expiration";
  public static final long    DFS_DATANODE_RESTART_REPLICA_EXPIRY_DEFAULT = 50;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java
    this.dnConf = new DNConf(conf);
    checkSecureConfig(dnConf, conf, resources);

    this.spanReceiverHost =
      SpanReceiverHost.get(conf, DFSConfigKeys.DFS_SERVER_HTRACE_PREFIX);

    if (dnConf.maxLockedMemory > 0) {
      if (!NativeIO.POSIX.getCacheManipulator().verifyCanMlock()) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNode.java
      startHttpServer(conf);
    }

    this.spanReceiverHost =
      SpanReceiverHost.get(conf, DFSConfigKeys.DFS_SERVER_HTRACE_PREFIX);

    loadNamesystem(conf);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/tracing/TestTraceAdmin.java
package org.apache.hadoop.tracing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.junit.Assert;
  public void testCreateAndDestroySpanReceiver() throws Exception {
    Configuration conf = new Configuration();
    conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_SERVER_HTRACE_PREFIX  +
        SpanReceiverHost.SPAN_RECEIVERS_CONF_SUFFIX, "");
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/tracing/TestTracing.java
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem dfs;

  @Test
  public void testTracing() throws Exception {
    String fileName = "testTracingDisabled.dat";
    writeTestFile(fileName);
  public static void setup() throws IOException {
    conf = new Configuration();
    conf.setLong("dfs.blocksize", 100 * 1024);
    conf.set(DFSConfigKeys.DFS_CLIENT_HTRACE_PREFIX +
        SpanReceiverHost.SPAN_RECEIVERS_CONF_SUFFIX,
        SetSpanReceiver.class.getName());
  }

  @Before

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/tracing/TestTracingShortCircuitLocalRead.java
  public void testShortCircuitTraceHooks() throws IOException {
    assumeTrue(NativeCodeLoader.isNativeCodeLoaded() && !Path.WINDOWS);
    conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_CLIENT_HTRACE_PREFIX +
        SpanReceiverHost.SPAN_RECEIVERS_CONF_SUFFIX,
        TestTracing.SetSpanReceiver.class.getName());
    conf.setLong("dfs.blocksize", 100 * 1024);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    dfs = cluster.getFileSystem();

    try {
      DFSTestUtil.createFile(dfs, TEST_PATH, TEST_LENGTH, (short)1, 5678L);

      TraceScope ts = Trace.startSpan("testShortCircuitTraceHooks", Sampler.ALWAYS);

