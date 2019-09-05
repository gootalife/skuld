hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
      this.namenode = rpcNamenode;
      dtService = null;
    } else {
      boolean noRetries = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_TEST_NO_PROXY_RETRIES,
          DFSConfigKeys.DFS_CLIENT_TEST_NO_PROXY_RETRIES_DEFAULT);
      Preconditions.checkArgument(nameNodeUri != null,
          "null URI");
      proxyInfo = NameNodeProxies.createProxy(conf, nameNodeUri,
          ClientProtocol.class, nnFallbackToSimpleAuth, !noRetries);
      this.dtService = proxyInfo.getDelegationTokenService();
      this.namenode = proxyInfo.getProxy();
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java

import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
  public static final String  DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY = "dfs.client.test.drop.namenode.response.number";
  public static final int     DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_DEFAULT = 0;

  @VisibleForTesting
  public static final String  DFS_CLIENT_TEST_NO_PROXY_RETRIES =
      "dfs.client.test.no.proxy.retries";
  @VisibleForTesting
  public static final boolean DFS_CLIENT_TEST_NO_PROXY_RETRIES_DEFAULT = false;

  public static final String  DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY =
      "dfs.client.slow.io.warning.threshold.ms";
  public static final long    DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT = 30000;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/NameNodeProxies.java
  public static <T> ProxyAndInfo<T> createProxy(Configuration conf,
      URI nameNodeUri, Class<T> xface, AtomicBoolean fallbackToSimpleAuth)
      throws IOException {
    return createProxy(conf, nameNodeUri, xface, fallbackToSimpleAuth, true);
  }

  @SuppressWarnings("unchecked")
  public static <T> ProxyAndInfo<T> createProxy(Configuration conf,
      URI nameNodeUri, Class<T> xface, AtomicBoolean fallbackToSimpleAuth,
      boolean withRetries)
      throws IOException {
    AbstractNNFailoverProxyProvider<T> failoverProxyProvider =
        createFailoverProxyProvider(conf, nameNodeUri, xface, true,
          fallbackToSimpleAuth);
    if (failoverProxyProvider == null) {
      return createNonHAProxy(conf, NameNode.getAddress(nameNodeUri), xface,
          UserGroupInformation.getCurrentUser(), withRetries,
          fallbackToSimpleAuth);
    } else {
      DfsClientConf config = new DfsClientConf(conf);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestFileCreation.java
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
    Configuration conf = new HdfsConfiguration();
    SimulatedFSDataset.setFactory(conf);
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);

    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_TEST_NO_PROXY_RETRIES, true);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = cluster.getFileSystem();

      } catch (IOException abce) {
        GenericTestUtils.assertExceptionContains("Failed to CREATE_FILE", abce);
      }
      assertCounter("AlreadyBeingCreatedExceptionNumOps",
          1L, getMetrics(metricsName));
      FSDataOutputStream stm2 = fs2.create(p, true);
      stm2.write(2);
      stm2.close();

