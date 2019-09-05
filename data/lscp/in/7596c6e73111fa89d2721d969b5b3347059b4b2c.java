hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ha/ZKFailoverController.java
  protected synchronized State getLastHealthState() {
    return lastHealthState;
  }

  protected synchronized void setLastHealthState(HealthMonitor.State newState) {
    LOG.info("Local service " + localTarget +
        " entered state: " + newState);
    lastHealthState = newState;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  public static final boolean DFS_HA_AUTO_FAILOVER_ENABLED_DEFAULT = false;
  public static final String DFS_HA_ZKFC_PORT_KEY = "dfs.ha.zkfc.port";
  public static final int DFS_HA_ZKFC_PORT_DEFAULT = 8019;
  public static final String DFS_HA_ZKFC_NN_HTTP_TIMEOUT_KEY = "dfs.ha.zkfc.nn.http.timeout.ms";
  public static final int DFS_HA_ZKFC_NN_HTTP_TIMEOUT_KEY_DEFAULT = 20000;

  public static final String DFS_ENCRYPT_DATA_TRANSFER_KEY = "dfs.encrypt.data.transfer";

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/DFSZKFailoverController.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.HealthMonitor;
import org.apache.hadoop.ha.ZKFailoverController;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos.ActiveNodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
  private final NNHAServiceTarget localNNTarget;

  private boolean isThreadDumpCaptured = false;

  @Override
  protected HAServiceTarget dataToTarget(byte[] data) {
    ActiveNodeInfo proto;
    LOG.warn(msg);
    throw new AccessControlException(msg);
  }

  private void getLocalNNThreadDump() {
    isThreadDumpCaptured = false;
    int httpTimeOut = conf.getInt(
        DFSConfigKeys.DFS_HA_ZKFC_NN_HTTP_TIMEOUT_KEY,
        DFSConfigKeys.DFS_HA_ZKFC_NN_HTTP_TIMEOUT_KEY_DEFAULT);
    if (httpTimeOut == 0) {
      return;
    }
    try {
      String stacksUrl = DFSUtil.getInfoServer(localNNTarget.getAddress(),
          conf, DFSUtil.getHttpClientScheme(conf)) + "/stacks";
      URL url = new URL(stacksUrl);
      HttpURLConnection conn = (HttpURLConnection)url.openConnection();
      conn.setReadTimeout(httpTimeOut);
      conn.setConnectTimeout(httpTimeOut);
      conn.connect();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copyBytes(conn.getInputStream(), out, 4096, true);
      StringBuilder localNNThreadDumpContent =
          new StringBuilder("-- Local NN thread dump -- \n");
      localNNThreadDumpContent.append(out);
      localNNThreadDumpContent.append("\n -- Local NN thread dump -- ");
      LOG.info(localNNThreadDumpContent);
      isThreadDumpCaptured = true;
    } catch (IOException e) {
      LOG.warn("Can't get local NN thread dump due to " + e.getMessage());
    }
  }

  @Override
  protected synchronized void setLastHealthState(HealthMonitor.State newState) {
    super.setLastHealthState(newState);
    if (getLastHealthState() == HealthMonitor.State.SERVICE_NOT_RESPONDING ||
        getLastHealthState() == HealthMonitor.State.SERVICE_UNHEALTHY) {
      getLocalNNThreadDump();
    }
  }

  @VisibleForTesting
  boolean isThreadDumpCaptured() {
    return isThreadDumpCaptured;
  }

}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/tools/TestDFSZKFailoverController.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/tools/TestDFSZKFailoverController.java
package org.apache.hadoop.hdfs.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HealthMonitor;
import org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer;
import org.apache.hadoop.ha.ZKFCTestUtil;
import org.apache.hadoop.ha.ZKFailoverController;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeResourceChecker;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.test.MultithreadedTestUtil.TestingThread;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;
import org.mockito.Mockito;

public class TestDFSZKFailoverController extends ClientBaseWithFixes {
  private Configuration conf;
  private MiniDFSCluster cluster;
  private TestContext ctx;
  private ZKFCThread thr1, thr2;
  private FileSystem fs;

  static {
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
  }
  
  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    conf.set(ZKFailoverController.ZK_QUORUM_KEY + ".ns1", hostPort);
    conf.set(DFSConfigKeys.DFS_HA_FENCE_METHODS_KEY,
        AlwaysSucceedFencer.class.getName());
    conf.setBoolean(DFSConfigKeys.DFS_HA_AUTO_FAILOVER_ENABLED_KEY, true);

    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        0);
    
    conf.setInt(DFSConfigKeys.DFS_HA_ZKFC_PORT_KEY + ".ns1.nn1", 10023);
    conf.setInt(DFSConfigKeys.DFS_HA_ZKFC_PORT_KEY + ".ns1.nn2", 10024);

    MiniDFSNNTopology topology = new MiniDFSNNTopology()
    .addNameservice(new MiniDFSNNTopology.NSConf("ns1")
        .addNN(new MiniDFSNNTopology.NNConf("nn1").setIpcPort(10021))
        .addNN(new MiniDFSNNTopology.NNConf("nn2").setIpcPort(10022)));
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(topology)
        .numDataNodes(0)
        .build();
    cluster.waitActive();

    ctx = new TestContext();
    ctx.addThread(thr1 = new ZKFCThread(ctx, 0));
    assertEquals(0, thr1.zkfc.run(new String[]{"-formatZK"}));

    thr1.start();
    waitForHAState(0, HAServiceState.ACTIVE);
    
    ctx.addThread(thr2 = new ZKFCThread(ctx, 1));
    thr2.start();
    
    ZKFCTestUtil.waitForHealthState(thr1.zkfc,
        HealthMonitor.State.SERVICE_HEALTHY, ctx);
    ZKFCTestUtil.waitForHealthState(thr2.zkfc,
        HealthMonitor.State.SERVICE_HEALTHY, ctx);
    
    fs = HATestUtil.configureFailoverFs(cluster, conf);
  }
  
  @After
  public void shutdown() throws Exception {
    cluster.shutdown();
    
    if (thr1 != null) {
      thr1.interrupt();
    }
    if (thr2 != null) {
      thr2.interrupt();
    }
    if (ctx != null) {
      ctx.stop();
    }
  }

  @Test(timeout=60000)
  public void testThreadDumpCaptureAfterNNStateChange() throws Exception {
    NameNodeResourceChecker mockResourceChecker = Mockito.mock(
        NameNodeResourceChecker.class);
    Mockito.doReturn(false).when(mockResourceChecker).hasAvailableDiskSpace();
    cluster.getNameNode(0).getNamesystem()
        .setNNResourceChecker(mockResourceChecker);
    waitForHAState(0, HAServiceState.STANDBY);
    while (!thr1.zkfc.isThreadDumpCaptured()) {
      Thread.sleep(1000);
    }
  }

  @Test(timeout=60000)
  public void testFailoverAndBackOnNNShutdown() throws Exception {
    Path p1 = new Path("/dir1");
    Path p2 = new Path("/dir2");

    fs.mkdirs(p1);
    cluster.shutdownNameNode(0);
    assertTrue(fs.exists(p1));
    fs.mkdirs(p2);
    assertEquals(AlwaysSucceedFencer.getLastFencedService().getAddress(),
        thr1.zkfc.getLocalTarget().getAddress());
    
    cluster.restartNameNode(0);
    waitForHAState(0, HAServiceState.STANDBY);
    assertTrue(fs.exists(p1));
    assertTrue(fs.exists(p2));
    cluster.shutdownNameNode(1);
    waitForHAState(0, HAServiceState.ACTIVE);

    assertTrue(fs.exists(p1));
    assertTrue(fs.exists(p2));
    assertEquals(AlwaysSucceedFencer.getLastFencedService().getAddress(),
        thr2.zkfc.getLocalTarget().getAddress());
  }
  
  @Test(timeout=30000)
  public void testManualFailover() throws Exception {
    thr2.zkfc.getLocalTarget().getZKFCProxy(conf, 15000).gracefulFailover();
    waitForHAState(0, HAServiceState.STANDBY);
    waitForHAState(1, HAServiceState.ACTIVE);

    thr1.zkfc.getLocalTarget().getZKFCProxy(conf, 15000).gracefulFailover();
    waitForHAState(0, HAServiceState.ACTIVE);
    waitForHAState(1, HAServiceState.STANDBY);
  }
  
  @Test(timeout=30000)
  public void testManualFailoverWithDFSHAAdmin() throws Exception {
    DFSHAAdmin tool = new DFSHAAdmin();
    tool.setConf(conf);
    assertEquals(0, 
        tool.run(new String[]{"-failover", "nn1", "nn2"}));
    waitForHAState(0, HAServiceState.STANDBY);
    waitForHAState(1, HAServiceState.ACTIVE);
    assertEquals(0,
        tool.run(new String[]{"-failover", "nn2", "nn1"}));
    waitForHAState(0, HAServiceState.ACTIVE);
    waitForHAState(1, HAServiceState.STANDBY);
  }

  private void waitForHAState(int nnidx, final HAServiceState state)
      throws TimeoutException, InterruptedException {
    final NameNode nn = cluster.getNameNode(nnidx);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          return nn.getRpcServer().getServiceStatus().getState() == state;
        } catch (Exception e) {
          e.printStackTrace();
          return false;
        }
      }
    }, 50, 15000);
  }

  private class ZKFCThread extends TestingThread {
    private final DFSZKFailoverController zkfc;

    public ZKFCThread(TestContext ctx, int idx) {
      super(ctx);
      this.zkfc = DFSZKFailoverController.create(
          cluster.getConfiguration(idx));
    }

    @Override
    public void doWork() throws Exception {
      try {
        assertEquals(0, zkfc.run(new String[0]));
      } catch (InterruptedException ie) {
      }
    }
  }

}

