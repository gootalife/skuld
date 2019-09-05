hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
  public InetSocketAddress updateConnectAddr(String name,
                                             InetSocketAddress addr) {
    String prefix = name;
    if (HAUtil.isHAEnabled(this) && getServiceAddressConfKeys(this).contains(name)) {
      prefix = HAUtil.addSuffix(prefix, HAUtil.getRMHAId(this));
    }
    return super.updateConnectAddr(prefix, addr);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/ProtocolHATestBase.java
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.yarn.server.resourcemanager.HATestUtil;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
  protected Thread failoverThread = null;
  private volatile boolean keepRunning;

  @Before
  public void setup() throws IOException {
    failoverThread = null;
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setInt(YarnConfiguration.CLIENT_FAILOVER_MAX_ATTEMPTS, 5);
    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);
    HATestUtil.setRpcAddressForRM(RM1_NODE_ID, RM1_PORT_BASE, conf);
    HATestUtil.setRpcAddressForRM(RM2_NODE_ID, RM2_PORT_BASE, conf);

    conf.setLong(YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS, 100L);


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/TestApplicationMasterServiceProtocolOnHA.java
  public void initialize() throws Exception {
    startHACluster(0, false, false, true);
    attemptId = this.cluster.createFakeApplicationAttemptId();

    Token<AMRMTokenIdentifier> appToken =
        this.cluster.getResourceManager().getRMContext()
          .getAMRMTokenSecretManager().createAndGetAMRMToken(attemptId);
    appToken.setService(ClientRMProxy.getAMRMTokenService(this.conf));
    UserGroupInformation.setLoginUser(UserGroupInformation
        .createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));
    UserGroupInformation.getCurrentUser().addToken(appToken);
    syncToken(appToken);

    amClient = ClientRMProxy
        .createRMProxy(this.conf, ApplicationMasterProtocol.class);
  }

  @After

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/TestRMFailover.java
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.AdminService;
import org.apache.hadoop.yarn.server.resourcemanager.HATestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServer;
import org.junit.After;
  private MiniYARNCluster cluster;
  private ApplicationId fakeAppId;

  @Before
  public void setup() throws IOException {
    fakeAppId = ApplicationId.newInstance(System.currentTimeMillis(), 0);
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);
    HATestUtil.setRpcAddressForRM(RM1_NODE_ID, RM1_PORT_BASE, conf);
    HATestUtil.setRpcAddressForRM(RM2_NODE_ID, RM2_PORT_BASE, conf);

    conf.setLong(YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS, 100L);


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/conf/TestYarnConfiguration.java
import java.net.SocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

        serverAddress);

    assertTrue(resourceTrackerConnectAddress.toString().startsWith("yo.yo.yo"));

    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS, "yo.yo.yo");
    conf.set(YarnConfiguration.NM_BIND_HOST, "0.0.0.0");
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_ID, "rm1");

    serverAddress = new InetSocketAddress(
        YarnConfiguration.DEFAULT_NM_LOCALIZER_ADDRESS.split(":")[0],
        Integer.valueOf(YarnConfiguration.DEFAULT_NM_LOCALIZER_ADDRESS.split(":")[1]));

    InetSocketAddress localizerAddress = conf.updateConnectAddr(
        YarnConfiguration.NM_BIND_HOST,
        YarnConfiguration.NM_LOCALIZER_ADDRESS,
        YarnConfiguration.DEFAULT_NM_LOCALIZER_ADDRESS,
        serverAddress);

    assertTrue(localizerAddress.toString().startsWith("yo.yo.yo"));
    assertNull(conf.get(
        HAUtil.addSuffix(YarnConfiguration.NM_LOCALIZER_ADDRESS, "rm1")));
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ApplicationMasterService.java
  private static final Log LOG = LogFactory.getLog(ApplicationMasterService.class);
  private final AMLivelinessMonitor amLivelinessMonitor;
  private YarnScheduler rScheduler;
  private InetSocketAddress masterServiceAddress;
  private Server server;
  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    masterServiceAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);

    Configuration serverConf = conf;
    }
    
    this.server.start();
    this.masterServiceAddress =
        conf.updateConnectAddr(YarnConfiguration.RM_BIND_HOST,
                               YarnConfiguration.RM_SCHEDULER_ADDRESS,
                               YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,

  @Private
  public InetSocketAddress getBindAddress() {
    return this.masterServiceAddress;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/HATestUtil.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/HATestUtil.java
package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class HATestUtil {

  public static void setRpcAddressForRM(String rmId, int base,
      Configuration conf) {
    for (String confKey : YarnConfiguration.getServiceAddressConfKeys(conf)) {
      setConfForRM(rmId, confKey, "0.0.0.0:" + (base +
          YarnConfiguration.getRMDefaultPortNumber(confKey, conf)), conf);
    }
  }

  public static void setConfForRM(String rmId, String prefix, String value,
      Configuration conf) {
    conf.set(HAUtil.addSuffix(prefix, rmId), value);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMEmbeddedElector.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Before;
import org.junit.Test;
  private Configuration conf;
  private AtomicBoolean callbackCalled;

  @Before
  public void setup() throws IOException {
    conf = new YarnConfiguration();

    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);
    conf.set(YarnConfiguration.RM_HA_ID, RM1_NODE_ID);
    HATestUtil.setRpcAddressForRM(RM1_NODE_ID, RM1_PORT_BASE, conf);
    HATestUtil.setRpcAddressForRM(RM2_NODE_ID, RM2_PORT_BASE, conf);

    conf.setLong(YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS, 100L);


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-tests/src/test/java/org/apache/hadoop/yarn/server/MiniYARNCluster.java
  }

  private synchronized void initResourceManager(int index, Configuration conf) {
    Configuration newConf = resourceManagers.length > 1 ?
        new YarnConfiguration(conf) : conf;
    if (HAUtil.isHAEnabled(newConf)) {
      newConf.set(YarnConfiguration.RM_HA_ID, rmIds[index]);
    }
    resourceManagers[index].init(newConf);
    resourceManagers[index].getRMContext().getDispatcher().register(
        RMAppAttemptEventType.class,
        new EventHandler<RMAppAttemptEvent>() {
    } catch (Throwable t) {
      throw new YarnRuntimeException(t);
    }
    Configuration conf = resourceManagers[index].getConfig();
    LOG.info("MiniYARN ResourceManager address: " +
        conf.get(YarnConfiguration.RM_ADDRESS));
    LOG.info("MiniYARN ResourceManager web address: " +
        WebAppUtils.getRMWebAppURLWithoutScheme(conf));
  }

  @InterfaceAudience.Private
      resourceManagers[index].stop();
      resourceManagers[index] = null;
    }
    resourceManagers[index] = new ResourceManager();
    initResourceManager(index, getConfig());
    startResourceManager(index);
    @Override
    protected synchronized void serviceStart() throws Exception {
      startResourceManager(index);
      Configuration conf = resourceManagers[index].getConfig();
      LOG.info("MiniYARN ResourceManager address: " +
          conf.get(YarnConfiguration.RM_ADDRESS));
      LOG.info("MiniYARN ResourceManager web address: " + WebAppUtils
          .getRMWebAppURLWithoutScheme(conf));
      super.serviceStart();
    }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-tests/src/test/java/org/apache/hadoop/yarn/server/TestMiniYarnCluster.java
package org.apache.hadoop.yarn.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.HATestUtil;
import org.junit.Assert;
import org.junit.Test;

      }
    }
  }

  @Test
  public void testMultiRMConf() {
    String RM1_NODE_ID = "rm1", RM2_NODE_ID = "rm2";
    int RM1_PORT_BASE = 10000, RM2_PORT_BASE = 20000;
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_CLUSTER_ID, "yarn-test-cluster");
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);
    HATestUtil.setRpcAddressForRM(RM1_NODE_ID, RM1_PORT_BASE, conf);
    HATestUtil.setRpcAddressForRM(RM2_NODE_ID, RM2_PORT_BASE, conf);
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_USE_RPC, true);

    MiniYARNCluster cluster =
        new MiniYARNCluster(TestMiniYarnCluster.class.getName(),
            2, 0, 1, 1);
    cluster.init(conf);
    Configuration conf1 = cluster.getResourceManager(0).getConfig(),
        conf2 = cluster.getResourceManager(1).getConfig();
    Assert.assertFalse(conf1 == conf2);
    Assert.assertEquals("0.0.0.0:18032",
        conf1.get(HAUtil.addSuffix(YarnConfiguration.RM_ADDRESS, RM1_NODE_ID)));
    Assert.assertEquals("0.0.0.0:28032",
        conf1.get(HAUtil.addSuffix(YarnConfiguration.RM_ADDRESS, RM2_NODE_ID)));
    Assert.assertEquals("rm1", conf1.get(YarnConfiguration.RM_HA_ID));

    Assert.assertEquals("0.0.0.0:18032",
        conf2.get(HAUtil.addSuffix(YarnConfiguration.RM_ADDRESS, RM1_NODE_ID)));
    Assert.assertEquals("0.0.0.0:28032",
        conf2.get(HAUtil.addSuffix(YarnConfiguration.RM_ADDRESS, RM2_NODE_ID)));
    Assert.assertEquals("rm2", conf2.get(YarnConfiguration.RM_HA_ID));
  }
}

