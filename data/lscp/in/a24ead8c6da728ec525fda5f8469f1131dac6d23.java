hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java

  public static final String RM_ZK_RETRY_INTERVAL_MS =
      RM_ZK_PREFIX + "retry-interval-ms";
  public static final int DEFAULT_RM_ZK_RETRY_INTERVAL_MS = 1000;

  public static final String RM_ZK_TIMEOUT_MS = RM_ZK_PREFIX + "timeout-ms";
  public static final int DEFAULT_RM_ZK_TIMEOUT_MS = 10000;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/ZKRMStateStore.java
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.EpochPBImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
public class ZKRMStateStore extends RMStateStore {

  public static final Log LOG = LogFactory.getLog(ZKRMStateStore.class);
  private final SecureRandom random = new SecureRandom();

  protected static final String ROOT_ZNODE_NAME = "ZKRMStateRoot";
  protected static final Version CURRENT_VERSION_INFO = Version
      .newInstance(1, 2);
  private static final String RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME =
      "RMDelegationTokensRoot";
  private static final String RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME =
      "RMDTSequentialNumber";
  private static final String RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME =
      "RMDTMasterKeysRoot";

  private String zkHostPort = null;
  private int numRetries;
  private int zkSessionTimeout;
  @VisibleForTesting
  int zkRetryInterval;

  private String zkRootNodePath;
  private String rmAppRoot;
  private String rmDTSecretManagerRoot;
  @VisibleForTesting
  protected String znodeWorkingPath;

  private static final String FENCING_LOCK = "RM_ZK_FENCING_LOCK";
  private boolean useDefaultFencingScheme = false;
  private String fencingNodePath;
  private Thread verifyActiveStatusThread;

  private List<ACL> zkAcl;
  private List<ZKUtil.ZKAuthInfo> zkAuths;
  @VisibleForTesting
  List<ACL> zkRootNodeAcl;
  private String zkRootNodeUsername;
  private final String zkRootNodePassword = Long.toString(random.nextLong());
  public static final int CREATE_DELETE_PERMS =
      ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE;
  private final String zkRootNodeAuthScheme =
      new DigestAuthenticationProvider().getScheme();

  @VisibleForTesting
  protected CuratorFramework curatorFramework;
  @Unstable
  protected List<ACL> constructZkRootNodeACL(
      Configuration conf, List<ACL> sourceACLs) throws NoSuchAlgorithmException {
    List<ACL> zkRootNodeAcl = new ArrayList<>();
    for (ACL acl : sourceACLs) {
      zkRootNodeAcl.add(new ACL(
          ZKUtil.removeSpecificPerms(acl.getPerms(), CREATE_DELETE_PERMS),
      zkRetryInterval = zkSessionTimeout / numRetries;
    } else {
      zkRetryInterval =
          conf.getInt(YarnConfiguration.RM_ZK_RETRY_INTERVAL_MS,
              YarnConfiguration.DEFAULT_RM_ZK_RETRY_INTERVAL_MS);
    }


    fencingNodePath = getNodePath(zkRootNodePath, FENCING_LOCK);
    if (HAUtil.isHAEnabled(conf)) {
      String zkRootNodeAclConf = HAUtil.getConfValueForRMInstance
          (YarnConfiguration.ZK_RM_STATE_STORE_ROOT_NODE_ACL, conf);

    createRootDirRecursively(znodeWorkingPath);
    create(zkRootNodePath);
    if (HAUtil.isHAEnabled(getConfig())){
      fence();
      verifyActiveStatusThread = new VerifyActiveStatusThread();
      verifyActiveStatusThread.start();
    }
    create(rmAppRoot);
    create(rmDTSecretManagerRoot);
    create(dtMasterKeysRootPath);
    create(delegationTokensRootPath);
    create(dtSequenceNumberPath);
    create(amrmTokenSecretManagerRoot);
  }

  private void logRootNodeAcls(String prefix) throws Exception {
    Stat getStat = new Stat();
    List<ACL> getAcls = getACL(zkRootNodePath);

    StringBuilder builder = new StringBuilder();
    builder.append(prefix);
      logRootNodeAcls("Before fencing\n");
    }

    curatorFramework.setACL().withACL(zkRootNodeAcl).forPath(zkRootNodePath);
    delete(fencingNodePath);

    if (LOG.isTraceEnabled()) {
      logRootNodeAcls("After fencing\n");
    }
  }

  @Override
  protected synchronized void closeInternal() throws Exception {
    if (verifyActiveStatusThread != null) {
      verifyActiveStatusThread.interrupt();
      verifyActiveStatusThread.join(1000);
    }
    curatorFramework.close();
  }

  @Override
    String versionNodePath = getNodePath(zkRootNodePath, VERSION_NODE);
    byte[] data =
        ((VersionPBImpl) CURRENT_VERSION_INFO).getProto().toByteArray();
    if (exists(versionNodePath)) {
      safeSetData(versionNodePath, data, -1);
    } else {
      safeCreate(versionNodePath, data, zkAcl, CreateMode.PERSISTENT);
    }
  }

  protected synchronized Version loadVersion() throws Exception {
    String versionNodePath = getNodePath(zkRootNodePath, VERSION_NODE);

    if (exists(versionNodePath)) {
      byte[] data = getData(versionNodePath);
      return new VersionPBImpl(VersionProto.parseFrom(data));
    }
    return null;
  }
  public synchronized long getAndIncrementEpoch() throws Exception {
    String epochNodePath = getNodePath(zkRootNodePath, EPOCH_NODE);
    long currentEpoch = 0;
    if (exists(epochNodePath)) {
      byte[] data = getData(epochNodePath);
      Epoch epoch = new EpochPBImpl(EpochProto.parseFrom(data));
      currentEpoch = epoch.getEpoch();
      byte[] storeData = Epoch.newInstance(currentEpoch + 1).getProto()
          .toByteArray();
      safeSetData(epochNodePath, storeData, -1);
    } else {
      byte[] storeData = Epoch.newInstance(currentEpoch + 1).getProto()
          .toByteArray();
      safeCreate(epochNodePath, storeData, zkAcl, CreateMode.PERSISTENT);
    }
    return currentEpoch;
  }

  private void loadAMRMTokenSecretManagerState(RMState rmState)
      throws Exception {
    byte[] data = getData(amrmTokenSecretManagerRoot);
    if (data == null) {
      LOG.warn("There is no data saved");
      return;

  private void loadRMDelegationKeyState(RMState rmState) throws Exception {
    List<String> childNodes =
        getChildren(dtMasterKeysRootPath);
    for (String childNodeName : childNodes) {
      String childNodePath = getNodePath(dtMasterKeysRootPath, childNodeName);
      byte[] childData = getData(childNodePath);

      if (childData == null) {
        LOG.warn("Content of " + childNodePath + " is broken.");
  }

  private void loadRMSequentialNumberState(RMState rmState) throws Exception {
    byte[] seqData = getData(dtSequenceNumberPath);
    if (seqData != null) {
      ByteArrayInputStream seqIs = new ByteArrayInputStream(seqData);
      DataInputStream seqIn = new DataInputStream(seqIs);

  private void loadRMDelegationTokenState(RMState rmState) throws Exception {
    List<String> childNodes =
        getChildren(delegationTokensRootPath);
    for (String childNodeName : childNodes) {
      String childNodePath =
          getNodePath(delegationTokensRootPath, childNodeName);
      byte[] childData = getData(childNodePath);

      if (childData == null) {
        LOG.warn("Content of " + childNodePath + " is broken.");
  }

  private synchronized void loadRMAppState(RMState rmState) throws Exception {
    List<String> childNodes = getChildren(rmAppRoot);
    for (String childNodeName : childNodes) {
      String childNodePath = getNodePath(rmAppRoot, childNodeName);
      byte[] childData = getData(childNodePath);
      if (childNodeName.startsWith(ApplicationId.appIdStrPrefix)) {
        if (LOG.isDebugEnabled()) {
      ApplicationId appId)
      throws Exception {
    String appPath = getNodePath(rmAppRoot, appId.toString());
    List<String> attempts = getChildren(appPath);
    for (String attemptIDStr : attempts) {
      if (attemptIDStr.startsWith(ApplicationAttemptId.appAttemptIdStrPrefix)) {
        String attemptPath = getNodePath(appPath, attemptIDStr);
        byte[] attemptData = getData(attemptPath);

        ApplicationAttemptStateDataPBImpl attemptState =
            new ApplicationAttemptStateDataPBImpl(
      LOG.debug("Storing info for app: " + appId + " at: " + nodeCreatePath);
    }
    byte[] appStateData = appStateDataPB.getProto().toByteArray();
    safeCreate(nodeCreatePath, appStateData, zkAcl,
        CreateMode.PERSISTENT);

  }
    }
    byte[] appStateData = appStateDataPB.getProto().toByteArray();

    if (exists(nodeUpdatePath)) {
      safeSetData(nodeUpdatePath, appStateData, -1);
    } else {
      safeCreate(nodeUpdatePath, appStateData, zkAcl,
          CreateMode.PERSISTENT);
      LOG.debug(appId + " znode didn't exist. Created a new znode to"
          + " update the application state.");
          + nodeCreatePath);
    }
    byte[] attemptStateData = attemptStateDataPB.getProto().toByteArray();
    safeCreate(nodeCreatePath, attemptStateData, zkAcl,
        CreateMode.PERSISTENT);
  }

    }
    byte[] attemptStateData = attemptStateDataPB.getProto().toByteArray();

    if (exists(nodeUpdatePath)) {
      safeSetData(nodeUpdatePath, attemptStateData, -1);
    } else {
      safeCreate(nodeUpdatePath, attemptStateData, zkAcl,
          CreateMode.PERSISTENT);
      LOG.debug(appAttemptId + " znode didn't exist. Created a new znode to"
          + " update the application attempt state.");
    String appId = appState.getApplicationSubmissionContext().getApplicationId()
        .toString();
    String appIdRemovePath = getNodePath(rmAppRoot, appId);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing info for app: " + appId + " at: " + appIdRemovePath
          + " and its attempts.");
    }

    for (ApplicationAttemptId attemptId : appState.attempts.keySet()) {
      String attemptRemovePath = getNodePath(appIdRemovePath, attemptId.toString());
      safeDelete(attemptRemovePath);
    }
    safeDelete(appIdRemovePath);
  }

  @Override
  protected synchronized void storeRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception {
    SafeTransaction trx = new SafeTransaction();
    addStoreOrUpdateOps(trx, rmDTIdentifier, renewDate, false);
    trx.commit();
  }

  @Override
      LOG.debug("Removing RMDelegationToken_"
          + rmDTIdentifier.getSequenceNumber());
    }
    safeDelete(nodeRemovePath);
  }

  @Override
  protected synchronized void updateRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception {
    SafeTransaction trx = new SafeTransaction();
    String nodeRemovePath =
        getNodePath(delegationTokensRootPath, DELEGATION_TOKEN_PREFIX
            + rmDTIdentifier.getSequenceNumber());
    if (exists(nodeRemovePath)) {
      addStoreOrUpdateOps(trx, rmDTIdentifier, renewDate, true);
    } else {
      addStoreOrUpdateOps(trx, rmDTIdentifier, renewDate, false);
      LOG.debug("Attempted to update a non-existing znode " + nodeRemovePath);
    }
    trx.commit();
  }

  private void addStoreOrUpdateOps(SafeTransaction trx,
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
      boolean isUpdate) throws Exception {
      }

      if (isUpdate) {
        trx.setData(nodeCreatePath, identifierData.toByteArray(), -1);
      } else {
        trx.create(nodeCreatePath, identifierData.toByteArray(), zkAcl,
            CreateMode.PERSISTENT);
        seqOut.writeInt(rmDTIdentifier.getSequenceNumber());
        if (LOG.isDebugEnabled()) {
              dtSequenceNumberPath + ". SequenceNumber: "
              + rmDTIdentifier.getSequenceNumber());
        }
        trx.setData(dtSequenceNumberPath, seqOs.toByteArray(), -1);
      }
    } finally {
      seqOs.close();
    }
    delegationKey.write(fsOut);
    try {
      safeCreate(nodeCreatePath, os.toByteArray(), zkAcl,
          CreateMode.PERSISTENT);
    } finally {
      os.close();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing RMDelegationKey_" + delegationKey.getKeyId());
    }
    safeDelete(nodeRemovePath);
  }

  @Override
  public synchronized void deleteStore() throws Exception {
    delete(zkRootNodePath);
  }

  @Override
  public synchronized void removeApplication(ApplicationId removeAppId)
      throws Exception {
    String appIdRemovePath = getNodePath(rmAppRoot, removeAppId.toString());
    delete(appIdRemovePath);
  }

  @VisibleForTesting
  String getNodePath(String root, String nodeName) {
    return (root + "/" + nodeName);
  }

  @Override
  public synchronized void storeOrUpdateAMRMTokenSecretManagerState(
      AMRMTokenSecretManagerState amrmTokenSecretManagerState, boolean isUpdate)
      throws Exception {
    AMRMTokenSecretManagerState data =
        AMRMTokenSecretManagerState.newInstance(amrmTokenSecretManagerState);
    byte[] stateData = data.getProto().toByteArray();
    safeSetData(amrmTokenSecretManagerRoot, stateData, -1);
  }

  private void createRootDirRecursively(String path) throws Exception {
    String pathParts[] = path.split("/");
    Preconditions.checkArgument(pathParts.length >= 1 && pathParts[0].isEmpty(),
        "Invalid path: %s", path);
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i < pathParts.length; i++) {
      sb.append("/").append(pathParts[i]);
      create(sb.toString());
    }
  }

  private void createConnection() throws Exception {
    CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
    builder = builder.connectString(zkHostPort)
        .connectionTimeoutMs(zkSessionTimeout)
        .retryPolicy(new RetryNTimes(numRetries, zkRetryInterval));

    List<AuthInfo> authInfos = new ArrayList<>();
    for (ZKUtil.ZKAuthInfo zkAuth : zkAuths) {
      authInfos.add(new AuthInfo(zkAuth.getScheme(), zkAuth.getAuth()));
    }
    if (useDefaultFencingScheme) {
      byte[] defaultFencingAuth =
          (zkRootNodeUsername + ":" + zkRootNodePassword).getBytes(
              Charset.forName("UTF-8"));
      authInfos.add(new AuthInfo(zkRootNodeAuthScheme, defaultFencingAuth));
    }
    builder = builder.authorization(authInfos);

    curatorFramework = builder.build();
    curatorFramework.start();
  }

  @VisibleForTesting
  byte[] getData(final String path) throws Exception {
    return curatorFramework.getData().forPath(path);
  }

  private List<ACL> getACL(final String path) throws Exception {
    return curatorFramework.getACL().forPath(path);
  }

  private List<String> getChildren(final String path) throws Exception {
    return curatorFramework.getChildren().forPath(path);
  }

  private boolean exists(final String path) throws Exception {
    return curatorFramework.checkExists().forPath(path) != null;
  }

  @VisibleForTesting
  void create(final String path) throws Exception {
    if (!exists(path)) {
      curatorFramework.create()
          .withMode(CreateMode.PERSISTENT).withACL(zkAcl)
          .forPath(path, null);
    }
  }

  @VisibleForTesting
  void delete(final String path) throws Exception {
    if (exists(path)) {
      curatorFramework.delete().deletingChildrenIfNeeded().forPath(path);
    }
  }

  private void safeCreate(String path, byte[] data, List<ACL> acl,
      CreateMode mode) throws Exception {
    if (!exists(path)) {
      SafeTransaction transaction = new SafeTransaction();
      transaction.create(path, data, acl, mode);
      transaction.commit();
    }
  }

  private void safeDelete(final String path) throws Exception {
    if (exists(path)) {
      SafeTransaction transaction = new SafeTransaction();
      transaction.delete(path);
      transaction.commit();
    }
  }

  private void safeSetData(String path, byte[] data, int version)
      throws Exception {
    SafeTransaction transaction = new SafeTransaction();
    transaction.setData(path, data, version);
    transaction.commit();
  }

  private class SafeTransaction {
    private CuratorTransactionFinal transactionFinal;

    SafeTransaction() throws Exception {
      CuratorTransaction transaction = curatorFramework.inTransaction();
      transactionFinal =
          transaction.create()
              .withMode(CreateMode.PERSISTENT).withACL(zkAcl)
              .forPath(fencingNodePath, new byte[0]).and();
    }

    public void commit() throws Exception {
      transactionFinal = transactionFinal.delete()
          .forPath(fencingNodePath).and();
      transactionFinal.commit();
    }

    public void create(String path, byte[] data, List<ACL> acl, CreateMode mode)
        throws Exception {
      transactionFinal = transactionFinal.create()
          .withMode(mode).withACL(acl).forPath(path, data).and();
    }

    public void delete(String path) throws Exception {
      transactionFinal = transactionFinal.delete().forPath(path).and();
    }

    public void setData(String path, byte[] data, int version)
        throws Exception {
      transactionFinal = transactionFinal.setData()
          .withVersion(version).forPath(path, data).and();
    }
  }

  private class VerifyActiveStatusThread extends Thread {
    VerifyActiveStatusThread() {
      super(VerifyActiveStatusThread.class.getName());
    }
          if(isFencedState()) {
            break;
          }
          new SafeTransaction().commit();
          Thread.sleep(zkSessionTimeout);
        }
      } catch (InterruptedException ie) {
      }
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/RMStateStoreTestBase.java
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class RMStateStoreTestBase {

  public static final Log LOG = LogFactory.getLog(RMStateStoreTestBase.class);


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/TestZKRMStateStore.java

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import javax.crypto.SecretKey;

public class TestZKRMStateStore extends RMStateStoreTestBase {

  public static final Log LOG = LogFactory.getLog(TestZKRMStateStore.class);
  private static final int ZK_TIMEOUT_MS = 1000;
  private TestingServer curatorTestingServer;
  private CuratorFramework curatorFramework;

  @Before
  public void setupCuratorServer() throws Exception {
    curatorTestingServer = new TestingServer();
    curatorTestingServer.start();
    curatorFramework = CuratorFrameworkFactory.builder()
        .connectString(curatorTestingServer.getConnectString())
        .retryPolicy(new RetryNTimes(100, 100))
        .build();
    curatorFramework.start();
  }

  @After
  public void cleanupCuratorServer() throws IOException {
    curatorFramework.close();
    curatorTestingServer.stop();
  }

  class TestZKRMStateStoreTester implements RMStateStoreHelper {

    TestZKRMStateStoreInternal store;
    String workingZnode;

        assertTrue(znodeWorkingPath.equals(workingZnode));
      }

      public String getVersionNode() {
        return znodeWorkingPath + "/" + ROOT_ZNODE_NAME + "/" + VERSION_NODE;
      }
      public void testRetryingCreateRootDir() throws Exception {
        create(znodeWorkingPath);
      }

    }
    public RMStateStore getRMStateStore() throws Exception {
      YarnConfiguration conf = new YarnConfiguration();
      workingZnode = "/jira/issue/3077/rmstore";
      conf.set(YarnConfiguration.RM_ZK_ADDRESS,
          curatorTestingServer.getConnectString());
      conf.set(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH, workingZnode);
      this.store = new TestZKRMStateStoreInternal(conf, workingZnode);
      return this.store;
    }

    @Override
    public boolean isFinalStateValid() throws Exception {
      return 1 ==
          curatorFramework.getChildren().forPath(store.znodeWorkingPath).size();
    }

    @Override
    public void writeVersion(Version version) throws Exception {
      curatorFramework.setData().withVersion(-1)
          .forPath(store.getVersionNode(),
              ((VersionPBImpl) version).getProto().toByteArray());
    }

    @Override
    }

    public boolean appExists(RMApp app) throws Exception {
      return null != curatorFramework.checkExists()
          .forPath(store.getAppNode(app.getApplicationId().toString()));
    }
  }

      public RMStateStore getRMStateStore() throws Exception {
        YarnConfiguration conf = new YarnConfiguration();
        workingZnode = "/jira/issue/3077/rmstore";
        conf.set(YarnConfiguration.RM_ZK_ADDRESS,
            curatorTestingServer.getConnectString());
        conf.set(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH, workingZnode);
        this.store = new TestZKRMStateStoreInternal(conf, workingZnode) {
          Version storedVersion = null;

    conf.set(YarnConfiguration.RM_HA_IDS, rmIds);
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.RM_STORE, ZKRMStateStore.class.getName());
    conf.set(YarnConfiguration.RM_ZK_ADDRESS,
        curatorTestingServer.getConnectString());
    conf.setInt(YarnConfiguration.RM_ZK_TIMEOUT_MS, ZK_TIMEOUT_MS);
    conf.set(YarnConfiguration.RM_HA_ID, rmId);
    conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:0");

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/TestZKRMStateStorePerf.java
import java.util.Map;
import java.util.Set;
import javax.crypto.SecretKey;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
  private ZKRMStateStore store;
  private AMRMTokenSecretManager appTokenMgr;
  private ClientToAMTokenSecretManagerInRM clientToAMTokenMgr;
  private TestingServer curatorTestingServer;

  @Before
  public void setUpZKServer() throws Exception {
    curatorTestingServer = new TestingServer();
  }

  @After
    if (appTokenMgr != null) {
      appTokenMgr.stop();
    }
    curatorTestingServer.stop();
  }

  private void initStore(String hostPort) {
    RMContext rmContext = mock(RMContext.class);

    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_ZK_ADDRESS,
        optHostPort.or(curatorTestingServer.getConnectString()));
    conf.set(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH, workingZnode);

    store = new ZKRMStateStore();

    if (launchLocalZK) {
      try {
        setUpZKServer();
      } catch (Exception e) {
        System.err.println("failed to setup. : " + e.getMessage());
        return -1;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/TestZKRMStateStoreZKClientConnections.java

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreTestBase.TestDispatcher;
import org.apache.hadoop.util.ZKUtil;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestZKRMStateStoreZKClientConnections {
  private Log LOG =
      LogFactory.getLog(TestZKRMStateStoreZKClientConnections.class);

  private static final int ZK_TIMEOUT_MS = 1000;
  private static final String DIGEST_USER_PASS="test-user:test-password";
  private static final String TEST_AUTH_GOOD = "digest:" + DIGEST_USER_PASS;
  private static final String DIGEST_USER_HASH;
  }
  private static final String TEST_ACL = "digest:" + DIGEST_USER_HASH + ":rwcda";

  private TestingServer testingServer;

  @Before
  public void setupZKServer() throws Exception {
    testingServer = new TestingServer();
    testingServer.start();
  }

  @After
  public void cleanupZKServer() throws Exception {
    testingServer.stop();
  }

  class TestZKClient {

    ZKRMStateStore store;

    protected class TestZKRMStateStore extends ZKRMStateStore {

        start();
        assertTrue(znodeWorkingPath.equals(workingZnode));
      }
    }

    public RMStateStore getRMStateStore(Configuration conf) throws Exception {
      String workingZnode = "/Test";
      conf.set(YarnConfiguration.RM_ZK_ADDRESS,
          testingServer.getConnectString());
      conf.set(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH, workingZnode);
      this.store = new TestZKRMStateStore(conf, workingZnode);
      return this.store;
    store.setRMDispatcher(dispatcher);
    final AtomicBoolean assertionFailedInThread = new AtomicBoolean(false);

    testingServer.stop();
    Thread clientThread = new Thread() {
      @Override
      public void run() {
        try {
          store.getData(path);
        } catch (Exception e) {
          e.printStackTrace();
          assertionFailedInThread.set(true);
      }
    };
    Thread.sleep(2000);
    testingServer.start();
    clientThread.join();
    Assert.assertFalse(assertionFailedInThread.get());
  }

  @Test(timeout = 20000)
  public void testSetZKAcl() {
    TestZKClient zkClientTester = new TestZKClient();
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_ZK_ACL, "world:anyone:rwca");
    try {
      zkClientTester.store.delete(zkClientTester.store
          .znodeWorkingPath);
      fail("Shouldn't be able to delete path");
    } catch (Exception e) {/* expected behavior */
    }

