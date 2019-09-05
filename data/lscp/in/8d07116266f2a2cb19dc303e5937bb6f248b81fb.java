hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java

  public static final String NM_RECOVERY_DIR = NM_RECOVERY_PREFIX + "dir";

  public static final String NM_RECOVERY_SUPERVISED =
      NM_RECOVERY_PREFIX + "supervised";
  public static final boolean DEFAULT_NM_RECOVERY_SUPERVISED = false;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/ContainerManagerImpl.java

    if (this.context.getNMStateStore().canRecover()
        && !this.context.getDecommissioned()) {
      if (getConfig().getBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED,
          YarnConfiguration.DEFAULT_NM_RECOVERY_SUPERVISED)) {
        return;
      }
    }

    List<ApplicationId> appIds =
        new ArrayList<ApplicationId>(applications.keySet());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/logaggregation/LogAggregationService.java
   
  private void stopAggregators() {
    threadPool.shutdown();
    boolean supervised = getConfig().getBoolean(
        YarnConfiguration.NM_RECOVERY_SUPERVISED,
        YarnConfiguration.DEFAULT_NM_RECOVERY_SUPERVISED);
    boolean shouldAbort = context.getNMStateStore().canRecover()
        && !context.getDecommissioned() && supervised;
    for (AppLogAggregator aggregator : appLogAggregators.values()) {
      if (shouldAbort) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/TestContainerManagerRecovery.java
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.LogHandler;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMMemoryStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
  public void testApplicationRecovery() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED, true);
    conf.set(YarnConfiguration.NM_ADDRESS, "localhost:1234");
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "yarn_admin_user");
    NMStateStoreService stateStore = new NMMemoryStateStoreService();
    stateStore.init(conf);
    stateStore.start();
    Context context = createContext(conf, stateStore);
    ContainerManagerImpl cm = createContainerManager(context);
    cm.init(conf);
    cm.start();

    String appUser = "app_user1";
    String modUser = "modify_user1";

    cm.stop();
    context = createContext(conf, stateStore);
    cm = createContainerManager(context);
    cm.init(conf);
    cm.start();

    cm.stop();
    context = createContext(conf, stateStore);
    cm = createContainerManager(context);
    cm.init(conf);
    cm.start();

    cm.stop();
    context = createContext(conf, stateStore);
    cm = createContainerManager(context);
    cm.init(conf);
    cm.start();
    cm.stop();
  }

  @Test
  public void testContainerCleanupOnShutdown() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId attemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cid = ContainerId.newContainerId(attemptId, 1);
    Map<String, LocalResource> localResources = Collections.emptyMap();
    Map<String, String> containerEnv = Collections.emptyMap();
    List<String> containerCmds = Collections.emptyList();
    Map<String, ByteBuffer> serviceData = Collections.emptyMap();
    Credentials containerCreds = new Credentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    containerCreds.writeTokenStorageToStream(dob);
    ByteBuffer containerTokens = ByteBuffer.wrap(dob.getData(), 0,
        dob.getLength());
    Map<ApplicationAccessType, String> acls = Collections.emptyMap();
    ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
        localResources, containerEnv, containerCmds, serviceData,
        containerTokens, acls);
    LogAggregationContext logAggregationContext =
        LogAggregationContext.newInstance("includePattern", "excludePattern");

    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED, false);
    conf.set(YarnConfiguration.NM_ADDRESS, "localhost:1234");
    Context context = createContext(conf, new NMNullStateStoreService());
    ContainerManagerImpl cm = spy(createContainerManager(context));
    cm.init(conf);
    cm.start();
    StartContainersResponse startResponse = startContainer(context, cm, cid,
        clc, logAggregationContext);
    assertEquals(1, startResponse.getSuccessfullyStartedContainers().size());
    cm.stop();
    verify(cm).handle(isA(CMgrCompletedAppsEvent.class));

    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED, false);
    NMMemoryStateStoreService memStore = new NMMemoryStateStoreService();
    memStore.init(conf);
    memStore.start();
    context = createContext(conf, memStore);
    cm = spy(createContainerManager(context));
    cm.init(conf);
    cm.start();
    startResponse = startContainer(context, cm, cid,
        clc, logAggregationContext);
    assertEquals(1, startResponse.getSuccessfullyStartedContainers().size());
    cm.stop();
    memStore.close();
    verify(cm).handle(isA(CMgrCompletedAppsEvent.class));

    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED, true);
    memStore = new NMMemoryStateStoreService();
    memStore.init(conf);
    memStore.start();
    context = createContext(conf, memStore);
    cm = spy(createContainerManager(context));
    cm.init(conf);
    cm.start();
    startResponse = startContainer(context, cm, cid,
        clc, logAggregationContext);
    assertEquals(1, startResponse.getSuccessfullyStartedContainers().size());
    cm.stop();
    memStore.close();
    verify(cm, never()).handle(isA(CMgrCompletedAppsEvent.class));
  }

  private NMContext createContext(YarnConfiguration conf,
      NMStateStoreService stateStore) {
    NMContext context = new NMContext(new NMContainerTokenSecretManager(
        conf), new NMTokenSecretManagerInNM(), null,
        new ApplicationACLsManager(conf), stateStore);

    MasterKey masterKey = new MasterKeyPBImpl();
    masterKey.setKeyId(123);
    masterKey.setBytes(ByteBuffer.wrap(new byte[] { new Integer(123)
      .byteValue() }));
    context.getContainerTokenSecretManager().setMasterKey(masterKey);
    context.getNMTokenSecretManager().setMasterKey(masterKey);
    return context;
  }

  private StartContainersResponse startContainer(Context context,
      final ContainerManagerImpl cm, ContainerId cid,
      ContainerLaunchContext clc, LogAggregationContext logAggregationContext)

