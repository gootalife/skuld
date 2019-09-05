hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/MockRM.java
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null);
  }
  
  public RMApp submitApp(Resource resource, String name, String user,
      Map<ApplicationAccessType, String> acls, String queue) throws Exception {
    return submitApp(resource, name, user, acls, false, queue,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null,
          true, false, false, null, 0, null, true);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
      Map<ApplicationAccessType, String> acls, String queue, 
      boolean waitForAccepted) throws Exception {
      Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
      int maxAppAttempts, Credentials ts, String appType,
      boolean waitForAccepted, boolean keepContainers) throws Exception {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(masterMemory);
    return submitApp(resource, name, user, acls, unmanaged, queue,
        maxAppAttempts, ts, appType, waitForAccepted, keepContainers,
        false, null, 0, null, true);
  }

  public RMApp submitApp(int masterMemory, long attemptFailuresValidityInterval)
      throws Exception {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(masterMemory);
    return submitApp(resource, "", UserGroupInformation.getCurrentUser()
      .getShortUserName(), null, false, null,
      super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null, true, false,
      int maxAppAttempts, Credentials ts, String appType,
      boolean waitForAccepted, boolean keepContainers, boolean isAppIdProvided,
      ApplicationId applicationId) throws Exception {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(masterMemory);
    return submitApp(resource, name, user, acls, unmanaged, queue,
      maxAppAttempts, ts, appType, waitForAccepted, keepContainers,
      isAppIdProvided, applicationId, 0, null, true);
  }

  public RMApp submitApp(int masterMemory,
      LogAggregationContext logAggregationContext) throws Exception {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(masterMemory);
    return submitApp(resource, "", UserGroupInformation.getCurrentUser()
      .getShortUserName(), null, false, null,
      super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null, true, false,
      false, null, 0, logAggregationContext, true);
   }

  public RMApp submitApp(Resource capability, String name, String user,
      Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
      int maxAppAttempts, Credentials ts, String appType,
      boolean waitForAccepted, boolean keepContainers, boolean isAppIdProvided,
    sub.setApplicationType(appType);
    ContainerLaunchContext clc = Records
        .newRecord(ContainerLaunchContext.class);
    sub.setResource(capability);
    clc.setApplicationACLs(acls);
    if (ts != null && UserGroupInformation.isSecurityEnabled()) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestWorkPreservingRMRestart.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
    nm1.registerNode();

    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(200);
    RMApp app0 = rm1.submitApp(resource, "", UserGroupInformation
        .getCurrentUser().getShortUserName(), null, false, null,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS, null, null, true, true,
        false, null, 0, null, true);
    MockAM am0 = MockRM.launchAndRegisterAM(app0, rm1, nm1);

    am0.allocate("127.0.0.1", 1000, 2, new ArrayList<ContainerId>());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestCapacityScheduler.java
        (LeafQueue) ((CapacityScheduler) scheduler).getQueue(queueName);
    Resource amResourceLimit = queueA.getAMResourceLimit();

    Resource amResource1 =
        Resource.newInstance(amResourceLimit.getMemory() + 1024,
            amResourceLimit.getVirtualCores() + 1);
    Resource amResource2 =
        Resource.newInstance(amResourceLimit.getMemory() + 2048,
            amResourceLimit.getVirtualCores() + 1);

    rm.submitApp(amResource1, "app-1", userName, null, queueName);

    rm.submitApp(amResource2, "app-2", userName, null, queueName);


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/security/TestDelegationTokenRenewer.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer.DelegationTokenToRenew;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
    credentials.addToken(userText1, token1);

    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(200);
    RMApp app1 = rm.submitApp(resource, "name", "user", null, false, null, 2,
        credentials, null, true, false, false, null, 0, null, false);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);
    rm.waitForState(app1.getApplicationId(), RMAppState.RUNNING);

    RMApp app2 = rm.submitApp(resource, "name", "user", null, false, null, 2,
        credentials, null, true, false, false, null, 0, null, true);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm1);
    rm.waitForState(app2.getApplicationId(), RMAppState.RUNNING);
    MockRM.finishAMAndVerifyAppState(app2, rm, nm1, am2);
    Assert.assertTrue(renewer.getAllTokens().isEmpty());
    Assert.assertFalse(Renewer.cancelled);

    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(200);
    RMApp app1 =
        rm.submitApp(resource, "name", "user", null, false, null, 2, credentials,
          null, true, false, false, null, 0, null, true);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);
    rm.waitForState(app1.getApplicationId(), RMAppState.RUNNING);
    DelegationTokenToRenew dttr = renewer.getAllTokens().get(token1);
    Assert.assertNotNull(dttr);
    Assert.assertTrue(dttr.referringAppIds.contains(app1.getApplicationId()));
    RMApp app2 = rm.submitApp(resource, "name", "user", null, false, null, 2,
        credentials, null, true, false, false, null, 0, null, true);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm1);
    rm.waitForState(app2.getApplicationId(), RMAppState.RUNNING);
    Assert.assertTrue(renewer.getAllTokens().containsKey(token1));
    Assert.assertFalse(dttr.isTimerCancelled());
    Assert.assertFalse(Renewer.cancelled);

    RMApp app3 = rm.submitApp(resource, "name", "user", null, false, null, 2,
        credentials, null, true, false, false, null, 0, null, true);
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm, nm1);
    rm.waitForState(app3.getApplicationId(), RMAppState.RUNNING);
    Assert.assertTrue(renewer.getAllTokens().containsKey(token1));

