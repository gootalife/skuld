hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/SchedulerApplicationAttempt.java
    AggregateAppResourceUsage runningResourceUsage =
        getRunningAggregateAppResourceUsage();
    Resource usedResourceClone =
        Resources.clone(attemptResourceUsage.getAllUsed());
    Resource reservedResourceClone =
        Resources.clone(attemptResourceUsage.getReserved());
    return ApplicationResourceUsageReport.newInstance(liveContainers.size(),

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/LeafQueue.java
    for (Map.Entry<String, User> entry : users.entrySet()) {
      User user = entry.getValue();
      usersToReturn.add(new UserInfo(entry.getKey(), Resources.clone(user
          .getAllUsed()), user.getActiveApplications(), user
          .getPendingApplications(), Resources.clone(user
          .getConsumedAMResources()), Resources.clone(user
          .getUserResourceLimit())));
      return userResourceUsage.getUsed();
    }

    public Resource getAllUsed() {
      return userResourceUsage.getAllUsed();
    }

    public Resource getUsed(String label) {
      return userResourceUsage.getUsed(label);
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestCapacitySchedulerNodeLabelUpdate.java
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
        .getMemory());
  }

  @Test(timeout = 60000)
  public void testResourceUsage() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y",
        "z"));

    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h2", 0), toSet("y")));

    MockRM rm = new MockRM(getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 2048);
    MockNM nm2 = rm.registerNode("h2:1234", 2048);
    MockNM nm3 = rm.registerNode("h3:1234", 2048);

    ContainerId containerId;
    RMApp app1 = rm.submitApp(GB, "app", "user", null, "a");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm3);
    ApplicationResourceUsageReport appResourceUsageReport =
        rm.getResourceScheduler().getAppResourceUsageReport(
            am1.getApplicationAttemptId());
    Assert.assertEquals(1024, appResourceUsageReport.getUsedResources()
        .getMemory());
    Assert.assertEquals(1, appResourceUsageReport.getUsedResources()
        .getVirtualCores());
    am1.allocate("*", GB, 1, new ArrayList<ContainerId>(), "x");
    containerId = ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    rm.waitForState(nm1, containerId, RMContainerState.ALLOCATED, 10 * 1000);
    appResourceUsageReport =
        rm.getResourceScheduler().getAppResourceUsageReport(
            am1.getApplicationAttemptId());
    Assert.assertEquals(2048, appResourceUsageReport.getUsedResources()
        .getMemory());
    Assert.assertEquals(2, appResourceUsageReport.getUsedResources()
        .getVirtualCores());
    LeafQueue queue =
        (LeafQueue) ((CapacityScheduler) rm.getResourceScheduler())
            .getQueue("a");
    ArrayList<UserInfo> users = queue.getUsers();
    for (UserInfo userInfo : users) {
      if (userInfo.getUsername().equals("user")) {
        ResourceInfo resourcesUsed = userInfo.getResourcesUsed();
        Assert.assertEquals(2048, resourcesUsed.getMemory());
        Assert.assertEquals(2, resourcesUsed.getvCores());
      }
    }
    rm.stop();
  }

  @Test (timeout = 60000)
  public void testNodeUpdate() throws Exception {

