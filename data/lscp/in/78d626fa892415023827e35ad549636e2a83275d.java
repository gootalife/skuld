hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/resource/DominantResourceCalculator.java
      return 0;
    }
    
    if (isInvalidDivisor(clusterResource)) {
      if ((lhs.getMemory() < rhs.getMemory() && lhs.getVirtualCores() > rhs
          .getVirtualCores())
          || (lhs.getMemory() > rhs.getMemory() && lhs.getVirtualCores() < rhs
              .getVirtualCores())) {
        return 0;
      } else if (lhs.getMemory() > rhs.getMemory()
          || lhs.getVirtualCores() > rhs.getVirtualCores()) {
        return 1;
      } else if (lhs.getMemory() < rhs.getMemory()
          || lhs.getVirtualCores() < rhs.getVirtualCores()) {
        return -1;
      }
    }

    float l = getResourceAsValue(clusterResource, lhs, true);
    float r = getResourceAsValue(clusterResource, rhs, true);
    

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestCapacityScheduler.java
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;

  private MockRM setUpMove() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    return setUpMove(conf);
  }

  private MockRM setUpMove(Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    Assert.assertEquals(queueInfoB.getDefaultNodeLabelExpression(), "y");
  }

  @Test(timeout = 30000)
  public void testAMLimitUsage() throws Exception {

    CapacitySchedulerConfiguration config =
        new CapacitySchedulerConfiguration();

    config.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DefaultResourceCalculator.class.getName());
    verifyAMLimitForLeafQueue(config);

    config.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class.getName());
    verifyAMLimitForLeafQueue(config);

  }

  private void verifyAMLimitForLeafQueue(CapacitySchedulerConfiguration config)
      throws Exception {
    MockRM rm = setUpMove(config);

    String queueName = "a1";
    String userName = "user_0";
    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    LeafQueue queueA =
        (LeafQueue) ((CapacityScheduler) scheduler).getQueue(queueName);
    Resource amResourceLimit = queueA.getAMResourceLimit();

    Resource amResource =
        Resource.newInstance(amResourceLimit.getMemory() + 1,
            amResourceLimit.getVirtualCores() + 1);

    rm.submitApp(amResource.getMemory(), "app-1", userName, null, queueName);

    rm.submitApp(amResource.getMemory(), "app-1", userName, null, queueName);

    Assert.assertEquals("PendingApplications should be 1", 1,
        queueA.getNumPendingApplications());
    Assert.assertEquals("Active applications should be 1", 1,
        queueA.getNumActiveApplications());

    Assert.assertEquals("User PendingApplications should be 1", 1, queueA
        .getUser(userName).getPendingApplications());
    Assert.assertEquals("User Active applications should be 1", 1, queueA
        .getUser(userName).getActiveApplications());
    rm.stop();
  }

  private void setMaxAllocMb(Configuration conf, int maxAllocMb) {
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        maxAllocMb);

