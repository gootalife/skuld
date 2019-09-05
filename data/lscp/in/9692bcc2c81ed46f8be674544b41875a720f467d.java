hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacitySchedulerConfiguration.java
  
  public static final String ORDERING_POLICY = "ordering-policy";
  
  public static final String FIFO_ORDERING_POLICY = "fifo";

  public static final String FAIR_ORDERING_POLICY = "fair";

  public static final String DEFAULT_ORDERING_POLICY = FIFO_ORDERING_POLICY;
  
  @Private
  public static final int DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS = 10000;
    
    OrderingPolicy<S> orderingPolicy;
    
    if (policyType.trim().equals(FIFO_ORDERING_POLICY)) {
       policyType = FifoOrderingPolicy.class.getName();
    }
    if (policyType.trim().equals(FAIR_ORDERING_POLICY)) {
       policyType = FairOrderingPolicy.class.getName();
    }
    try {
      orderingPolicy = (OrderingPolicy<S>)
        Class.forName(policyType).newInstance();
      String message = "Unable to construct ordering policy for: " + policyType + ", " + e.getMessage();
      throw new RuntimeException(message, e);
    }

    Map<String, String> config = new HashMap<String, String>();
    String confPrefix = getQueuePrefix(queue) + ORDERING_POLICY + ".";
    for (Map.Entry<String, String> kv : this) {
      if (kv.getKey().startsWith(confPrefix)) {
         config.put(kv.getKey().substring(confPrefix.length()), kv.getValue());
      }
    }
    orderingPolicy.configure(config);
    return orderingPolicy;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/CompoundComparator.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/CompoundComparator.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;

class CompoundComparator implements Comparator<SchedulableEntity> {

    List<Comparator<SchedulableEntity>> comparators;

    CompoundComparator(List<Comparator<SchedulableEntity>> comparators) {
      this.comparators = comparators;
    }

    @Override
    public int compare(final SchedulableEntity r1, final SchedulableEntity r2) {
      for (Comparator<SchedulableEntity> comparator : comparators) {
        int result = comparator.compare(r1, r2);
        if (result != 0) return result;
      }
      return 0;
    }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/FairOrderingPolicy.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/FairOrderingPolicy.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;

public class FairOrderingPolicy<S extends SchedulableEntity> extends AbstractComparatorOrderingPolicy<S> {

  public static final String ENABLE_SIZE_BASED_WEIGHT =
        "fair.enable-size-based-weight";

  protected class FairComparator implements Comparator<SchedulableEntity> {
    @Override
    public int compare(final SchedulableEntity r1, final SchedulableEntity r2) {
      int res = (int) Math.signum( getMagnitude(r1) - getMagnitude(r2) );
      return res;
    }
  }

  private CompoundComparator fairComparator;

  private boolean sizeBasedWeight = false;

  public FairOrderingPolicy() {
    List<Comparator<SchedulableEntity>> comparators =
      new ArrayList<Comparator<SchedulableEntity>>();
    comparators.add(new FairComparator());
    comparators.add(new FifoComparator());
    fairComparator = new CompoundComparator(
      comparators
      );
    this.comparator = fairComparator;
    this.schedulableEntities = new TreeSet<S>(comparator);
  }

  private double getMagnitude(SchedulableEntity r) {
    double mag = r.getSchedulingResourceUsage().getCachedUsed(
      CommonNodeLabelsManager.ANY).getMemory();
    if (sizeBasedWeight) {
      double weight = Math.log1p(r.getSchedulingResourceUsage().getCachedDemand(
        CommonNodeLabelsManager.ANY).getMemory()) / Math.log(2);
      mag = mag / weight;
    }
    return mag;
  }

  @VisibleForTesting
  public boolean getSizeBasedWeight() {
   return sizeBasedWeight;
  }

  @VisibleForTesting
  public void setSizeBasedWeight(boolean sizeBasedWeight) {
   this.sizeBasedWeight = sizeBasedWeight;
  }

  @Override
  public void configure(Map<String, String> conf) {
    if (conf.containsKey(ENABLE_SIZE_BASED_WEIGHT)) {
      sizeBasedWeight = Boolean.valueOf(conf.get(ENABLE_SIZE_BASED_WEIGHT));
    }
  }

  @Override
  public void containerAllocated(S schedulableEntity,
    RMContainer r) {
      reorderSchedulableEntity(schedulableEntity);
    }

  @Override
  public void containerReleased(S schedulableEntity,
    RMContainer r) {
      reorderSchedulableEntity(schedulableEntity);
    }

  @Override
  public String getInfo() {
    String sbw = sizeBasedWeight ? " with sizeBasedWeight" : "";
    return "FairOrderingPolicy" + sbw;
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestLeafQueue.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
    assertEquals(1, userMetrics.getAppsSubmitted());
  }

  @Test
  public void testFairConfiguration() throws Exception {

    CapacitySchedulerConfiguration testConf =
        new CapacitySchedulerConfiguration();

    String tproot = CapacitySchedulerConfiguration.ROOT + "." +
      "testPolicyRoot" + System.currentTimeMillis();

    OrderingPolicy<FiCaSchedulerApp> schedOrder =
      testConf.<FiCaSchedulerApp>getOrderingPolicy(tproot);

    String policyType = CapacitySchedulerConfiguration.PREFIX + tproot +
      "." + CapacitySchedulerConfiguration.ORDERING_POLICY;

    testConf.set(policyType,
      CapacitySchedulerConfiguration.FAIR_ORDERING_POLICY);
    schedOrder =
      testConf.<FiCaSchedulerApp>getOrderingPolicy(tproot);
    FairOrderingPolicy fop = (FairOrderingPolicy<FiCaSchedulerApp>) schedOrder;
    assertFalse(fop.getSizeBasedWeight());

    String sbwConfig = CapacitySchedulerConfiguration.PREFIX + tproot +
      "." + CapacitySchedulerConfiguration.ORDERING_POLICY + "." +
      FairOrderingPolicy.ENABLE_SIZE_BASED_WEIGHT;
    testConf.set(sbwConfig, "true");
    schedOrder =
      testConf.<FiCaSchedulerApp>getOrderingPolicy(tproot);
    fop = (FairOrderingPolicy<FiCaSchedulerApp>) schedOrder;
    assertTrue(fop.getSizeBasedWeight());

  }

  @Test
  public void testSingleQueueWithOneUser() throws Exception {


  }

  @Test
  public void testFairAssignment() throws Exception {

    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));

    OrderingPolicy<FiCaSchedulerApp> schedulingOrder =
      new FairOrderingPolicy<FiCaSchedulerApp>();

    a.setOrderingPolicy(schedulingOrder);

    String host_0_0 = "127.0.0.1";
    String rack_0 = "rack_0";
    FiCaSchedulerNode node_0_0 = TestUtils.getMockNode(host_0_0, rack_0, 0, 16*GB);

    final int numNodes = 4;
    Resource clusterResource = Resources.createResource(
        numNodes * (16*GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    String user_0 = "user_0";

    final ApplicationAttemptId appAttemptId_0 =
        TestUtils.getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 =
        spy(new FiCaSchedulerApp(appAttemptId_0, user_0, a,
            mock(ActiveUsersManager.class), spyRMContext));
    a.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 =
        TestUtils.getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 =
        spy(new FiCaSchedulerApp(appAttemptId_1, user_0, a,
            mock(ActiveUsersManager.class), spyRMContext));
    a.submitApplicationAttempt(app_1, user_0);

    Priority priority = TestUtils.createMockPriority(1);
    List<ResourceRequest> app_0_requests_0 = new ArrayList<ResourceRequest>();
    List<ResourceRequest> app_1_requests_0 = new ArrayList<ResourceRequest>();

    app_0_requests_0.clear();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 2*GB, 1,
            true, priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);

    app_1_requests_0.clear();
    app_1_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1,
            true, priority, recordFactory));
    app_1.updateResourceRequests(app_1_requests_0);

    a.assignContainers(clusterResource, node_0_0, new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    Assert.assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    a.assignContainers(clusterResource, node_0_0, new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    Assert.assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());

    app_0_requests_0.clear();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1,
            true, priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);

    app_1_requests_0.clear();
    app_1_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1,
            true, priority, recordFactory));
    app_1.updateResourceRequests(app_1_requests_0);

    a.assignContainers(clusterResource, node_0_0, new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    Assert.assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    Assert.assertEquals(2*GB, app_1.getCurrentConsumption().getMemory());

    a.assignContainers(clusterResource, node_0_0, new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    Assert.assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());

  }

  private List<FiCaSchedulerApp> createListOfApps(int noOfApps, String user,
      LeafQueue defaultQueue) {
    List<FiCaSchedulerApp> appsLists = new ArrayList<FiCaSchedulerApp>();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/TestFairOrderingPolicy.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/TestFairOrderingPolicy.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.*;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;

public class TestFairOrderingPolicy {

  final static int GB = 1024;

  @Test
  public void testSimpleComparison() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
      new FairOrderingPolicy<MockSchedulableEntity>();
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    Assert.assertTrue(policy.getComparator().compare(r1, r2) == 0);

    r1.setUsed(Resources.createResource(1, 0));
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r1.getSchedulingResourceUsage());
    Assert.assertTrue(policy.getComparator().compare(r1, r2) > 0);
  }

  @Test
  public void testSizeBasedWeight() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
      new FairOrderingPolicy<MockSchedulableEntity>();
    policy.setSizeBasedWeight(true);
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    Assert.assertTrue(policy.getComparator().compare(r1, r2) == 0);

    r1.setUsed(Resources.createResource(4 * GB));
    r2.setUsed(Resources.createResource(4 * GB));

    r1.setPending(Resources.createResource(4 * GB));
    r2.setPending(Resources.createResource(4 * GB));

    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r2.getSchedulingResourceUsage());

    Assert.assertTrue(policy.getComparator().compare(r1, r2) == 0);

    r2.setUsed(Resources.createResource(5 * GB));
    r2.setPending(Resources.createResource(5 * GB));

    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r2.getSchedulingResourceUsage());

    Assert.assertTrue(policy.getComparator().compare(r1, r2) < 0);

    r2.setPending(Resources.createResource(100 * GB));
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r2.getSchedulingResourceUsage());
    Assert.assertTrue(policy.getComparator().compare(r1, r2) > 0);
  }

  @Test
  public void testIterators() {
    OrderingPolicy<MockSchedulableEntity> schedOrder =
     new FairOrderingPolicy<MockSchedulableEntity>();

    MockSchedulableEntity msp1 = new MockSchedulableEntity();
    MockSchedulableEntity msp2 = new MockSchedulableEntity();
    MockSchedulableEntity msp3 = new MockSchedulableEntity();

    msp1.setId("1");
    msp2.setId("2");
    msp3.setId("3");

    msp1.setUsed(Resources.createResource(3));
    msp2.setUsed(Resources.createResource(2));
    msp3.setUsed(Resources.createResource(1));

    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      msp1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      msp2.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      msp2.getSchedulingResourceUsage());

    schedOrder.addSchedulableEntity(msp1);
    schedOrder.addSchedulableEntity(msp2);
    schedOrder.addSchedulableEntity(msp3);


    checkIds(schedOrder.getAssignmentIterator(), new String[]{"3", "2", "1"});

    checkIds(schedOrder.getPreemptionIterator(), new String[]{"1", "2", "3"});

    msp2.setUsed(Resources.createResource(6));
    checkIds(schedOrder.getAssignmentIterator(), new String[]{"3", "2", "1"});
    checkIds(schedOrder.getPreemptionIterator(), new String[]{"1", "2", "3"});

    schedOrder.containerAllocated(msp2, null);
    checkIds(schedOrder.getAssignmentIterator(), new String[]{"3", "1", "2"});
    checkIds(schedOrder.getPreemptionIterator(), new String[]{"2", "1", "3"});
  }

  public void checkIds(Iterator<MockSchedulableEntity> si,
      String[] ids) {
    for (int i = 0;i < ids.length;i++) {
      Assert.assertEquals(si.next().getId(),
        ids[i]);
    }
  }

}

