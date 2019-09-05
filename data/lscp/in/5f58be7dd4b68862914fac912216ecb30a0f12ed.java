hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FSLeafQueue.java
  }

  private boolean isStarved(Resource share) {
    Resource desiredShare = Resources.min(policy.getResourceCalculator(),
            scheduler.getClusterResource(), share, getDemand());
    Resource resourceUsage = getResourceUsage();
    return Resources.lessThan(policy.getResourceCalculator(),
            scheduler.getClusterResource(), resourceUsage, desiredShare);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FairScheduler.java

    Resource resToPreempt = Resources.clone(Resources.none());
    for (FSLeafQueue sched : queueMgr.getLeafQueues()) {
      Resources.addTo(resToPreempt, resourceDeficit(sched, curTime));
    }
    if (isResourceGreaterThanNone(resToPreempt)) {
      preemptResources(resToPreempt);
    }
  }
      RMContainer container = warnedIter.next();
      if ((container.getState() == RMContainerState.RUNNING ||
              container.getState() == RMContainerState.ALLOCATED) &&
              isResourceGreaterThanNone(toPreempt)) {
        warnOrKillContainer(container);
        Resources.subtractFrom(toPreempt, container.getContainer().getResource());
      } else {
        queue.resetPreemptedResources();
      }

      while (isResourceGreaterThanNone(toPreempt)) {
        RMContainer container =
            getQueueManager().getRootQueue().preemptContainer();
        if (container == null) {
    fsOpDurations.addPreemptCallDuration(duration);
  }

  private boolean isResourceGreaterThanNone(Resource toPreempt) {
    return (toPreempt.getMemory() > 0) || (toPreempt.getVirtualCores() > 0);
  }

  protected void warnOrKillContainer(RMContainer container) {
    ApplicationAttemptId appAttemptId = container.getApplicationAttemptId();
    FSAppAttempt app = getSchedulerApp(appAttemptId);
  protected Resource resourceDeficit(FSLeafQueue sched, long curTime) {
    long minShareTimeout = sched.getMinSharePreemptionTimeout();
    long fairShareTimeout = sched.getFairSharePreemptionTimeout();
    Resource resDueToMinShare = Resources.none();
    Resource resDueToFairShare = Resources.none();
    ResourceCalculator calc = sched.getPolicy().getResourceCalculator();
    if (curTime - sched.getLastTimeAtMinShare() > minShareTimeout) {
      Resource target = Resources.componentwiseMin(
          sched.getMinShare(), sched.getDemand());
      resDueToMinShare = Resources.max(calc, clusterResource,
          Resources.none(), Resources.subtract(target, sched.getResourceUsage()));
    }
    if (curTime - sched.getLastTimeAtFairShareThreshold() > fairShareTimeout) {
      Resource target = Resources.componentwiseMin(
              sched.getFairShare(), sched.getDemand());
      resDueToFairShare = Resources.max(calc, clusterResource,
          Resources.none(), Resources.subtract(target, sched.getResourceUsage()));
    }
    Resource deficit = Resources.max(calc, clusterResource,
        resDueToMinShare, resDueToFairShare);
    if (Resources.greaterThan(calc, clusterResource,
        deficit, Resources.none())) {
      String message = "Should preempt " + deficit + " res for queue "
          + sched.getName() + ": resDueToMinShare = " + resDueToMinShare
          + ", resDueToFairShare = " + resDueToFairShare;
      LOG.info(message);
    }
    return deficit;
  }

  public synchronized RMContainerTokenSecretManager

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/SchedulingPolicy.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;


import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
  
  public void initialize(Resource clusterCapacity) {}

  public abstract ResourceCalculator getResourceCalculator();


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/policies/DominantResourceFairnessPolicy.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;

import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceType.*;

  public static final String NAME = "DRF";

  private static final DominantResourceFairnessComparator COMPARATOR =
      new DominantResourceFairnessComparator();
  private static final DominantResourceCalculator CALCULATOR =
      new DominantResourceCalculator();

  @Override
  public String getName() {

  @Override
  public Comparator<Schedulable> getComparator() {
    return COMPARATOR;
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return CALCULATOR;
  }

  @Override

  @Override
  public void initialize(Resource clusterCapacity) {
    COMPARATOR.setClusterCapacity(clusterCapacity);
  }

  public static class DominantResourceFairnessComparator implements Comparator<Schedulable> {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/policies/FairSharePolicy.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;
  public static final String NAME = "fair";
  private static final DefaultResourceCalculator RESOURCE_CALCULATOR =
      new DefaultResourceCalculator();
  private static final FairShareComparator COMPARATOR =
          new FairShareComparator();

  @Override
  public String getName() {

  @Override
  public Comparator<Schedulable> getComparator() {
    return COMPARATOR;
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return RESOURCE_CALCULATOR;
  }

  @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/policies/FifoPolicy.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;


import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;
public class FifoPolicy extends SchedulingPolicy {
  @VisibleForTesting
  public static final String NAME = "FIFO";
  private static final FifoComparator COMPARATOR = new FifoComparator();
  private static final DefaultResourceCalculator CALCULATOR =
          new DefaultResourceCalculator();

  @Override
  public String getName() {

  @Override
  public Comparator<Schedulable> getComparator() {
    return COMPARATOR;
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return CALCULATOR;
  }

  @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/TestFSLeafQueue.java
    assertFalse(queueB2.isStarvedForFairShare());
  }

  @Test (timeout = 5000)
  public void testIsStarvedForFairShareDRF() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>.5</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>.5</weight>");
    out.println("</queue>");
    out.println("<defaultFairSharePreemptionThreshold>1</defaultFairSharePreemptionThreshold>");
    out.println("<defaultQueueSchedulingPolicy>drf</defaultQueueSchedulingPolicy>");
    out.println("</allocations>");
    out.close();

    resourceManager = new MockRM(conf);
    resourceManager.start();
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();

    RMNode node1 =
            MockNodes.newNodeInfo(1, Resources.createResource(10 * 1024, 10), 1,
                    "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    scheduler.update();

    createSchedulingRequest(7 * 1024, 1, "queueA", "user1", 1);
    scheduler.update();
    NodeUpdateSchedulerEvent nodeEvent2 = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(nodeEvent2);

    QueueManager queueMgr = scheduler.getQueueManager();
    FSLeafQueue queueA = queueMgr.getLeafQueue("queueA", false);
    assertEquals(7 * 1024, queueA.getResourceUsage().getMemory());
    assertEquals(1, queueA.getResourceUsage().getVirtualCores());

    createSchedulingRequest(2 * 1024, 5, "queueB", "user1", 1);
    createSchedulingRequest(1 * 1024, 2, "queueB", "user1", 2);
    scheduler.update();
    for (int i = 0; i < 3; i ++) {
      scheduler.handle(nodeEvent2);
    }

    FSLeafQueue queueB = queueMgr.getLeafQueue("queueB", false);
    assertEquals(3 * 1024, queueB.getResourceUsage().getMemory());
    assertEquals(6, queueB.getResourceUsage().getVirtualCores());

    scheduler.update();

    assertFalse(queueB.isStarvedForFairShare());
  }

  @Test
  public void testConcurrentAccess() {
    conf.set(FairSchedulerConfiguration.ASSIGN_MULTIPLE, "false");

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/TestFairScheduler.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
    clock.tickSec(11);

    scheduler.update();
    Resource toPreempt = scheduler.resourceDeficit(scheduler.getQueueManager()
            .getLeafQueue("queueA.queueA2", false), clock.getTime());
    assertEquals(3277, toPreempt.getMemory());

        scheduler.getQueueManager().getLeafQueue("queueD", true);

    assertTrue(Resources.equals(
        Resources.none(), scheduler.resourceDeficit(schedC, clock.getTime())));
    assertTrue(Resources.equals(
        Resources.none(), scheduler.resourceDeficit(schedD, clock.getTime())));
    clock.tickSec(6);
    assertEquals(
        1024, scheduler.resourceDeficit(schedC, clock.getTime()).getMemory());
    assertEquals(
        1024, scheduler.resourceDeficit(schedD, clock.getTime()).getMemory());

    scheduler.update();
    clock.tickSec(6);
    assertEquals(
        1536 , scheduler.resourceDeficit(schedC, clock.getTime()).getMemory());
    assertEquals(
        1536, scheduler.resourceDeficit(schedD, clock.getTime()).getMemory());
  }

  @Test
  public void testPreemptionDecisionWithDRF() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<maxResources>0mb,0vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,1vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,2vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueC\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,3vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueD\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,2vcores</minResources>");
    out.println("</queue>");
    out.println("<defaultMinSharePreemptionTimeout>5</defaultMinSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionTimeout>10</defaultFairSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionThreshold>.5</defaultFairSharePreemptionThreshold>");
    out.println("<defaultQueueSchedulingPolicy>drf</defaultQueueSchedulingPolicy>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 =
            MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024, 4), 1,
                    "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 =
            MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024, 4), 2,
                    "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    RMNode node3 =
            MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024, 4), 3,
                    "127.0.0.3");
    NodeAddedSchedulerEvent nodeEvent3 = new NodeAddedSchedulerEvent(node3);
    scheduler.handle(nodeEvent3);

    ApplicationAttemptId app1 =
            createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 1);
    ApplicationAttemptId app2 =
            createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 2);
    ApplicationAttemptId app3 =
            createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 3);

    ApplicationAttemptId app4 =
            createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 1);
    ApplicationAttemptId app5 =
            createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 2);
    ApplicationAttemptId app6 =
            createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 3);

    scheduler.update();

    for (int i = 0; i < 2; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
      scheduler.handle(nodeUpdate1);

      NodeUpdateSchedulerEvent nodeUpdate2 = new NodeUpdateSchedulerEvent(node2);
      scheduler.handle(nodeUpdate2);

      NodeUpdateSchedulerEvent nodeUpdate3 = new NodeUpdateSchedulerEvent(node3);
      scheduler.handle(nodeUpdate3);
    }

    ApplicationAttemptId app7 =
            createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 1);
    ApplicationAttemptId app8 =
            createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 2);
    ApplicationAttemptId app9 =
            createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 3);

    ApplicationAttemptId app10 =
            createSchedulingRequest(1 * 1024, "queueD", "user1", 2, 1);
    ApplicationAttemptId app11 =
            createSchedulingRequest(1 * 1024, "queueD", "user1", 2, 2);
    ApplicationAttemptId app12 =
            createSchedulingRequest(1 * 1024, "queueD", "user1", 2, 3);

    scheduler.update();

    FSLeafQueue schedC =
            scheduler.getQueueManager().getLeafQueue("queueC", true);
    FSLeafQueue schedD =
            scheduler.getQueueManager().getLeafQueue("queueD", true);

    assertTrue(Resources.equals(
            Resources.none(), scheduler.resourceDeficit(schedC, clock.getTime())));
    assertTrue(Resources.equals(
            Resources.none(), scheduler.resourceDeficit(schedD, clock.getTime())));


    clock.tickSec(6);
    Resource res = scheduler.resourceDeficit(schedC, clock.getTime());
    assertEquals(1024, res.getMemory());
    assertEquals(3, res.getVirtualCores());

    res = scheduler.resourceDeficit(schedD, clock.getTime());
    assertEquals(1024, res.getMemory());
    assertEquals(2, res.getVirtualCores());

    scheduler.update();
    clock.tickSec(6);
    res = scheduler.resourceDeficit(schedC, clock.getTime());
    assertEquals(1536, res.getMemory());
    assertEquals(3, res.getVirtualCores());

    res = scheduler.resourceDeficit(schedD, clock.getTime());
    assertEquals(1536, res.getMemory());
    assertEquals(3, res.getVirtualCores());
  }

  @Test
    FSLeafQueue queueC = queueMgr.getLeafQueue("queueC", true);

    assertTrue(Resources.equals(
        Resources.none(), scheduler.resourceDeficit(queueB1, clock.getTime())));
    assertTrue(Resources.equals(
        Resources.none(), scheduler.resourceDeficit(queueB2, clock.getTime())));
    assertTrue(Resources.equals(
        Resources.none(), scheduler.resourceDeficit(queueC, clock.getTime())));

    scheduler.update();
    clock.tickSec(6);
    assertEquals(
       1024, scheduler.resourceDeficit(queueB1, clock.getTime()).getMemory());
    assertEquals(
        0, scheduler.resourceDeficit(queueB2, clock.getTime()).getMemory());
    assertEquals(
        0, scheduler.resourceDeficit(queueC, clock.getTime()).getMemory());

    scheduler.update();
    clock.tickSec(5);
    assertEquals(
        1024, scheduler.resourceDeficit(queueB1, clock.getTime()).getMemory());
    assertEquals(
        1024, scheduler.resourceDeficit(queueB2, clock.getTime()).getMemory());
    assertEquals(
        0, scheduler.resourceDeficit(queueC, clock.getTime()).getMemory());

    scheduler.update();
    clock.tickSec(5);
    assertEquals(
        1024, scheduler.resourceDeficit(queueB1, clock.getTime()).getMemory());
    assertEquals(
        1024, scheduler.resourceDeficit(queueB2, clock.getTime()).getMemory());
    assertEquals(
        1024, scheduler.resourceDeficit(queueC, clock.getTime()).getMemory());

    scheduler.update();
    clock.tickSec(5);
    assertEquals(
        1024, scheduler.resourceDeficit(queueB1, clock.getTime()).getMemory());
    assertEquals(
        1536, scheduler.resourceDeficit(queueB2, clock.getTime()).getMemory());
    assertEquals(
        1024, scheduler.resourceDeficit(queueC, clock.getTime()).getMemory());

    scheduler.update();
    clock.tickSec(5);
    assertEquals(
        1536, scheduler.resourceDeficit(queueB1, clock.getTime()).getMemory());
    assertEquals(
        1536, scheduler.resourceDeficit(queueB2, clock.getTime()).getMemory());
    assertEquals(
        1024, scheduler.resourceDeficit(queueC, clock.getTime()).getMemory());

    scheduler.update();
    clock.tickSec(5);
    assertEquals(
        1536, scheduler.resourceDeficit(queueB1, clock.getTime()).getMemory());
    assertEquals(
        1536, scheduler.resourceDeficit(queueB2, clock.getTime()).getMemory());
    assertEquals(
        1536, scheduler.resourceDeficit(queueC, clock.getTime()).getMemory());
  }

  @Test

