hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ResourceManager.java
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
            YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
            SchedulingEditPolicy.class);
        if (policies.size() > 0) {
          for (SchedulingEditPolicy policy : policies) {
            LOG.info("LOADING SchedulingEditPolicy:" + policy.getPolicyName());
    }
  }

  @Private
  public static final class ApplicationAttemptEventDispatcher implements
      EventHandler<RMAppAttemptEvent> {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/SchedulingEditPolicy.java
package org.apache.hadoop.yarn.server.resourcemanager.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;

public interface SchedulingEditPolicy {

  public void init(Configuration config, RMContext context,
      PreemptableResourceScheduler scheduler);


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/SchedulingMonitor.java
    return scheduleEditPolicy;
  }

  public void serviceInit(Configuration conf) throws Exception {
    scheduleEditPolicy.init(conf, rmContext,
        (PreemptableResourceScheduler) rmContext.getScheduler());
    this.monitorInterval = scheduleEditPolicy.getMonitoringInterval();
    super.serviceInit(conf);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/capacity/ProportionalCapacityPreemptionPolicy.java
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
  public static final String NATURAL_TERMINATION_FACTOR =
      "yarn.resourcemanager.monitor.capacity.preemption.natural_termination_factor";

  private RMContext rmContext;

  private final Clock clock;
  private double maxIgnoredOverCapacity;
  }

  public ProportionalCapacityPreemptionPolicy(Configuration config,
      RMContext context, CapacityScheduler scheduler) {
    this(config, context, scheduler, new SystemClock());
  }

  public ProportionalCapacityPreemptionPolicy(Configuration config,
      RMContext context, CapacityScheduler scheduler, Clock clock) {
    init(config, context, scheduler);
    this.clock = clock;
  }

  public void init(Configuration config, RMContext context,
      PreemptableResourceScheduler sched) {
    LOG.info("Preemption monitor:" + this.getClass().getCanonicalName());
    assert null == scheduler : "Unexpected duplicate call to init";
          sched.getClass().getCanonicalName() + " not instance of " +
          CapacityScheduler.class.getCanonicalName());
    }
    rmContext = context;
    scheduler = (CapacityScheduler) sched;
    maxIgnoredOverCapacity = config.getDouble(MAX_IGNORED_OVER_CAPACITY, 0.1);
    naturalTerminationFactor =
  @SuppressWarnings("unchecked")
  private void containerBasedPreemptOrKill(CSQueue root,
      Resource clusterResources) {
    for (Map.Entry<ApplicationAttemptId,Set<RMContainer>> e
         : toPreempt.entrySet()) {
      ApplicationAttemptId appAttemptId = e.getKey();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Send to scheduler: in app=" + appAttemptId
            + " #containers-to-be-preempted=" + e.getValue().size());
      }
      for (RMContainer container : e.getValue()) {
        if (preempted.get(container) != null &&
            preempted.get(container) + maxWaitTime < clock.getTime()) {
          rmContext.getDispatcher().getEventHandler().handle(
              new ContainerPreemptEvent(appAttemptId, container,
                  SchedulerEventType.KILL_CONTAINER));
          preempted.remove(container);
        } else {
          rmContext.getDispatcher().getEventHandler().handle(
              new ContainerPreemptEvent(appAttemptId, container,
                  SchedulerEventType.PREEMPT_CONTAINER));
          if (preempted.get(container) == null) {
            preempted.put(container, clock.getTime());
          }
  @SuppressWarnings("unchecked")
  private void preemptFrom(FiCaSchedulerApp app,
      Resource clusterResource, Map<String, Resource> resToObtainByPartition,
      List<RMContainer> skippedAMContainerlist, Resource skippedAMSize,
          clusterResource, preemptMap);

      if (!observeOnly) {
        rmContext.getDispatcher().getEventHandler().handle(
            new ContainerPreemptEvent(
                appId, c, SchedulerEventType.DROP_RESERVATION));
      }
    }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/ContainerPreemptEvent.java
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;

public class ContainerPreemptEvent extends SchedulerEvent {

  private final ApplicationAttemptId aid;
  private final RMContainer container;

  public ContainerPreemptEvent(ApplicationAttemptId aid, RMContainer container,
      SchedulerEventType type) {
    super(type);
    this.aid = aid;
    this.container = container;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacityScheduler.java
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
          RMContainerEventType.EXPIRE);
    }
    break;
    case DROP_RESERVATION:
    {
      ContainerPreemptEvent dropReservationEvent = (ContainerPreemptEvent)event;
      RMContainer container = dropReservationEvent.getContainer();
      dropContainerReservation(container);
    }
    break;
    case PREEMPT_CONTAINER:
    {
      ContainerPreemptEvent preemptContainerEvent =
          (ContainerPreemptEvent)event;
      ApplicationAttemptId aid = preemptContainerEvent.getAppId();
      RMContainer containerToBePreempted = preemptContainerEvent.getContainer();
      preemptContainer(aid, containerToBePreempted);
    }
    break;
    case KILL_CONTAINER:
    {
      ContainerPreemptEvent killContainerEvent = (ContainerPreemptEvent)event;
      RMContainer containerToBeKilled = killContainerEvent.getContainer();
      killContainer(containerToBeKilled);
    }
    break;
    default:
      LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/event/SchedulerEventType.java
  APP_ATTEMPT_REMOVED,

  CONTAINER_EXPIRED,

  DROP_RESERVATION,
  PREEMPT_CONTAINER,
  KILL_CONTAINER
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMDispatcher.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMDispatcher.java

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.SchedulerEventDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.junit.Assert;
import org.junit.Test;

public class TestRMDispatcher {

  @SuppressWarnings("unchecked")
  @Test(timeout=10000)
  public void testSchedulerEventDispatcherForPreemptionEvents() {
    AsyncDispatcher rmDispatcher = new AsyncDispatcher();
    CapacityScheduler sched = spy(new CapacityScheduler());
    YarnConfiguration conf = new YarnConfiguration();
    SchedulerEventDispatcher schedulerDispatcher =
        new SchedulerEventDispatcher(sched);
    rmDispatcher.register(SchedulerEventType.class, schedulerDispatcher);
    rmDispatcher.init(conf);
    rmDispatcher.start();
    schedulerDispatcher.init(conf);
    schedulerDispatcher.start();
    try {
      ApplicationAttemptId appAttemptId = mock(ApplicationAttemptId.class);
      RMContainer container = mock(RMContainer.class);
      ContainerPreemptEvent event1 = new ContainerPreemptEvent(
          appAttemptId, container, SchedulerEventType.DROP_RESERVATION);
      rmDispatcher.getEventHandler().handle(event1);
      ContainerPreemptEvent event2 = new ContainerPreemptEvent(
           appAttemptId, container, SchedulerEventType.KILL_CONTAINER);
      rmDispatcher.getEventHandler().handle(event2);
      ContainerPreemptEvent event3 = new ContainerPreemptEvent(
          appAttemptId, container, SchedulerEventType.PREEMPT_CONTAINER);
      rmDispatcher.getEventHandler().handle(event3);
      Thread.sleep(1000);
      verify(sched, times(3)).handle(any(SchedulerEvent.class));
      verify(sched).dropContainerReservation(container);
      verify(sched).preemptContainer(appAttemptId, container);
      verify(sched).killContainer(container);
    } catch (InterruptedException e) {
      Assert.fail();
    } finally {
      schedulerDispatcher.stop();
      rmDispatcher.stop();
    }
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/capacity/TestProportionalCapacityPreemptionPolicy.java
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.OBSERVE_ONLY;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.TOTAL_PREEMPTION_PER_ROUND;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.WAIT_TIME_BEFORE_KILL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType.KILL_CONTAINER;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType.PREEMPT_CONTAINER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
  RMContext rmContext = null;
  RMNodeLabelsManager lm = null;
  CapacitySchedulerConfiguration schedConf = null;
  EventHandler<SchedulerEvent> mDisp = null;
  ResourceCalculator rc = new DefaultResourceCalculator();
  Resource clusterResources = null;
  final ApplicationAttemptId appA = ApplicationAttemptId.newInstance(
    when(mCS.getRMContext()).thenReturn(rmContext);
    when(rmContext.getNodeLabelManager()).thenReturn(lm);
    mDisp = mock(EventHandler.class);
    Dispatcher disp = mock(Dispatcher.class);
    when(rmContext.getDispatcher()).thenReturn(disp);
    when(disp.getEventHandler()).thenReturn(mDisp);
    rand = new Random();
    long seed = rand.nextLong();
    System.out.println(name.getMethodName() + " SEED: " + seed);
  static class IsPreemptionRequestFor
      extends ArgumentMatcher<ContainerPreemptEvent> {
    private final ApplicationAttemptId appAttId;
    private final SchedulerEventType type;
    IsPreemptionRequestFor(ApplicationAttemptId appAttId) {
      this(appAttId, PREEMPT_CONTAINER);
    }
    IsPreemptionRequestFor(ApplicationAttemptId appAttId,
        SchedulerEventType type) {
      this.appAttId = appAttId;
      this.type = type;
    }

  ProportionalCapacityPreemptionPolicy buildPolicy(int[][] qData) {
    ProportionalCapacityPreemptionPolicy policy =
      new ProportionalCapacityPreemptionPolicy(conf, rmContext, mCS, mClock);
    ParentQueue mRoot = buildMockRootQueue(rand, qData);
    when(mCS.getRootQueue()).thenReturn(mRoot);


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/capacity/TestProportionalCapacityPreemptionPolicyForNodePartitions.java
