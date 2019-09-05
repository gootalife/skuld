hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/TestRuntimeEstimators.java
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.Assert;
import org.junit.Test;
  private static int MAP_TASKS = 200;
  private static int REDUCE_TASKS = 150;

  ControlledClock clock;

  Job myJob;

  private void coreTestEstimator
      (TaskRuntimeEstimator testedEstimator, int expectedSpeculations) {
    estimator = testedEstimator;
	clock = new ControlledClock();
	dispatcher = new AsyncDispatcher();
    myJob = null;
    slotsInUse.set(0);
    successfulSpeculations.set(0);
    taskTimeSavedBySpeculation.set(0);

    clock.tickMsec(1000);

    Configuration conf = new Configuration();

        }
      }

      clock.tickMsec(1000L);

      if (clock.getTime() % 10000L == 0L) {
        speculator.scanForSpeculations();
    }
  }

  class MyAppMaster extends CompositeService {
    final Clock clock;
      public MyAppMaster(Clock clock) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/util/ControlledClock.java
public class ControlledClock implements Clock {
  private long time = -1;
  private final Clock actualClock;
  public ControlledClock() {
    this(new SystemClock());
    setTime(0);
  }
  public ControlledClock(Clock actualClock) {
    this.actualClock = actualClock;
  }
  public synchronized void reset() {
    time = -1;
  }
  public synchronized void tickSec(int seconds) {
    tickMsec(seconds * 1000L);
  }
  public synchronized void tickMsec(long millisec) {
    if (time == -1) {
      throw new IllegalStateException("ControlledClock setTime should be " +
          "called before incrementing time");
    }
    time = time + millisec;
  }

  @Override
  public synchronized long getTime() {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/util/TestCgroupsLCEResourcesHandler.java
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.TestCGroupsHandlerImpl;

import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.junit.Assert;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;
import org.junit.After;
import org.junit.Before;
public class TestCgroupsLCEResourcesHandler {
  static File cgroupDir = null;

  @Before
  public void setUp() throws Exception {
    cgroupDir =
  @Test
  public void testDeleteCgroup() throws Exception {
    final ControlledClock clock = new ControlledClock();
    CgroupsLCEResourcesHandler handler = new CgroupsLCEResourcesHandler();
    handler.setConf(new YarnConfiguration());
    handler.initConfig();
        } catch (InterruptedException ex) {
        }
        clock.tickMsec(YarnConfiguration.
            DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT);
      }
    }.start();
    latch.await();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FairSchedulerTestBase.java
import org.apache.hadoop.yarn.util.Clock;

public class FairSchedulerTestBase {
  public final static String TEST_DIR =
      new File(System.getProperty("test.build.data", "/tmp")).getAbsolutePath();


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/TestAllocationFileLoaderService.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueuePlacementRule.NestedUserQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Test;

  final static String ALLOC_FILE = new File(TEST_DIR,
      "test-queues").getAbsolutePath();
  
  @Test
  public void testGetAllocationFileFromClasspath() {
    Configuration conf = new Configuration();
    out.println("</allocations>");
    out.close();
    
    ControlledClock clock = new ControlledClock();
    clock.setTime(0);
    Configuration conf = new Configuration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    out.println("</allocations>");
    out.close();
    
    clock.tickMsec(System.currentTimeMillis()
        + AllocationFileLoaderService.ALLOC_RELOAD_WAIT_MS + 10000);
    allocLoader.start();
    

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/TestContinuousScheduling.java
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestContinuousScheduling extends FairSchedulerTestBase {
  private ControlledClock mockClock;

  @Override
  public Configuration createConfiguration() {

  @Before
  public void setup() {
    mockClock = new ControlledClock();
    conf = createConfiguration();
    resourceManager = new MockRM(conf);
    resourceManager.start();
    FSAppAttempt app = scheduler.getSchedulerApp(appAttemptId);

    mockClock.tickSec(1);
    while (1024 != app.getCurrentConsumption().getMemory()) {
      Thread.sleep(100);
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/TestFSAppAttempt.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;

import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;

public class TestFSAppAttempt extends FairSchedulerTestBase {

  @Before
  public void setup() {
    Configuration conf = createConfiguration();
    Priority prio = Mockito.mock(Priority.class);
    Mockito.when(prio.getPriority()).thenReturn(1);

    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    long nodeLocalityDelayMs = 5 * 1000L;    // 5 seconds
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    clock.tickSec(4);
    assertEquals(NodeType.NODE_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    clock.tickSec(2);
    assertEquals(NodeType.RACK_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    clock.tickSec(6);
    assertEquals(NodeType.RACK_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    clock.tickSec(7);
    assertEquals(NodeType.OFF_SWITCH,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/TestFairScheduler.java
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE + ".allocation.file", ALLOC_FILE);
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "false");
    
    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);
    
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
            scheduler.getSchedulerApp(app2).getPreemptionContainers()));

    clock.tickSec(15);

    scheduler.preemptResources(Resources.createResource(2 * 1024));
    scheduler.preemptResources(Resources.createResource(2 * 1024));

    clock.tickSec(15);

    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "false");

    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    scheduler.update();

    clock.tickSec(11);

    scheduler.update();
    Resource toPreempt = scheduler.resToPreempt(scheduler.getQueueManager()
  public void testPreemptionDecision() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
        Resources.none(), scheduler.resToPreempt(schedD, clock.getTime())));
    clock.tickSec(6);
    assertEquals(
        1024, scheduler.resToPreempt(schedC, clock.getTime()).getMemory());
    assertEquals(
    scheduler.update();
    clock.tickSec(6);
    assertEquals(
        1536 , scheduler.resToPreempt(schedC, clock.getTime()).getMemory());
    assertEquals(
  public void testPreemptionDecisionWithVariousTimeout() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));

    scheduler.update();
    clock.tickSec(6);
    assertEquals(
       1024, scheduler.resToPreempt(queueB1, clock.getTime()).getMemory());
    assertEquals(

    scheduler.update();
    clock.tickSec(5);
    assertEquals(
        1024, scheduler.resToPreempt(queueB1, clock.getTime()).getMemory());
    assertEquals(

    scheduler.update();
    clock.tickSec(5);
    assertEquals(
        1024, scheduler.resToPreempt(queueB1, clock.getTime()).getMemory());
    assertEquals(

    scheduler.update();
    clock.tickSec(5);
    assertEquals(
        1024, scheduler.resToPreempt(queueB1, clock.getTime()).getMemory());
    assertEquals(

    scheduler.update();
    clock.tickSec(5);
    assertEquals(
        1536, scheduler.resToPreempt(queueB1, clock.getTime()).getMemory());
    assertEquals(

    scheduler.update();
    clock.tickSec(5);
    assertEquals(
        1536, scheduler.resToPreempt(queueB1, clock.getTime()).getMemory());
    assertEquals(
  @Test
  public void testMaxRunningAppsHierarchicalQueues() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1.sub1", "user1");
    verifyAppRunnable(attId1, true);
    verifyQueueNumRunnable("queue1.sub1", 1, 0);
    clock.tickSec(10);
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue1.sub3", "user1");
    verifyAppRunnable(attId2, true);
    verifyQueueNumRunnable("queue1.sub3", 1, 0);
    clock.tickSec(10);
    ApplicationAttemptId attId3 = createSchedulingRequest(1024, "queue1.sub2", "user1");
    verifyAppRunnable(attId3, true);
    verifyQueueNumRunnable("queue1.sub2", 1, 0);
    clock.tickSec(10);
    ApplicationAttemptId attId4 = createSchedulingRequest(1024, "queue1.sub2", "user1");
    verifyAppRunnable(attId4, false);
    verifyQueueNumRunnable("queue1.sub2", 1, 1);
    clock.tickSec(10);
    ApplicationAttemptId attId5 = createSchedulingRequest(1024, "queue1.sub3", "user1");
    verifyAppRunnable(attId5, false);
    verifyQueueNumRunnable("queue1.sub3", 1, 1);
    clock.tickSec(10);

  public void testRecoverRequestAfterPreemption() throws Exception {
    conf.setLong(FairSchedulerConfiguration.WAIT_TIME_BEFORE_KILL, 10);
    
    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);
    scheduler.init(conf);
    scheduler.start();
    scheduler.warnOrKillContainer(rmContainer);
    
    clock.tickSec(5);

    scheduler.warnOrKillContainer(rmContainer);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/TestFairSchedulerPreemption.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;

import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Before;
  private final static String ALLOC_FILE = new File(TEST_DIR,
      TestFairSchedulerPreemption.class.getName() + ".xml").getAbsolutePath();

  private ControlledClock clock;

  private static class StubbedFairScheduler extends FairScheduler {
    public int lastPreemptMemory = -1;
  @Before
  public void setup() throws IOException {
    conf = createConfiguration();
    clock = new ControlledClock();
  }

  @After
    createSchedulingRequest(1024, "queueB", "user1", 1, 1);
    scheduler.update();
    clock.tickSec(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    createSchedulingRequest(1024, "queueB", "user1", 1, 1);
    scheduler.update();
    clock.tickSec(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    createSchedulingRequest(1024, "queueB", "user1", 1, 1);
    scheduler.update();
    clock.tickSec(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/TestMaxRunningAppsEnforcer.java
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.junit.Before;
import org.junit.Test;

  private Map<String, Integer> userMaxApps;
  private MaxRunningAppsEnforcer maxAppsEnforcer;
  private int appNum;
  private ControlledClock clock;
  private RMContext rmContext;
  private FairScheduler scheduler;
  
  @Before
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    clock = new ControlledClock();
    scheduler = mock(FairScheduler.class);
    when(scheduler.getConf()).thenReturn(
        new FairSchedulerConfiguration(conf));
    FSAppAttempt app1 = addApp(leaf1, "user");
    addApp(leaf2, "user");
    addApp(leaf2, "user");
    clock.tickSec(20);
    addApp(leaf1, "user");
    assertEquals(1, leaf1.getNumRunnableApps());
    assertEquals(1, leaf2.getNumRunnableApps());

