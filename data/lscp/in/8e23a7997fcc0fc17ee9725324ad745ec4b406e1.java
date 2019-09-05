hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/Plan.java
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/PlanView.java
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.Set;

  Resource getTotalCommittedResources(long tick);

  Resource getTotalCapacity();


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/ReservationSchedulerConfiguration.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;

public abstract class ReservationSchedulerConfiguration extends Configuration {


  public abstract boolean isReservable(String queue);
  }

  public String getReservationAgent(String queue) {
    return DEFAULT_RESERVATION_AGENT_NAME;
  }

  public String getReplanner(String queue) {
    return DEFAULT_RESERVATION_PLANNER_NAME;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/ReservationSystem.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

import java.util.Map;

  void setRMContext(RMContext rmContext);

  void reinitialize(Configuration conf, RMContext rmContext)

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/ReservationSystemTestUtil.java
  public static ReservationSchedulerConfiguration createConf(
      String reservationQ, long timeWindow, float instConstraint,
      float avgConstraint) {
    ReservationSchedulerConfiguration conf =
        mock(ReservationSchedulerConfiguration.class);
    when(conf.getReservationWindow(reservationQ)).thenReturn(timeWindow);
    when(conf.getInstantaneousMaxCapacity(reservationQ))
        .thenReturn(instConstraint);
    when(conf.getAverageCapacity(reservationQ)).thenReturn(avgConstraint);
    return conf;
  }
    Assert.assertEquals(8192, plan.getTotalCapacity().getMemory());
    Assert.assertTrue(
        plan.getReservationAgent() instanceof AlignedPlannerWithGreedy);
    Assert
        .assertTrue(plan.getSharingPolicy() instanceof CapacityOverTimePolicy);
  }

  public static void setupFSAllocationFile(String allocationFile)
    out.println("<reservation></reservation>");
    out.println("<weight>8</weight>");
    out.println("</queue>");
    out.println(
        "<defaultQueueSchedulingPolicy>drf</defaultQueueSchedulingPolicy>");
    out.println("</allocations>");
    out.close();
  }
    out.println("</queue>");
    out.println("<queue name=\"dedicated\">");
    out.println("<reservation></reservation>");
    out.println("<weight>10</weight>");
    out.println("</queue>");
    out.println("<queue name=\"reservation\">");
    out.println("<reservation></reservation>");
    out.println("<weight>80</weight>");
    out.println("</queue>");
    out.println(
        "<defaultQueueSchedulingPolicy>drf</defaultQueueSchedulingPolicy>");
    out.println("</allocations>");
    out.close();
  }

  public static FairScheduler setupFairScheduler(RMContext rmContext,
      Configuration conf, int numContainers) throws IOException {
    FairScheduler scheduler = new FairScheduler();
    scheduler.setRMContext(rmContext);

    scheduler.reinitialize(conf, rmContext);


    Resource resource =
        ReservationSystemTestUtil.calculateClusterResource(numContainers);
    RMNode node1 = MockNodes.newNodeInfo(1, resource, 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    return cs;
  }

  @SuppressWarnings("rawtypes") public static void initializeRMContext(
      int numContainers, AbstractYarnScheduler scheduler,
      RMContext mockRMContext) {

    when(mockRMContext.getScheduler()).thenReturn(scheduler);
    Resource r = calculateClusterResource(numContainers);
  }

  public static RMContext createRMContext(Configuration conf) {
    RMContext mockRmContext = Mockito.spy(
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(conf),
            new NMTokenSecretManagerInRM(conf),
            new ClientToAMTokenSecretManagerInRM(), null));

    RMNodeLabelsManager nlm = mock(RMNodeLabelsManager.class);
    when(nlm.getQueueResource(any(String.class), anySetOf(String.class),
            any(Resource.class))).thenAnswer(new Answer<Resource>() {
      @Override public Resource answer(InvocationOnMock invocation)
          throws Throwable {
        Object[] args = invocation.getArguments();
        return (Resource) args[2];
      }

    when(nlm.getResourceByLabel(any(String.class), any(Resource.class)))
        .thenAnswer(new Answer<Resource>() {
          @Override public Resource answer(InvocationOnMock invocation)
              throws Throwable {
            Object[] args = invocation.getArguments();
            return (Resource) args[1];
          }
    return mockRmContext;
  }

  public static void setupQueueConfiguration(
      CapacitySchedulerConfiguration conf) {
    final String defQ = CapacitySchedulerConfiguration.ROOT + ".default";
    conf.setCapacity(defQ, 10);

    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "default", "a", reservationQ });

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);

    final String dedicated =
        CapacitySchedulerConfiguration.ROOT + CapacitySchedulerConfiguration.DOT
            + reservationQ;
    conf.setCapacity(dedicated, 80);
    conf.setReservable(dedicated, true);
    conf.setCapacity(A2, 70);
  }

  public static String getFullReservationQueueName() {
    return CapacitySchedulerConfiguration.ROOT
        + CapacitySchedulerConfiguration.DOT + reservationQ;
  }

  public static String getReservationQueueName() {
    return reservationQ;
  }

  public static void updateQueueConfiguration(
      CapacitySchedulerConfiguration conf, String newQ) {
    final String prefix = CapacitySchedulerConfiguration.ROOT
        + CapacitySchedulerConfiguration.DOT;
    final String defQ = prefix + "default";
    conf.setCapacity(defQ, 5);

    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "default", "a", reservationQ, newQ });

    final String A = prefix + "a";
    conf.setCapacity(A, 5);

    final String dedicated = prefix + reservationQ;
    conf.setCapacity(dedicated, 10);
    conf.setReservable(dedicated, true);

    conf.setCapacity(prefix + newQ, 80);
    conf.setReservable(prefix + newQ, true);

    int gang = 1 + rand.nextInt(9);
    int par = (rand.nextInt(1000) + 1) * gang;
    long dur = rand.nextInt(2 * 3600 * 1000); // random duration within 2h
    ReservationRequest r = ReservationRequest
        .newInstance(Resource.newInstance(1024, 1), par, gang, dur);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setReservationResources(Collections.singletonList(r));
    rand.nextInt(3);

  }

  public static Map<ReservationInterval, Resource> generateAllocation(
      long startTime, long step, int[] alloc) {
    Map<ReservationInterval, Resource> req = new TreeMap<>();
    for (int i = 0; i < alloc.length; i++) {
      req.put(new ReservationInterval(startTime + i * step,
          startTime + (i + 1) * step), ReservationSystemUtil.toResource(
          ReservationRequest
              .newInstance(Resource.newInstance(1024, 1), alloc[i])));
    }
    return req;
  }

  public static Resource calculateClusterResource(int numContainers) {
    return Resource.newInstance(numContainers * 1024, numContainers);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestCapacitySchedulerPlanFollower.java
import org.mockito.Matchers;
import org.mockito.Mockito;

public class TestCapacitySchedulerPlanFollower extends
    TestSchedulerPlanFollowerBase {

  private RMContext rmContext;
  private RMContext spyRMContext;
  }

  private void setupPlanFollower() throws Exception {
    mClock = mock(Clock.class);
    mAgent = mock(ReservationAgent.class);

    String reservationQ =
        ReservationSystemTestUtil.getFullReservationQueueName();
    CapacitySchedulerConfiguration csConf = cs.getConfiguration();
    csConf.setReservationWindow(reservationQ, 20L);
    csConf.setMaximumCapacity(reservationQ, 40);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestFairSchedulerPlanFollower.java

public class TestFairSchedulerPlanFollower extends
    TestSchedulerPlanFollowerBase {
  private final static String ALLOC_FILE = new File(
      FairSchedulerTestBase.TEST_DIR,
      TestSchedulerPlanFollowerBase.class.getName() + ".xml").getAbsolutePath();
  private RMContext rmContext;
  private RMContext spyRMContext;
  private FairScheduler fs;
  public void setUp() throws Exception {
    conf = createConfiguration();
    ReservationSystemTestUtil.setupFSAllocationFile(ALLOC_FILE);

    rmContext = TestUtils.getMockRMContext();
    spyRMContext = spy(rmContext);
    fs = ReservationSystemTestUtil.setupFairScheduler(spyRMContext, conf, 125);
    scheduler = fs;

    ConcurrentMap<ApplicationId, RMApp> spyApps =
  }

  private void setupPlanFollower() throws Exception {
    mClock = mock(Clock.class);
    mAgent = mock(ReservationAgent.class);

    String reservationQ =
        ReservationSystemTestUtil.getFullReservationQueueName();
    AllocationConfiguration allocConf = fs.getAllocationConfiguration();
    allocConf.setReservationWindow(20L);
    allocConf.setAverageCapacity(20);

  @Override
  protected void verifyCapacity(Queue defQ) {
    assertTrue(((FSQueue) defQ).getWeights().getWeight(ResourceType.MEMORY) > 0.9);
  }

  @Override
  protected Queue getDefaultQueue() {
    return getReservationQueue("dedicated"
        + ReservationConstants.DEFAULT_QUEUE_SUFFIX);
  }

  @Override

  @Override
  protected AbstractSchedulerPlanFollower createPlanFollower() {
    FairSchedulerPlanFollower planFollower = new FairSchedulerPlanFollower();
    planFollower.init(mClock, scheduler, Collections.singletonList(plan));
    return planFollower;
  }
  @Override
  protected void assertReservationQueueExists(ReservationId r,
      double expectedCapacity, double expectedMaxCapacity) {
    FSLeafQueue q =
        fs.getQueueManager().getLeafQueue(plan.getQueueName() + "" + "." + r,
            false);
    assertNotNull(q);
    Assert.assertEquals(expectedCapacity,
        q.getWeights().getWeight(ResourceType.MEMORY), 0.01);
  }

  @Override

  @Override
  protected Queue getReservationQueue(String r) {
    return fs.getQueueManager().getLeafQueue(
        plan.getQueueName() + "" + "." + r, false);
  }

  public static ApplicationACLsManager mockAppACLsManager() {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestReservationSystem.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestReservationSystem.java
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ParameterizedSchedulerTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;

@SuppressWarnings({ "rawtypes" })
public class TestReservationSystem extends
    ParameterizedSchedulerTestBase {

  private final static String ALLOC_FILE = new File(
      FairSchedulerTestBase.TEST_DIR, TestReservationSystem.class.getName()
          + ".xml").getAbsolutePath();
  private AbstractYarnScheduler scheduler;
  private AbstractReservationSystem reservationSystem;
  private RMContext rmContext;
  private Configuration conf;
  private RMContext mockRMContext;

  public TestReservationSystem(SchedulerType type) {
    super(type);
  }

  @Before
  public void setUp() throws IOException {
    scheduler = initializeScheduler();
    rmContext = getRMContext();
    reservationSystem = configureReservationSystem();
    reservationSystem.setRMContext(rmContext);
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  @After
  public void tearDown() {
    conf = null;
    reservationSystem = null;
    rmContext = null;
    scheduler = null;
    clearRMContext();
    QueueMetrics.clearQueueMetrics();
  }

  @Test
  public void testInitialize() throws IOException {
    try {
      reservationSystem.reinitialize(scheduler.getConfig(), rmContext);
    } catch (YarnException e) {
      Assert.fail(e.getMessage());
    }
    if (getSchedulerType().equals(SchedulerType.CAPACITY)) {
      ReservationSystemTestUtil.validateReservationQueue(reservationSystem,
          ReservationSystemTestUtil.getReservationQueueName());
    } else {
      ReservationSystemTestUtil.validateReservationQueue(reservationSystem,
          ReservationSystemTestUtil.getFullReservationQueueName());
    }

  }

  @Test
  public void testReinitialize() throws IOException {
    conf = scheduler.getConfig();
    try {
      reservationSystem.reinitialize(conf, rmContext);
    } catch (YarnException e) {
      Assert.fail(e.getMessage());
    }
    if (getSchedulerType().equals(SchedulerType.CAPACITY)) {
      ReservationSystemTestUtil.validateReservationQueue(reservationSystem,
          ReservationSystemTestUtil.getReservationQueueName());
    } else {
      ReservationSystemTestUtil.validateReservationQueue(reservationSystem,
          ReservationSystemTestUtil.getFullReservationQueueName());
    }

    String newQ = "reservation";
    Assert.assertNull(reservationSystem.getPlan(newQ));
    updateSchedulerConf(conf, newQ);
    try {
      scheduler.reinitialize(conf, rmContext);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
    try {
      reservationSystem.reinitialize(conf, rmContext);
    } catch (YarnException e) {
      Assert.fail(e.getMessage());
    }
    if (getSchedulerType().equals(SchedulerType.CAPACITY)) {
      ReservationSystemTestUtil.validateReservationQueue(reservationSystem,
          newQ);
    } else {
      ReservationSystemTestUtil.validateReservationQueue(reservationSystem,
          "root." + newQ);
    }
  }

  @SuppressWarnings("rawtypes")
  public AbstractYarnScheduler initializeScheduler() throws IOException {
    switch (getSchedulerType()) {
    case CAPACITY:
      return initializeCapacityScheduler();
    case FAIR:
      return initializeFairScheduler();
    }
    return null;
  }

  public AbstractReservationSystem configureReservationSystem() {
    switch (getSchedulerType()) {
    case CAPACITY:
      return new CapacityReservationSystem();
    case FAIR:
      return new FairReservationSystem();
    }
    return null;
  }

  public void updateSchedulerConf(Configuration conf, String newQ)
      throws IOException {
    switch (getSchedulerType()) {
    case CAPACITY:
      ReservationSystemTestUtil.updateQueueConfiguration(
          (CapacitySchedulerConfiguration) conf, newQ);
    case FAIR:
      ReservationSystemTestUtil.updateFSAllocationFile(ALLOC_FILE);
    }
  }

  public RMContext getRMContext() {
    return mockRMContext;
  }

  public void clearRMContext() {
    mockRMContext = null;
  }

  private CapacityScheduler initializeCapacityScheduler() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    ReservationSystemTestUtil.setupQueueConfiguration(conf);

    CapacityScheduler cs = Mockito.spy(new CapacityScheduler());
    cs.setConf(conf);

    mockRMContext = ReservationSystemTestUtil.createRMContext(conf);

    cs.setRMContext(mockRMContext);
    try {
      cs.serviceInit(conf);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    ReservationSystemTestUtil.initializeRMContext(10, cs, mockRMContext);
    return cs;
  }

  private Configuration createFSConfiguration() {
    FairSchedulerTestBase testHelper = new FairSchedulerTestBase();
    Configuration conf = testHelper.createConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
        ResourceScheduler.class);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    return conf;
  }

  private FairScheduler initializeFairScheduler() throws IOException {
    Configuration conf = createFSConfiguration();
    ReservationSystemTestUtil.setupFSAllocationFile(ALLOC_FILE);

    mockRMContext = ReservationSystemTestUtil.createRMContext(conf);
    return ReservationSystemTestUtil
        .setupFairScheduler(mockRMContext, conf, 10);
  }
}

