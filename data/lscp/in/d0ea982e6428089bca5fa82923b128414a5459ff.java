hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/capacity/ProportionalCapacityPreemptionPolicy.java

        synchronized (qT.leafQueue) {
          Iterator<FiCaSchedulerApp> desc =   
            qT.leafQueue.getOrderingPolicy().getPreemptionIterator();
          qT.actuallyPreempted = Resources.clone(resToObtain);
          while (desc.hasNext()) {
            FiCaSchedulerApp fc = desc.next();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/SchedulerApplicationAttempt.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.SchedulableEntity;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
@Private
@Unstable
public class SchedulerApplicationAttempt implements SchedulableEntity {
  
  private static final Log LOG = LogFactory
    .getLog(SchedulerApplicationAttempt.class);
  public ResourceUsage getAppAttemptResourceUsage() {
    return this.attemptResourceUsage;
  }
  
  @Override
  public String getId() {
    return getApplicationId().toString();
  }
  
  @Override
  public int compareInputOrderTo(SchedulableEntity other) {
    if (other instanceof SchedulerApplicationAttempt) {
      return getApplicationId().compareTo(
        ((SchedulerApplicationAttempt)other).getApplicationId());
    }
    return 1;//let other types go before this, if any
  }
  
  @Override
  public synchronized ResourceUsage getSchedulingResourceUsage() {
    return attemptResourceUsage;
  }
  
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacitySchedulerConfiguration.java
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.*;


import com.google.common.collect.ImmutableSet;

public class CapacitySchedulerConfiguration extends ReservationSchedulerConfiguration {
  public static final String MAXIMUM_ALLOCATION_VCORES =
      "maximum-allocation-vcores";
  
  public static final String ORDERING_POLICY = "ordering-policy";
  
  public static final String DEFAULT_ORDERING_POLICY = "fifo";
  
  @Private
  public static final int DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS = 10000;
  
    return userLimit;
  }
  
  @SuppressWarnings("unchecked")
  public <S extends SchedulableEntity> OrderingPolicy<S> getOrderingPolicy(
      String queue) {
  
    String policyType = get(getQueuePrefix(queue) + ORDERING_POLICY, 
      DEFAULT_ORDERING_POLICY);
    
    OrderingPolicy<S> orderingPolicy;
    
    if (policyType.trim().equals("fifo")) {
       policyType = FifoOrderingPolicy.class.getName();
    }
    try {
      orderingPolicy = (OrderingPolicy<S>)
        Class.forName(policyType).newInstance();
    } catch (Exception e) {
      String message = "Unable to construct ordering policy for: " + policyType + ", " + e.getMessage();
      throw new RuntimeException(message, e);
    }
    return orderingPolicy;
  }

  public void setUserLimit(String queue, int userLimit) {
    setInt(getQueuePrefix(queue) + USER_LIMIT, userLimit);
    LOG.debug("here setUserLimit: queuePrefix=" + getQueuePrefix(queue) + 

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/LeafQueue.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
  
  private int nodeLocalityDelay;

  Map<ApplicationAttemptId, FiCaSchedulerApp> applicationAttemptMap = 
      new HashMap<ApplicationAttemptId, FiCaSchedulerApp>();
  
  
  private volatile ResourceLimits currentResourceLimits = null;
  
  private OrderingPolicy<FiCaSchedulerApp> 
    orderingPolicy = new FifoOrderingPolicy<FiCaSchedulerApp>();
  
  public LeafQueue(CapacitySchedulerContext cs, 
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    super(cs, queueName, parent, old);
        cs.getApplicationComparator();
    this.pendingApplications = 
        new TreeSet<FiCaSchedulerApp>(applicationComparator);
    
    setupQueueConfigs(cs.getClusterResource());
  }
    setQueueResourceLimitsInfo(clusterResource);

    CapacitySchedulerConfiguration conf = csContext.getConfiguration();
    
    setOrderingPolicy(conf.<FiCaSchedulerApp>getOrderingPolicy(getQueuePath()));
    
    userLimit = conf.getUserLimit(getQueuePath());
    userLimitFactor = conf.getUserLimitFactor(getQueuePath());

  }

  public synchronized int getNumActiveApplications() {
    return orderingPolicy.getNumSchedulableEntities();
  }

  @Private
        }
      }
      user.activateApplication();
      orderingPolicy.addSchedulableEntity(application);
      queueUsage.incAMUsed(application.getAMResource());
      user.getResourceUsage().incAMUsed(application.getAMResource());
      i.remove();

  public synchronized void removeApplicationAttempt(
      FiCaSchedulerApp application, User user) {
    boolean wasActive =
      orderingPolicy.removeSchedulableEntity(application);
    if (!wasActive) {
      pendingApplications.remove(application);
    } else {
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("assignContainers: node=" + node.getNodeName()
        + " #applications=" + 
        orderingPolicy.getNumSchedulableEntities());
    }
    
      return NULL_ASSIGNMENT;
    }
    
    for (Iterator<FiCaSchedulerApp> assignmentIterator =
        orderingPolicy.getAssignmentIterator();
        assignmentIterator.hasNext();) {
      FiCaSchedulerApp application = assignmentIterator.next();
      if(LOG.isDebugEnabled()) {
        LOG.debug("pre-assignContainers for application "
        + application.getApplicationId());
      node.allocateContainer(allocatedContainer);
            
      orderingPolicy.containerAllocated(application, allocatedContainer);

      LOG.info("assignedContainer" +
          " application attempt=" + application.getApplicationAttemptId() +
          " container=" + container + 

        if (removed) {
          
          orderingPolicy.containerReleased(application, rmContainer);
          
          releaseResource(clusterResource, application,
              container.getResource(), node.getPartition());
          LOG.info("completedContainer" +
    activateApplications();

    for (FiCaSchedulerApp application :
      orderingPolicy.getSchedulableEntities()) {
      synchronized (application) {
        computeUserLimitAndSetHeadroom(application, clusterResource,
            Resources.none(), RMNodeLabelsManager.NO_LABEL,
  public Collection<FiCaSchedulerApp> getApplications() {
    return orderingPolicy.getSchedulableEntities();
  }

  public synchronized Resource getTotalResourcePending() {
    Resource ret = BuilderUtils.newResource(0, 0);
    for (FiCaSchedulerApp f : 
      orderingPolicy.getSchedulableEntities()) {
      Resources.addTo(ret, f.getTotalPendingRequests());
    }
    return ret;
    for (FiCaSchedulerApp pendingApp : pendingApplications) {
      apps.add(pendingApp.getApplicationAttemptId());
    }
    for (FiCaSchedulerApp app : 
      orderingPolicy.getSchedulableEntities()) {
      apps.add(app.getApplicationAttemptId());
    }
  }
    this.maxApplications = maxApplications;
  }
  
  public synchronized OrderingPolicy<FiCaSchedulerApp>
      getOrderingPolicy() {
    return orderingPolicy;
  }
  
  public synchronized void setOrderingPolicy(
      OrderingPolicy<FiCaSchedulerApp> orderingPolicy) {
   orderingPolicy.addAllSchedulableEntities(
     this.orderingPolicy.getSchedulableEntities()
     );
    this.orderingPolicy = orderingPolicy;
  }
  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/AbstractComparatorOrderingPolicy.java
    schedulableEntities.add(schedulableEntity);
  }
  
  @VisibleForTesting
  public Comparator<SchedulableEntity> getComparator() {
    return comparator; 
  }
  
  @Override
  public abstract void configure(Map<String, String> conf);
  
  @Override
  public abstract void containerAllocated(S schedulableEntity, 
    RMContainer r);
  
  @Override
  public abstract String getInfo();
  
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/FifoOrderingPolicy.java
public class FifoOrderingPolicy<S extends SchedulableEntity> extends AbstractComparatorOrderingPolicy<S> {
  
  public FifoOrderingPolicy() {
    this.comparator = new FifoComparator();
    this.schedulableEntities = new TreeSet<S>(comparator);
  }
  
  @Override
  public void configure(Map<String, String> conf) {
    
  }
  
    }
  
  @Override
  public String getInfo() {
    return "FifoOrderingPolicy";
  }
  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/OrderingPolicy.java
  public void configure(Map<String, String> conf);
  
  public String getInfo();
  
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/CapacitySchedulerPage.java
          _("Configured Minimum User Limit Percent:", Integer.toString(lqinfo.getUserLimit()) + "%").
          _("Configured User Limit Factor:", String.format("%.1f", lqinfo.getUserLimitFactor())).
          _("Accessible Node Labels:", StringUtils.join(",", lqinfo.getNodeLabels())).
          _("Ordering Policy: ", lqinfo.getOrderingPolicyInfo()).
          _("Preemption:", lqinfo.getPreemptionDisabled() ? "disabled" : "enabled");

      html._(InfoBlock.class);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/CapacitySchedulerLeafQueueInfo.java
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;

  protected ResourceInfo userAMResourceLimit;
  protected boolean preemptionDisabled;
  
  @XmlTransient
  protected String orderingPolicyInfo;

  CapacitySchedulerLeafQueueInfo() {
  };

    usedAMResource = new ResourceInfo(q.getQueueResourceUsage().getAMUsed());
    userAMResourceLimit = new ResourceInfo(q.getUserAMResourceLimit());
    preemptionDisabled = q.getPreemptionDisabled();
    orderingPolicyInfo = q.getOrderingPolicy().getInfo();
  }

  public int getNumActiveApplications() {
  public boolean getPreemptionDisabled() {
    return preemptionDisabled;
  }
  
  public String getOrderingPolicyInfo() {
    return orderingPolicyInfo;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/capacity/TestProportionalCapacityPreemptionPolicy.java
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.WAIT_TIME_BEFORE_KILL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEventType.KILL_CONTAINER;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEventType.PREEMPT_CONTAINER;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer; 

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
    when(lq.getTotalResourcePending()).thenReturn(
        Resource.newInstance(pending[i], 0));
    final NavigableSet<FiCaSchedulerApp> qApps = new TreeSet<FiCaSchedulerApp>(
      new Comparator<FiCaSchedulerApp>() {
        @Override
        public int compare(FiCaSchedulerApp a1, FiCaSchedulerApp a2) {
              .thenReturn(appAttemptIdList);
    }
    when(lq.getApplications()).thenReturn(qApps);
    @SuppressWarnings("unchecked")
    OrderingPolicy<FiCaSchedulerApp> so = mock(OrderingPolicy.class);
    when(so.getPreemptionIterator()).thenAnswer(new Answer() {
     public Object answer(InvocationOnMock invocation) {
         return qApps.descendingIterator();
       }
     });
    when(lq.getOrderingPolicy()).thenReturn(so);
    if(setAMResourcePercent != 0.0f){
      when(lq.getMaxAMResourcePerQueuePercent()).thenReturn(setAMResourcePercent);
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestApplicationLimits.java
    doReturn(applicationAttemptId). when(application).getApplicationAttemptId();
    doReturn(user).when(application).getUser();
    doReturn(amResource).when(application).getAMResource();
    when(application.compareInputOrderTo(any(FiCaSchedulerApp.class))).thenCallRealMethod(); 
    return application;
  }
  
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertTrue(queue.getApplications().contains(app_0));

    FiCaSchedulerApp app_1 = getMockApplication(APPLICATION_ID++, user_0,
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertTrue(queue.getApplications().contains(app_1));

    FiCaSchedulerApp app_2 = getMockApplication(APPLICATION_ID++, user_0,
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertFalse(queue.pendingApplications.contains(app_2));
    assertFalse(queue.getApplications().contains(app_2));

    queue.finishApplicationAttempt(app_0, A);
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertTrue(queue.getApplications().contains(app_3));
    assertFalse(queue.pendingApplications.contains(app_3));
    assertFalse(queue.getApplications().contains(app_0));

    queue.finishApplicationAttempt(app_1, A);
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertFalse(queue.getApplications().contains(app_1));

    queue.finishApplicationAttempt(app_3, A);
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(0, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertFalse(queue.getApplications().contains(app_3));
  }

  @Test

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestLeafQueue.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.SchedulableEntity;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
    d.submitApplicationAttempt(app_1, user_d); // same user
  }

  @Test
  public void testPolicyConfiguration() throws Exception {
    
    CapacitySchedulerConfiguration testConf = 
        new CapacitySchedulerConfiguration();
    
    String tproot = CapacitySchedulerConfiguration.ROOT + "." + 
      "testPolicyRoot" + System.currentTimeMillis();

    OrderingPolicy<FiCaSchedulerApp> comPol =    
      testConf.<FiCaSchedulerApp>getOrderingPolicy(tproot);
    
    
  }

  @Test
  public void testAppAttemptMetrics() throws Exception {
    e.submitApplicationAttempt(app_2, user_e);  // same user

    assertEquals(2, e.getNumActiveApplications());
    assertEquals(1, e.pendingApplications.size());

    csConf.setDouble(CapacitySchedulerConfiguration
    root.reinitialize(newRoot, csContext.getClusterResource());

    assertEquals(3, e.getNumActiveApplications());
    assertEquals(0, e.pendingApplications.size());
  }
  
    e.submitApplicationAttempt(app_2, user_e);  // same user

    assertEquals(2, e.getNumActiveApplications());
    assertEquals(1, e.pendingApplications.size());

    Resource clusterResource = Resources.createResource(200 * 16 * GB, 100 * 32); 
        new ResourceLimits(clusterResource));

    assertEquals(3, e.getNumActiveApplications());
    assertEquals(0, e.pendingApplications.size());
  }

    }
  }
  
    @Test
  public void testFifoAssignment() throws Exception {

    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));
    
    a.setOrderingPolicy(new FifoOrderingPolicy<FiCaSchedulerApp>()); 
    
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
    Assert.assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    Assert.assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());
    
    a.assignContainers(clusterResource, node_0_0, new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    Assert.assertEquals(2*GB, app_1.getCurrentConsumption().getMemory());

  }

  @Test
  public void testConcurrentAccess() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();

