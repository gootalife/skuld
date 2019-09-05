hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/AppSchedulingInfo.java
  synchronized public boolean updateResourceRequests(
      List<ResourceRequest> requests, boolean recoverPreemptedRequest) {
    QueueMetrics metrics = queue.getMetrics();
    
    boolean anyResourcesUpdated = false;

    for (ResourceRequest request : requests) {
      Priority priority = request.getPriority();
              + request);
        }
        updatePendingResources = true;
        anyResourcesUpdated = true;
        
        }
      }
    }
    return anyResourcesUpdated;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/SchedulerApplicationAttempt.java
    return queue;
  }
  
  public synchronized boolean updateResourceRequests(
      List<ResourceRequest> requests) {
    if (!isStopped) {
      return appSchedulingInfo.updateResourceRequests(requests, false);
    }
    return false;
  }
  
  public synchronized void recoverResourceRequests(

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacityScheduler.java
    releaseContainers(release, application);

    Allocation allocation;

    LeafQueue updateDemandForQueue = null;

    synchronized (application) {

        application.showRequests();
  
        if (application.updateResourceRequests(ask)) {
          updateDemandForQueue = (LeafQueue) application.getQueue();
        }

        LOG.debug("allocate: post-update");
        application.showRequests();

      application.updateBlacklist(blacklistAdditions, blacklistRemovals);

      allocation = application.getAllocation(getResourceCalculator(),
                   clusterResource, getMinimumResourceCapability());
    }

    if (updateDemandForQueue != null) {
      updateDemandForQueue.getOrderingPolicy().demandUpdated(application);
    }

    return allocation;

  }

  @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/AbstractComparatorOrderingPolicy.java
                                            
  protected TreeSet<S> schedulableEntities;
  protected Comparator<SchedulableEntity> comparator;
  protected Map<String, S> entitiesToReorder = new HashMap<String, S>();
  
  public AbstractComparatorOrderingPolicy() { }
  
  
  @Override
  public Iterator<S> getAssignmentIterator() {
    reorderScheduleEntities();
    return schedulableEntities.iterator();
  }
  
  @Override
  public Iterator<S> getPreemptionIterator() {
    reorderScheduleEntities();
    return schedulableEntities.descendingIterator();
  }
  
    schedulableEntities.add(schedulableEntity);
  }
  
  protected void reorderScheduleEntities() {
    synchronized (entitiesToReorder) {
      for (Map.Entry<String, S> entry :
          entitiesToReorder.entrySet()) {
        reorderSchedulableEntity(entry.getValue());
      }
      entitiesToReorder.clear();
    }
  }

  protected void entityRequiresReordering(S schedulableEntity) {
    synchronized (entitiesToReorder) {
      entitiesToReorder.put(schedulableEntity.getId(), schedulableEntity);
    }
  }

  @VisibleForTesting
  public Comparator<SchedulableEntity> getComparator() {
    return comparator; 
  
  @Override
  public boolean removeSchedulableEntity(S s) {
    synchronized (entitiesToReorder) {
      entitiesToReorder.remove(s.getId());
    }
    return schedulableEntities.remove(s); 
  }
  
  public abstract void containerReleased(S schedulableEntity, 
    RMContainer r);
  
  @Override
  public abstract void demandUpdated(S schedulableEntity);

  @Override
  public abstract String getInfo();
  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/FairOrderingPolicy.java
  @Override
  public void containerAllocated(S schedulableEntity,
    RMContainer r) {
      entityRequiresReordering(schedulableEntity);
    }

  @Override
  public void containerReleased(S schedulableEntity,
    RMContainer r) {
      entityRequiresReordering(schedulableEntity);
    }

  @Override
  public void demandUpdated(S schedulableEntity) {
    if (sizeBasedWeight) {
      entityRequiresReordering(schedulableEntity);
    }
  }

  @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/FifoOrderingPolicy.java
    RMContainer r) {
    }

  @Override
  public void demandUpdated(S schedulableEntity) {
  }

  @Override
  public String getInfo() {
    return "FifoOrderingPolicy";

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/OrderingPolicy.java
  public void containerReleased(S schedulableEntity, 
    RMContainer r);
  
  void demandUpdated(S schedulableEntity);


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestCapacityScheduler.java
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
    rm.stop();
  }
  
  @Test
  public void testAllocateReorder() throws Exception {


    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    LeafQueue q = (LeafQueue) cs.getQueue("default");
    Assert.assertNotNull(q);

    FairOrderingPolicy fop = new FairOrderingPolicy();
    fop.setSizeBasedWeight(true);
    q.setOrderingPolicy(fop);

    String host = "127.0.0.1";
    RMNode node =
        MockNodes.newNodeInfo(0, MockNodes.newResource(4 * GB), 1, host);
    cs.handle(new NodeAddedSchedulerEvent(node));

    ApplicationId appId1 = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId1 = BuilderUtils.newApplicationAttemptId(
        appId1, 1);

    RMAppAttemptMetrics attemptMetric1 =
        new RMAppAttemptMetrics(appAttemptId1, rm.getRMContext());
    RMAppImpl app1 = mock(RMAppImpl.class);
    when(app1.getApplicationId()).thenReturn(appId1);
    RMAppAttemptImpl attempt1 = mock(RMAppAttemptImpl.class);
    when(attempt1.getAppAttemptId()).thenReturn(appAttemptId1);
    when(attempt1.getRMAppAttemptMetrics()).thenReturn(attemptMetric1);
    when(app1.getCurrentAppAttempt()).thenReturn(attempt1);

    rm.getRMContext().getRMApps().put(appId1, app1);

    SchedulerEvent addAppEvent1 =
        new AppAddedSchedulerEvent(appId1, "default", "user");
    cs.handle(addAppEvent1);
    SchedulerEvent addAttemptEvent1 =
        new AppAttemptAddedSchedulerEvent(appAttemptId1, false);
    cs.handle(addAttemptEvent1);

    ApplicationId appId2 = BuilderUtils.newApplicationId(100, 2);
    ApplicationAttemptId appAttemptId2 = BuilderUtils.newApplicationAttemptId(
        appId2, 1);

    RMAppAttemptMetrics attemptMetric2 =
        new RMAppAttemptMetrics(appAttemptId2, rm.getRMContext());
    RMAppImpl app2 = mock(RMAppImpl.class);
    when(app2.getApplicationId()).thenReturn(appId2);
    RMAppAttemptImpl attempt2 = mock(RMAppAttemptImpl.class);
    when(attempt2.getAppAttemptId()).thenReturn(appAttemptId2);
    when(attempt2.getRMAppAttemptMetrics()).thenReturn(attemptMetric2);
    when(app2.getCurrentAppAttempt()).thenReturn(attempt2);

    rm.getRMContext().getRMApps().put(appId2, app2);

    SchedulerEvent addAppEvent2 =
        new AppAddedSchedulerEvent(appId2, "default", "user");
    cs.handle(addAppEvent2);
    SchedulerEvent addAttemptEvent2 =
        new AppAttemptAddedSchedulerEvent(appAttemptId2, false);
    cs.handle(addAttemptEvent2);

    RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

    Priority priority = TestUtils.createMockPriority(1);
    ResourceRequest r1 = TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, true, priority, recordFactory);

    cs.allocate(appAttemptId1,
        Collections.<ResourceRequest>singletonList(r1),
        Collections.<ContainerId>emptyList(),
        null, null);

    CapacityScheduler.schedule(cs);

    assertEquals(q.getOrderingPolicy().getAssignmentIterator().next().getId(),
      appId1.toString());

    ResourceRequest r2 = TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, true, priority, recordFactory);
    cs.allocate(appAttemptId2,
        Collections.<ResourceRequest>singletonList(r2),
        Collections.<ContainerId>emptyList(),
        null, null);


    assertEquals(q.getOrderingPolicy().getAssignmentIterator().next().getId(),
      appId2.toString());

    rm.stop();
  }

  @Test
  public void testResourceOverCommit() throws Exception {
    Configuration conf = new Configuration();

