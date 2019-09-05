hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/capacity/ProportionalCapacityPreemptionPolicy.java
    synchronized (curQueue) {
      String queueName = curQueue.getQueueName();
      QueueCapacities qc = curQueue.getQueueCapacities();
      float absCap = qc.getAbsoluteCapacity(partitionToLookAt);
      float absMaxCap = qc.getAbsoluteMaximumCapacity(partitionToLookAt);
      boolean preemptionDisabled = curQueue.getPreemptionDisabled();

      Resource current = curQueue.getQueueResourceUsage().getUsed(
          partitionToLookAt);
      Resource guaranteed = Resources.multiply(partitionResource, absCap);
      Resource maxCapacity = Resources.multiply(partitionResource, absMaxCap);


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/capacity/TestProportionalCapacityPreemptionPolicy.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
            appA.getApplicationId(), appA.getAttemptId());
    assertTrue("appA should be running on queueB",
        mCS.getAppsInQueue("queueB").contains(expectedAttemptOnQueueB));
    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appA)));

    setup();
    verify(mDisp, never()).handle(argThat(new IsPreemptionRequestFor(appA)));
    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appC)));
  }

  @Test
    verify(mDisp, times(17)).handle(argThat(new IsPreemptionRequestFor(appA)));
    verify(mDisp, times(183)).handle(argThat(new IsPreemptionRequestFor(appB)));

    policy.editSchedule();
    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appA)));
  }

  @Test
    verify(mDisp, times(15)).handle(argThat(new IsPreemptionRequestFor(appC)));
  }
  
  

    verify(mDisp, times(9)).handle(argThat(new IsPreemptionRequestFor(appA)));
    verify(mDisp, times(6)).handle(argThat(new IsPreemptionRequestFor(appE)));
  }

  @Test
    setAMContainer = false;
  }

  @Test
  public void testPreemptionWithVCoreResource() {
    int[][] qData = new int[][]{
        {100, 100, 100}, // maxcap
        {5, 1, 1}, // apps
        {2, 0, 0}, // subqueues
    };

    String[][] resData = new String[][]{
        {"100:100", "50:50", "50:50"}, // abs
        {"10:100", "10:100", "0"}, // used
        {"70:20", "70:20", "10:100"}, // pending
        {"0", "0", "0"}, // reserved
        {"-1", "1:10", "1:10"}, // req granularity
    };

    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData, resData,
        true);
    policy.editSchedule();

    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appA)));
  }

  static class IsPreemptionRequestFor
      extends ArgumentMatcher<ContainerPreemptEvent> {
    private final ApplicationAttemptId appAttId;
  }

  ProportionalCapacityPreemptionPolicy buildPolicy(int[][] qData) {
    ProportionalCapacityPreemptionPolicy policy = new ProportionalCapacityPreemptionPolicy(
        conf, rmContext, mCS, mClock);
    clusterResources = Resource.newInstance(
        leafAbsCapacities(qData[0], qData[7]), 0);
    ParentQueue mRoot = buildMockRootQueue(rand, qData);
    when(mCS.getRootQueue()).thenReturn(mRoot);

    setResourceAndNodeDetails();
    return policy;
  }

  ProportionalCapacityPreemptionPolicy buildPolicy(int[][] qData,
      String[][] resData) {
    return buildPolicy(qData, resData, false);
  }

  ProportionalCapacityPreemptionPolicy buildPolicy(int[][] qData,
      String[][] resData, boolean useDominantResourceCalculator) {
    if (useDominantResourceCalculator) {
      when(mCS.getResourceCalculator()).thenReturn(
          new DominantResourceCalculator());
    }
    ProportionalCapacityPreemptionPolicy policy =
        new ProportionalCapacityPreemptionPolicy(conf, rmContext, mCS, mClock);
    clusterResources = leafAbsCapacities(parseResourceDetails(resData[0]),
        qData[2]);
    ParentQueue mRoot = buildMockRootQueue(rand, resData, qData);
    when(mCS.getRootQueue()).thenReturn(mRoot);

    setResourceAndNodeDetails();
    return policy;
  }

  private void setResourceAndNodeDetails() {
    when(mCS.getClusterResource()).thenReturn(clusterResources);
    when(lm.getResourceByLabel(anyString(), any(Resource.class))).thenReturn(
        clusterResources);
    SchedulerNode mNode = mock(SchedulerNode.class);
    when(mNode.getPartition()).thenReturn(RMNodeLabelsManager.NO_LABEL);
    when(mCS.getSchedulerNode(any(NodeId.class))).thenReturn(mNode);
  }

  ParentQueue buildMockRootQueue(Random r, int[]... queueData) {
    Resource[] abs = generateResourceList(queueData[0]);
    Resource[] used = generateResourceList(queueData[2]);
    Resource[] pending = generateResourceList(queueData[3]);
    Resource[] reserved = generateResourceList(queueData[4]);
    Resource[] gran = generateResourceList(queueData[6]);
    int[] maxCap = queueData[1];
    int[] apps = queueData[5];
    int[] queues = queueData[7];

    return mockNested(abs, maxCap, used, pending, reserved, apps, gran, queues);
  }

  ParentQueue buildMockRootQueue(Random r, String[][] resData,
      int[]... queueData) {
    Resource[] abs = parseResourceDetails(resData[0]);
    Resource[] used = parseResourceDetails(resData[1]);
    Resource[] pending = parseResourceDetails(resData[2]);
    Resource[] reserved = parseResourceDetails(resData[3]);
    Resource[] gran = parseResourceDetails(resData[4]);
    int[] maxCap = queueData[0];
    int[] apps = queueData[1];
    int[] queues = queueData[2];

    return mockNested(abs, maxCap, used, pending, reserved, apps, gran, queues);
  }

  Resource[] parseResourceDetails(String[] resData) {
    List<Resource> resourceList = new ArrayList<Resource>();
    for (int i = 0; i < resData.length; i++) {
      String[] resource = resData[i].split(":");
      if (resource.length == 1) {
        resourceList.add(Resource.newInstance(Integer.valueOf(resource[0]), 0));
      } else {
        resourceList.add(Resource.newInstance(Integer.valueOf(resource[0]),
            Integer.valueOf(resource[1])));
      }
    }
    return resourceList.toArray(new Resource[resourceList.size()]);
  }

  Resource[] generateResourceList(int[] qData) {
    List<Resource> resourceList = new ArrayList<Resource>();
    for (int i = 0; i < qData.length; i++) {
      resourceList.add(Resource.newInstance(qData[i], 0));
    }
    return resourceList.toArray(new Resource[resourceList.size()]);
  }

  ParentQueue mockNested(Resource[] abs, int[] maxCap, Resource[] used,
      Resource[] pending, Resource[] reserved, int[] apps, Resource[] gran,
      int[] queues) {
    ResourceCalculator rc = mCS.getResourceCalculator();
    Resource tot = leafAbsCapacities(abs, queues);
    Deque<ParentQueue> pqs = new LinkedList<ParentQueue>();
    ParentQueue root = mockParentQueue(null, queues[0], pqs);
    ResourceUsage resUsage = new ResourceUsage();
    resUsage.setUsed(used[0]);
    when(root.getQueueName()).thenReturn(CapacitySchedulerConfiguration.ROOT);
    when(root.getAbsoluteUsedCapacity()).thenReturn(
        Resources.divide(rc, tot, used[0], tot));
    when(root.getAbsoluteCapacity()).thenReturn(
        Resources.divide(rc, tot, abs[0], tot));
    when(root.getAbsoluteMaximumCapacity()).thenReturn(
        maxCap[0] / (float) tot.getMemory());
    when(root.getQueueResourceUsage()).thenReturn(resUsage);
    QueueCapacities rootQc = new QueueCapacities(true);
    rootQc.setAbsoluteUsedCapacity(Resources.divide(rc, tot, used[0], tot));
    rootQc.setAbsoluteCapacity(Resources.divide(rc, tot, abs[0], tot));
    rootQc.setAbsoluteMaximumCapacity(maxCap[0] / (float) tot.getMemory());
    when(root.getQueueCapacities()).thenReturn(rootQc);
    when(root.getQueuePath()).thenReturn(CapacitySchedulerConfiguration.ROOT);
    boolean preemptionDisabled = mockPreemptionStatus("root");
      final String queueName = "queue" + ((char) ('A' + i - 1));
      if (queues[i] > 0) {
        q = mockParentQueue(p, queues[i], pqs);
        ResourceUsage resUsagePerQueue = new ResourceUsage();
        resUsagePerQueue.setUsed(used[i]);
        when(q.getQueueResourceUsage()).thenReturn(resUsagePerQueue);
      } else {
        q = mockLeafQueue(p, tot, i, abs, used, pending, reserved, apps, gran);
      }
      when(q.getParent()).thenReturn(p);
      when(q.getQueueName()).thenReturn(queueName);
      when(q.getAbsoluteUsedCapacity()).thenReturn(
          Resources.divide(rc, tot, used[i], tot));
      when(q.getAbsoluteCapacity()).thenReturn(
          Resources.divide(rc, tot, abs[i], tot));
      when(q.getAbsoluteMaximumCapacity()).thenReturn(
          maxCap[i] / (float) tot.getMemory());

      QueueCapacities qc = new QueueCapacities(false);
      qc.setAbsoluteUsedCapacity(Resources.divide(rc, tot, used[i], tot));
      qc.setAbsoluteCapacity(Resources.divide(rc, tot, abs[i], tot));
      qc.setAbsoluteMaximumCapacity(maxCap[i] / (float) tot.getMemory());
      when(q.getQueueCapacities()).thenReturn(qc);

      String parentPathName = p.getQueuePath();
      parentPathName = (parentPathName == null) ? "root" : parentPathName;
      String queuePathName = (parentPathName + "." + queueName).replace("/",
          "root");
      when(q.getQueuePath()).thenReturn(queuePathName);
      preemptionDisabled = mockPreemptionStatus(queuePathName);
      when(q.getPreemptionDisabled()).thenReturn(preemptionDisabled);
  }

  @SuppressWarnings("rawtypes")
  LeafQueue mockLeafQueue(ParentQueue p, Resource tot, int i, Resource[] abs,
      Resource[] used, Resource[] pending, Resource[] reserved, int[] apps,
      Resource[] gran) {
    LeafQueue lq = mock(LeafQueue.class);
    ResourceCalculator rc = mCS.getResourceCalculator();
    List<ApplicationAttemptId> appAttemptIdList = 
        new ArrayList<ApplicationAttemptId>();
    when(lq.getTotalResourcePending()).thenReturn(pending[i]);
    ResourceUsage ru = new ResourceUsage();
    ru.setPending(pending[i]);
    ru.setUsed(used[i]);
    when(lq.getQueueResourceUsage()).thenReturn(ru);
    final NavigableSet<FiCaSchedulerApp> qApps = new TreeSet<FiCaSchedulerApp>(
      });
    if (apps[i] != 0) {
      Resource aUsed = Resources.divideAndCeil(rc, used[i], apps[i]);
      Resource aPending = Resources.divideAndCeil(rc, pending[i], apps[i]);
      Resource aReserve = Resources.divideAndCeil(rc, reserved[i], apps[i]);
      for (int a = 0; a < apps[i]; ++a) {
        FiCaSchedulerApp mockFiCaApp =
            mockApp(i, appAlloc, aUsed, aPending, aReserve, gran[i]);
    return lq;
  }

  FiCaSchedulerApp mockApp(int qid, int id, Resource used, Resource pending,
      Resource reserved, Resource gran) {
    FiCaSchedulerApp app = mock(FiCaSchedulerApp.class);
    ResourceCalculator rc = mCS.getResourceCalculator();

    ApplicationId appId = ApplicationId.newInstance(TS, id);
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(appId, 0);
    when(app.getApplicationAttemptId()).thenReturn(appAttId);

    int cAlloc = 0;
    Resource unit = gran;
    List<RMContainer> cReserved = new ArrayList<RMContainer>();
    Resource resIter = Resource.newInstance(0, 0);
    for (; Resources.lessThan(rc, clusterResources, resIter, reserved); Resources
        .addTo(resIter, gran)) {
      cReserved.add(mockContainer(appAttId, cAlloc, unit,
          priority.CONTAINER.getValue()));
      ++cAlloc;
    }
    when(app.getReservedContainers()).thenReturn(cReserved);

    List<RMContainer> cLive = new ArrayList<RMContainer>();
    Resource usedIter = Resource.newInstance(0, 0);
    int i = 0;
    for (; Resources.lessThan(rc, clusterResources, usedIter, used); Resources
        .addTo(usedIter, gran)) {
      if (setAMContainer && i == 0) {
        cLive.add(mockContainer(appAttId, cAlloc, unit,
            priority.AMCONTAINER.getValue()));
      } else if (setLabeledContainer && i == 1) {
        cLive.add(mockContainer(appAttId, cAlloc, unit,
            priority.LABELEDCONTAINER.getValue()));
        Resources.addTo(used, Resource.newInstance(1, 1));
      } else {
        cLive.add(mockContainer(appAttId, cAlloc, unit,
            priority.CONTAINER.getValue()));
      }
      ++cAlloc;
      ++i;
    }
    when(app.getLiveContainers()).thenReturn(cLive);
    return app;
    return ret;
  }

  static Resource leafAbsCapacities(Resource[] abs, int[] subqueues) {
    Resource ret = Resource.newInstance(0, 0);
    for (int i = 0; i < abs.length; ++i) {
      if (0 == subqueues[i]) {
        Resources.addTo(ret, abs[i]);
      }
    }
    return ret;
  }

  void printString(CSQueue nq, String indent) {
    if (nq instanceof ParentQueue) {
      System.out.println(indent + nq.getQueueName()

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/capacity/TestProportionalCapacityPreemptionPolicyForNodePartitions.java
