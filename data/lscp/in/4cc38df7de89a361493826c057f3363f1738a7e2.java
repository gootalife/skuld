hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FSAppAttempt.java
        + this.attemptResourceUsage.getReserved());
  }

  @Override
  public Resource getHeadroom() {
    final FSQueue queue = (FSQueue) this.queue;
    Resource clusterResource = this.scheduler.getClusterResource();
    Resource clusterUsage = this.scheduler.getRootQueueMetrics()
        .getAllocatedResources();

    Resource clusterAvailableResources =
        Resources.subtract(clusterResource, clusterUsage);
    Resource queueMaxAvailableResources =
        Resources.subtract(queue.getMaxShare(), queueUsage);
    Resource maxAvailableResource = Resources.componentwiseMin(
        clusterAvailableResources, queueMaxAvailableResources);

    Resource headroom = policy.getHeadroom(queueFairShare,
        queueUsage, maxAvailableResource);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Headroom calculation for " + this.getName() + ":" +
          "Min(" +
          "(queueFairShare=" + queueFairShare +
          " - queueUsage=" + queueUsage + ")," +
          " maxAvailableResource=" + maxAvailableResource +
          "Headroom=" + headroom);
    }
    return headroom;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/SchedulingPolicy.java
  public abstract Resource getHeadroom(Resource queueFairShare,
      Resource queueUsage, Resource maxAvailable);

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/policies/DominantResourceFairnessPolicy.java

  @Override
  public Resource getHeadroom(Resource queueFairShare, Resource queueUsage,
                              Resource maxAvailable) {
    int queueAvailableMemory =
        Math.max(queueFairShare.getMemory() - queueUsage.getMemory(), 0);
    int queueAvailableCPU =
        Math.max(queueFairShare.getVirtualCores() - queueUsage
            .getVirtualCores(), 0);
    Resource headroom = Resources.createResource(
        Math.min(maxAvailable.getMemory(), queueAvailableMemory),
        Math.min(maxAvailable.getVirtualCores(),
            queueAvailableCPU));
    return headroom;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/policies/FairSharePolicy.java

  @Override
  public Resource getHeadroom(Resource queueFairShare,
                              Resource queueUsage, Resource maxAvailable) {
    int queueAvailableMemory = Math.max(
        queueFairShare.getMemory() - queueUsage.getMemory(), 0);
    Resource headroom = Resources.createResource(
        Math.min(maxAvailable.getMemory(), queueAvailableMemory),
        maxAvailable.getVirtualCores());
    return headroom;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/policies/FifoPolicy.java

  @Override
  public Resource getHeadroom(Resource queueFairShare,
                              Resource queueUsage, Resource maxAvailable) {
    int queueAvailableMemory = Math.max(
        queueFairShare.getMemory() - queueUsage.getMemory(), 0);
    Resource headroom = Resources.createResource(
        Math.min(maxAvailable.getMemory(), queueAvailableMemory),
        maxAvailable.getVirtualCores());
    return headroom;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/TestFSAppAttempt.java
    Mockito.when(mockScheduler.getClock()).thenReturn(scheduler.getClock());

    final FSLeafQueue mockQueue = Mockito.mock(FSLeafQueue.class);

    final Resource queueMaxResources = Resource.newInstance(5 * 1024, 3);
    final Resource queueFairShare = Resources.createResource(4096, 2);
    final Resource queueUsage = Resource.newInstance(2048, 2);

    final Resource queueStarvation =
        Resources.subtract(queueFairShare, queueUsage);
    final Resource queueMaxResourcesAvailable =
        Resources.subtract(queueMaxResources, queueUsage);

    final Resource clusterResource = Resources.createResource(8192, 8);
    final Resource clusterUsage = Resources.createResource(2048, 2);
    final Resource clusterAvailable =
        Resources.subtract(clusterResource, clusterUsage);

    final QueueMetrics fakeRootQueueMetrics = Mockito.mock(QueueMetrics.class);

    Mockito.when(mockQueue.getMaxShare()).thenReturn(queueMaxResources);
    Mockito.when(mockQueue.getFairShare()).thenReturn(queueFairShare);
    Mockito.when(mockQueue.getResourceUsage()).thenReturn(queueUsage);
    Mockito.when(mockScheduler.getClusterResource()).thenReturn
    Mockito.when(mockScheduler.getRootQueueMetrics()).thenReturn
        (fakeRootQueueMetrics);

    ApplicationAttemptId applicationAttemptId = createAppAttemptId(1, 1);
    RMContext rmContext = resourceManager.getRMContext();
    FSAppAttempt schedulerApp =
        new FSAppAttempt(mockScheduler, applicationAttemptId, "user1", mockQueue ,
            null, rmContext);

    Mockito.when(mockQueue.getPolicy()).thenReturn(SchedulingPolicy
        .getInstance(DominantResourceFairnessPolicy.class));
    verifyHeadroom(schedulerApp,
        min(queueStarvation.getMemory(),
            clusterAvailable.getMemory(),
            queueMaxResourcesAvailable.getMemory()),
        min(queueStarvation.getVirtualCores(),
            clusterAvailable.getVirtualCores(),
            queueMaxResourcesAvailable.getVirtualCores())
    );

    Mockito.when(mockQueue.getPolicy()).thenReturn(SchedulingPolicy
        .getInstance(FairSharePolicy.class));
    verifyHeadroom(schedulerApp,
        min(queueStarvation.getMemory(),
            clusterAvailable.getMemory(),
            queueMaxResourcesAvailable.getMemory()),
        Math.min(
            clusterAvailable.getVirtualCores(),
            queueMaxResourcesAvailable.getVirtualCores())
    );

    Mockito.when(mockQueue.getPolicy()).thenReturn(SchedulingPolicy
        .getInstance(FifoPolicy.class));
    verifyHeadroom(schedulerApp,
        min(queueStarvation.getMemory(),
            clusterAvailable.getMemory(),
            queueMaxResourcesAvailable.getMemory()),
        Math.min(
            clusterAvailable.getVirtualCores(),
            queueMaxResourcesAvailable.getVirtualCores())
    );
  }

  private static int min(int value1, int value2, int value3) {
    return Math.min(Math.min(value1, value2), value3);
  }

  protected void verifyHeadroom(FSAppAttempt schedulerApp,

