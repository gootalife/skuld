hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/QueueStatistics.java
  public abstract void setPendingVCores(long pendingVCores);

  public abstract long getPendingContainers();

  public abstract void setPendingContainers(long pendingContainers);

  public abstract long getAllocatedContainers();

  public abstract void setAllocatedContainers(long allocatedContainers);

  public abstract long getReservedContainers();

  public abstract void setReservedContainers(long reservedContainers);


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/cli/TopCLI.java
  private String SET_CURSOR_HOME = "\u001b[H";
  private String CHANGE_BACKGROUND = "\u001b[7m";
  private String RESET_BACKGROUND = "\u001b[0m";
  private String SET_CURSOR_LINE_7_COLUMN_0 = "\u001b[7;0f";

  protected Cache<GetApplicationsRequest, List<ApplicationReport>>
    long allocatedVCores;
    long pendingVCores;
    long reservedVCores;
    long allocatedContainers;
    long reservedContainers;
    long pendingContainers;
  }

  private class KeyboardMonitor extends Thread {
    String[] tput_cursor_home = { "tput", "cup", "0", "0" };
    String[] tput_clear = { "tput", "clear" };
    String[] tput_clear_line = { "tput", "el" };
    String[] tput_set_cursor_line_7_column_0 = { "tput", "cup", "6", "0" };
    String[] tput_change_background = { "tput", "smso" };
    String[] tput_reset_background = { "tput", "rmso" };
    SET_CURSOR_HOME = getCommandOutput(tput_cursor_home);
    CLEAR = getCommandOutput(tput_clear);
    CLEAR_LINE = getCommandOutput(tput_clear_line);
    SET_CURSOR_LINE_7_COLUMN_0 =
        getCommandOutput(tput_set_cursor_line_7_column_0);
    CHANGE_BACKGROUND = getCommandOutput(tput_change_background);
    RESET_BACKGROUND = getCommandOutput(tput_reset_background);
  }
        queueMetrics.allocatedVCores += stats.getAllocatedVCores();
        queueMetrics.pendingVCores += stats.getPendingVCores();
        queueMetrics.reservedVCores += stats.getReservedVCores();
        queueMetrics.allocatedContainers += stats.getAllocatedContainers();
        queueMetrics.pendingContainers += stats.getPendingContainers();
        queueMetrics.reservedContainers += stats.getReservedContainers();
      }
    }
    queueMetrics.availableMemoryGB = queueMetrics.availableMemoryGB / 1024;
      queueMetrics.availableVCores, queueMetrics.allocatedVCores,
      queueMetrics.pendingVCores, queueMetrics.reservedVCores), terminalWidth,
      true));

    ret.append(CLEAR_LINE);
    ret.append(limitLineLength(String.format(
        "Queue(s) Containers: %d allocated, %d pending, %d reserved%n",
            queueMetrics.allocatedContainers, queueMetrics.pendingContainers,
            queueMetrics.reservedContainers), terminalWidth, true));
    return ret.toString();
  }

  String getPrintableAppInformation(List<ApplicationInformation> appsInfo) {
    StringBuilder ret = new StringBuilder();
    int limit = terminalHeight - 9;
    List<String> columns = new ArrayList<>();
    for (int i = 0; i < limit; ++i) {
      ret.append(CLEAR_LINE);
    synchronized (lock) {
      printHeader(header);
      printApps(appsStr);
      System.out.print(SET_CURSOR_LINE_7_COLUMN_0);
      System.out.print(CLEAR_LINE);
    }
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/QueueStatisticsPBImpl.java
    maybeInitBuilder();
    builder.setReservedVCores(reservedVCores);
  }

  @Override
  public long getPendingContainers() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasPendingContainers()) ? p.getPendingContainers() : -1;
  }

  @Override
  public void setPendingContainers(long pendingContainers) {
    maybeInitBuilder();
    builder.setPendingContainers(pendingContainers);
  }

  @Override
  public long getAllocatedContainers() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasAllocatedContainers()) ? p.getAllocatedContainers() : -1;
  }

  @Override
  public void setAllocatedContainers(long allocatedContainers) {
    maybeInitBuilder();
    builder.setAllocatedContainers(allocatedContainers);
  }

  @Override
  public long getReservedContainers() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasReservedContainers()) ? p.getReservedContainers() : -1;
  }

  @Override
  public void setReservedContainers(long reservedContainers) {
    maybeInitBuilder();
    builder.setReservedContainers(reservedContainers);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/AbstractCSQueue.java
    stats.setAllocatedVCores(getMetrics().getAllocatedVirtualCores());
    stats.setPendingVCores(getMetrics().getPendingVirtualCores());
    stats.setReservedVCores(getMetrics().getReservedVirtualCores());
    stats.setPendingContainers(getMetrics().getPendingContainers());
    stats.setAllocatedContainers(getMetrics().getAllocatedContainers());
    stats.setReservedContainers(getMetrics().getReservedContainers());
    return stats;
  }
  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/CapacitySchedulerQueueInfo.java
  protected ResourceInfo resourcesUsed;
  private boolean hideReservationQueues = false;
  protected ArrayList<String> nodeLabels = new ArrayList<String>();
  protected long allocatedContainers;
  protected long reservedContainers;
  protected long pendingContainers;

  CapacitySchedulerQueueInfo() {
  };
    absoluteUsedCapacity =
        cap(qCapacities.getAbsoluteUsedCapacity(nodeLabel), 0f, 1f) * 100;
    numApplications = q.getNumApplications();
    allocatedContainers = q.getMetrics().getAllocatedContainers();
    pendingContainers = q.getMetrics().getPendingContainers();
    reservedContainers = q.getMetrics().getReservedContainers();
    queueName = q.getQueueName();
    state = q.getState();
    resourcesUsed = new ResourceInfo(queueResourceUsage.getUsed(nodeLabel));
    return numApplications;
  }

  public long getAllocatedContainers() {
    return allocatedContainers;
  }

  public long getReservedContainers() {
    return reservedContainers;
  }

  public long getPendingContainers() {
    return pendingContainers;
  }

  public String getQueueName() {
    return this.queueName;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/FairSchedulerQueueInfo.java
  private ResourceInfo fairResources;
  private ResourceInfo clusterResources;

  private long pendingContainers;
  private long allocatedContainers;
  private long reservedContainers;

  private String queueName;
  private String schedulingPolicy;

    
    maxApps = allocConf.getQueueMaxApps(queueName);

    pendingContainers = queue.getMetrics().getPendingContainers();
    allocatedContainers = queue.getMetrics().getAllocatedContainers();
    reservedContainers = queue.getMetrics().getReservedContainers();

    if (allocConf.isReservable(queueName) &&
        !allocConf.getShowReservationAsQueues(queueName)) {
      return;
    childQueues = getChildQueues(queue, scheduler);
  }

  public long getPendingContainers() {
    return pendingContainers;
  }

  public long getAllocatedContainers() {
    return allocatedContainers;
  }

  public long getReservedContainers() {
    return reservedContainers;
  }

  protected FairSchedulerQueueInfoList getChildQueues(FSQueue queue,
                                                      FairScheduler scheduler) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServicesCapacitySched.java
  private void verifySubQueue(JSONObject info, String q, 
      float parentAbsCapacity, float parentAbsMaxCapacity)
      throws JSONException, Exception {
    int numExpectedElements = 16;
    boolean isParentQueue = true;
    if (!info.has("queues")) {
      numExpectedElements = 28;
      isParentQueue = false;
    }
    assertEquals("incorrect number of elements", numExpectedElements, info.length());

