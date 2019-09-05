hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/WebPageUtils.java
      .append(", 'mRender': renderHadoopDate }")
      .append("\n, {'sType':'numeric', bSearchable:false, 'aTargets':");
    if (isFairSchedulerPage) {
      sb.append("[13]");
    } else if (isResourceManager) {
      sb.append("[12]");
    } else {
      sb.append("[9]");
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/dao/AppInfo.java
  protected long finishedTime;
  protected long elapsedTime;
  protected String applicationTags;
  private int allocatedCpuVcores;
  private int allocatedMemoryMB;

  public AppInfo() {
    if (app.getApplicationResourceUsageReport() != null) {
      runningContainers =
          app.getApplicationResourceUsageReport().getNumUsedContainers();
      allocatedCpuVcores = app.getApplicationResourceUsageReport()
          .getUsedResources().getVirtualCores();
      allocatedMemoryMB = app.getApplicationResourceUsageReport()
          .getUsedResources().getMemory();
    }
    progress = app.getProgress() * 100; // in percent
    if (app.getApplicationTags() != null && !app.getApplicationTags().isEmpty()) {
    return runningContainers;
  }

  public int getAllocatedCpuVcores() {
    return allocatedCpuVcores;
  }

  public int getAllocatedMemoryMB() {
    return allocatedMemoryMB;
  }

  public float getProgress() {
    return progress;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/FairSchedulerAppsBlock.java
            th(".state", "State").
            th(".finalstatus", "FinalStatus").
            th(".runningcontainer", "Running Containers").
            th(".allocatedCpu", "Allocated CPU VCores").
            th(".allocatedMemory", "Allocated Memory MB").
            th(".progress", "Progress").
            th(".ui", "Tracking UI")._()._().
        tbody();
      .append(appInfo.getFinalStatus()).append("\",\"")
      .append(appInfo.getRunningContainers() == -1 ? "N/A" : String
         .valueOf(appInfo.getRunningContainers())).append("\",\"")
      .append(appInfo.getAllocatedVCores() == -1 ? "N/A" : String
        .valueOf(appInfo.getAllocatedVCores())).append("\",\"")
      .append(appInfo.getAllocatedMB() == -1 ? "N/A" : String
        .valueOf(appInfo.getAllocatedMB())).append("\",\"")
      .append("<br title='").append(percent)
      .append("'> <div class='").append(C_PROGRESSBAR).append("' title='")

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppsBlock.java
          .th(".finishtime", "FinishTime").th(".state", "State")
          .th(".finalstatus", "FinalStatus")
          .th(".runningcontainer", "Running Containers")
          .th(".allocatedCpu", "Allocated CPU VCores")
          .th(".allocatedMemory", "Allocated Memory MB")
          .th(".progress", "Progress")
          .th(".ui", "Tracking UI").th(".blacklisted", "Blacklisted Nodes")._()
          ._().tbody();
        .append(app.getRunningContainers() == -1 ? "N/A" : String
            .valueOf(app.getRunningContainers()))
        .append("\",\"")
        .append(app.getAllocatedCpuVcores() == -1 ? "N/A" : String
            .valueOf(app.getAllocatedCpuVcores()))
        .append("\",\"")
        .append(app.getAllocatedMemoryMB() == -1 ? "N/A" : String
            .valueOf(app.getAllocatedMemoryMB()))
        .append("\",\"")
        .append("<br title='").append(percent).append("'> <div class='")
        .append(C_PROGRESSBAR).append("' title='").append(join(percent, '%'))

