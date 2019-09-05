hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/dao/AppInfo.java
    elapsedTime = Times.elapsed(startedTime, finishedTime);
    finalAppStatus = app.getFinalApplicationStatus();
    if (app.getApplicationResourceUsageReport() != null) {
      runningContainers = app.getApplicationResourceUsageReport()
          .getNumUsedContainers();
      if (app.getApplicationResourceUsageReport().getUsedResources() != null) {
        allocatedCpuVcores = app.getApplicationResourceUsageReport()
            .getUsedResources().getVirtualCores();
        allocatedMemoryMB = app.getApplicationResourceUsageReport()
            .getUsedResources().getMemory();
      }
    }
    progress = app.getProgress() * 100; // in percent
    if (app.getApplicationTags() != null && !app.getApplicationTags().isEmpty()) {
      this.applicationTags = CSV_JOINER.join(app.getApplicationTags());

