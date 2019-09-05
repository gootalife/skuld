hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/logaggregation/AppLogAggregatorImpl.java
  public void run() {
    try {
      doAppLogAggregation();
    } catch (Exception e) {
      LOG.error("Error occured while aggregating the log for the application "
          + appId, e);
      doAppLogAggregationPostCleanUp();
    } finally {
      if (!this.appAggregationFinished.get()) {
        LOG.warn("Aggregation did not complete for application " + appId);
    uploadLogsForContainers(true);

    doAppLogAggregationPostCleanUp();

    this.dispatcher.getEventHandler().handle(
        new ApplicationEvent(this.appId,
            ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED));
    this.appAggregationFinished.set(true);
  }

  private void doAppLogAggregationPostCleanUp() {
    List<Path> localAppLogDirs = new ArrayList<Path>();
    for (String rootLogDir : dirsHandler.getLogDirsForCleanup()) {
      this.delService.delete(this.userUgi.getShortUserName(), null,
        localAppLogDirs.toArray(new Path[localAppLogDirs.size()]));
    }
  }

  private Path getRemoteNodeTmpLogFileForApp() {

