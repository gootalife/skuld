hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/logaggregation/AppLogAggregatorImpl.java
            aggregator.doContainerLogAggregation(writer, appFinished);
        if (uploadedFilePathsInThisCycle.size() > 0) {
          uploadedLogsInThisCycle = true;
          this.delService.delete(this.userUgi.getShortUserName(), null,
              uploadedFilePathsInThisCycle
                  .toArray(new Path[uploadedFilePathsInThisCycle.size()]));
        }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/logaggregation/TestLogAggregationService.java
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

    verifyLocalFileDeletion(logAggregationService);
  }

  @Test
  public void testNoLogsUploadedOnAppFinish() throws Exception {
    this.delSrvc = new DeletionService(createContainerExecutor());
    delSrvc = spy(delSrvc);
    this.delSrvc.init(conf);
    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());

    LogAggregationService logAggregationService = new LogAggregationService(
        dispatcher, this.context, this.delSrvc, super.dirsHandler);
    logAggregationService.init(this.conf);
    logAggregationService.start();

    ApplicationId app = BuilderUtils.newApplicationId(1234, 1);
    File appLogDir = new File(localLogDir, ConverterUtils.toString(app));
    appLogDir.mkdir();
    LogAggregationContext context =
        LogAggregationContext.newInstance("HOST*", "sys*");
    logAggregationService.handle(new LogHandlerAppStartedEvent(app, this.user,
        null, ContainerLogsRetentionPolicy.ALL_CONTAINERS, this.acls, context));

    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(app, 1);
    ContainerId cont = BuilderUtils.newContainerId(appAttemptId, 1);
    writeContainerLogs(appLogDir, cont, new String[] { "stdout",
        "stderr", "syslog" });
    logAggregationService.handle(new LogHandlerContainerFinishedEvent(cont, 0));
    logAggregationService.handle(new LogHandlerAppFinishedEvent(app));
    logAggregationService.stop();
    delSrvc.stop();
    verify(delSrvc, times(0)).delete(user, null);
  }

  @Test
  public void testNoContainerOnNode() throws Exception {

