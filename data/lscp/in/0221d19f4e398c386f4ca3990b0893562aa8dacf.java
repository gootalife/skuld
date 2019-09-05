hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/LocalDirsHandlerService.java
    return logDirs.getFullDirs();
  }

  public List<String> getLocalDirsForRead() {
    return DirectoryCollection.concat(localDirs.getGoodDirs(),
        localDirs.getFullDirs());
  }

        localDirs.getFullDirs());
  }

  public List<String> getLogDirsForRead() {
    return DirectoryCollection.concat(logDirs.getGoodDirs(),
        logDirs.getFullDirs());
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/launcher/RecoveredContainerLaunch.java

  private File locatePidFile(String appIdStr, String containerIdStr) {
    String pidSubpath= getPidFileSubpath(appIdStr, containerIdStr);
    for (String dir : getContext().getLocalDirsHandler().
        getLocalDirsForRead()) {
      File pidFile = new File(dir, pidSubpath);
      if (pidFile.exists()) {
        return pidFile;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/logaggregation/AppLogAggregatorImpl.java
        boolean appFinished) {
      LOG.info("Uploading logs for container " + containerId
          + ". Current good log dirs are "
          + StringUtils.join(",", dirsHandler.getLogDirsForRead()));
      final LogKey logKey = new LogKey(containerId);
      final LogValue logValue =
          new LogValue(dirsHandler.getLogDirsForRead(), containerId,
            userUgi.getShortUserName(), logAggregationContext,
            this.uploadedFileMeta, appFinished);
      try {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/webapp/ContainerLogsUtils.java
  
  static List<File> getContainerLogDirs(ContainerId containerId,
      LocalDirsHandlerService dirsHandler) throws YarnException {
    List<String> logDirs = dirsHandler.getLogDirsForRead();
    List<File> containerLogDirs = new ArrayList<File>(logDirs.size());
    for (String logDir : logDirs) {
      logDir = new File(logDir).toURI().getPath();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/logaggregation/TestLogAggregationService.java
    dispatcher.close();
  }

  private void verifyLocalFileDeletion(
      LogAggregationService logAggregationService) throws Exception {
    logAggregationService.init(this.conf);
    logAggregationService.start();

            ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED)
    };

    checkEvents(appEventHandler, expectedEvents, true, "getType",
        "getApplicationID");
  }

  @Test
  public void testLocalFileDeletionAfterUpload() throws Exception {
    this.delSrvc = new DeletionService(createContainerExecutor());
    delSrvc = spy(delSrvc);
    this.delSrvc.init(conf);
    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());

    LogAggregationService logAggregationService = spy(
        new LogAggregationService(dispatcher, this.context, this.delSrvc,
                                  super.dirsHandler));
    verifyLocalFileDeletion(logAggregationService);
  }

  @Test
  public void testLocalFileDeletionOnDiskFull() throws Exception {
    this.delSrvc = new DeletionService(createContainerExecutor());
    delSrvc = spy(delSrvc);
    this.delSrvc.init(conf);
    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());
    List<String> logDirs = super.dirsHandler.getLogDirs();
    LocalDirsHandlerService dirsHandler = spy(super.dirsHandler);
    when(dirsHandler.getLogDirs()).thenReturn(new ArrayList<String>());
    when(dirsHandler.getLogDirsForRead()).thenReturn(logDirs);
    LogAggregationService logAggregationService = spy(
        new LogAggregationService(dispatcher, this.context, this.delSrvc,
            dirsHandler));
    verifyLocalFileDeletion(logAggregationService);
  }


  @Test
  public void testNoContainerOnNode() throws Exception {
    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/webapp/TestContainerLogsPage.java

import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
    Assert.assertNull(nmContext.getContainers().get(container1));
    files = ContainerLogsUtils.getContainerLogDirs(container1, user, nmContext);
    Assert.assertTrue(!(files.get(0).toString().contains("file:")));

    LocalDirsHandlerService dirsHandlerForFullDisk = spy(dirsHandler);
    when(dirsHandlerForFullDisk.getLogDirs()).
        thenReturn(new ArrayList<String>());
    when(dirsHandlerForFullDisk.getLogDirsForRead()).
        thenReturn(Arrays.asList(new String[] {absLogDir.getAbsolutePath()}));
    nmContext = new NodeManager.NMContext(null, null, dirsHandlerForFullDisk,
        new ApplicationACLsManager(conf), new NMNullStateStoreService());
    nmContext.getApplications().put(appId, app);
    container.setState(ContainerState.RUNNING);
    nmContext.getContainers().put(container1, container);
    List<File> dirs =
        ContainerLogsUtils.getContainerLogDirs(container1, user, nmContext);
    File containerLogDir = new File(absLogDir, appId + "/" + container1);
    Assert.assertTrue(dirs.contains(containerLogDir));
  }

  @Test(timeout = 10000)
    LocalDirsHandlerService localDirs = mock(LocalDirsHandlerService.class);
    List<String> logDirs = new ArrayList<String>();
    logDirs.add("F:/nmlogs");
    when(localDirs.getLogDirsForRead()).thenReturn(logDirs);
    
    ApplicationIdPBImpl appId = mock(ApplicationIdPBImpl.class);
    when(appId.toString()).thenReturn("app_id_1");

