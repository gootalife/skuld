hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/LocalDirsHandlerService.java

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
    return disksTurnedGood;
  }

  private Path getPathToRead(String pathStr, List<String> dirs)
      throws IOException {
    if (pathStr.startsWith("/")) {
      pathStr = pathStr.substring(1);
    }

    FileSystem localFS = FileSystem.getLocal(getConfig());
    for (String dir : dirs) {
      try {
        Path tmpDir = new Path(dir);
        File tmpFile = tmpDir.isAbsolute()
            ? new File(localFS.makeQualified(tmpDir).toUri())
            : new File(dir);
        Path file = new Path(tmpFile.getPath(), pathStr);
        if (localFS.exists(file)) {
          return file;
        }
      } catch (IOException ie) {
        LOG.warn("Failed to find " + pathStr + " at " + dir, ie);
      }
    }

    throw new IOException("Could not find " + pathStr + " in any of" +
        " the directories");
  }

  public Path getLocalPathForWrite(String pathStr) throws IOException {
    return localDirsAllocator.getLocalPathForWrite(pathStr, getConfig());
  }
  }

  public Path getLogPathToRead(String pathStr) throws IOException {
    return getPathToRead(pathStr, getLogDirsForRead());
  }

  public static String[] validatePaths(String[] paths) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/webapp/TestContainerLogsPage.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.webapp.ContainerLogsPage.ContainersLogsBlock;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
    Assert.assertTrue(dirs.contains(containerLogDir));
  }

  @Test(timeout=30000)
  public void testContainerLogFile() throws IOException, YarnException {
    File absLogDir = new File("target",
        TestNMWebServer.class.getSimpleName() + "LogDir").getAbsoluteFile();
    String logdirwithFile = absLogDir.toURI().toString();
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_LOG_DIRS, logdirwithFile);
    conf.setFloat(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE,
        0.0f);
    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);
    NMContext nmContext = new NodeManager.NMContext(null, null, dirsHandler,
        new ApplicationACLsManager(conf), new NMNullStateStoreService());
    String user = "nobody";
    long clusterTimeStamp = 1234;
    ApplicationId appId = BuilderUtils.newApplicationId(
        clusterTimeStamp, 1);
    Application app = mock(Application.class);
    when(app.getUser()).thenReturn(user);
    when(app.getAppId()).thenReturn(appId);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);
    ContainerId containerId = BuilderUtils.newContainerId(
        appAttemptId, 1);
    nmContext.getApplications().put(appId, app);

    MockContainer container =
        new MockContainer(appAttemptId, new AsyncDispatcher(), conf, user,
            appId, 1);
    container.setState(ContainerState.RUNNING);
    nmContext.getContainers().put(containerId, container);
    File containerLogDir = new File(absLogDir,
        ContainerLaunch.getRelativeContainerLogDir(appId.toString(),
            containerId.toString()));
    containerLogDir.mkdirs();
    String fileName = "fileName";
    File containerLogFile = new File(containerLogDir, fileName);
    containerLogFile.createNewFile();
    File file = ContainerLogsUtils.getContainerLogFile(containerId,
        fileName, user, nmContext);
    Assert.assertEquals(containerLogFile.toURI().toString(),
        file.toURI().toString());
    FileUtil.fullyDelete(absLogDir);
  }

  @Test(timeout = 10000)
  public void testContainerLogPageAccess() throws IOException {

