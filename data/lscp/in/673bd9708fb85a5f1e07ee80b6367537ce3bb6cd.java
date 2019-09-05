hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/ContainerExecutor.java
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReacquisitionContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.util.ProcessIdFileReader;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
  public abstract void startLocalizer(LocalizerStartContext ctx)
    throws IOException, InterruptedException;


  public abstract int launchContainer(ContainerStartContext ctx) throws
      IOException;

  public abstract boolean signalContainer(ContainerSignalContext ctx)
      throws IOException;

  public abstract void deleteAsUser(DeletionAsUserContext ctx)
      throws IOException, InterruptedException;

  public abstract boolean isContainerProcessAlive(ContainerLivenessContext ctx)
      throws IOException;

  public int reacquireContainer(ContainerReacquisitionContext ctx)
      throws IOException, InterruptedException {
    String user = ctx.getUser();
    ContainerId containerId = ctx.getContainerId();


    Path pidPath = getPidFilePath(containerId);
    if (pidPath == null) {
      LOG.warn(containerId + " is not active, returning terminated error");
    }

    LOG.info("Reacquiring " + containerId + " with pid " + pid);
    ContainerLivenessContext livenessContext = new ContainerLivenessContext
        .Builder()
        .setUser(user)
        .setPid(pid)
        .build();
    while(isContainerProcessAlive(livenessContext)) {
      Thread.sleep(1000);
    }

    public void run() {
      try {
        Thread.sleep(delay);
        containerExecutor.signalContainer(new ContainerSignalContext.Builder()
            .setUser(user)
            .setPid(pid)
            .setSignal(signal)
            .build());
      } catch (InterruptedException e) {
        return;
      } catch (IOException e) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/DefaultContainerExecutor.java
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;
  }

  @Override
  public void startLocalizer(LocalizerStartContext ctx)
      throws IOException, InterruptedException {
    Path nmPrivateContainerTokensPath = ctx.getNmPrivateContainerTokens();
    InetSocketAddress nmAddr = ctx.getNmAddr();
    String user = ctx.getUser();
    String appId = ctx.getAppId();
    String locId = ctx.getLocId();
    LocalDirsHandlerService dirsHandler = ctx.getDirsHandler();

    List<String> localDirs = dirsHandler.getLocalDirs();
    List<String> logDirs = dirsHandler.getLogDirs();
  }

  @Override
  public int launchContainer(ContainerStartContext ctx) throws IOException {
    Container container = ctx.getContainer();
    Path nmPrivateContainerScriptPath = ctx.getNmPrivateContainerScriptPath();
    Path nmPrivateTokensPath = ctx.getNmPrivateTokensPath();
    String user = ctx.getUser();
    Path containerWorkDir = ctx.getContainerWorkDir();
    List<String> localDirs = ctx.getLocalDirs();
    List<String> logDirs = ctx.getLogDirs();

    FsPermission dirPerm = new FsPermission(APPDIR_PERM);
    ContainerId containerId = container.getContainerId();
  }

  @Override
  public boolean signalContainer(ContainerSignalContext ctx)
      throws IOException {
    String user = ctx.getUser();
    String pid = ctx.getPid();
    Signal signal = ctx.getSignal();

    LOG.debug("Sending signal " + signal.getValue() + " to pid " + pid
        + " as user " + user);
    if (!containerIsAlive(pid)) {
  }

  @Override
  public boolean isContainerProcessAlive(ContainerLivenessContext ctx)
      throws IOException {
    String pid = ctx.getPid();

    return containerIsAlive(pid);
  }

  }

  @Override
  public void deleteAsUser(DeletionAsUserContext ctx)
      throws IOException, InterruptedException {
    Path subDir = ctx.getSubDir();
    List<Path> baseDirs = ctx.getBasedirs();

    if (baseDirs == null || baseDirs.size() == 0) {
      LOG.info("Deleting absolute path : " + subDir);
      if (!lfs.delete(subDir, true)) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/DeletionService.java
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredDeletionServiceState;
        try {
          LOG.debug("Deleting path: [" + subDir + "] as user: [" + user + "]");
          if (baseDirs == null || baseDirs.size() == 0) {
            delService.exec.deleteAsUser(new DeletionAsUserContext.Builder()
                .setUser(user)
                .setSubDir(subDir)
                .build());
          } else {
            delService.exec.deleteAsUser(new DeletionAsUserContext.Builder()
                .setUser(user)
                .setSubDir(subDir)
                .setBasedirs(baseDirs.toArray(new Path[0]))
                .build());
          }
        } catch (IOException e) {
          error = true;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/DockerContainerExecutor.java
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;
  }

  @Override
  public synchronized void startLocalizer(LocalizerStartContext ctx)
    throws IOException, InterruptedException {
    Path nmPrivateContainerTokensPath = ctx.getNmPrivateContainerTokens();
    InetSocketAddress nmAddr = ctx.getNmAddr();
    String user = ctx.getUser();
    String appId = ctx.getAppId();
    String locId = ctx.getLocId();
    LocalDirsHandlerService dirsHandler = ctx.getDirsHandler();
    List<String> localDirs = dirsHandler.getLocalDirs();
    List<String> logDirs = dirsHandler.getLogDirs();



  @Override
  public int launchContainer(ContainerStartContext ctx) throws IOException {
    Container container = ctx.getContainer();
    Path nmPrivateContainerScriptPath = ctx.getNmPrivateContainerScriptPath();
    Path nmPrivateTokensPath = ctx.getNmPrivateTokensPath();
    String userName = ctx.getUser();
    Path containerWorkDir = ctx.getContainerWorkDir();
    List<String> localDirs = ctx.getLocalDirs();
    List<String> logDirs = ctx.getLogDirs();

    String containerImageName = container.getLaunchContext().getEnvironment()
  }

  @Override
  public boolean signalContainer(ContainerSignalContext ctx)
    throws IOException {
    String user = ctx.getUser();
    String pid = ctx.getPid();
    Signal signal = ctx.getSignal();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending signal " + signal.getValue() + " to pid " + pid
        + " as user " + user);
  }

  @Override
  public boolean isContainerProcessAlive(ContainerLivenessContext ctx)
    throws IOException {
    String pid = ctx.getPid();

    return containerIsAlive(pid);
  }

  }

  @Override
  public void deleteAsUser(DeletionAsUserContext ctx)
    throws IOException, InterruptedException {
    Path subDir = ctx.getSubDir();
    List<Path> baseDirs = ctx.getBasedirs();

    if (baseDirs == null || baseDirs.size() == 0) {
      LOG.info("Deleting absolute path : " + subDir);
      if (!lfs.delete(subDir, true)) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/LinuxContainerExecutor.java
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReacquisitionContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler;
import org.apache.hadoop.yarn.server.nodemanager.util.LCEResourcesHandler;
import org.apache.hadoop.yarn.util.ConverterUtils;
  }
  
  @Override
  public void startLocalizer(LocalizerStartContext ctx)
      throws IOException, InterruptedException {
    Path nmPrivateContainerTokensPath = ctx.getNmPrivateContainerTokens();
    InetSocketAddress nmAddr = ctx.getNmAddr();
    String user = ctx.getUser();
    String appId = ctx.getAppId();
    String locId = ctx.getLocId();
    LocalDirsHandlerService dirsHandler = ctx.getDirsHandler();
    List<String> localDirs = dirsHandler.getLocalDirs();
    List<String> logDirs = dirsHandler.getLogDirs();
    
  }

  @Override
  public int launchContainer(ContainerStartContext ctx) throws IOException {
    Container container = ctx.getContainer();
    Path nmPrivateContainerScriptPath = ctx.getNmPrivateContainerScriptPath();
    Path nmPrivateTokensPath = ctx.getNmPrivateTokensPath();
    String user = ctx.getUser();
    String appId = ctx.getAppId();
    Path containerWorkDir = ctx.getContainerWorkDir();
    List<String> localDirs = ctx.getLocalDirs();
    List<String> logDirs = ctx.getLogDirs();

    verifyUsernamePattern(user);
    String runAsUser = getRunAsUser(user);
            containerExecutorExe, runAsUser, user, Integer
                .toString(Commands.LAUNCH_CONTAINER.getValue()), appId,
            containerIdStr, containerWorkDir.toString(),
            nmPrivateContainerScriptPath.toUri().getPath().toString(),
            nmPrivateTokensPath.toUri().getPath().toString(),
            pidFilePath.toString(),
            StringUtils.join(",", localDirs),
  }

  @Override
  public int reacquireContainer(ContainerReacquisitionContext ctx)
      throws IOException, InterruptedException {
    ContainerId containerId = ctx.getContainerId();

    try {
        }
      }

      return super.reacquireContainer(ctx);
    } finally {
      resourcesHandler.postExecute(containerId);
      if (resourceHandlerChain != null) {
  }

  @Override
  public boolean signalContainer(ContainerSignalContext ctx)
      throws IOException {
    String user = ctx.getUser();
    String pid = ctx.getPid();
    Signal signal = ctx.getSignal();

    verifyUsernamePattern(user);
    String runAsUser = getRunAsUser(user);
  }

  @Override
  public void deleteAsUser(DeletionAsUserContext ctx) {
    String user = ctx.getUser();
    Path dir = ctx.getSubDir();
    List<Path> baseDirs = ctx.getBasedirs();

    verifyUsernamePattern(user);
    String runAsUser = getRunAsUser(user);

                    Integer.toString(Commands.DELETE_AS_USER.getValue()),
                    dirString));
    List<String> pathsToDelete = new ArrayList<String>();
    if (baseDirs == null || baseDirs.size() == 0) {
      LOG.info("Deleting absolute path : " + dir);
      pathsToDelete.add(dirString);
    } else {
  }
  
  @Override
  public boolean isContainerProcessAlive(ContainerLivenessContext ctx)
      throws IOException {
    String user = ctx.getUser();
    String pid = ctx.getPid();

    return signalContainer(new ContainerSignalContext.Builder()
        .setUser(user)
        .setPid(pid)
        .setSignal(Signal.NULL)
        .build());
  }

  public void mountCgroups(List<String> cgroupKVs, String hierarchy)

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/WindowsSecureContainerExecutor.java
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;

  }

  @Override
  public void startLocalizer(LocalizerStartContext ctx) throws IOException,
      InterruptedException {
    Path nmPrivateContainerTokensPath = ctx.getNmPrivateContainerTokens();
    InetSocketAddress nmAddr = ctx.getNmAddr();
    String user = ctx.getUser();
    String appId = ctx.getAppId();
    String locId = ctx.getLocId();
    LocalDirsHandlerService dirsHandler = ctx.getDirsHandler();
    List<String> localDirs = dirsHandler.getLocalDirs();
    List<String> logDirs = dirsHandler.getLogDirs();

    String tokenFn = String.format(
        ContainerLocalizer.TOKEN_FILE_NAME_FMT, locId);
    Path tokenDst = new Path(appStorageDir, tokenFn);
    copyFile(nmPrivateContainerTokensPath, tokenDst, user);

    File cwdApp = new File(appStorageDir.toString());
    if (LOG.isDebugEnabled()) {
    try {
      stubExecutor.execute();
      stubExecutor.validateResult();
    } finally {
      stubExecutor.close();
      try
      {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/launcher/ContainerLaunch.java
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.WindowsSecureContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.util.ProcessIdFileReader;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
      }
      else {
        exec.activateContainer(containerID, pidFilePath);
        ret = exec.launchContainer(new ContainerStartContext.Builder()
            .setContainer(container)
            .setNmPrivateContainerScriptPath(nmPrivateContainerScriptPath)
            .setNmPrivateTokensPath(nmPrivateTokensPath)
            .setUser(user)
            .setAppId(appIdStr)
            .setContainerWorkDir(containerWorkDir)
            .setLocalDirs(localDirs)
            .setLogDirs(logDirs)
            .build());
      }
    } catch (Throwable e) {
      LOG.warn("Failed to launch container.", e);
          ? Signal.TERM
          : Signal.KILL;

        boolean result = exec.signalContainer(
            new ContainerSignalContext.Builder()
                .setUser(user)
                .setPid(processId)
                .setSignal(signal)
                .build());

        LOG.debug("Sent signal " + signal + " to pid " + processId
          + " as user " + user

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/launcher/RecoveredContainerLaunch.java
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReacquisitionContext;
import org.apache.hadoop.yarn.util.ConverterUtils;

        String pidPathStr = pidFile.getPath();
        pidFilePath = new Path(pidPathStr);
        exec.activateContainer(containerId, pidFilePath);
        retCode = exec.reacquireContainer(
            new ContainerReacquisitionContext.Builder()
            .setUser(container.getUser())
            .setContainerId(containerId)
            .build());
      } else {
        LOG.warn("Unable to locate pid file for container " + containerIdStr);
      }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/ResourceLocalizationService.java
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.LocalResourceTrackerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredLocalizationState;
        writeCredentials(nmPrivateCTokensPath);
        if (dirsHandler.areDisksHealthy()) {
          exec.startLocalizer(new LocalizerStartContext.Builder()
              .setNmPrivateContainerTokens(nmPrivateCTokensPath)
              .setNmAddr(localizationServerAddress)
              .setUser(context.getUser())
              .setAppId(ConverterUtils.toString(context.getContainerId()
                  .getApplicationAttemptId().getApplicationId()))
              .setLocId(localizerId)
              .setDirsHandler(dirsHandler)
              .build());
        } else {
          throw new IOException("All disks failed. "
              + dirsHandler.getDisksHealthReport(false));

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/executor/ContainerLivenessContext.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/executor/ContainerLivenessContext.java

package org.apache.hadoop.yarn.server.nodemanager.executor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerLivenessContext {
  private final String user;
  private final String pid;

  public static final class Builder {
    private String user;
    private String pid;

    public Builder() {
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setPid(String pid) {
      this.pid = pid;
      return this;
    }

    public ContainerLivenessContext build() {
      return new ContainerLivenessContext(this);
    }
  }

  private ContainerLivenessContext(Builder builder) {
    this.user = builder.user;
    this.pid = builder.pid;
  }

  public String getUser() {
    return this.user;
  }

  public String getPid() {
    return this.pid;
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/executor/ContainerReacquisitionContext.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/executor/ContainerReacquisitionContext.java

package org.apache.hadoop.yarn.server.nodemanager.executor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ContainerId;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerReacquisitionContext {
  private final String user;
  private final ContainerId containerId;

  public static final class Builder {
    private String user;
    private ContainerId containerId;

    public Builder() {
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setContainerId(ContainerId containerId) {
      this.containerId = containerId;
      return this;
    }

    public ContainerReacquisitionContext build() {
      return new ContainerReacquisitionContext(this);
    }
  }

  private ContainerReacquisitionContext(Builder builder) {
    this.user = builder.user;
    this.containerId = builder.containerId;
  }

  public String getUser() {
    return this.user;
  }

  public ContainerId getContainerId() {
    return this.containerId;
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/executor/ContainerSignalContext.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/executor/ContainerSignalContext.java

package org.apache.hadoop.yarn.server.nodemanager.executor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerSignalContext {
  private final String user;
  private final String pid;
  private final Signal signal;

  public static final class Builder {
    private String user;
    private String pid;
    private Signal signal;

    public Builder() {
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setPid(String pid) {
      this.pid = pid;
      return this;
    }

    public Builder setSignal(Signal signal) {
      this.signal = signal;
      return this;
    }

    public ContainerSignalContext build() {
      return new ContainerSignalContext(this);
    }
  }

  private ContainerSignalContext(Builder builder) {
    this.user = builder.user;
    this.pid = builder.pid;
    this.signal = builder.signal;
  }

  public String getUser() {
    return this.user;
  }

  public String getPid() {
    return this.pid;
  }

  public Signal getSignal() {
    return this.signal;
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/executor/ContainerStartContext.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/executor/ContainerStartContext.java

package org.apache.hadoop.yarn.server.nodemanager.executor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

import java.util.List;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerStartContext {
  private final Container container;
  private final Path nmPrivateContainerScriptPath;
  private final Path nmPrivateTokensPath;
  private final String user;
  private final String appId;
  private final Path containerWorkDir;
  private final List<String> localDirs;
  private final List<String> logDirs;

  public static final class Builder {
    private Container container;
    private Path nmPrivateContainerScriptPath;
    private Path nmPrivateTokensPath;
    private String user;
    private String appId;
    private Path containerWorkDir;
    private List<String> localDirs;
    private List<String> logDirs;

    public Builder() {
    }

    public Builder setContainer(Container container) {
      this.container = container;
      return this;
    }

    public Builder setNmPrivateContainerScriptPath(
        Path nmPrivateContainerScriptPath) {
      this.nmPrivateContainerScriptPath = nmPrivateContainerScriptPath;
      return this;
    }

    public Builder setNmPrivateTokensPath(Path nmPrivateTokensPath) {
      this.nmPrivateTokensPath = nmPrivateTokensPath;
      return this;
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setAppId(String appId) {
      this.appId = appId;
      return this;
    }

    public Builder setContainerWorkDir(Path containerWorkDir) {
      this.containerWorkDir = containerWorkDir;
      return this;
    }

    public Builder setLocalDirs(List<String> localDirs) {
      this.localDirs = localDirs;
      return this;
    }

    public Builder setLogDirs(List<String> logDirs) {
      this.logDirs = logDirs;
      return this;
    }

    public ContainerStartContext build() {
      return new ContainerStartContext(this);
    }
  }

  private ContainerStartContext(Builder builder) {
    this.container = builder.container;
    this.nmPrivateContainerScriptPath = builder.nmPrivateContainerScriptPath;
    this.nmPrivateTokensPath = builder.nmPrivateTokensPath;
    this.user = builder.user;
    this.appId = builder.appId;
    this.containerWorkDir = builder.containerWorkDir;
    this.localDirs = builder.localDirs;
    this.logDirs = builder.logDirs;
  }

  public Container getContainer() {
    return this.container;
  }

  public Path getNmPrivateContainerScriptPath() {
    return this.nmPrivateContainerScriptPath;
  }

  public Path getNmPrivateTokensPath() {
    return this.nmPrivateTokensPath;
  }

  public String getUser() {
    return this.user;
  }

  public String getAppId() {
    return this.appId;
  }

  public Path getContainerWorkDir() {
    return this.containerWorkDir;
  }

  public List<String> getLocalDirs() {
    return this.localDirs;
  }

  public List<String> getLogDirs() {
    return this.logDirs;
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/executor/DeletionAsUserContext.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/executor/DeletionAsUserContext.java

package org.apache.hadoop.yarn.server.nodemanager.executor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class DeletionAsUserContext {
  private final String user;
  private final Path subDir;
  private final List<Path> basedirs;

  public static final class Builder {
    private String user;
    private Path subDir;
    private List<Path> basedirs;

    public Builder() {
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setSubDir(Path subDir) {
      this.subDir = subDir;
      return this;
    }

    public Builder setBasedirs(Path... basedirs) {
      this.basedirs = Arrays.asList(basedirs);
      return this;
    }

    public DeletionAsUserContext build() {
      return new DeletionAsUserContext(this);
    }
  }

  private DeletionAsUserContext(Builder builder) {
    this.user = builder.user;
    this.subDir = builder.subDir;
    this.basedirs = builder.basedirs;
  }

  public String getUser() {
    return this.user;
  }

  public Path getSubDir() {
    return this.subDir;
  }

  public List<Path> getBasedirs() {
    if (this.basedirs != null) {
      return Collections.unmodifiableList(this.basedirs);
    } else {
      return null;
    }
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/executor/LocalizerStartContext.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/executor/LocalizerStartContext.java

package org.apache.hadoop.yarn.server.nodemanager.executor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;

import java.net.InetSocketAddress;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class LocalizerStartContext {
  private final Path nmPrivateContainerTokens;
  private final InetSocketAddress nmAddr;
  private final String user;
  private final String appId;
  private final String locId;
  private final LocalDirsHandlerService dirsHandler;

  public static final class Builder {
    private Path nmPrivateContainerTokens;
    private InetSocketAddress nmAddr;
    private String user;
    private String appId;
    private String locId;
    private LocalDirsHandlerService dirsHandler;

    public Builder() {
    }

    public Builder setNmPrivateContainerTokens(Path nmPrivateContainerTokens) {
      this.nmPrivateContainerTokens = nmPrivateContainerTokens;
      return this;
    }

    public Builder setNmAddr(InetSocketAddress nmAddr) {
      this.nmAddr = nmAddr;
      return this;
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setAppId(String appId) {
      this.appId = appId;
      return this;
    }

    public Builder setLocId(String locId) {
      this.locId = locId;
      return this;
    }

    public Builder setDirsHandler(LocalDirsHandlerService dirsHandler) {
      this.dirsHandler = dirsHandler;
      return this;
    }

    public LocalizerStartContext build() {
      return new LocalizerStartContext(this);
    }
  }

  private LocalizerStartContext(Builder builder) {
    this.nmPrivateContainerTokens = builder.nmPrivateContainerTokens;
    this.nmAddr = builder.nmAddr;
    this.user = builder.user;
    this.appId = builder.appId;
    this.locId = builder.locId;
    this.dirsHandler = builder.dirsHandler;
  }

  public Path getNmPrivateContainerTokens() {
    return this.nmPrivateContainerTokens;
  }

  public InetSocketAddress getNmAddr() {
    return this.nmAddr;
  }

  public String getUser() {
    return this.user;
  }

  public String getAppId() {
    return this.appId;
  }

  public String getLocId() {
    return this.locId;
  }

  public LocalDirsHandlerService getDirsHandler() {
    return this.dirsHandler;
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestDefaultContainerExecutor.java
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.FakeFSDataInputStream;

import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;

      mockExec.init();
      mockExec.activateContainer(cId, pidFile);
      int ret = mockExec.launchContainer(new ContainerStartContext.Builder()
          .setContainer(container)
          .setNmPrivateContainerScriptPath(scriptPath)
          .setNmPrivateTokensPath(tokensPath)
          .setUser(appSubmitter)
          .setAppId(appId)
          .setContainerWorkDir(workDir)
          .setLocalDirs(localDirs)
          .setLogDirs(logDirs)
          .build());
      Assert.assertNotSame(0, ret);
    } finally {
      mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
          .setUser(appSubmitter)
          .setSubDir(localDir)
          .build());
      mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
          .setUser(appSubmitter)
          .setSubDir(logDir)
          .build());
    }
  }

    when(dirsHandler.getLogDirs()).thenReturn(logDirs);
    
    try {
      mockExec.startLocalizer(new LocalizerStartContext.Builder()
          .setNmPrivateContainerTokens(nmPrivateCTokensPath)
          .setNmAddr(localizationServerAddress)
          .setUser(appSubmitter)
          .setAppId(appId)
          .setLocId(locId)
          .setDirsHandler(dirsHandler)
          .build());
    } catch (IOException e) {
      Assert.fail("StartLocalizer failed to copy token file " + e);
    } finally {
      mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
          .setUser(appSubmitter)
          .setSubDir(firstDir)
          .build());
      mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
          .setUser(appSubmitter)
          .setSubDir(secondDir)
          .build());
      mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
          .setUser(appSubmitter)
          .setSubDir(logDir)
          .build());
      deleteTmpFiles();
    }
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestDeletionService.java
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService.FileDeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMMemoryStateStoreService;
import org.junit.AfterClass;
import org.junit.Test;

  static class FakeDefaultContainerExecutor extends DefaultContainerExecutor {
    @Override
    public void deleteAsUser(DeletionAsUserContext ctx)
        throws IOException, InterruptedException {
      String user = ctx.getUser();
      Path subDir = ctx.getSubDir();
      List<Path> basedirs = ctx.getBasedirs();

      if ((Long.parseLong(subDir.getName()) % 2) == 0) {
        assertNull(user);
      } else {
        assertEquals("dingo", user);
      }

      DeletionAsUserContext.Builder builder = new DeletionAsUserContext
          .Builder()
          .setUser(user)
          .setSubDir(subDir);

      if (basedirs != null) {
        builder.setBasedirs(basedirs.toArray(new Path[basedirs.size()]));
      }

      super.deleteAsUser(builder.build());
      assertFalse(lfs.util().exists(subDir));
    }
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestDockerContainerExecutor.java
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
    Path pidFile = new Path(workDir, "pid.txt");

    exec.activateContainer(cId, pidFile);
    return exec.launchContainer(new ContainerStartContext.Builder()
        .setContainer(container)
        .setNmPrivateContainerScriptPath(scriptPath)
        .setNmPrivateTokensPath(tokensPath)
        .setUser(appSubmitter)
        .setAppId(appId)
        .setContainerWorkDir(workDir)
        .setLocalDirs(dirsHandler.getLocalDirs())
        .setLogDirs(dirsHandler.getLogDirs())
        .build());
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestDockerContainerExecutorWithMocks.java
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
    Path pidFile = new Path(workDir, "pid.txt");

    dockerContainerExecutor.activateContainer(cId, pidFile);
    dockerContainerExecutor.launchContainer(new ContainerStartContext.Builder()
        .setContainer(container)
        .setNmPrivateContainerScriptPath(scriptPath)
        .setNmPrivateTokensPath(tokensPath)
        .setUser(appSubmitter)
        .setAppId(appId)
        .setContainerWorkDir(workDir)
        .setLocalDirs(dirsHandler.getLocalDirs())
        .setLogDirs(dirsHandler.getLogDirs())
        .build());
  }

  @Test(expected = IllegalArgumentException.class)
    Path pidFile = new Path(workDir, "pid.txt");

    dockerContainerExecutor.activateContainer(cId, pidFile);
    dockerContainerExecutor.launchContainer(
        new ContainerStartContext.Builder()
            .setContainer(container)
            .setNmPrivateContainerScriptPath(scriptPath)
            .setNmPrivateTokensPath(tokensPath)
            .setUser(appSubmitter)
            .setAppId(appId)
            .setContainerWorkDir(workDir)
            .setLocalDirs(dirsHandler.getLocalDirs())
            .setLogDirs(dirsHandler.getLogDirs())
            .build());
  }

  @Test
    Path pidFile = new Path(workDir, "pid");

    dockerContainerExecutor.activateContainer(cId, pidFile);
    int ret = dockerContainerExecutor.launchContainer(
        new ContainerStartContext.Builder()
            .setContainer(container)
            .setNmPrivateContainerScriptPath(scriptPath)
            .setNmPrivateTokensPath(tokensPath)
            .setUser(appSubmitter)
            .setAppId(appId)
            .setContainerWorkDir(workDir)
            .setLocalDirs(dirsHandler.getLocalDirs())
            .setLogDirs(dirsHandler.getLogDirs())
            .build());
    assertEquals(0, ret);
    Path sessionScriptPath = new Path(workDir,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestLinuxContainerExecutor.java
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReacquisitionContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.util.LCEResourcesHandler;
import org.junit.After;
import org.junit.Assert;
      Path usercachedir = new Path(dir, ContainerLocalizer.USERCACHE);
      Path userdir = new Path(usercachedir, user);
      Path appcachedir = new Path(userdir, ContainerLocalizer.APPCACHE);
      exec.deleteAsUser(new DeletionAsUserContext.Builder()
          .setUser(user)
          .setSubDir(appcachedir)
          .build());
      FileContext.getLocalFSFileContext().delete(usercachedir, true);
    }
  }
    for (String dir : localDirs) {
      Path filecache = new Path(dir, ContainerLocalizer.FILECACHE);
      Path filedir = new Path(filecache, user);
      exec.deleteAsUser(new DeletionAsUserContext.Builder()
          .setUser(user)
          .setSubDir(filedir)
          .build());
    }
  }

      String containerId = "CONTAINER_" + (id - 1);
      Path appdir = new Path(dir, appId);
      Path containerdir = new Path(appdir, containerId);
      exec.deleteAsUser(new DeletionAsUserContext.Builder()
          .setUser(user)
          .setSubDir(containerdir)
          .build());
    }
  }

    for (String file : files) {
      File f = new File(workSpace, file);
      if (f.exists()) {
        exec.deleteAsUser(new DeletionAsUserContext.Builder()
            .setUser(user)
            .setSubDir(new Path(file))
            .setBasedirs(ws)
            .build());
      }
    }
  }
    Path pidFile = new Path(workDir, "pid.txt");

    exec.activateContainer(cId, pidFile);
    return exec.launchContainer(new ContainerStartContext.Builder()
        .setContainer(container)
        .setNmPrivateContainerScriptPath(scriptPath)
        .setNmPrivateTokensPath(tokensPath)
        .setUser(appSubmitter)
        .setAppId(appId)
        .setContainerWorkDir(workDir)
        .setLocalDirs(dirsHandler.getLocalDirs())
        .setLogDirs(dirsHandler.getLogDirs())
        .build());
  }

  @Test
    };
    exec.setConf(conf);

    exec.startLocalizer(new LocalizerStartContext.Builder()
        .setNmPrivateContainerTokens(nmPrivateContainerTokensPath)
        .setNmAddr(nmAddr)
        .setUser(appSubmitter)
        .setAppId(appId)
        .setLocId(locId)
        .setDirsHandler(dirsHandler)
        .build());

    String locId2 = "container_01_02";
    Path nmPrivateContainerTokensPath2 =
              + Path.SEPARATOR
              + String.format(ContainerLocalizer.TOKEN_FILE_NAME_FMT, locId2));
    files.create(nmPrivateContainerTokensPath2, EnumSet.of(CREATE, OVERWRITE));
    exec.startLocalizer(new LocalizerStartContext.Builder()
            .setNmPrivateContainerTokens(nmPrivateContainerTokensPath2)
            .setNmAddr(nmAddr)
            .setUser(appSubmitter)
            .setAppId(appId)
            .setLocId(locId2)
            .setDirsHandler(dirsHandler)
            .build());


    cleanupUserAppCache(appSubmitter);
  }

    assertNotNull(pid);

    LOG.info("Going to killing the process.");
    exec.signalContainer(new ContainerSignalContext.Builder()
        .setUser(appSubmitter)
        .setPid(pid)
        .setSignal(Signal.TERM)
        .build());
    LOG.info("sleeping for 100ms to let the sleep be killed");
    Thread.sleep(100);

    } catch (IOException e) {
    }
    lce.reacquireContainer(new ContainerReacquisitionContext.Builder()
        .setUser("foouser")
        .setContainerId(cid)
        .build());
    assertTrue("postExec not called after reacquisition",
        TestResourceHandler.postExecContainers.contains(cid));
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestLinuxContainerExecutorWithMocks.java
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
    Path pidFile = new Path(workDir, "pid.txt");

    mockExec.activateContainer(cId, pidFile);
    int ret = mockExec.launchContainer(new ContainerStartContext.Builder()
        .setContainer(container)
        .setNmPrivateContainerScriptPath(scriptPath)
        .setNmPrivateTokensPath(tokensPath)
        .setUser(appSubmitter)
        .setAppId(appId)
        .setContainerWorkDir(workDir)
        .setLocalDirs(dirsHandler.getLocalDirs())
        .setLogDirs(dirsHandler.getLogDirs())
        .build());
    assertEquals(0, ret);
    assertEquals(Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
        appSubmitter, cmd, appId, containerId,
    Path nmPrivateCTokensPath= new Path("file:///bin/nmPrivateCTokensPath");
 
    try {
      mockExec.startLocalizer(new LocalizerStartContext.Builder()
          .setNmPrivateContainerTokens(nmPrivateCTokensPath)
          .setNmAddr(address)
          .setUser("test")
          .setAppId("application_0")
          .setLocId("12345")
          .setDirsHandler(dirsHandler)
          .build());

      List<String> result=readMockParams();
      Assert.assertEquals(result.size(), 18);
      Assert.assertEquals(result.get(0), YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER);
    Path pidFile = new Path(workDir, "pid.txt");

    mockExec.activateContainer(cId, pidFile);
    int ret = mockExec.launchContainer(new ContainerStartContext.Builder()
        .setContainer(container)
        .setNmPrivateContainerScriptPath(scriptPath)
        .setNmPrivateTokensPath(tokensPath)
        .setUser(appSubmitter)
        .setAppId(appId)
        .setContainerWorkDir(workDir)
        .setLocalDirs(dirsHandler.getLocalDirs())
        .setLogDirs(dirsHandler.getLogDirs())
        .build());

    Assert.assertNotSame(0, ret);
    assertEquals(Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
        appSubmitter, cmd, appId, containerId,
    ContainerExecutor.Signal signal = ContainerExecutor.Signal.QUIT;
    String sigVal = String.valueOf(signal.getValue());
    
    mockExec.signalContainer(new ContainerSignalContext.Builder()
        .setUser(appSubmitter)
        .setPid("1000")
        .setSignal(signal)
        .build());
    assertEquals(Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
        appSubmitter, cmd, "1000", sigVal),
        readMockParams());
    Path baseDir0 = new Path("/grid/0/BaseDir");
    Path baseDir1 = new Path("/grid/1/BaseDir");

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .setSubDir(dir)
        .build());
    assertEquals(
        Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
            appSubmitter, cmd, "/tmp/testdir"),
        readMockParams());

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .build());
    assertEquals(
        Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
            appSubmitter, cmd, ""),
        readMockParams());

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .setSubDir(testFile)
        .setBasedirs(baseDir0, baseDir1)
        .build());
    assertEquals(
        Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
            appSubmitter, cmd, testFile.toString(), baseDir0.toString(),
            baseDir1.toString()),
        readMockParams());

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .setBasedirs(baseDir0, baseDir1)
        .build());
    assertEquals(
        Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
            appSubmitter, cmd, "", baseDir0.toString(), baseDir1.toString()),
        readMockParams());

    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, executorPath);
    mockExec.setConf(conf);

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .setSubDir(dir)
        .build());
    assertEquals(
        Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
            appSubmitter, cmd, "/tmp/testdir"),
        readMockParams());

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .build());
    assertEquals(
        Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
            appSubmitter, cmd, ""),
        readMockParams());

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .setSubDir(testFile)
        .setBasedirs(baseDir0, baseDir1)
        .build());
    assertEquals(
        Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
            appSubmitter, cmd, testFile.toString(), baseDir0.toString(),
            baseDir1.toString()),
        readMockParams());

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .setBasedirs(baseDir0, baseDir1)
        .build());
    assertEquals(Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
        appSubmitter, cmd, "", baseDir0.toString(), baseDir1.toString()),
        readMockParams());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/BaseContainerManagerTest.java
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.junit.Assert;

import org.apache.commons.logging.Log;
    if (containerManager != null) {
      containerManager.stop();
    }
    createContainerExecutor().deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(user)
        .setSubDir(new Path(localDir.getAbsolutePath()))
        .setBasedirs(new Path[] {})
        .build());
  }

  public static void waitForContainerState(ContainerManagementProtocol containerManager,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/TestResourceLocalizationService.java
import java.util.concurrent.Future;

import org.apache.hadoop.fs.Options;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.junit.Assert;

import org.apache.commons.io.FileUtils;
      dispatcher.await();
      String appStr = ConverterUtils.toString(appId);
      String ctnrStr = c.getContainerId().toString();
      ArgumentCaptor<LocalizerStartContext> contextCaptor = ArgumentCaptor
          .forClass(LocalizerStartContext.class);
      verify(exec).startLocalizer(contextCaptor.capture());

      LocalizerStartContext context = contextCaptor.getValue();
      Path localizationTokenPath = context.getNmPrivateContainerTokens();

      assertEquals("user0", context.getUser());
      assertEquals(appStr, context.getAppId());
      assertEquals(ctnrStr, context.getLocId());

      LocalResourceStatus rsrc1success = mock(LocalResourceStatus.class);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/logaggregation/TestLogAggregationService.java
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
  @Override
  public void tearDown() throws IOException, InterruptedException {
    super.tearDown();
    createContainerExecutor().deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(user)
        .setSubDir(new Path(remoteRootLogDir.getAbsolutePath()))
        .setBasedirs(new Path[] {})
        .build());

    dispatcher.await();
    dispatcher.stop();
    dispatcher.close();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/monitor/TestContainersMonitor.java
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.LinuxResourceCalculatorPlugin;

    Assert.assertFalse("Process is still alive!",
        exec.signalContainer(new ContainerSignalContext.Builder()
            .setUser(user)
            .setPid(pid)
            .setSignal(Signal.NULL)
            .build()));
  }

  @Test(timeout = 20000)

