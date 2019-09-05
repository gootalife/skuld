hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/ContainerExecutor.java
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
  public abstract boolean isContainerAlive(ContainerLivenessContext ctx)
      throws IOException;

  public int reacquireContainer(ContainerReacquisitionContext ctx)
      throws IOException, InterruptedException {
    Container container = ctx.getContainer();
    String user = ctx.getUser();
    ContainerId containerId = ctx.getContainerId();

    LOG.info("Reacquiring " + containerId + " with pid " + pid);
    ContainerLivenessContext livenessContext = new ContainerLivenessContext
        .Builder()
        .setContainer(container)
        .setUser(user)
        .setPid(pid)
        .build();
    while(isContainerAlive(livenessContext)) {
      Thread.sleep(1000);
    }

    Map<Path, List<String>> resources, List<String> command) throws IOException{
    ContainerLaunch.ShellScriptBuilder sb =
      ContainerLaunch.ShellScriptBuilder.create();
    Set<String> whitelist = new HashSet<String>();
    whitelist.add(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME);
    whitelist.add(ApplicationConstants.Environment.HADOOP_YARN_HOME.name());
    whitelist.add(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name());
    whitelist.add(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name());
    whitelist.add(ApplicationConstants.Environment.HADOOP_CONF_DIR.name());
    whitelist.add(ApplicationConstants.Environment.JAVA_HOME.name());
    if (environment != null) {
      for (Map.Entry<String,String> env : environment.entrySet()) {
        if (!whitelist.contains(env.getKey())) {
          sb.env(env.getKey().toString(), env.getValue().toString());
        } else {
          sb.whitelistedEnv(env.getKey().toString(), env.getValue().toString());
        }
      }
    }
    if (resources != null) {
      try {
        Thread.sleep(delay);
        containerExecutor.signalContainer(new ContainerSignalContext.Builder()
            .setContainer(container)
            .setUser(user)
            .setPid(pid)
            .setSignal(signal)

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/DefaultContainerExecutor.java
  }

  @Override
  public boolean isContainerAlive(ContainerLivenessContext ctx)
      throws IOException {
    String pid = ctx.getPid();


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/DockerContainerExecutor.java
  }

  @Override
  public boolean isContainerAlive(ContainerLivenessContext ctx)
    throws IOException {
    String pid = ctx.getPid();


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/LinuxContainerExecutor.java

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DelegatingLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReacquisitionContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.util.LCEResourcesHandler;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;


public class LinuxContainerExecutor extends ContainerExecutor {

  private static final Log LOG = LogFactory
  private int containerSchedPriorityAdjustment = 0;
  private boolean containerLimitUsers;
  private ResourceHandler resourceHandlerChain;
  private LinuxContainerRuntime linuxContainerRuntime;

  public LinuxContainerExecutor() {
  }

  public LinuxContainerExecutor(LinuxContainerRuntime linuxContainerRuntime) {
    this.linuxContainerRuntime = linuxContainerRuntime;
  }

  @Override
  public void setConf(Configuration conf) {
    }
  }

  protected String getContainerExecutorExecutablePath(Configuration conf) {
    String yarnHomeEnvVar =
        System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key());
          + " (error=" + exitCode + ")", e);
    }

    Configuration conf = super.getConf();

    try {
      resourceHandlerChain = ResourceHandlerModule
          .getConfiguredResourceHandlerChain(conf);
      if (resourceHandlerChain != null) {
      throw new IOException("Failed to bootstrap configured resource subsystems!");
    }

    try {
      if (linuxContainerRuntime == null) {
        LinuxContainerRuntime runtime = new DelegatingLinuxContainerRuntime();

        runtime.initialize(conf);
        this.linuxContainerRuntime = runtime;
      }
    } catch (ContainerExecutionException e) {
     throw new IOException("Failed to initialize linux container runtime(s)!");
    }

    resourcesHandler.init(this);
  }

    command.addAll(Arrays.asList(containerExecutorExe, 
                   runAsUser,
                   user, 
                   Integer.toString(PrivilegedOperation.RunAsUserCommand.INITIALIZE_CONTAINER.getValue()),
                   appId,
                   nmPrivateContainerTokensPath.toUri().getPath().toString(),
                   StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
    Path containerWorkDir = ctx.getContainerWorkDir();
    List<String> localDirs = ctx.getLocalDirs();
    List<String> logDirs = ctx.getLogDirs();
    Map<Path, List<String>> localizedResources = ctx.getLocalizedResources();

    verifyUsernamePattern(user);
    String runAsUser = getRunAsUser(user);
      throw new IOException("ResourceHandlerChain.preStart() failed!");
    }

    try {
      Path pidFilePath = getPidFilePath(containerId);
      if (pidFilePath != null) {
        List<String> prefixCommands= new ArrayList<>();
        ContainerRuntimeContext.Builder builder = new ContainerRuntimeContext
            .Builder(container);

        addSchedPriorityCommand(prefixCommands);
        if (prefixCommands.size() > 0) {
          builder.setExecutionAttribute(CONTAINER_LAUNCH_PREFIX_COMMANDS,
              prefixCommands);
        }

        builder.setExecutionAttribute(LOCALIZED_RESOURCES, localizedResources)
            .setExecutionAttribute(RUN_AS_USER, runAsUser)
            .setExecutionAttribute(USER, user)
            .setExecutionAttribute(APPID, appId)
            .setExecutionAttribute(CONTAINER_ID_STR, containerIdStr)
            .setExecutionAttribute(CONTAINER_WORK_DIR, containerWorkDir)
            .setExecutionAttribute(NM_PRIVATE_CONTAINER_SCRIPT_PATH,
                nmPrivateContainerScriptPath)
            .setExecutionAttribute(NM_PRIVATE_TOKENS_PATH, nmPrivateTokensPath)
            .setExecutionAttribute(PID_FILE_PATH, pidFilePath)
            .setExecutionAttribute(LOCAL_DIRS, localDirs)
            .setExecutionAttribute(LOG_DIRS, logDirs)
            .setExecutionAttribute(RESOURCES_OPTIONS, resourcesOptions);

        if (tcCommandFile != null) {
          builder.setExecutionAttribute(TC_COMMAND_FILE, tcCommandFile);
        }

        linuxContainerRuntime.launchContainer(builder.build());
      } else {
        LOG.info("Container was marked as inactive. Returning terminated error");
        return ExitCode.TERMINATED.getExitCode();
      }
    } catch (ContainerExecutionException e) {
      int exitCode = e.getExitCode();
      LOG.warn("Exit code from container " + containerId + " is : " + exitCode);
      if (exitCode != ExitCode.FORCE_KILLED.getExitCode()
          && exitCode != ExitCode.TERMINATED.getExitCode()) {
        LOG.warn("Exception from container-launch with container ID: "
        builder.append("Exception from container-launch.\n");
        builder.append("Container id: " + containerId + "\n");
        builder.append("Exit code: " + exitCode + "\n");
        if (!Optional.fromNullable(e.getErrorOutput()).or("").isEmpty()) {
          builder.append("Exception message: " + e.getErrorOutput() + "\n");
        }
        builder.append("Stack trace: "
            + StringUtils.stringifyException(e) + "\n");
        if (!e.getOutput().isEmpty()) {
          builder.append("Shell output: " + e.getOutput() + "\n");
        }
        String diagnostics = builder.toString();
        logOutput(diagnostics);
            "containerId: " + containerId + ". Exception: " + e);
      }
    }

    return 0;
  }

  @Override
  public boolean signalContainer(ContainerSignalContext ctx)
      throws IOException {
    Container container = ctx.getContainer();
    String user = ctx.getUser();
    String pid = ctx.getPid();
    Signal signal = ctx.getSignal();
    verifyUsernamePattern(user);
    String runAsUser = getRunAsUser(user);

    ContainerRuntimeContext runtimeContext = new ContainerRuntimeContext
        .Builder(container)
        .setExecutionAttribute(RUN_AS_USER, runAsUser)
        .setExecutionAttribute(USER, user)
        .setExecutionAttribute(PID, pid)
        .setExecutionAttribute(SIGNAL, signal)
        .build();

    try {
      linuxContainerRuntime.signalContainer(runtimeContext);
    } catch (ContainerExecutionException e) {
      int retCode = e.getExitCode();
      if (retCode == PrivilegedOperation.ResultCode.INVALID_CONTAINER_PID.getValue()) {
        return false;
      }
      LOG.warn("Error in signalling container " + pid + " with " + signal
          + "; exit = " + retCode, e);
      logOutput(e.getOutput());
      throw new IOException("Problem signalling container " + pid + " with "
          + signal + "; output: " + e.getOutput() + " and exitCode: "
          + retCode, e);
    }
    return true;
  }
        Arrays.asList(containerExecutorExe,
                    runAsUser,
                    user,
                    Integer.toString(PrivilegedOperation.
                        RunAsUserCommand.DELETE_AS_USER.getValue()),
                    dirString));
    List<String> pathsToDelete = new ArrayList<String>();
    if (baseDirs == null || baseDirs.size() == 0) {
  }
  
  @Override
  public boolean isContainerAlive(ContainerLivenessContext ctx)
      throws IOException {
    String user = ctx.getUser();
    String pid = ctx.getPid();
    Container container = ctx.getContainer();

    return signalContainer(new ContainerSignalContext.Builder()
        .setContainer(container)
        .setUser(user)
        .setPid(pid)
        .setSignal(Signal.NULL)

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/launcher/ContainerLaunch.java
        exec.activateContainer(containerID, pidFilePath);
        ret = exec.launchContainer(new ContainerStartContext.Builder()
            .setContainer(container)
            .setLocalizedResources(localResources)
            .setNmPrivateContainerScriptPath(nmPrivateContainerScriptPath)
            .setNmPrivateTokensPath(nmPrivateTokensPath)
            .setUser(user)

        boolean result = exec.signalContainer(
            new ContainerSignalContext.Builder()
                .setContainer(container)
                .setUser(user)
                .setPid(processId)
                .setSignal(signal)

    public abstract void command(List<String> command) throws IOException;

    public abstract void whitelistedEnv(String key, String value) throws IOException;

    public abstract void env(String key, String value) throws IOException;

    public final void symlink(Path src, Path dst) throws IOException {
      errorCheck();
    }

    @Override
    public void whitelistedEnv(String key, String value) {
      line("export ", key, "=${", key, ":-", "\"", value, "\"}");
    }

    @Override
    public void env(String key, String value) {
      line("export ", key, "=\"", value, "\"");
      errorCheck();
    }

    @Override
    public void whitelistedEnv(String key, String value) throws IOException {
      lineWithLenCheck("@set ", key, "=", value);
      errorCheck();
    }

    @Override
    public void env(String key, String value) throws IOException {
      lineWithLenCheck("@set ", key, "=", value);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/privileged/PrivilegedOperation.java
    LAUNCH_CONTAINER(""), //no CLI switch supported yet
    SIGNAL_CONTAINER(""), //no CLI switch supported yet
    DELETE_AS_USER(""), //no CLI switch supported yet
    LAUNCH_DOCKER_CONTAINER(""), //no CLI switch supported yet
    TC_MODIFY_STATE("--tc-modify-state"),
    TC_READ_STATE("--tc-read-state"),
    TC_READ_STATS("--tc-read-stats"),
    ADD_PID_TO_CGROUP(""), //no CLI switch supported yet.
    RUN_DOCKER_CMD("--run-docker");

    private final String option;

  }

  public static final String CGROUP_ARG_PREFIX = "cgroups=";
  public static final String CGROUP_ARG_NO_TASKS = "none";

  private final OperationType opType;
  private final List<String> args;
  public int hashCode() {
    return opType.hashCode() + 97 * args.hashCode();
  }

  public enum RunAsUserCommand {
    INITIALIZE_CONTAINER(0),
    LAUNCH_CONTAINER(1),
    SIGNAL_CONTAINER(2),
    DELETE_AS_USER(3),
    LAUNCH_DOCKER_CONTAINER(4);

    private int value;
    RunAsUserCommand(int value) {
      this.value = value;
    }
    public int getValue() {
      return value;
    }
  }

  public enum ResultCode {
    OK(0),
    INVALID_USER_NAME(2),
    UNABLE_TO_EXECUTE_CONTAINER_SCRIPT(7),
    INVALID_CONTAINER_PID(9),
    INVALID_CONTAINER_EXEC_PERMISSIONS(22),
    INVALID_CONFIG_FILE(24),
    WRITE_CGROUP_FAILED(27);

    private final int value;
    ResultCode(int value) {
      this.value = value;
    }
    public int getValue() {
      return value;
    }
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/privileged/PrivilegedOperationException.java

public class PrivilegedOperationException extends YarnException {
  private static final long serialVersionUID = 1L;
  private Integer exitCode;
  private String output;
  private String errorOutput;

  public PrivilegedOperationException() {
    super();
    super(message);
  }

  public PrivilegedOperationException(String message, Integer exitCode,
      String output, String errorOutput) {
    super(message);
    this.exitCode = exitCode;
    this.output = output;
    this.errorOutput = errorOutput;
  }

  public PrivilegedOperationException(Throwable cause) {
    super(cause);
  }

  public PrivilegedOperationException(Throwable cause, Integer exitCode, String
      output, String errorOutput) {
    super(cause);
    this.exitCode = exitCode;
    this.output = output;
    this.errorOutput = errorOutput;
  }
  public PrivilegedOperationException(String message, Throwable cause) {
    super(message, cause);
  }

  public Integer getExitCode() {
    return exitCode;
  }

  public String getOutput() {
    return output;
  }

  public String getErrorOutput() { return errorOutput; }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/privileged/PrivilegedOperationExecutor.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
    }

    fullCommand.add(containerExecutorExe);

    String cliSwitch = operation.getOperationType().getOption();

    if (!cliSwitch.isEmpty()) {
      fullCommand.add(cliSwitch);
    }

    fullCommand.addAll(operation.getArguments());

    String[] fullCommandArray =
    try {
      exec.execute();
      if (LOG.isDebugEnabled()) {
        LOG.debug("command array:");
        LOG.debug(Arrays.toString(fullCommandArray));
        LOG.debug("Privileged Execution Operation Output:");
        LOG.debug(exec.getOutput());
      }
          .append(System.lineSeparator()).append(exec.getOutput()).toString();

      LOG.warn(logLine);

      throw new PrivilegedOperationException(e, e.getExitCode(),
          exec.getOutput(), e.getMessage());
    } catch (IOException e) {
      LOG.warn("IOException executing command: ", e);
      throw new PrivilegedOperationException(e);

    StringBuffer finalOpArg = new StringBuffer(PrivilegedOperation
        .CGROUP_ARG_PREFIX);
    boolean noTasks = true;

    for (PrivilegedOperation op : ops) {
      if (!op.getOperationType()
        throw new PrivilegedOperationException("Invalid argument: " + arg);
      }

      if (tasksFile.equals(PrivilegedOperation.CGROUP_ARG_NO_TASKS)) {
        continue;
      }

      if (noTasks == false) {
        finalOpArg.append(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR);
        finalOpArg.append(tasksFile);
      } else {
        finalOpArg.append(tasksFile);
        noTasks = false;
      }
    }

    if (noTasks) {
      finalOpArg.append(PrivilegedOperation.CGROUP_ARG_NO_TASKS); //there
    }

    PrivilegedOperation finalOp = new PrivilegedOperation(

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/CGroupsHandler.java
  public void deleteCGroup(CGroupController controller, String cGroupId) throws
      ResourceHandlerException;

  public String getRelativePathForCGroup(String cGroupId);


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/CGroupsHandlerImpl.java
    }
  }

  @Override
  public String getRelativePathForCGroup(String cGroupId) {
    return new StringBuffer(cGroupPrefix).append("/")
        .append(cGroupId).toString();
  }

  @Override
  public String getPathForCGroup(CGroupController controller, String cGroupId) {
    return new StringBuffer(getControllerPath(controller))

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/DefaultLinuxContainerRuntime.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/DefaultLinuxContainerRuntime.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;

import java.util.List;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DefaultLinuxContainerRuntime implements LinuxContainerRuntime {
  private static final Log LOG = LogFactory
      .getLog(DefaultLinuxContainerRuntime.class);
  private Configuration conf;
  private final PrivilegedOperationExecutor privilegedOperationExecutor;

  public DefaultLinuxContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor) {
    this.privilegedOperationExecutor = privilegedOperationExecutor;
  }

  @Override
  public void initialize(Configuration conf)
      throws ContainerExecutionException {
    this.conf = conf;
  }

  @Override
  public void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
  }

  @Override
  public void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    PrivilegedOperation launchOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.LAUNCH_CONTAINER, (String) null);

    launchOp.appendArgs(ctx.getExecutionAttribute(RUN_AS_USER),
        ctx.getExecutionAttribute(USER),
        Integer.toString(PrivilegedOperation.
            RunAsUserCommand.LAUNCH_CONTAINER.getValue()),
        ctx.getExecutionAttribute(APPID),
        ctx.getExecutionAttribute(CONTAINER_ID_STR),
        ctx.getExecutionAttribute(CONTAINER_WORK_DIR).toString(),
        ctx.getExecutionAttribute(NM_PRIVATE_CONTAINER_SCRIPT_PATH).toUri()
            .getPath(),
        ctx.getExecutionAttribute(NM_PRIVATE_TOKENS_PATH).toUri().getPath(),
        ctx.getExecutionAttribute(PID_FILE_PATH).toString(),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            ctx.getExecutionAttribute(LOCAL_DIRS)),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            ctx.getExecutionAttribute(LOG_DIRS)),
        ctx.getExecutionAttribute(RESOURCES_OPTIONS));

    String tcCommandFile = ctx.getExecutionAttribute(TC_COMMAND_FILE);

    if (tcCommandFile != null) {
      launchOp.appendArgs(tcCommandFile);
    }

    @SuppressWarnings("unchecked")
    List<String> prefixCommands = (List<String>) ctx.getExecutionAttribute(
        CONTAINER_LAUNCH_PREFIX_COMMANDS);

    try {
      privilegedOperationExecutor.executePrivilegedOperation(prefixCommands,
            launchOp, null, container.getLaunchContext().getEnvironment(),
            false);
    } catch (PrivilegedOperationException e) {
      LOG.warn("Launch container failed. Exception: ", e);

      throw new ContainerExecutionException("Launch container failed", e
          .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  @Override
  public void signalContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    PrivilegedOperation signalOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.SIGNAL_CONTAINER, (String) null);

    signalOp.appendArgs(ctx.getExecutionAttribute(RUN_AS_USER),
        ctx.getExecutionAttribute(USER),
        Integer.toString(PrivilegedOperation.RunAsUserCommand
            .SIGNAL_CONTAINER.getValue()),
        ctx.getExecutionAttribute(PID),
        Integer.toString(ctx.getExecutionAttribute(SIGNAL).getValue()));

    try {
      PrivilegedOperationExecutor executor = PrivilegedOperationExecutor
          .getInstance(conf);

      executor.executePrivilegedOperation(null,
          signalOp, null, container.getLaunchContext().getEnvironment(),
          false);
    } catch (PrivilegedOperationException e) {
      LOG.warn("Signal container failed. Exception: ", e);

      throw new ContainerExecutionException("Signal container failed", e
          .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  @Override
  public void reapContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {

  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/DelegatingLinuxContainerRuntime.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/DelegatingLinuxContainerRuntime.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;

import java.util.Map;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DelegatingLinuxContainerRuntime implements LinuxContainerRuntime {
  private static final Log LOG = LogFactory
      .getLog(DelegatingLinuxContainerRuntime.class);
  private DefaultLinuxContainerRuntime defaultLinuxContainerRuntime;
  private DockerLinuxContainerRuntime dockerLinuxContainerRuntime;

  @Override
  public void initialize(Configuration conf)
      throws ContainerExecutionException {
    PrivilegedOperationExecutor privilegedOperationExecutor =
        PrivilegedOperationExecutor.getInstance(conf);

    defaultLinuxContainerRuntime = new DefaultLinuxContainerRuntime(
        privilegedOperationExecutor);
    defaultLinuxContainerRuntime.initialize(conf);
    dockerLinuxContainerRuntime = new DockerLinuxContainerRuntime(
        privilegedOperationExecutor);
    dockerLinuxContainerRuntime.initialize(conf);
  }

  private LinuxContainerRuntime pickContainerRuntime(Container container) {
    Map<String, String> env = container.getLaunchContext().getEnvironment();
    LinuxContainerRuntime runtime;

    if (DockerLinuxContainerRuntime.isDockerContainerRequested(env)){
      runtime = dockerLinuxContainerRuntime;
    } else  {
      runtime = defaultLinuxContainerRuntime;
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Using container runtime: " + runtime.getClass()
          .getSimpleName());
    }

    return runtime;
  }

  @Override
  public void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    LinuxContainerRuntime runtime = pickContainerRuntime(container);

    runtime.prepareContainer(ctx);
  }

  @Override
  public void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    LinuxContainerRuntime runtime = pickContainerRuntime(container);

    runtime.launchContainer(ctx);
  }

  @Override
  public void signalContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    LinuxContainerRuntime runtime = pickContainerRuntime(container);

    runtime.signalContainer(ctx);
  }

  @Override
  public void reapContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    LinuxContainerRuntime runtime = pickContainerRuntime(container);

    runtime.reapContainer(ctx);
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/DockerLinuxContainerRuntime.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/DockerLinuxContainerRuntime.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerClient;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DockerLinuxContainerRuntime implements LinuxContainerRuntime {
  private static final Log LOG = LogFactory.getLog(
      DockerLinuxContainerRuntime.class);

  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_IMAGE =
      "YARN_CONTAINER_RUNTIME_DOCKER_IMAGE";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_IMAGE_FILE =
      "YARN_CONTAINER_RUNTIME_DOCKER_IMAGE_FILE";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_RUN_OVERRIDE_DISABLE =
      "YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE";


  private Configuration conf;
  private DockerClient dockerClient;
  private PrivilegedOperationExecutor privilegedOperationExecutor;

  public static boolean isDockerContainerRequested(
      Map<String, String> env) {
    if (env == null) {
      return false;
    }

    String type = env.get(ContainerRuntimeConstants.ENV_CONTAINER_TYPE);

    return type != null && type.equals("docker");
  }

  public DockerLinuxContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor) {
    this.privilegedOperationExecutor = privilegedOperationExecutor;
  }

  @Override
  public void initialize(Configuration conf)
      throws ContainerExecutionException {
    this.conf = conf;
    dockerClient = new DockerClient(conf);
  }

  @Override
  public void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {

  }

  public void addCGroupParentIfRequired(String resourcesOptions,
      String containerIdStr, DockerRunCommand runCommand)
      throws ContainerExecutionException {
    if (resourcesOptions.equals(
        (PrivilegedOperation.CGROUP_ARG_PREFIX + PrivilegedOperation
            .CGROUP_ARG_NO_TASKS))) {
      if (LOG.isInfoEnabled()) {
        LOG.info("no resource restrictions specified. not using docker's "
            + "cgroup options");
      }
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("using docker's cgroups options");
      }

      try {
        CGroupsHandler cGroupsHandler = ResourceHandlerModule
            .getCGroupsHandler(conf);
        String cGroupPath = "/" + cGroupsHandler.getRelativePathForCGroup(
            containerIdStr);

        if (LOG.isInfoEnabled()) {
          LOG.info("using cgroup parent: " + cGroupPath);
        }

        runCommand.setCGroupParent(cGroupPath);
      } catch (ResourceHandlerException e) {
        LOG.warn("unable to use cgroups handler. Exception: ", e);
        throw new ContainerExecutionException(e);
      }
    }
  }


  @Override
  public void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    Map<String, String> environment = container.getLaunchContext()
        .getEnvironment();
    String imageName = environment.get(ENV_DOCKER_CONTAINER_IMAGE);

    if (imageName == null) {
      throw new ContainerExecutionException(ENV_DOCKER_CONTAINER_IMAGE
          + " not set!");
    }

    String containerIdStr = container.getContainerId().toString();
    String runAsUser = ctx.getExecutionAttribute(RUN_AS_USER);
    Path containerWorkDir = ctx.getExecutionAttribute(CONTAINER_WORK_DIR);
    @SuppressWarnings("unchecked")
    List<String> localDirs = ctx.getExecutionAttribute(LOCAL_DIRS);
    @SuppressWarnings("unchecked")
    List<String> logDirs = ctx.getExecutionAttribute(LOG_DIRS);
    @SuppressWarnings("unchecked")
    DockerRunCommand runCommand = new DockerRunCommand(containerIdStr,
        runAsUser, imageName)
        .detachOnRun()
        .setContainerWorkDir(containerWorkDir.toString())
        .setNetworkType("host")
        .addMountLocation("/etc/passwd", "/etc/password:ro");
    List<String> allDirs = new ArrayList<>(localDirs);

    allDirs.add(containerWorkDir.toString());
    allDirs.addAll(logDirs);
    for (String dir: allDirs) {
      runCommand.addMountLocation(dir, dir);
    }

    String resourcesOpts = ctx.getExecutionAttribute(RESOURCES_OPTIONS);


   Path nmPrivateContainerScriptPath = ctx.getExecutionAttribute(
        NM_PRIVATE_CONTAINER_SCRIPT_PATH);

    String disableOverride = environment.get(
        ENV_DOCKER_CONTAINER_RUN_OVERRIDE_DISABLE);

    if (disableOverride != null && disableOverride.equals("true")) {
      if (LOG.isInfoEnabled()) {
        LOG.info("command override disabled");
      }
    } else {
      List<String> overrideCommands = new ArrayList<>();
      Path launchDst =
          new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);

      overrideCommands.add("bash");
      overrideCommands.add(launchDst.toUri().getPath());
      runCommand.setOverrideCommandWithArgs(overrideCommands);
    }

    String commandFile = dockerClient.writeCommandToTempFile(runCommand,
        containerIdStr);
    PrivilegedOperation launchOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.LAUNCH_DOCKER_CONTAINER, (String)
        null);

    launchOp.appendArgs(runAsUser, ctx.getExecutionAttribute(USER),
        Integer.toString(PrivilegedOperation
            .RunAsUserCommand.LAUNCH_DOCKER_CONTAINER.getValue()),
        ctx.getExecutionAttribute(APPID),
        containerIdStr, containerWorkDir.toString(),
        nmPrivateContainerScriptPath.toUri().getPath(),
        ctx.getExecutionAttribute(NM_PRIVATE_TOKENS_PATH).toUri().getPath(),
        ctx.getExecutionAttribute(PID_FILE_PATH).toString(),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            localDirs),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            logDirs),
        commandFile,
        resourcesOpts);

    String tcCommandFile = ctx.getExecutionAttribute(TC_COMMAND_FILE);

    if (tcCommandFile != null) {
      launchOp.appendArgs(tcCommandFile);
    }

    try {
      privilegedOperationExecutor.executePrivilegedOperation(null,
          launchOp, null, container.getLaunchContext().getEnvironment(),
          false);
    } catch (PrivilegedOperationException e) {
      LOG.warn("Launch container failed. Exception: ", e);

      throw new ContainerExecutionException("Launch container failed", e
          .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  @Override
  public void signalContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    PrivilegedOperation signalOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.SIGNAL_CONTAINER, (String) null);

    signalOp.appendArgs(ctx.getExecutionAttribute(RUN_AS_USER),
        ctx.getExecutionAttribute(USER),
        Integer.toString(PrivilegedOperation
            .RunAsUserCommand.SIGNAL_CONTAINER.getValue()),
        ctx.getExecutionAttribute(PID),
        Integer.toString(ctx.getExecutionAttribute(SIGNAL).getValue()));

    try {
      PrivilegedOperationExecutor executor = PrivilegedOperationExecutor
          .getInstance(conf);

      executor.executePrivilegedOperation(null,
          signalOp, null, container.getLaunchContext().getEnvironment(),
          false);
    } catch (PrivilegedOperationException e) {
      LOG.warn("Signal container failed. Exception: ", e);

      throw new ContainerExecutionException("Signal container failed", e
          .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  @Override
  public void reapContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {

  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/LinuxContainerRuntime.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/LinuxContainerRuntime.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntime;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface LinuxContainerRuntime extends ContainerRuntime {
  void initialize(Configuration conf) throws ContainerExecutionException;
}


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/LinuxContainerRuntimeConstants.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/LinuxContainerRuntimeConstants.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext.Attribute;

import java.util.List;
import java.util.Map;

public final class LinuxContainerRuntimeConstants {
  private LinuxContainerRuntimeConstants() {
  }

  public static final Attribute<Map> LOCALIZED_RESOURCES = Attribute
      .attribute(Map.class, "localized_resources");
  public static final Attribute<List> CONTAINER_LAUNCH_PREFIX_COMMANDS =
      Attribute.attribute(List.class, "container_launch_prefix_commands");
  public static final Attribute<String> RUN_AS_USER =
      Attribute.attribute(String.class, "run_as_user");
  public static final Attribute<String> USER = Attribute.attribute(String.class,
      "user");
  public static final Attribute<String> APPID =
      Attribute.attribute(String.class, "appid");
  public static final Attribute<String> CONTAINER_ID_STR = Attribute
      .attribute(String.class, "container_id_str");
  public static final Attribute<Path> CONTAINER_WORK_DIR = Attribute
      .attribute(Path.class, "container_work_dir");
  public static final Attribute<Path> NM_PRIVATE_CONTAINER_SCRIPT_PATH =
      Attribute.attribute(Path.class, "nm_private_container_script_path");
  public static final Attribute<Path> NM_PRIVATE_TOKENS_PATH = Attribute
      .attribute(Path.class, "nm_private_tokens_path");
  public static final Attribute<Path> PID_FILE_PATH = Attribute.attribute(
      Path.class, "pid_file_path");
  public static final Attribute<List> LOCAL_DIRS = Attribute.attribute(
      List.class, "local_dirs");
  public static final Attribute<List> LOG_DIRS = Attribute.attribute(
      List.class, "log_dirs");
  public static final Attribute<String> RESOURCES_OPTIONS = Attribute.attribute(
      String.class, "resources_options");
  public static final Attribute<String> TC_COMMAND_FILE = Attribute.attribute(
      String.class, "tc_command_file");
  public static final Attribute<String> CGROUP_RELATIVE_PATH = Attribute
      .attribute(String.class, "cgroup_relative_path");

  public static final Attribute<String> PID = Attribute.attribute(
      String.class, "pid");
  public static final Attribute<ContainerExecutor.Signal> SIGNAL = Attribute
      .attribute(ContainerExecutor.Signal.class, "signal");
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/docker/DockerClient.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/docker/DockerClient.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class DockerClient {
  private static final Log LOG = LogFactory.getLog(DockerClient.class);
  private static final String TMP_FILE_PREFIX = "docker.";
  private static final String TMP_FILE_SUFFIX = ".cmd";
  private final String tmpDirPath;

  public DockerClient(Configuration conf) throws ContainerExecutionException {

    String tmpDirBase = conf.get("hadoop.tmp.dir");
    if (tmpDirBase == null) {
      throw new ContainerExecutionException("hadoop.tmp.dir not set!");
    }
    tmpDirPath = tmpDirBase + "/nm-docker-cmds";

    File tmpDir = new File(tmpDirPath);
    if (!(tmpDir.exists() || tmpDir.mkdirs())) {
      LOG.warn("Unable to create directory: " + tmpDirPath);
      throw new ContainerExecutionException("Unable to create directory: " +
          tmpDirPath);
    }
  }

  public String writeCommandToTempFile(DockerCommand cmd, String filePrefix)
      throws ContainerExecutionException {
    File dockerCommandFile = null;
    try {
      dockerCommandFile = File.createTempFile(TMP_FILE_PREFIX + filePrefix,
          TMP_FILE_SUFFIX, new
          File(tmpDirPath));

      Writer writer = new OutputStreamWriter(new FileOutputStream(dockerCommandFile),
          "UTF-8");
      PrintWriter printWriter = new PrintWriter(writer);
      printWriter.print(cmd.getCommandWithArguments());
      printWriter.close();

      return dockerCommandFile.getAbsolutePath();
    } catch (IOException e) {
      LOG.warn("Unable to write docker command to temporary file!");
      throw new ContainerExecutionException(e);
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/docker/DockerCommand.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/docker/DockerCommand.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@InterfaceAudience.Private
@InterfaceStability.Unstable


public abstract class DockerCommand  {
  private final String command;
  private final List<String> commandWithArguments;

  protected DockerCommand(String command) {
    this.command = command;
    this.commandWithArguments = new ArrayList<>();
    commandWithArguments.add(command);
  }

  public final String getCommandOption() {
    return this.command;
  }

  protected final void addCommandArguments(String... arguments) {
    this.commandWithArguments.addAll(Arrays.asList(arguments));
  }

  public String getCommandWithArguments() {
    return StringUtils.join(" ", commandWithArguments);
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/docker/DockerLoadCommand.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/docker/DockerLoadCommand.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

public class DockerLoadCommand extends DockerCommand {
  private static final String LOAD_COMMAND = "load";

  public DockerLoadCommand(String localImageFile) {
    super(LOAD_COMMAND);
    super.addCommandArguments("--i=" + localImageFile);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/docker/DockerRunCommand.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/docker/DockerRunCommand.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class DockerRunCommand extends DockerCommand {
  private static final String RUN_COMMAND = "run";
  private final String image;
  private List<String> overrrideCommandWithArgs;

  public DockerRunCommand(String containerId, String user, String image) {
    super(RUN_COMMAND);
    super.addCommandArguments("--name=" + containerId, "--user=" + user);
    this.image = image;
  }

  public DockerRunCommand removeContainerOnExit() {
    super.addCommandArguments("--rm");
    return this;
  }

  public DockerRunCommand detachOnRun() {
    super.addCommandArguments("-d");
    return this;
  }

  public DockerRunCommand setContainerWorkDir(String workdir) {
    super.addCommandArguments("--workdir=" + workdir);
    return this;
  }

  public DockerRunCommand setNetworkType(String type) {
    super.addCommandArguments("--net=" + type);
    return this;
  }

  public DockerRunCommand addMountLocation(String sourcePath, String
      destinationPath) {
    super.addCommandArguments("-v", sourcePath + ":" + destinationPath);
    return this;
  }

  public DockerRunCommand setCGroupParent(String parentPath) {
    super.addCommandArguments("--cgroup-parent=" + parentPath);
    return this;
  }

  public DockerRunCommand addDevice(String sourceDevice, String
      destinationDevice) {
    super.addCommandArguments("--device=" + sourceDevice + ":" +
        destinationDevice);
    return this;
  }

  public DockerRunCommand enableDetach() {
    super.addCommandArguments("--detach=true");
    return this;
  }

  public DockerRunCommand disableDetach() {
    super.addCommandArguments("--detach=false");
    return this;
  }

  public DockerRunCommand setOverrideCommandWithArgs(
      List<String> overrideCommandWithArgs) {
    this.overrrideCommandWithArgs = overrideCommandWithArgs;
    return this;
  }

  @Override
  public String getCommandWithArguments() {
    List<String> argList = new ArrayList<>();

    argList.add(super.getCommandWithArguments());
    argList.add(image);

    if (overrrideCommandWithArgs != null) {
      argList.addAll(overrrideCommandWithArgs);
    }

    return StringUtils.join(" ", argList);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/runtime/ContainerExecutionException.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/runtime/ContainerExecutionException.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.exceptions.YarnException;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ContainerExecutionException extends YarnException {
  private static final long serialVersionUID = 1L;
  private static final Integer EXIT_CODE_UNSET = -1;
  private static final String OUTPUT_UNSET = "<unknown>";

  private Integer exitCode;
  private String output;
  private String errorOutput;

  public ContainerExecutionException(String message) {
    super(message);
    exitCode = EXIT_CODE_UNSET;
    output = OUTPUT_UNSET;
    errorOutput = OUTPUT_UNSET;
  }

  public ContainerExecutionException(Throwable throwable) {
    super(throwable);
    exitCode = EXIT_CODE_UNSET;
    output = OUTPUT_UNSET;
    errorOutput = OUTPUT_UNSET;
  }


  public ContainerExecutionException(String message, Integer exitCode, String
      output, String errorOutput) {
    super(message);
    this.exitCode = exitCode;
    this.output = output;
    this.errorOutput = errorOutput;
  }

  public ContainerExecutionException(Throwable cause, Integer exitCode, String
      output, String errorOutput) {
    super(cause);
    this.exitCode = exitCode;
    this.output = output;
    this.errorOutput = errorOutput;
  }

  public Integer getExitCode() {
    return exitCode;
  }

  public String getOutput() {
    return output;
  }

  public String getErrorOutput() {
    return errorOutput;
  }

}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/runtime/ContainerRuntime.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/runtime/ContainerRuntime.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ContainerRuntime {
  void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException;

  void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException;

  void signalContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException;

  void reapContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException;
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/runtime/ContainerRuntimeConstants.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/runtime/ContainerRuntimeConstants.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime;

import org.apache.hadoop.classification.InterfaceAudience.Private;

public class ContainerRuntimeConstants {


  @Private
  public static final String ENV_CONTAINER_TYPE =
      "YARN_CONTAINER_RUNTIME_TYPE";
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/runtime/ContainerRuntimeContext.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/runtime/ContainerRuntimeContext.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerRuntimeContext {
  private final Container container;
  private final Map<Attribute<?>, Object> executionAttributes;

  public static final class Attribute<T> {
    private final Class<T> valueClass;
    private final String id;

    private Attribute(Class<T> valueClass, String id) {
        this.valueClass = valueClass;
        this.id = id;
    }

    @Override
    public int hashCode() {
      return valueClass.hashCode() + 31 * id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof Attribute)){
        return false;
      }

      Attribute<?> attribute = (Attribute<?>) obj;

      return valueClass.equals(attribute.valueClass) && id.equals(attribute.id);
    }
    public static <T> Attribute<T> attribute(Class<T> valueClass, String id) {
      return new Attribute<T>(valueClass, id);
    }
  }

  public static final class Builder {
    private final Container container;
    private Map<Attribute<?>, Object> executionAttributes;

    public Builder(Container container) {
      executionAttributes = new HashMap<>();
      this.container = container;
    }

    public <E> Builder setExecutionAttribute(Attribute<E> attribute, E value) {
      this.executionAttributes.put(attribute, attribute.valueClass.cast(value));
      return this;
    }

    public ContainerRuntimeContext build() {
      return new ContainerRuntimeContext(this);
    }
  }

  private ContainerRuntimeContext(Builder builder) {
    this.container = builder.container;
    this.executionAttributes = builder.executionAttributes;
  }

  public Container getContainer() {
    return this.container;
  }

  public Map<Attribute<?>, Object> getExecutionAttributes() {
    return Collections.unmodifiableMap(this.executionAttributes);
  }

  public <E> E getExecutionAttribute(Attribute<E> attribute) {
    return attribute.valueClass.cast(executionAttributes.get(attribute));
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/executor/ContainerLivenessContext.java

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerLivenessContext {
  private final Container container;
  private final String user;
  private final String pid;

  public static final class Builder {
    private Container container;
    private String user;
    private String pid;

    public Builder() {
    }

    public Builder setContainer(Container container) {
      this.container = container;
      return this;
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
  }

  private ContainerLivenessContext(Builder builder) {
    this.container = builder.container;
    this.user = builder.user;
    this.pid = builder.pid;
  }

  public Container getContainer() {
    return this.container;
  }

  public String getUser() {
    return this.user;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/executor/ContainerReacquisitionContext.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerReacquisitionContext {
  private final Container container;
  private final String user;
  private final ContainerId containerId;

  public static final class Builder {
    private Container container;
    private String user;
    private ContainerId containerId;

    public Builder() {
    }

    public Builder setContainer(Container container) {
      this.container = container;
      return this;
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
  }

  private ContainerReacquisitionContext(Builder builder) {
    this.container = builder.container;
    this.user = builder.user;
    this.containerId = builder.containerId;
  }

  public Container getContainer() {
    return this.container;
  }

  public String getUser() {
    return this.user;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/executor/ContainerSignalContext.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerSignalContext {
  private final Container container;
  private final String user;
  private final String pid;
  private final Signal signal;

  public static final class Builder {
    private Container container;
    private String user;
    private String pid;
    private Signal signal;
    public Builder() {
    }

    public Builder setContainer(Container container) {
      this.container = container;
      return this;
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
  }

  private ContainerSignalContext(Builder builder) {
    this.container = builder.container;
    this.user = builder.user;
    this.pid = builder.pid;
    this.signal = builder.signal;
  }

  public Container getContainer() {
    return this.container;
  }

  public String getUser() {
    return this.user;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/executor/ContainerStartContext.java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@InterfaceStability.Unstable
public final class ContainerStartContext {
  private final Container container;
  private final Map<Path, List<String>> localizedResources;
  private final Path nmPrivateContainerScriptPath;
  private final Path nmPrivateTokensPath;
  private final String user;

  public static final class Builder {
    private Container container;
    private Map<Path, List<String>> localizedResources;
    private Path nmPrivateContainerScriptPath;
    private Path nmPrivateTokensPath;
    private String user;
      return this;
    }

    public Builder setLocalizedResources(Map<Path,
        List<String>> localizedResources) {
      this.localizedResources = localizedResources;
      return this;
    }

    public Builder setNmPrivateContainerScriptPath(
        Path nmPrivateContainerScriptPath) {
      this.nmPrivateContainerScriptPath = nmPrivateContainerScriptPath;

  private ContainerStartContext(Builder builder) {
    this.container = builder.container;
    this.localizedResources = builder.localizedResources;
    this.nmPrivateContainerScriptPath = builder.nmPrivateContainerScriptPath;
    this.nmPrivateTokensPath = builder.nmPrivateTokensPath;
    this.user = builder.user;
    return this.container;
  }

  public Map<Path, List<String>> getLocalizedResources() {
    if (this.localizedResources != null) {
      return Collections.unmodifiableMap(this.localizedResources);
    } else {
      return null;
    }
  }

  public Path getNmPrivateContainerScriptPath() {
    return this.nmPrivateContainerScriptPath;
  }
  }

  public List<String> getLocalDirs() {
    return Collections.unmodifiableList(this.localDirs);
  }

  public List<String> getLogDirs() {
    return Collections.unmodifiableList(this.logDirs);
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestLinuxContainerExecutorWithMocks.java
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DefaultLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class TestLinuxContainerExecutorWithMocks {

  private static final Log LOG = LogFactory
      .getLog(TestLinuxContainerExecutorWithMocks.class);

  private static final String MOCK_EXECUTOR =
      "./src/test/resources/mock-container-executor";
  private static final String MOCK_EXECUTOR_WITH_ERROR =
      "./src/test/resources/mock-container-executer-with-error";

  private String tmpMockExecutor;
  private LinuxContainerExecutor mockExec = null;
  private final File mockParamFile = new File("./params.txt");
  private LocalDirsHandlerService dirsHandler;
    return ret;
  }

  private void setupMockExecutor(String executorPath, Configuration conf)
      throws IOException {

    Files.copy(Paths.get(executorPath), Paths.get(tmpMockExecutor),
        REPLACE_EXISTING);

    File executor = new File(tmpMockExecutor);

    if (!FileUtil.canExecute(executor)) {
      FileUtil.setExecutable(executor, true);
    }
    String executorAbsolutePath = executor.getAbsolutePath();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH,
        executorAbsolutePath);
  }

  @Before
  public void setup() throws IOException, ContainerExecutionException {
    assumeTrue(!Path.WINDOWS);

    tmpMockExecutor = System.getProperty("test.build.data") +
        "/tmp-mock-container-executor";

    Configuration conf = new Configuration();
    LinuxContainerRuntime linuxContainerRuntime;

    setupMockExecutor(MOCK_EXECUTOR, conf);
    linuxContainerRuntime = new DefaultLinuxContainerRuntime(
        PrivilegedOperationExecutor.getInstance(conf));
    dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);
    linuxContainerRuntime.initialize(conf);
    mockExec = new LinuxContainerExecutor(linuxContainerRuntime);
    mockExec.setConf(conf);
  }

  public void testContainerLaunch() throws IOException {
    String appSubmitter = "nobody";
    String cmd = String.valueOf(
        PrivilegedOperation.RunAsUserCommand.LAUNCH_CONTAINER.getValue());
    String appId = "APP_ID";
    String containerId = "CONTAINER_ID";
    Container container = mock(Container.class);
  public void testContainerLaunchWithPriority() throws IOException {

    Configuration conf = new Configuration();
    setupMockExecutor(MOCK_EXECUTOR, conf);
    conf.setInt(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY, 2);

    mockExec.setConf(conf);
  
  
  @Test
  public void testContainerLaunchError()
      throws IOException, ContainerExecutionException {

    Configuration conf = new Configuration();
    setupMockExecutor(MOCK_EXECUTOR_WITH_ERROR, conf);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, "file:///bin/echo");
    conf.set(YarnConfiguration.NM_LOG_DIRS, "file:///dev/null");


    LinuxContainerExecutor exec;
    LinuxContainerRuntime linuxContainerRuntime = new
        DefaultLinuxContainerRuntime(PrivilegedOperationExecutor.getInstance
        (conf));

    linuxContainerRuntime.initialize(conf);
    exec = new LinuxContainerExecutor(linuxContainerRuntime);

    mockExec = spy(exec);
    doAnswer(
        new Answer() {
          @Override

    String appSubmitter = "nobody";
    String cmd = String
        .valueOf(PrivilegedOperation.RunAsUserCommand.LAUNCH_CONTAINER.getValue());
    String appId = "APP_ID";
    String containerId = "CONTAINER_ID";
    Container container = mock(Container.class);
  public void testContainerKill() throws IOException {
    String appSubmitter = "nobody";
    String cmd = String.valueOf(
        PrivilegedOperation.RunAsUserCommand.SIGNAL_CONTAINER.getValue());
    ContainerExecutor.Signal signal = ContainerExecutor.Signal.QUIT;
    String sigVal = String.valueOf(signal.getValue());

    Container container = mock(Container.class);
    ContainerId cId = mock(ContainerId.class);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);

    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);

    mockExec.signalContainer(new ContainerSignalContext.Builder()
        .setContainer(container)
        .setUser(appSubmitter)
        .setPid("1000")
        .setSignal(signal)
  public void testDeleteAsUser() throws IOException {
    String appSubmitter = "nobody";
    String cmd = String.valueOf(
        PrivilegedOperation.RunAsUserCommand.DELETE_AS_USER.getValue());
    Path dir = new Path("/tmp/testdir");
    Path testFile = new Path("testfile");
    Path baseDir0 = new Path("/grid/0/BaseDir");
        Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
            appSubmitter, cmd, "", baseDir0.toString(), baseDir1.toString()),
        readMockParams());
    ;
    Configuration conf = new Configuration();
    setupMockExecutor(MOCK_EXECUTOR, conf);
    mockExec.setConf(conf);

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/privileged/TestPrivilegedOperationExecutor.java
    PrivilegedOperationExecutor exec = PrivilegedOperationExecutor
        .getInstance(confWithExecutorPath);
    PrivilegedOperation op = new PrivilegedOperation(PrivilegedOperation
        .OperationType.TC_MODIFY_STATE, (String) null);
    String[] cmdArray = exec.getPrivilegedOperationExecutionCommand(null, op);

    Assert.assertEquals(customExecutorPath, cmdArray[0]);
    Assert.assertEquals(op.getOperationType().getOption(), cmdArray[1]);

    String[] additionalArgs = { "cmd_file_1", "cmd_file_2", "cmd_file_3"};

    op.appendArgs(additionalArgs);
    cmdArray = exec.getPrivilegedOperationExecutionCommand(null, op);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/TestDockerContainerRuntime.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/runtime/TestDockerContainerRuntime.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class TestDockerContainerRuntime {
  private Configuration conf;
  PrivilegedOperationExecutor mockExecutor;
  String containerId;
  Container container;
  ContainerId cId;
  ContainerLaunchContext context;
  HashMap<String, String> env;
  String image;
  String runAsUser;
  String user;
  String appId;
  String containerIdStr = containerId;
  Path containerWorkDir;
  Path nmPrivateContainerScriptPath;
  Path nmPrivateTokensPath;
  Path pidFilePath;
  List<String> localDirs;
  List<String> logDirs;
  String resourcesOptions;

  @Before
  public void setup() {
    String tmpPath = new StringBuffer(System.getProperty("test.build.data"))
        .append
        ('/').append("hadoop.tmp.dir").toString();

    conf = new Configuration();
    conf.set("hadoop.tmp.dir", tmpPath);

    mockExecutor = Mockito
        .mock(PrivilegedOperationExecutor.class);
    containerId = "container_id";
    container = mock(Container.class);
    cId = mock(ContainerId.class);
    context = mock(ContainerLaunchContext.class);
    env = new HashMap<String, String>();
    image = "busybox:latest";

    env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_IMAGE, image);
    when(container.getContainerId()).thenReturn(cId);
    when(cId.toString()).thenReturn(containerId);
    when(container.getLaunchContext()).thenReturn(context);
    when(context.getEnvironment()).thenReturn(env);

    runAsUser = "run_as_user";
    user = "user";
    appId = "app_id";
    containerIdStr = containerId;
    containerWorkDir = new Path("/test_container_work_dir");
    nmPrivateContainerScriptPath = new Path("/test_script_path");
    nmPrivateTokensPath = new Path("/test_private_tokens_path");
    pidFilePath = new Path("/test_pid_file_path");
    localDirs = new ArrayList<>();
    logDirs = new ArrayList<>();
    resourcesOptions = "cgroups:none";

    localDirs.add("/test_local_dir");
    logDirs.add("/test_log_dir");
  }

  @Test
  public void testSelectDockerContainerType() {
    Map<String, String> envDockerType = new HashMap<>();
    Map<String, String> envOtherType = new HashMap<>();

    envDockerType.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE, "docker");
    envOtherType.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE, "other");

    Assert.assertEquals(false, DockerLinuxContainerRuntime
        .isDockerContainerRequested(null));
    Assert.assertEquals(true, DockerLinuxContainerRuntime
        .isDockerContainerRequested(envDockerType));
    Assert.assertEquals(false, DockerLinuxContainerRuntime
        .isDockerContainerRequested(envOtherType));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDockerContainerLaunch()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor);
    runtime.initialize(conf);

    ContainerRuntimeContext.Builder builder = new ContainerRuntimeContext
        .Builder(container);

    builder.setExecutionAttribute(RUN_AS_USER, runAsUser)
        .setExecutionAttribute(USER, user)
        .setExecutionAttribute(APPID, appId)
        .setExecutionAttribute(CONTAINER_ID_STR, containerIdStr)
        .setExecutionAttribute(CONTAINER_WORK_DIR, containerWorkDir)
        .setExecutionAttribute(NM_PRIVATE_CONTAINER_SCRIPT_PATH,
            nmPrivateContainerScriptPath)
        .setExecutionAttribute(NM_PRIVATE_TOKENS_PATH, nmPrivateTokensPath)
        .setExecutionAttribute(PID_FILE_PATH, pidFilePath)
        .setExecutionAttribute(LOCAL_DIRS, localDirs)
        .setExecutionAttribute(LOG_DIRS, logDirs)
        .setExecutionAttribute(RESOURCES_OPTIONS, resourcesOptions);

    runtime.launchContainer(builder.build());

    ArgumentCaptor<PrivilegedOperation> opCaptor = ArgumentCaptor.forClass(
        PrivilegedOperation.class);

    verify(mockExecutor, times(1))
        .executePrivilegedOperation(anyList(), opCaptor.capture(), any(
            File.class), any(Map.class), eq(false));

    PrivilegedOperation op = opCaptor.getValue();

    Assert.assertEquals(PrivilegedOperation.OperationType
        .LAUNCH_DOCKER_CONTAINER, op.getOperationType());

    List<String> args = op.getArguments();

    Assert.assertEquals(13, args.size());

    Assert.assertEquals(runAsUser, args.get(0));
    Assert.assertEquals(user, args.get(1));
    Assert.assertEquals(Integer.toString(PrivilegedOperation.RunAsUserCommand
        .LAUNCH_DOCKER_CONTAINER.getValue()), args.get(2));
    Assert.assertEquals(appId, args.get(3));
    Assert.assertEquals(containerId, args.get(4));
    Assert.assertEquals(containerWorkDir.toString(), args.get(5));
    Assert.assertEquals(nmPrivateContainerScriptPath.toUri()
            .toString(), args.get(6));
    Assert.assertEquals(nmPrivateTokensPath.toUri().getPath(), args.get(7));
    Assert.assertEquals(pidFilePath.toString(), args.get(8));
    Assert.assertEquals(localDirs.get(0), args.get(9));
    Assert.assertEquals(logDirs.get(0), args.get(10));
    Assert.assertEquals(resourcesOptions, args.get(12));

    String dockerCommandFile = args.get(11);

    StringBuffer expectedCommandTemplate = new StringBuffer("run --name=%1$s ")
        .append("--user=%2$s -d ")
        .append("--workdir=%3$s ")
        .append("--net=host -v /etc/passwd:/etc/password:ro ")
        .append("-v %4$s:%4$s ")
        .append("-v %5$s:%5$s ")
        .append("-v %6$s:%6$s ")
        .append("%7$s ")
        .append("bash %8$s/launch_container.sh");

    String expectedCommand = String.format(expectedCommandTemplate.toString(),
        containerId, runAsUser, containerWorkDir, localDirs.get(0),
        containerWorkDir, logDirs.get(0), image, containerWorkDir);

    List<String> dockerCommands = Files.readAllLines(Paths.get
            (dockerCommandFile), Charset.forName("UTF-8"));

    Assert.assertEquals(1, dockerCommands.size());
    Assert.assertEquals(expectedCommand, dockerCommands.get(0));
  }
}

