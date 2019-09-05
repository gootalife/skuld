hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/privileged/PrivilegedOperation.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/privileged/PrivilegedOperation.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PrivilegedOperation {

  public enum OperationType {
    CHECK_SETUP("--checksetup"),
    MOUNT_CGROUPS("--mount-cgroups"),
    INITIALIZE_CONTAINER(""), //no CLI switch supported yet
    LAUNCH_CONTAINER(""), //no CLI switch supported yet
    SIGNAL_CONTAINER(""), //no CLI switch supported yet
    DELETE_AS_USER(""), //no CLI switch supported yet
    TC_MODIFY_STATE("--tc-modify-state"),
    TC_READ_STATE("--tc-read-state"),
    TC_READ_STATS("--tc-read-stats"),
    ADD_PID_TO_CGROUP(""); //no CLI switch supported yet.

    private final String option;

    OperationType(String option) {
      this.option = option;
    }

    public String getOption() {
      return option;
    }
  }

  public static final String CGROUP_ARG_PREFIX = "cgroups=";

  private final OperationType opType;
  private final List<String> args;

  public PrivilegedOperation(OperationType opType, String arg) {
    this.opType = opType;
    this.args = new ArrayList<String>();

    if (arg != null) {
      this.args.add(arg);
    }
  }

  public PrivilegedOperation(OperationType opType, List<String> args) {
    this.opType = opType;
    this.args = new ArrayList<String>();

    if (args != null) {
      this.args.addAll(args);
    }
  }

  public void appendArgs(String... args) {
    for (String arg : args) {
      this.args.add(arg);
    }
  }

  public void appendArgs(List<String> args) {
    this.args.addAll(args);
  }

  public OperationType getOperationType() {
    return opType;
  }

  public List<String> getArguments() {
    return Collections.unmodifiableList(this.args);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof PrivilegedOperation)) {
      return false;
    }

    PrivilegedOperation otherOp = (PrivilegedOperation) other;

    return otherOp.opType.equals(opType) && otherOp.args.equals(args);
  }

  @Override
  public int hashCode() {
    return opType.hashCode() + 97 * args.hashCode();
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/privileged/PrivilegedOperationException.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/privileged/PrivilegedOperationException.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged;

import org.apache.hadoop.yarn.exceptions.YarnException;

public class PrivilegedOperationException extends YarnException {
  private static final long serialVersionUID = 1L;

  public PrivilegedOperationException() {
    super();
  }

  public PrivilegedOperationException(String message) {
    super(message);
  }

  public PrivilegedOperationException(Throwable cause) {
    super(cause);
  }

  public PrivilegedOperationException(String message, Throwable cause) {
    super(message, cause);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/privileged/PrivilegedOperationExecutor.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/privileged/PrivilegedOperationExecutor.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PrivilegedOperationExecutor {
  private static final Log LOG = LogFactory.getLog(PrivilegedOperationExecutor
      .class);
  private volatile static PrivilegedOperationExecutor instance;

  private String containerExecutorExe;

  public static String getContainerExecutorExecutablePath(Configuration conf) {
    String yarnHomeEnvVar =
        System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key());
    File hadoopBin = new File(yarnHomeEnvVar, "bin");
    String defaultPath =
        new File(hadoopBin, "container-executor").getAbsolutePath();
    return null == conf
        ? defaultPath
        : conf.get(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH,
        defaultPath);
  }

  private void init(Configuration conf) {
    containerExecutorExe = getContainerExecutorExecutablePath(conf);
  }

  private PrivilegedOperationExecutor(Configuration conf) {
    init(conf);
  }

  public static PrivilegedOperationExecutor getInstance(Configuration conf) {
    if (instance == null) {
      synchronized (PrivilegedOperationExecutor.class) {
        if (instance == null) {
          instance = new PrivilegedOperationExecutor(conf);
        }
      }
    }

    return instance;
  }


  public String[] getPrivilegedOperationExecutionCommand(List<String>
      prefixCommands,
      PrivilegedOperation operation) {
    List<String> fullCommand = new ArrayList<String>();

    if (prefixCommands != null && !prefixCommands.isEmpty()) {
      fullCommand.addAll(prefixCommands);
    }

    fullCommand.add(containerExecutorExe);
    fullCommand.add(operation.getOperationType().getOption());
    fullCommand.addAll(operation.getArguments());

    String[] fullCommandArray =
        fullCommand.toArray(new String[fullCommand.size()]);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Privileged Execution Command Array: " +
          Arrays.toString(fullCommandArray));
    }

    return fullCommandArray;
  }

  public String executePrivilegedOperation(List<String> prefixCommands,
      PrivilegedOperation operation, File workingDir,
      Map<String, String> env, boolean grabOutput)
      throws PrivilegedOperationException {
    String[] fullCommandArray = getPrivilegedOperationExecutionCommand
        (prefixCommands, operation);
    ShellCommandExecutor exec = new ShellCommandExecutor(fullCommandArray,
        workingDir, env);

    try {
      exec.execute();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Privileged Execution Operation Output:");
        LOG.debug(exec.getOutput());
      }
    } catch (ExitCodeException e) {
      String logLine = new StringBuffer("Shell execution returned exit code: ")
          .append(exec.getExitCode())
          .append(". Privileged Execution Operation Output: ")
          .append(System.lineSeparator()).append(exec.getOutput()).toString();

      LOG.warn(logLine);
      throw new PrivilegedOperationException(e);
    } catch (IOException e) {
      LOG.warn("IOException executing command: ", e);
      throw new PrivilegedOperationException(e);
    }

    if (grabOutput) {
      return exec.getOutput();
    }

    return null;
  }

  public String executePrivilegedOperation(PrivilegedOperation operation,
      boolean grabOutput) throws PrivilegedOperationException {
    return executePrivilegedOperation(null, operation, null, null, grabOutput);
  }



  public static PrivilegedOperation squashCGroupOperations
  (List<PrivilegedOperation> ops) throws PrivilegedOperationException {
    if (ops.size() == 0) {
      return null;
    }

    StringBuffer finalOpArg = new StringBuffer(PrivilegedOperation
        .CGROUP_ARG_PREFIX);
    boolean noneArgsOnly = true;

    for (PrivilegedOperation op : ops) {
      if (!op.getOperationType()
          .equals(PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP)) {
        LOG.warn("Unsupported operation type: " + op.getOperationType());
        throw new PrivilegedOperationException("Unsupported operation type:"
            + op.getOperationType());
      }

      List<String> args = op.getArguments();
      if (args.size() != 1) {
        LOG.warn("Invalid number of args: " + args.size());
        throw new PrivilegedOperationException("Invalid number of args: "
            + args.size());
      }

      String arg = args.get(0);
      String tasksFile = StringUtils.substringAfter(arg,
          PrivilegedOperation.CGROUP_ARG_PREFIX);
      if (tasksFile == null || tasksFile.isEmpty()) {
        LOG.warn("Invalid argument: " + arg);
        throw new PrivilegedOperationException("Invalid argument: " + arg);
      }

      if (tasksFile.equals("none")) {
        continue;
      }

      if (noneArgsOnly == false) {
        finalOpArg.append(",");
        finalOpArg.append(tasksFile);
      } else {
        finalOpArg.append(tasksFile);
        noneArgsOnly = false;
      }
    }

    if (noneArgsOnly) {
      finalOpArg.append("none"); //there were no tasks file to append
    }

    PrivilegedOperation finalOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP, finalOpArg
        .toString());

    return finalOp;
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/CGroupsHandler.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/CGroupsHandler.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface CGroupsHandler {
  public enum CGroupController {
    CPU("cpu"),
    NET_CLS("net_cls");

    private final String name;

    CGroupController(String name) {
      this.name = name;
    }

    String getName() {
      return name;
    }
  }

  public static final String CGROUP_FILE_TASKS = "tasks";
  public static final String CGROUP_PARAM_CLASSID = "classid";

  public void mountCGroupController(CGroupController controller)
      throws ResourceHandlerException;

  public String createCGroup(CGroupController controller, String cGroupId)
      throws ResourceHandlerException;

  public void deleteCGroup(CGroupController controller, String cGroupId) throws
      ResourceHandlerException;

  public String getPathForCGroup(CGroupController controller, String
      cGroupId);

  public String getPathForCGroupTasks(CGroupController controller, String
      cGroupId);

  public String getPathForCGroupParam(CGroupController controller, String
      cGroupId, String param);

  public void updateCGroupParam(CGroupController controller, String cGroupId,
      String param, String value) throws ResourceHandlerException;

  public String getCGroupParam(CGroupController controller, String cGroupId,
      String param) throws ResourceHandlerException;
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/CGroupsHandlerImpl.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/CGroupsHandlerImpl.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@InterfaceAudience.Private
@InterfaceStability.Unstable
class CGroupsHandlerImpl implements CGroupsHandler {

  private static final Log LOG = LogFactory.getLog(CGroupsHandlerImpl.class);
  private static final String MTAB_FILE = "/proc/mounts";
  private static final String CGROUPS_FSTYPE = "cgroup";

  private final String cGroupPrefix;
  private final boolean enableCGroupMount;
  private final String cGroupMountPath;
  private final long deleteCGroupTimeout;
  private final long deleteCGroupDelay;
  private final Map<CGroupController, String> controllerPaths;
  private final ReadWriteLock rwLock;
  private final PrivilegedOperationExecutor privilegedOperationExecutor;
  private final Clock clock;

  public CGroupsHandlerImpl(Configuration conf, PrivilegedOperationExecutor
      privilegedOperationExecutor) throws ResourceHandlerException {
    this.cGroupPrefix = conf.get(YarnConfiguration.
        NM_LINUX_CONTAINER_CGROUPS_HIERARCHY, "/hadoop-yarn")
        .replaceAll("^/", "").replaceAll("$/", "");
    this.enableCGroupMount = conf.getBoolean(YarnConfiguration.
        NM_LINUX_CONTAINER_CGROUPS_MOUNT, false);
    this.cGroupMountPath = conf.get(YarnConfiguration.
        NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, null);
    this.deleteCGroupTimeout = conf.getLong(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT,
        YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT);
    this.deleteCGroupDelay =
        conf.getLong(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_DELETE_DELAY,
            YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_DELAY);
    this.controllerPaths = new HashMap<>();
    this.rwLock = new ReentrantReadWriteLock();
    this.privilegedOperationExecutor = privilegedOperationExecutor;
    this.clock = new SystemClock();

    init();
  }

  private void init() throws ResourceHandlerException {
    initializeControllerPaths();
  }

  private String getControllerPath(CGroupController controller) {
    try {
      rwLock.readLock().lock();
      return controllerPaths.get(controller);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  private void initializeControllerPaths() throws ResourceHandlerException {
    if (enableCGroupMount) {
      LOG.info("CGroup controller mounting enabled.");
    } else {
      initializeControllerPathsFromMtab();
    }
  }

  private void initializeControllerPathsFromMtab()
      throws ResourceHandlerException {
    try {
      Map<String, List<String>> parsedMtab = parseMtab();

      rwLock.writeLock().lock();

      for (CGroupController controller : CGroupController.values()) {
        String name = controller.getName();
        String controllerPath = findControllerInMtab(name, parsedMtab);

        if (controllerPath != null) {
          File f = new File(controllerPath + "/" + this.cGroupPrefix);

          if (FileUtil.canWrite(f)) {
            controllerPaths.put(controller, controllerPath);
          } else {
            String error =
                new StringBuffer("Mount point Based on mtab file: ")
                    .append(MTAB_FILE).append(
                    ". Controller mount point not writable for: ")
                    .append(name).toString();

            LOG.error(error);
            throw new ResourceHandlerException(error);
          }
        } else {

            LOG.warn("Controller not mounted but automount disabled: " + name);
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to initialize controller paths! Exception: " + e);
      throw new ResourceHandlerException(
          "Failed to initialize controller paths!");
    } finally {
      rwLock.writeLock().unlock();
    }
  }


  private static final Pattern MTAB_FILE_FORMAT = Pattern.compile(
      "^[^\\s]+\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s[^\\s]+\\s[^\\s]+$");

  private Map<String, List<String>> parseMtab() throws IOException {
    Map<String, List<String>> ret = new HashMap<String, List<String>>();
    BufferedReader in = null;

    try {
      FileInputStream fis = new FileInputStream(new File(getMtabFileName()));
      in = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

      for (String str = in.readLine(); str != null;
           str = in.readLine()) {
        Matcher m = MTAB_FILE_FORMAT.matcher(str);
        boolean mat = m.find();
        if (mat) {
          String path = m.group(1);
          String type = m.group(2);
          String options = m.group(3);

          if (type.equals(CGROUPS_FSTYPE)) {
            List<String> value = Arrays.asList(options.split(","));
            ret.put(path, value);
          }
        }
      }
    } catch (IOException e) {
      throw new IOException("Error while reading " + getMtabFileName(), e);
    } finally {
      IOUtils.cleanup(LOG, in);
    }

    return ret;
  }

  private String findControllerInMtab(String controller,
      Map<String, List<String>> entries) {
    for (Map.Entry<String, List<String>> e : entries.entrySet()) {
      if (e.getValue().contains(controller))
        return e.getKey();
    }

    return null;
  }

  String getMtabFileName() {
    return MTAB_FILE;
  }

  @Override
  public void mountCGroupController(CGroupController controller)
      throws ResourceHandlerException {
    if (!enableCGroupMount) {
      LOG.warn("CGroup mounting is disabled - ignoring mount request for: " +
          controller.getName());
      return;
    }

    String path = getControllerPath(controller);

    if (path == null) {
      try {
        rwLock.writeLock().lock();

        String hierarchy = cGroupPrefix;
        StringBuffer controllerPath = new StringBuffer()
            .append(cGroupMountPath).append('/').append(controller.getName());
        StringBuffer cGroupKV = new StringBuffer()
            .append(controller.getName()).append('=').append(controllerPath);
        PrivilegedOperation.OperationType opType = PrivilegedOperation
            .OperationType.MOUNT_CGROUPS;
        PrivilegedOperation op = new PrivilegedOperation(opType, (String) null);

        op.appendArgs(hierarchy, cGroupKV.toString());
        LOG.info("Mounting controller " + controller.getName() + " at " +
              controllerPath);
        privilegedOperationExecutor.executePrivilegedOperation(op, false);

        controllerPaths.put(controller, controllerPath.toString());

        return;
      } catch (PrivilegedOperationException e) {
        LOG.error("Failed to mount controller: " + controller.getName());
        throw new ResourceHandlerException("Failed to mount controller: "
            + controller.getName());
      } finally {
        rwLock.writeLock().unlock();
      }
    } else {
      LOG.info("CGroup controller already mounted at: " + path);
      return;
    }
  }

  @Override
  public String getPathForCGroup(CGroupController controller, String cGroupId) {
    return new StringBuffer(getControllerPath(controller))
        .append('/').append(cGroupPrefix).append("/")
        .append(cGroupId).toString();
  }

  @Override
  public String getPathForCGroupTasks(CGroupController controller,
      String cGroupId) {
    return new StringBuffer(getPathForCGroup(controller, cGroupId))
        .append('/').append(CGROUP_FILE_TASKS).toString();
  }

  @Override
  public String getPathForCGroupParam(CGroupController controller,
      String cGroupId, String param) {
    return new StringBuffer(getPathForCGroup(controller, cGroupId))
        .append('/').append(controller.getName()).append('.')
        .append(param).toString();
  }

  @Override
  public String createCGroup(CGroupController controller, String cGroupId)
      throws ResourceHandlerException {
    String path = getPathForCGroup(controller, cGroupId);

    if (LOG.isDebugEnabled()) {
      LOG.debug("createCgroup: " + path);
    }

    if (!new File(path).mkdir()) {
      throw new ResourceHandlerException("Failed to create cgroup at " + path);
    }

    return path;
  }

  private void logLineFromTasksFile(File cgf) {
    String str;
    if (LOG.isDebugEnabled()) {
      try (BufferedReader inl =
          new BufferedReader(new InputStreamReader(new FileInputStream(cgf
              + "/tasks"), "UTF-8"))) {
        if ((str = inl.readLine()) != null) {
          LOG.debug("First line in cgroup tasks file: " + cgf + " " + str);
        }
      } catch (IOException e) {
        LOG.warn("Failed to read cgroup tasks file. ", e);
      }
    }
  }

  boolean checkAndDeleteCgroup(File cgf) throws InterruptedException {
    boolean deleted = false;
    try (FileInputStream in = new FileInputStream(cgf + "/tasks")) {
      if (in.read() == -1) {
        Thread.sleep(deleteCGroupDelay);
        deleted = cgf.delete();
        if (!deleted) {
          LOG.warn("Failed attempt to delete cgroup: " + cgf);
        }
      } else {
        logLineFromTasksFile(cgf);
      }
    } catch (IOException e) {
      LOG.warn("Failed to read cgroup tasks file. ", e);
    }
    return deleted;
  }

  @Override
  public void deleteCGroup(CGroupController controller, String cGroupId)
      throws ResourceHandlerException {
    boolean deleted = false;
    String cGroupPath = getPathForCGroup(controller, cGroupId);

    if (LOG.isDebugEnabled()) {
      LOG.debug("deleteCGroup: " + cGroupPath);
    }

    long start = clock.getTime();

    do {
      try {
        deleted = checkAndDeleteCgroup(new File(cGroupPath));
        if (!deleted) {
          Thread.sleep(deleteCGroupDelay);
        }
      } catch (InterruptedException ex) {
      }
    } while (!deleted && (clock.getTime() - start) < deleteCGroupTimeout);

    if (!deleted) {
      LOG.warn("Unable to delete  " + cGroupPath +
          ", tried to delete for " + deleteCGroupTimeout + "ms");
    }
  }

  @Override
  public void updateCGroupParam(CGroupController controller, String cGroupId,
      String param, String value) throws ResourceHandlerException {
    String cGroupParamPath = getPathForCGroupParam(controller, cGroupId, param);
    PrintWriter pw = null;

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "updateCGroupParam for path: " + cGroupParamPath + " with value " +
              value);
    }

    try {
      File file = new File(cGroupParamPath);
      Writer w = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
      pw = new PrintWriter(w);
      pw.write(value);
    } catch (IOException e) {
      throw new ResourceHandlerException(new StringBuffer("Unable to write to ")
          .append(cGroupParamPath).append(" with value: ").append(value)
          .toString(), e);
    } finally {
      if (pw != null) {
        boolean hasError = pw.checkError();
        pw.close();
        if (hasError) {
          throw new ResourceHandlerException(
              new StringBuffer("Unable to write to ")
                  .append(cGroupParamPath).append(" with value: ").append(value)
                  .toString());
        }
        if (pw.checkError()) {
          throw new ResourceHandlerException("Error while closing cgroup file" +
              " " + cGroupParamPath);
        }
      }
    }
  }

  @Override
  public String getCGroupParam(CGroupController controller, String cGroupId,
      String param) throws ResourceHandlerException {
    String cGroupParamPath = getPathForCGroupParam(controller, cGroupId, param);

    try {
      byte[] contents = Files.readAllBytes(Paths.get(cGroupParamPath));
      return new String(contents, "UTF-8").trim();
    } catch (IOException e) {
      throw new ResourceHandlerException(
          "Unable to read from " + cGroupParamPath);
    }
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/ResourceHandler.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/ResourceHandler.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

import java.util.List;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ResourceHandler {

  List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException;

  List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException;


  List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException;

  List<PrivilegedOperation> postComplete(ContainerId containerId) throws
      ResourceHandlerException;

  List<PrivilegedOperation> teardown() throws ResourceHandlerException;
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/ResourceHandlerChain.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/ResourceHandlerChain.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ResourceHandlerChain implements ResourceHandler {
  private final List<ResourceHandler> resourceHandlers;

  public ResourceHandlerChain(List<ResourceHandler> resourceHandlers) {
    this.resourceHandlers = resourceHandlers;
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {

    List<PrivilegedOperation> allOperations = new
        ArrayList<PrivilegedOperation>();

    for (ResourceHandler resourceHandler : resourceHandlers) {
      List<PrivilegedOperation> handlerOperations =
          resourceHandler.bootstrap(configuration);
      if (handlerOperations != null) {
        allOperations.addAll(handlerOperations);
      }

    }
    return allOperations;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    List<PrivilegedOperation> allOperations = new
        ArrayList<PrivilegedOperation>();

    for (ResourceHandler resourceHandler : resourceHandlers) {
      List<PrivilegedOperation> handlerOperations =
          resourceHandler.preStart(container);

      if (handlerOperations != null) {
        allOperations.addAll(handlerOperations);
      }

    }
    return allOperations;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    List<PrivilegedOperation> allOperations = new
        ArrayList<PrivilegedOperation>();

    for (ResourceHandler resourceHandler : resourceHandlers) {
      List<PrivilegedOperation> handlerOperations =
          resourceHandler.reacquireContainer(containerId);

      if (handlerOperations != null) {
        allOperations.addAll(handlerOperations);
      }

    }
    return allOperations;
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    List<PrivilegedOperation> allOperations = new
        ArrayList<PrivilegedOperation>();

    for (ResourceHandler resourceHandler : resourceHandlers) {
      List<PrivilegedOperation> handlerOperations =
          resourceHandler.postComplete(containerId);

      if (handlerOperations != null) {
        allOperations.addAll(handlerOperations);
      }

    }
    return allOperations;
  }

  @Override
  public List<PrivilegedOperation> teardown()
      throws ResourceHandlerException {
    List<PrivilegedOperation> allOperations = new
        ArrayList<PrivilegedOperation>();

    for (ResourceHandler resourceHandler : resourceHandlers) {
      List<PrivilegedOperation> handlerOperations =
          resourceHandler.teardown();

      if (handlerOperations != null) {
        allOperations.addAll(handlerOperations);
      }

    }
    return allOperations;
  }

  List<ResourceHandler> getResourceHandlerList() {
    return Collections.unmodifiableList(resourceHandlers);
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/ResourceHandlerException.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/ResourceHandlerException.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.exceptions.YarnException;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ResourceHandlerException extends YarnException {
  private static final long serialVersionUID = 1L;

  public ResourceHandlerException() {
    super();
  }

  public ResourceHandlerException(String message) {
    super(message);
  }

  public ResourceHandlerException(Throwable cause) {
    super(cause);
  }

  public ResourceHandlerException(String message, Throwable cause) {
    super(message, cause);
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/privileged/TestPrivilegedOperationExecutor.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/privileged/TestPrivilegedOperationExecutor.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestPrivilegedOperationExecutor {
  private static final Log LOG = LogFactory
      .getLog(TestPrivilegedOperationExecutor.class);
  private String localDataDir;
  private String customExecutorPath;
  private Configuration nullConf = null;
  private Configuration emptyConf;
  private Configuration confWithExecutorPath;

  private String cGroupTasksNone;
  private String cGroupTasksInvalid;
  private String cGroupTasks1;
  private String cGroupTasks2;
  private String cGroupTasks3;
  private PrivilegedOperation opDisallowed;
  private PrivilegedOperation opTasksNone;
  private PrivilegedOperation opTasksInvalid;
  private PrivilegedOperation opTasks1;
  private PrivilegedOperation opTasks2;
  private PrivilegedOperation opTasks3;

  @Before
  public void setup() {
    localDataDir = System.getProperty("test.build.data");
    customExecutorPath = localDataDir + "/bin/container-executor";
    emptyConf = new YarnConfiguration();
    confWithExecutorPath = new YarnConfiguration();
    confWithExecutorPath.set(YarnConfiguration
        .NM_LINUX_CONTAINER_EXECUTOR_PATH, customExecutorPath);

    cGroupTasksNone = "none";
    cGroupTasksInvalid = "invalid_string";
    cGroupTasks1 = "cpu/hadoop_yarn/container_01/tasks";
    cGroupTasks2 = "net_cls/hadoop_yarn/container_01/tasks";
    cGroupTasks3 = "blkio/hadoop_yarn/container_01/tasks";
    opDisallowed = new PrivilegedOperation
        (PrivilegedOperation.OperationType.DELETE_AS_USER, (String) null);
    opTasksNone = new PrivilegedOperation
        (PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
            PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupTasksNone);
    opTasksInvalid = new PrivilegedOperation
        (PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
            cGroupTasksInvalid);
    opTasks1 = new PrivilegedOperation
        (PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
            PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupTasks1);
    opTasks2 = new PrivilegedOperation
        (PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
            PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupTasks2);
    opTasks3 = new PrivilegedOperation
        (PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
            PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupTasks3);
  }

  @Test
  public void testExecutorPath() {
    String containerExePath = PrivilegedOperationExecutor
        .getContainerExecutorExecutablePath(nullConf);

    String yarnHomeEnvVar = System.getenv("HADOOP_YARN_HOME");
    String yarnHome = yarnHomeEnvVar != null ? yarnHomeEnvVar
        : new File("").getAbsolutePath();
    String expectedPath = yarnHome + "/bin/container-executor";

    Assert.assertEquals(expectedPath, containerExePath);

    containerExePath = PrivilegedOperationExecutor
        .getContainerExecutorExecutablePath(emptyConf);
    Assert.assertEquals(expectedPath, containerExePath);

    expectedPath = customExecutorPath;
    containerExePath = PrivilegedOperationExecutor
        .getContainerExecutorExecutablePath(confWithExecutorPath);
    Assert.assertEquals(expectedPath, containerExePath);
  }

  @Test
  public void testExecutionCommand() {
    PrivilegedOperationExecutor exec = PrivilegedOperationExecutor
        .getInstance(confWithExecutorPath);
    PrivilegedOperation op = new PrivilegedOperation(PrivilegedOperation
        .OperationType.LAUNCH_CONTAINER, (String) null);
    String[] cmdArray = exec.getPrivilegedOperationExecutionCommand(null, op);

    Assert.assertEquals(2, cmdArray.length);
    Assert.assertEquals(customExecutorPath, cmdArray[0]);
    Assert.assertEquals(op.getOperationType().getOption(), cmdArray[1]);

    String[] additionalArgs = { "test_user", "yarn", "1", "app_01",
        "container_01", "workdir", "launch_script.sh", "tokens", "pidfile",
        "nm-local-dirs", "nm-log-dirs", "resource-spec" };

    op.appendArgs(additionalArgs);
    cmdArray = exec.getPrivilegedOperationExecutionCommand(null, op);


    Assert.assertEquals(2 + additionalArgs.length, cmdArray.length);
    Assert.assertEquals(customExecutorPath, cmdArray[0]);
    Assert.assertEquals(op.getOperationType().getOption(), cmdArray[1]);

    for (int i = 0; i < additionalArgs.length; ++i) {
      Assert.assertEquals(additionalArgs[i], cmdArray[2 + i]);
    }

    List<String> prefixCommands = Arrays.asList("nice", "-10");
    cmdArray = exec.getPrivilegedOperationExecutionCommand(prefixCommands, op);
    int prefixLength = prefixCommands.size();
    Assert.assertEquals(prefixLength + 2 + additionalArgs.length,
        cmdArray.length);

    for (int i = 0; i < prefixLength; ++i) {
      Assert.assertEquals(prefixCommands.get(i), cmdArray[i]);
    }

    Assert.assertEquals(customExecutorPath, cmdArray[prefixLength]);
    Assert.assertEquals(op.getOperationType().getOption(),
        cmdArray[prefixLength + 1]);

    for (int i = 0; i < additionalArgs.length; ++i) {
      Assert.assertEquals(additionalArgs[i], cmdArray[prefixLength + 2 + i]);
    }
  }

  @Test
  public void testSquashCGroupOperationsWithInvalidOperations() {
    List<PrivilegedOperation> ops = new ArrayList<>();

    ops.add(opTasksNone);
    ops.add(opDisallowed);

    try {
      PrivilegedOperationExecutor.squashCGroupOperations(ops);
      Assert.fail("Expected squash operation to fail with an exception!");
    } catch (PrivilegedOperationException e) {
      LOG.info("Caught expected exception : " + e);
    }

    ops.clear();
    ops.add(opTasksNone);
    ops.add(opTasksInvalid);

    try {
      PrivilegedOperationExecutor.squashCGroupOperations(ops);
      Assert.fail("Expected squash operation to fail with an exception!");
    } catch (PrivilegedOperationException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testSquashCGroupOperationsWithValidOperations() {
    List<PrivilegedOperation> ops = new ArrayList<>();
    ops.clear();
    ops.add(opTasks1);
    ops.add(opTasksNone);
    ops.add(opTasks2);
    ops.add(opTasks3);

    try {
      PrivilegedOperation op = PrivilegedOperationExecutor
          .squashCGroupOperations(ops);
      String expected = new StringBuffer
          (PrivilegedOperation.CGROUP_ARG_PREFIX)
          .append(cGroupTasks1).append(',')
          .append(cGroupTasks2).append(',')
          .append(cGroupTasks3).toString();

      Assert.assertEquals(1, op.getArguments().size());
      Assert.assertEquals(expected, op.getArguments().get(0));
    } catch (PrivilegedOperationException e) {
      LOG.info("Caught unexpected exception : " + e);
      Assert.fail("Caught unexpected exception: " + e);
    }
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/TestCGroupsHandlerImpl.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/TestCGroupsHandlerImpl.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TestCGroupsHandlerImpl {
  private static final Log LOG =
      LogFactory.getLog(TestCGroupsHandlerImpl.class);

  private PrivilegedOperationExecutor privilegedOperationExecutorMock;
  private Configuration conf;
  private String tmpPath;
  private String hierarchy;
  private CGroupsHandler.CGroupController controller;
  private String controllerPath;

  @Before
  public void setup() {
    privilegedOperationExecutorMock = mock(PrivilegedOperationExecutor.class);
    conf = new YarnConfiguration();
    tmpPath = System.getProperty("test.build.data") + "/cgroups";
    hierarchy = "test-hadoop-yarn";

    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY, hierarchy);
    conf.setBoolean(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT, true);
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, tmpPath);
    controller = CGroupsHandler.CGroupController.NET_CLS;
    controllerPath = new StringBuffer(tmpPath).append('/')
        .append(controller.getName()).append('/').append(hierarchy).toString();
  }

  @Test
  public void testMountController() {
    CGroupsHandler cGroupsHandler = null;
    verifyZeroInteractions(privilegedOperationExecutorMock);

    try {
      cGroupsHandler = new CGroupsHandlerImpl(conf,
          privilegedOperationExecutorMock);
      PrivilegedOperation expectedOp = new PrivilegedOperation
          (PrivilegedOperation.OperationType.MOUNT_CGROUPS, (String) null);
      StringBuffer controllerKV = new StringBuffer(controller.getName())
          .append('=').append(tmpPath).append('/').append(controller.getName());
      expectedOp.appendArgs(hierarchy, controllerKV.toString());

      cGroupsHandler.mountCGroupController(controller);
      try {
        ArgumentCaptor<PrivilegedOperation> opCaptor = ArgumentCaptor.forClass
            (PrivilegedOperation.class);
        verify(privilegedOperationExecutorMock)
            .executePrivilegedOperation(opCaptor.capture(), eq(false));

        Assert.assertEquals(expectedOp, opCaptor.getValue());
        verifyNoMoreInteractions(privilegedOperationExecutorMock);

        cGroupsHandler.mountCGroupController(controller);
        verifyNoMoreInteractions(privilegedOperationExecutorMock);
      } catch (PrivilegedOperationException e) {
        LOG.error("Caught exception: " + e);
        Assert.assertTrue("Unexpected PrivilegedOperationException from mock!",
            false);
      }
    } catch (ResourceHandlerException e) {
      LOG.error("Caught exception: " + e);
      Assert.assertTrue("Unexpected ResourceHandler Exception!", false);
    }
  }

  @Test
  public void testCGroupPaths() {
    verifyZeroInteractions(privilegedOperationExecutorMock);
    CGroupsHandler cGroupsHandler = null;
    try {
      cGroupsHandler = new CGroupsHandlerImpl(conf,
          privilegedOperationExecutorMock);
      cGroupsHandler.mountCGroupController(controller);
    } catch (ResourceHandlerException e) {
      LOG.error("Caught exception: " + e);
      Assert.assertTrue(
          "Unexpected ResourceHandlerException when mounting controller!",
          false);
    }

    String testCGroup = "container_01";
    String expectedPath = new StringBuffer(controllerPath).append('/')
        .append(testCGroup).toString();
    String path = cGroupsHandler.getPathForCGroup(controller, testCGroup);
    Assert.assertEquals(expectedPath, path);

    String expectedPathTasks = new StringBuffer(expectedPath).append('/')
        .append(CGroupsHandler.CGROUP_FILE_TASKS).toString();
    path = cGroupsHandler.getPathForCGroupTasks(controller, testCGroup);
    Assert.assertEquals(expectedPathTasks, path);

    String param = CGroupsHandler.CGROUP_PARAM_CLASSID;
    String expectedPathParam = new StringBuffer(expectedPath).append('/')
        .append(controller.getName()).append('.').append(param).toString();
    path = cGroupsHandler.getPathForCGroupParam(controller, testCGroup, param);
    Assert.assertEquals(expectedPathParam, path);
  }

  @Test
  public void testCGroupOperations() {
    verifyZeroInteractions(privilegedOperationExecutorMock);
    CGroupsHandler cGroupsHandler = null;

    try {
      cGroupsHandler = new CGroupsHandlerImpl(conf,
          privilegedOperationExecutorMock);
      cGroupsHandler.mountCGroupController(controller);
    } catch (ResourceHandlerException e) {
      LOG.error("Caught exception: " + e);
      Assert.assertTrue(
          "Unexpected ResourceHandlerException when mounting controller!",
          false);
    }
    new File(controllerPath).mkdirs();

    String testCGroup = "container_01";
    String expectedPath = new StringBuffer(controllerPath).append('/')
        .append(testCGroup).toString();
    try {
      String path = cGroupsHandler.createCGroup(controller, testCGroup);

      Assert.assertTrue(new File(expectedPath).exists());
      Assert.assertEquals(expectedPath, path);

      String param = "test_param";
      String paramValue = "test_param_value";

      cGroupsHandler
          .updateCGroupParam(controller, testCGroup, param, paramValue);
      String paramPath = new StringBuffer(expectedPath).append('/')
          .append(controller.getName()).append('.').append(param).toString();
      File paramFile = new File(paramPath);

      Assert.assertTrue(paramFile.exists());
      try {
        Assert.assertEquals(paramValue, new String(Files.readAllBytes
            (paramFile
                .toPath())));
      } catch (IOException e) {
        LOG.error("Caught exception: " + e);
        Assert.assertTrue("Unexpected IOException trying to read cgroup param!",
            false);
      }

      Assert.assertEquals(paramValue, cGroupsHandler.getCGroupParam
          (controller, testCGroup, param));

    } catch (ResourceHandlerException e) {
      LOG.error("Caught exception: " + e);
      Assert.assertTrue(
          "Unexpected ResourceHandlerException during cgroup operations!",
          false);
    }
  }

  @After
  public void teardown() {
    FileUtil.fullyDelete(new File(tmpPath));
  }
}
\No newline at end of file

