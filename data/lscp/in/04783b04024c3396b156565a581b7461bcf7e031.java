hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
  public static final int DEFAULT_NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT =
      100;


  public static final String NM_NETWORK_RESOURCE_PREFIX = NM_PREFIX + "resource.network.";

  @Private
  public static final String NM_NETWORK_RESOURCE_ENABLED =
      NM_NETWORK_RESOURCE_PREFIX + "enabled";
  @Private
  public static final boolean DEFAULT_NM_NETWORK_RESOURCE_ENABLED = false;

  @Private
  public static final String NM_NETWORK_RESOURCE_INTERFACE =
      NM_NETWORK_RESOURCE_PREFIX + "interface";
  @Private
  public static final String DEFAULT_NM_NETWORK_RESOURCE_INTERFACE = "eth0";

  @Private
  public static final String NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_MBIT =
      NM_NETWORK_RESOURCE_PREFIX + "outbound-bandwidth-mbit";
  @Private
  public static final int DEFAULT_NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_MBIT = 1000;

  @Private
  public static final String NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_YARN_MBIT =
      NM_NETWORK_RESOURCE_PREFIX + "outbound-bandwidth-yarn-mbit";

  public static final String NM_WEBAPP_ADDRESS = NM_PREFIX + "webapp.address";
  public static final int DEFAULT_NM_WEBAPP_PORT = 8042;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/LinuxContainerExecutor.java
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler;
import org.apache.hadoop.yarn.server.nodemanager.util.LCEResourcesHandler;
import org.apache.hadoop.yarn.util.ConverterUtils;
  private boolean containerSchedPriorityIsSet = false;
  private int containerSchedPriorityAdjustment = 0;
  private boolean containerLimitUsers;
  private ResourceHandler resourceHandlerChain;

  @Override
  public void setConf(Configuration conf) {
          + " (error=" + exitCode + ")", e);
    }

    try {
      Configuration conf = super.getConf();

      resourceHandlerChain = ResourceHandlerModule
          .getConfiguredResourceHandlerChain(conf);
      if (resourceHandlerChain != null) {
        resourceHandlerChain.bootstrap(conf);
      }
    } catch (ResourceHandlerException e) {
      LOG.error("Failed to bootstrap configured resource subsystems! ", e);
      throw new IOException("Failed to bootstrap configured resource subsystems!");
    }

    resourcesHandler.init(this);
  }
  
            container.getResource());
    String resourcesOptions = resourcesHandler.getResourcesOption(
            containerId);
    String tcCommandFile = null;

    try {
      if (resourceHandlerChain != null) {
        List<PrivilegedOperation> ops = resourceHandlerChain
            .preStart(container);

        if (ops != null) {
          List<PrivilegedOperation> resourceOps = new ArrayList<>();

          resourceOps.add(new PrivilegedOperation
              (PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
                  resourcesOptions));

          for (PrivilegedOperation op : ops) {
            switch (op.getOperationType()) {
              case ADD_PID_TO_CGROUP:
                resourceOps.add(op);
                break;
              case TC_MODIFY_STATE:
                tcCommandFile = op.getArguments().get(0);
                break;
              default:
                LOG.warn("PrivilegedOperation type unsupported in launch: "
                    + op.getOperationType());
            }
          }

          if (resourceOps.size() > 1) {
            try {
              PrivilegedOperation operation = PrivilegedOperationExecutor
                  .squashCGroupOperations(resourceOps);
              resourcesOptions = operation.getArguments().get(0);
            } catch (PrivilegedOperationException e) {
              LOG.error("Failed to squash cgroup operations!", e);
              throw new ResourceHandlerException("Failed to squash cgroup operations!");
            }
          }
        }
      }
    } catch (ResourceHandlerException e) {
      LOG.error("ResourceHandlerChain.preStart() failed!", e);
      throw new IOException("ResourceHandlerChain.preStart() failed!");
    }

    ShellCommandExecutor shExec = null;

            StringUtils.join(",", localDirs),
            StringUtils.join(",", logDirs),
            resourcesOptions));

        if (tcCommandFile != null) {
            command.add(tcCommandFile);
        }

        String[] commandArray = command.toArray(new String[command.size()]);
        shExec = new ShellCommandExecutor(commandArray, null, // NM's cwd
            container.getLaunchContext().getEnvironment()); // sanitized env
      return exitCode;
    } finally {
      resourcesHandler.postExecute(containerId);

      try {
        if (resourceHandlerChain != null) {
          resourceHandlerChain.postComplete(containerId);
        }
      } catch (ResourceHandlerException e) {
        LOG.warn("ResourceHandlerChain.postComplete failed for " +
            "containerId: " + containerId + ". Exception: " + e);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Output from LinuxContainerExecutor's launchContainer follows:");
  public int reacquireContainer(String user, ContainerId containerId)
      throws IOException, InterruptedException {
    try {
      if (resourceHandlerChain != null) {
        try {
          resourceHandlerChain.reacquireContainer(containerId);
        } catch (ResourceHandlerException e) {
          LOG.warn("ResourceHandlerChain.reacquireContainer failed for " +
              "containerId: " + containerId + " Exception: " + e);
        }
      }

      return super.reacquireContainer(user, containerId);
    } finally {
      resourcesHandler.postExecute(containerId);
      if (resourceHandlerChain != null) {
        try {
          resourceHandlerChain.postComplete(containerId);
        } catch (ResourceHandlerException e) {
          LOG.warn("ResourceHandlerChain.postComplete failed for " +
              "containerId: " + containerId + " Exception: " + e);
        }
      }
    }
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/OutboundBandwidthResourceHandler.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/OutboundBandwidthResourceHandler.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface OutboundBandwidthResourceHandler extends ResourceHandler {
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/ResourceHandlerModule.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/ResourceHandlerModule.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;

import java.util.ArrayList;
import java.util.List;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ResourceHandlerModule {
  private volatile static ResourceHandlerChain resourceHandlerChain;

  private volatile static TrafficControlBandwidthHandlerImpl
      trafficControlBandwidthHandler;
  private volatile static CGroupsHandler cGroupsHandler;

  public static CGroupsHandler getCGroupsHandler(Configuration conf)
      throws ResourceHandlerException {
    if (cGroupsHandler == null) {
      synchronized (CGroupsHandler.class) {
        if (cGroupsHandler == null) {
          cGroupsHandler = new CGroupsHandlerImpl(conf,
              PrivilegedOperationExecutor.getInstance(conf));
        }
      }
    }

    return cGroupsHandler;
  }

  private static TrafficControlBandwidthHandlerImpl
  getTrafficControlBandwidthHandler(Configuration conf)
      throws ResourceHandlerException {
    if (conf.getBoolean(YarnConfiguration.NM_NETWORK_RESOURCE_ENABLED,
        YarnConfiguration.DEFAULT_NM_NETWORK_RESOURCE_ENABLED)) {
      if (trafficControlBandwidthHandler == null) {
        synchronized (OutboundBandwidthResourceHandler.class) {
          if (trafficControlBandwidthHandler == null) {
            trafficControlBandwidthHandler = new
                TrafficControlBandwidthHandlerImpl(PrivilegedOperationExecutor
                .getInstance(conf), getCGroupsHandler(conf),
                new TrafficController(conf, PrivilegedOperationExecutor
                    .getInstance(conf)));
          }
        }
      }

      return trafficControlBandwidthHandler;
    } else {
      return null;
    }
  }

  public static OutboundBandwidthResourceHandler
  getOutboundBandwidthResourceHandler(Configuration conf)
      throws ResourceHandlerException {
    return getTrafficControlBandwidthHandler(conf);
  }

  private static void addHandlerIfNotNull(List<ResourceHandler> handlerList,
      ResourceHandler handler) {
    if (handler != null) {
      handlerList.add(handler);
    }
  }

  private static void initializeConfiguredResourceHandlerChain(
      Configuration conf) throws ResourceHandlerException {
    ArrayList<ResourceHandler> handlerList = new ArrayList<>();

    addHandlerIfNotNull(handlerList, getOutboundBandwidthResourceHandler(conf));
    resourceHandlerChain = new ResourceHandlerChain(handlerList);
  }

  public static ResourceHandlerChain getConfiguredResourceHandlerChain
      (Configuration conf) throws ResourceHandlerException {
    if (resourceHandlerChain == null) {
      synchronized (ResourceHandlerModule.class) {
        if (resourceHandlerChain == null) {
          initializeConfiguredResourceHandlerChain(conf);
        }
      }
    }

    if (resourceHandlerChain.getResourceHandlerList().size() != 0) {
      return resourceHandlerChain;
    } else {
      return null;
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/TrafficControlBandwidthHandlerImpl.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/TrafficControlBandwidthHandlerImpl.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.util.SystemClock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TrafficControlBandwidthHandlerImpl
    implements OutboundBandwidthResourceHandler {

  private static final Log LOG = LogFactory
      .getLog(TrafficControlBandwidthHandlerImpl.class);
  private static final int MAX_CONTAINER_COUNT = 50;

  private final PrivilegedOperationExecutor privilegedOperationExecutor;
  private final CGroupsHandler cGroupsHandler;
  private final TrafficController trafficController;
  private final ConcurrentHashMap<ContainerId, Integer> containerIdClassIdMap;

  private Configuration conf;
  private String device;
  private boolean strictMode;
  private int containerBandwidthMbit;
  private int rootBandwidthMbit;
  private int yarnBandwidthMbit;

  public TrafficControlBandwidthHandlerImpl(PrivilegedOperationExecutor
      privilegedOperationExecutor, CGroupsHandler cGroupsHandler,
      TrafficController trafficController) {
    this.privilegedOperationExecutor = privilegedOperationExecutor;
    this.cGroupsHandler = cGroupsHandler;
    this.trafficController = trafficController;
    this.containerIdClassIdMap = new ConcurrentHashMap<>();
  }


  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    conf = configuration;
    cGroupsHandler
        .mountCGroupController(CGroupsHandler.CGroupController.NET_CLS);
    device = conf.get(YarnConfiguration.NM_NETWORK_RESOURCE_INTERFACE,
        YarnConfiguration.DEFAULT_NM_NETWORK_RESOURCE_INTERFACE);
    strictMode = configuration.getBoolean(YarnConfiguration
        .NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE, YarnConfiguration
        .DEFAULT_NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE);
    rootBandwidthMbit = conf.getInt(YarnConfiguration
        .NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_MBIT, YarnConfiguration
        .DEFAULT_NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_MBIT);
    yarnBandwidthMbit = conf.getInt(YarnConfiguration
        .NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_YARN_MBIT, rootBandwidthMbit);
    containerBandwidthMbit = (int) Math.ceil((double) yarnBandwidthMbit /
        MAX_CONTAINER_COUNT);

    StringBuffer logLine = new StringBuffer("strict mode is set to :")
        .append(strictMode).append(System.lineSeparator());

    if (strictMode) {
      logLine.append("container bandwidth will be capped to soft limit.")
          .append(System.lineSeparator());
    } else {
      logLine.append(
          "containers will be allowed to use spare YARN bandwidth.")
          .append(System.lineSeparator());
    }

    logLine
        .append("containerBandwidthMbit soft limit (in mbit/sec) is set to : ")
        .append(containerBandwidthMbit);

    LOG.info(logLine);
    trafficController.bootstrap(device, rootBandwidthMbit, yarnBandwidthMbit);

    return null;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String containerIdStr = container.getContainerId().toString();
    int classId = trafficController.getNextClassId();
    String classIdStr = trafficController.getStringForNetClsClassId(classId);

    cGroupsHandler.createCGroup(CGroupsHandler.CGroupController
            .NET_CLS,
        containerIdStr);
    cGroupsHandler.updateCGroupParam(CGroupsHandler.CGroupController.NET_CLS,
        containerIdStr, CGroupsHandler.CGROUP_PARAM_CLASSID, classIdStr);
    containerIdClassIdMap.put(container.getContainerId(), classId);

    String tasksFile = cGroupsHandler.getPathForCGroupTasks(
        CGroupsHandler.CGroupController.NET_CLS, containerIdStr);
    String opArg = new StringBuffer(PrivilegedOperation.CGROUP_ARG_PREFIX)
        .append(tasksFile).toString();
    List<PrivilegedOperation> ops = new ArrayList<>();

    ops.add(new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP, opArg));

    TrafficController.BatchBuilder builder = trafficController.new
        BatchBuilder(PrivilegedOperation.OperationType.TC_MODIFY_STATE);

    builder.addContainerClass(classId, containerBandwidthMbit, strictMode);
    ops.add(builder.commitBatchToTempFile());

    return ops;
  }


  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    String containerIdStr = containerId.toString();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Attempting to reacquire classId for container: " +
          containerIdStr);
    }

    String classIdStrFromFile = cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.NET_CLS, containerIdStr,
        CGroupsHandler.CGROUP_PARAM_CLASSID);
    int classId = trafficController
        .getClassIdFromFileContents(classIdStrFromFile);

    LOG.info("Reacquired containerId -> classId mapping: " + containerIdStr
        + " -> " + classId);
    containerIdClassIdMap.put(containerId, classId);

    return null;
  }

  public Map<ContainerId, Integer> getBytesSentPerContainer()
      throws ResourceHandlerException {
    Map<Integer, Integer> classIdStats = trafficController.readStats();
    Map<ContainerId, Integer> containerIdStats = new HashMap<>();

    for (Map.Entry<ContainerId, Integer> entry : containerIdClassIdMap
        .entrySet()) {
      ContainerId containerId = entry.getKey();
      Integer classId = entry.getValue();
      Integer bytesSent = classIdStats.get(classId);

      if (bytesSent == null) {
        LOG.warn("No bytes sent metric found for container: " + containerId +
            " with classId: " + classId);
        continue;
      }
      containerIdStats.put(containerId, bytesSent);
    }

    return containerIdStats;
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    LOG.info("postComplete for container: " + containerId.toString());
    cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.NET_CLS,
        containerId.toString());

    Integer classId = containerIdClassIdMap.get(containerId);

    if (classId != null) {
      PrivilegedOperation op = trafficController.new
          BatchBuilder(PrivilegedOperation.OperationType.TC_MODIFY_STATE)
          .deleteContainerClass(classId).commitBatchToTempFile();

      try {
        privilegedOperationExecutor.executePrivilegedOperation(op, false);
        trafficController.releaseClassId(classId);
      } catch (PrivilegedOperationException e) {
        LOG.warn("Failed to delete tc rule for classId: " + classId);
        throw new ResourceHandlerException(
            "Failed to delete tc rule for classId:" + classId);
      }
    } else {
      LOG.warn("Not cleaning up tc rules. classId unknown for container: " +
          containerId.toString());
    }

    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown()
      throws ResourceHandlerException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("teardown(): Nothing to do");
    }

    return null;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/TrafficController.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/TrafficController.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;

import java.io.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@InterfaceAudience.Private
@InterfaceStability.Unstable class TrafficController {
  private static final Log LOG = LogFactory.getLog(TrafficController.class);
  private static final int ROOT_QDISC_HANDLE = 42;
  private static final int ZERO_CLASS_ID = 0;
  private static final int ROOT_CLASS_ID = 1;
  private static final int DEFAULT_CLASS_ID = 2;
  private static final int YARN_ROOT_CLASS_ID = 3;
  private static final int MIN_CONTAINER_CLASS_ID = 4;
  private static final int MAX_CONTAINER_CLASSES = 1024;

  private static final String MBIT_SUFFIX = "mbit";
  private static final String TMP_FILE_PREFIX = "tc.";
  private static final String TMP_FILE_SUFFIX = ".cmds";

  private static final String FORMAT_QDISC_ADD_TO_ROOT_WITH_DEFAULT =
      "qdisc add dev %s root handle %d: htb default %s";
  private static final String FORMAT_FILTER_CGROUP_ADD_TO_PARENT =
      "filter add dev %s parent %d: protocol ip prio 10 handle 1: cgroup";
  private static final String FORMAT_CLASS_ADD_TO_PARENT_WITH_RATES =
      "class add dev %s parent %d:%d classid %d:%d htb rate %s ceil %s";
  private static final String FORMAT_DELETE_CLASS =
      "class del dev %s classid %d:%d";
  private static final String FORMAT_NET_CLS_CLASS_ID = "0x%04d%04d";
  private static final String FORMAT_READ_STATE =
      "qdisc show dev %1$s%n" +
          "filter show dev %1$s%n" +
          "class show dev %1$s";
  private static final String FORMAT_READ_CLASSES = "class show dev %s";
  private static final String FORMAT_WIPE_STATE =
      "qdisc del dev %s parent root";

  private final Configuration conf;
  private final BitSet classIdSet;
  private final PrivilegedOperationExecutor privilegedOperationExecutor;

  private String tmpDirPath;
  private String device;
  private int rootBandwidthMbit;
  private int yarnBandwidthMbit;
  private int defaultClassBandwidthMbit;

  TrafficController(Configuration conf, PrivilegedOperationExecutor exec) {
    this.conf = conf;
    this.classIdSet = new BitSet(MAX_CONTAINER_CLASSES);
    this.privilegedOperationExecutor = exec;
  }

  public void bootstrap(String device, int rootBandwidthMbit, int
      yarnBandwidthMbit)
      throws ResourceHandlerException {
    if (device == null) {
      throw new ResourceHandlerException("device cannot be null!");
    }

    String tmpDirBase = conf.get("hadoop.tmp.dir");
    if (tmpDirBase == null) {
      throw new ResourceHandlerException("hadoop.tmp.dir not set!");
    }
    tmpDirPath = tmpDirBase + "/nm-tc-rules";

    File tmpDir = new File(tmpDirPath);
    if (!(tmpDir.exists() || tmpDir.mkdirs())) {
      LOG.warn("Unable to create directory: " + tmpDirPath);
      throw new ResourceHandlerException("Unable to create directory: " +
          tmpDirPath);
    }

    this.device = device;
    this.rootBandwidthMbit = rootBandwidthMbit;
    this.yarnBandwidthMbit = yarnBandwidthMbit;
    defaultClassBandwidthMbit = (rootBandwidthMbit - yarnBandwidthMbit) <= 0
        ? rootBandwidthMbit : (rootBandwidthMbit - yarnBandwidthMbit);

    boolean recoveryEnabled = conf.getBoolean(YarnConfiguration
        .NM_RECOVERY_ENABLED, YarnConfiguration.DEFAULT_NM_RECOVERY_ENABLED);
    String state = null;

    if (!recoveryEnabled) {
      LOG.info("NM recovery is not enabled. We'll wipe tc state before proceeding.");
    } else {
      state = readState();
      if (checkIfAlreadyBootstrapped(state)) {
        LOG.info("TC configuration is already in place. Not wiping state.");

        reacquireContainerClasses(state);
        return;
      } else {
        LOG.info("TC configuration is incomplete. Wiping tc state before proceeding");
      }
    }

    wipeState(); //start over in case preview bootstrap was incomplete
    initializeState();
  }

  private void initializeState() throws ResourceHandlerException {
    LOG.info("Initializing tc state.");

    BatchBuilder builder = new BatchBuilder(PrivilegedOperation.
        OperationType.TC_MODIFY_STATE)
        .addRootQDisc()
        .addCGroupFilter()
        .addClassToRootQDisc(rootBandwidthMbit)
        .addDefaultClass(defaultClassBandwidthMbit, rootBandwidthMbit)
        .addYARNRootClass(yarnBandwidthMbit, yarnBandwidthMbit);
    PrivilegedOperation op = builder.commitBatchToTempFile();

    try {
      privilegedOperationExecutor.executePrivilegedOperation(op, false);
    } catch (PrivilegedOperationException e) {
      LOG.warn("Failed to bootstrap outbound bandwidth configuration");

      throw new ResourceHandlerException(
          "Failed to bootstrap outbound bandwidth configuration", e);
    }
  }

  private boolean checkIfAlreadyBootstrapped(String state)
      throws ResourceHandlerException {
    List<String> regexes = new ArrayList<>();

    regexes.add(String.format("^qdisc htb %d: root(.)*$",
        ROOT_QDISC_HANDLE));
    regexes.add(String.format("^filter parent %d: protocol ip " +
        "(.)*cgroup(.)*$", ROOT_QDISC_HANDLE));
    regexes.add(String.format("^class htb %d:%d root(.)*$",
        ROOT_QDISC_HANDLE, ROOT_CLASS_ID));
    regexes.add(String.format("^class htb %d:%d parent %d:%d(.)*$",
        ROOT_QDISC_HANDLE, DEFAULT_CLASS_ID, ROOT_QDISC_HANDLE, ROOT_CLASS_ID));
    regexes.add(String.format("^class htb %d:%d parent %d:%d(.)*$",
        ROOT_QDISC_HANDLE, YARN_ROOT_CLASS_ID, ROOT_QDISC_HANDLE,
        ROOT_CLASS_ID));

    for (String regex : regexes) {
      Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);

      if (pattern.matcher(state).find()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Matched regex: " + regex);
        }
      } else {
        String logLine = new StringBuffer("Failed to match regex: ")
              .append(regex).append(" Current state: ").append(state).toString();
        LOG.warn(logLine);
        return false;
      }
    }

    LOG.info("Bootstrap check succeeded");

    return true;
  }

  private String readState() throws ResourceHandlerException {

    BatchBuilder builder = new BatchBuilder(PrivilegedOperation.
        OperationType.TC_READ_STATE)
        .readState();
    PrivilegedOperation op = builder.commitBatchToTempFile();

    try {
      String output =
          privilegedOperationExecutor.executePrivilegedOperation(op, true);

      if (LOG.isDebugEnabled()) {
        LOG.debug("TC state: %n" + output);
      }

      return output;
    } catch (PrivilegedOperationException e) {
      LOG.warn("Failed to bootstrap outbound bandwidth rules");
      throw new ResourceHandlerException(
          "Failed to bootstrap outbound bandwidth rules", e);
    }
  }

  private void wipeState() throws ResourceHandlerException {
    BatchBuilder builder = new BatchBuilder(PrivilegedOperation.
        OperationType.TC_MODIFY_STATE)
        .wipeState();
    PrivilegedOperation op = builder.commitBatchToTempFile();

    try {
      LOG.info("Wiping tc state.");
      privilegedOperationExecutor.executePrivilegedOperation(op, false);
    } catch (PrivilegedOperationException e) {
      LOG.warn("Failed to wipe tc state. This could happen if the interface" +
          " is already in its default state. Ignoring.");
    }
  }

  private void reacquireContainerClasses(String state) {
    String tcClassesStr = state.substring(state.indexOf("class"));
    String[] tcClasses = Pattern.compile("$", Pattern.MULTILINE)
        .split(tcClassesStr);
    Pattern tcClassPattern = Pattern.compile(String.format(
        "class htb %d:(\\d+) .*", ROOT_QDISC_HANDLE));

    synchronized (classIdSet) {
      for (String tcClassSplit : tcClasses) {
        String tcClass = tcClassSplit.trim();

        if (!tcClass.isEmpty()) {
          Matcher classMatcher = tcClassPattern.matcher(tcClass);
          if (classMatcher.matches()) {
            int classId = Integer.parseInt(classMatcher.group(1));
            if (classId >= MIN_CONTAINER_CLASS_ID) {
              classIdSet.set(classId - MIN_CONTAINER_CLASS_ID);
              LOG.info("Reacquired container classid: " + classId);
            }
          } else {
            LOG.warn("Unable to match classid in string:" + tcClass);
          }
        }
      }
    }
  }

  public Map<Integer, Integer> readStats() throws ResourceHandlerException {
    BatchBuilder builder = new BatchBuilder(PrivilegedOperation.
        OperationType.TC_READ_STATS)
        .readClasses();
    PrivilegedOperation op = builder.commitBatchToTempFile();

    try {
      String output =
          privilegedOperationExecutor.executePrivilegedOperation(op, true);

      if (LOG.isDebugEnabled()) {
        LOG.debug("TC stats output:" + output);
      }

      Map<Integer, Integer> classIdBytesStats = parseStatsString(output);

      if (LOG.isDebugEnabled()) {
        LOG.debug("classId -> bytes sent %n" + classIdBytesStats);
      }

      return classIdBytesStats;
    } catch (PrivilegedOperationException e) {
      LOG.warn("Failed to get tc stats");
      throw new ResourceHandlerException("Failed to get tc stats", e);
    }
  }

  private Map<Integer, Integer> parseStatsString(String stats) {

    String[] lines = Pattern.compile("$", Pattern.MULTILINE)
        .split(stats);
    Pattern tcClassPattern = Pattern.compile(String.format(
        "class htb %d:(\\d+) .*", ROOT_QDISC_HANDLE));
    Pattern bytesPattern = Pattern.compile("Sent (\\d+) bytes.*");

    int currentClassId = -1;
    Map<Integer, Integer> containerClassIdStats = new HashMap<>();

    for (String lineSplit : lines) {
      String line = lineSplit.trim();

      if (!line.isEmpty()) {
        Matcher classMatcher = tcClassPattern.matcher(line);
        if (classMatcher.matches()) {
          int classId = Integer.parseInt(classMatcher.group(1));
          if (classId >= MIN_CONTAINER_CLASS_ID) {
            currentClassId = classId;
            continue;
          }
        }

        Matcher bytesMatcher = bytesPattern.matcher(line);
        if (bytesMatcher.matches()) {
          if (currentClassId != -1) {
            int bytes = Integer.parseInt(bytesMatcher.group(1));
            containerClassIdStats.put(currentClassId, bytes);
          } else {
            LOG.warn("Matched a 'bytes sent' line outside of a class stats " +
                  "segment : " + line);
          }
          continue;
        }

      }
    }

    return containerClassIdStats;
  }

  private String getStringForAddRootQDisc() {
    return String.format(FORMAT_QDISC_ADD_TO_ROOT_WITH_DEFAULT, device,
        ROOT_QDISC_HANDLE, DEFAULT_CLASS_ID);
  }

  private String getStringForaAddCGroupFilter() {
    return String.format(FORMAT_FILTER_CGROUP_ADD_TO_PARENT, device,
        ROOT_QDISC_HANDLE);
  }

  public int getNextClassId() throws ResourceHandlerException {
    synchronized (classIdSet) {
      int index = classIdSet.nextClearBit(0);
      if (index >= MAX_CONTAINER_CLASSES) {
        throw new ResourceHandlerException("Reached max container classes: "
            + MAX_CONTAINER_CLASSES);
      }
      classIdSet.set(index);
      return (index + MIN_CONTAINER_CLASS_ID);
    }
  }

  public void releaseClassId(int classId) throws ResourceHandlerException {
    synchronized (classIdSet) {
      int index = classId - MIN_CONTAINER_CLASS_ID;
      if (index < 0 || index >= MAX_CONTAINER_CLASSES) {
        throw new ResourceHandlerException("Invalid incoming classId: "
            + classId);
      }
      classIdSet.clear(index);
    }
  }

  public String getStringForNetClsClassId(int classId) {
    return String.format(FORMAT_NET_CLS_CLASS_ID, ROOT_QDISC_HANDLE, classId);
  }

  public int getClassIdFromFileContents(String input) {
    String classIdStr = String.format("%08x", Integer.parseInt(input));

    if (LOG.isDebugEnabled()) {
      LOG.debug("ClassId hex string : " + classIdStr);
    }

    return Integer.parseInt(classIdStr.substring(4));
  }

  private String getStringForAddClassToRootQDisc(int rateMbit) {
    String rateMbitStr = rateMbit + MBIT_SUFFIX;
    return String.format(FORMAT_CLASS_ADD_TO_PARENT_WITH_RATES, device,
        ROOT_QDISC_HANDLE, ZERO_CLASS_ID, ROOT_QDISC_HANDLE, ROOT_CLASS_ID,
        rateMbitStr, rateMbitStr);
  }

  private String getStringForAddDefaultClass(int rateMbit, int ceilMbit) {
    String rateMbitStr = rateMbit + MBIT_SUFFIX;
    String ceilMbitStr = ceilMbit + MBIT_SUFFIX;
    return String.format(FORMAT_CLASS_ADD_TO_PARENT_WITH_RATES, device,
        ROOT_QDISC_HANDLE, ROOT_CLASS_ID, ROOT_QDISC_HANDLE, DEFAULT_CLASS_ID,
        rateMbitStr, ceilMbitStr);
  }

  private String getStringForAddYARNRootClass(int rateMbit, int ceilMbit) {
    String rateMbitStr = rateMbit + MBIT_SUFFIX;
    String ceilMbitStr = ceilMbit + MBIT_SUFFIX;
    return String.format(FORMAT_CLASS_ADD_TO_PARENT_WITH_RATES, device,
        ROOT_QDISC_HANDLE, ROOT_CLASS_ID, ROOT_QDISC_HANDLE, YARN_ROOT_CLASS_ID,
        rateMbitStr, ceilMbitStr);
  }

  private String getStringForAddContainerClass(int classId, int rateMbit, int
      ceilMbit) {
    String rateMbitStr = rateMbit + MBIT_SUFFIX;
    String ceilMbitStr = ceilMbit + MBIT_SUFFIX;
    return String.format(FORMAT_CLASS_ADD_TO_PARENT_WITH_RATES, device,
        ROOT_QDISC_HANDLE, YARN_ROOT_CLASS_ID, ROOT_QDISC_HANDLE, classId,
        rateMbitStr, ceilMbitStr);
  }

  private String getStringForDeleteContainerClass(int classId) {
    return String.format(FORMAT_DELETE_CLASS, device, ROOT_QDISC_HANDLE,
        classId);
  }

  private String getStringForReadState() {
    return String.format(FORMAT_READ_STATE, device);
  }

  private String getStringForReadClasses() {
    return String.format(FORMAT_READ_CLASSES, device);
  }

  private String getStringForWipeState() {
    return String.format(FORMAT_WIPE_STATE, device);
  }

  public class BatchBuilder {
    final PrivilegedOperation operation;
    final List<String> commands;

    public BatchBuilder(PrivilegedOperation.OperationType opType)
        throws ResourceHandlerException {
      switch (opType) {
      case TC_MODIFY_STATE:
      case TC_READ_STATE:
      case TC_READ_STATS:
        operation = new PrivilegedOperation(opType, (String) null);
        commands = new ArrayList<>();
        break;
      default:
        throw new ResourceHandlerException("Not a tc operation type : " +
            opType);
      }
    }

    private BatchBuilder addRootQDisc() {
      commands.add(getStringForAddRootQDisc());
      return this;
    }

    private BatchBuilder addCGroupFilter() {
      commands.add(getStringForaAddCGroupFilter());
      return this;
    }

    private BatchBuilder addClassToRootQDisc(int rateMbit) {
      commands.add(getStringForAddClassToRootQDisc(rateMbit));
      return this;
    }

    private BatchBuilder addDefaultClass(int rateMbit, int ceilMbit) {
      commands.add(getStringForAddDefaultClass(rateMbit, ceilMbit));
      return this;
    }

    private BatchBuilder addYARNRootClass(int rateMbit, int ceilMbit) {
      commands.add(getStringForAddYARNRootClass(rateMbit, ceilMbit));
      return this;
    }

    public BatchBuilder addContainerClass(int classId, int rateMbit, boolean
        strictMode) {
      int ceilMbit;

      if (strictMode) {
        ceilMbit = rateMbit;
      } else {
        ceilMbit = yarnBandwidthMbit;
      }

      commands.add(getStringForAddContainerClass(classId, rateMbit, ceilMbit));
      return this;
    }

    public BatchBuilder deleteContainerClass(int classId) {
      commands.add(getStringForDeleteContainerClass(classId));
      return this;
    }

    private BatchBuilder readState() {
      commands.add(getStringForReadState());
      return this;
    }


    private BatchBuilder readClasses() {
      commands.add(getStringForReadClasses());
      return this;
    }

    private BatchBuilder wipeState() {
      commands.add(getStringForWipeState());
      return this;
    }

    public PrivilegedOperation commitBatchToTempFile()
        throws ResourceHandlerException {
      try {
        File tcCmds = File.createTempFile(TMP_FILE_PREFIX, TMP_FILE_SUFFIX, new
            File(tmpDirPath));
        Writer writer = new OutputStreamWriter(new FileOutputStream(tcCmds),
            "UTF-8");
        PrintWriter printWriter = new PrintWriter(writer);

        for (String command : commands) {
          printWriter.println(command);
        }

        printWriter.close();
        operation.appendArgs(tcCmds.getAbsolutePath());

        return operation;
      } catch (IOException e) {
        LOG.warn("Failed to create or write to temporary file in dir: " +
            tmpDirPath);
        throw new ResourceHandlerException(
            "Failed to create or write to temporary file in dir: "
                + tmpDirPath);
      }
    }
  } //end BatchBuilder
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/TestResourceHandlerModule.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/TestResourceHandlerModule.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TestResourceHandlerModule {
  private static final Log LOG = LogFactory.
      getLog(TestResourceHandlerModule.class);
  Configuration emptyConf;
  Configuration networkEnabledConf;

  @Before
  public void setup() {
    emptyConf = new YarnConfiguration();
    networkEnabledConf = new YarnConfiguration();

    networkEnabledConf.setBoolean(YarnConfiguration.NM_NETWORK_RESOURCE_ENABLED,
        true);
    networkEnabledConf.setBoolean(YarnConfiguration
        .NM_LINUX_CONTAINER_CGROUPS_MOUNT, true);
  }

  @Test
  public void testOutboundBandwidthHandler() {
    try {
      OutboundBandwidthResourceHandler resourceHandler = ResourceHandlerModule
          .getOutboundBandwidthResourceHandler(emptyConf);
      Assert.assertNull(resourceHandler);

      resourceHandler = ResourceHandlerModule
          .getOutboundBandwidthResourceHandler(networkEnabledConf);
      Assert.assertNotNull(resourceHandler);

      ResourceHandlerChain resourceHandlerChain = ResourceHandlerModule
          .getConfiguredResourceHandlerChain(networkEnabledConf);
      List<ResourceHandler> resourceHandlers = resourceHandlerChain
          .getResourceHandlerList();
      Assert.assertEquals(resourceHandlers.size(), 1);
      Assert.assertTrue(resourceHandlers.get(0) == resourceHandler);
    } catch (ResourceHandlerException e) {
      Assert.fail("Unexpected ResourceHandlerException: " + e);
    }
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/TestTrafficControlBandwidthHandlerImpl.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/TestTrafficControlBandwidthHandlerImpl.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.List;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestTrafficControlBandwidthHandlerImpl {
  private static final Log LOG =
      LogFactory.getLog(TestTrafficControlBandwidthHandlerImpl.class);
  private static final int ROOT_BANDWIDTH_MBIT = 100;
  private static final int YARN_BANDWIDTH_MBIT = 70;
  private static final int TEST_CLASSID = 100;
  private static final String TEST_CLASSID_STR = "42:100";
  private static final String TEST_CONTAINER_ID_STR = "container_01";
  private static final String TEST_TASKS_FILE = "testTasksFile";

  private PrivilegedOperationExecutor privilegedOperationExecutorMock;
  private CGroupsHandler cGroupsHandlerMock;
  private TrafficController trafficControllerMock;
  private Configuration conf;
  private String tmpPath;
  private String device;
  ContainerId containerIdMock;
  Container containerMock;

  @Before
  public void setup() {
    privilegedOperationExecutorMock = mock(PrivilegedOperationExecutor.class);
    cGroupsHandlerMock = mock(CGroupsHandler.class);
    trafficControllerMock = mock(TrafficController.class);
    conf = new YarnConfiguration();
    tmpPath = new StringBuffer(System.getProperty("test.build.data")).append
        ('/').append("hadoop.tmp.dir").toString();
    device = YarnConfiguration.DEFAULT_NM_NETWORK_RESOURCE_INTERFACE;
    containerIdMock = mock(ContainerId.class);
    containerMock = mock(Container.class);
    when(containerIdMock.toString()).thenReturn(TEST_CONTAINER_ID_STR);
    when(containerMock.getContainerId()).thenReturn(containerIdMock);

    conf.setInt(YarnConfiguration
        .NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_MBIT, ROOT_BANDWIDTH_MBIT);
    conf.setInt(YarnConfiguration
        .NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_YARN_MBIT, YARN_BANDWIDTH_MBIT);
    conf.set("hadoop.tmp.dir", tmpPath);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, false);
  }

  @Test
  public void testBootstrap() {
    TrafficControlBandwidthHandlerImpl handlerImpl = new
        TrafficControlBandwidthHandlerImpl(privilegedOperationExecutorMock,
        cGroupsHandlerMock, trafficControllerMock);

    try {
      handlerImpl.bootstrap(conf);
      verify(cGroupsHandlerMock).mountCGroupController(
          eq(CGroupsHandler.CGroupController.NET_CLS));
      verifyNoMoreInteractions(cGroupsHandlerMock);
      verify(trafficControllerMock).bootstrap(eq(device),
          eq(ROOT_BANDWIDTH_MBIT),
          eq(YARN_BANDWIDTH_MBIT));
      verifyNoMoreInteractions(trafficControllerMock);
    } catch (ResourceHandlerException e) {
      LOG.error("Unexpected exception: " + e);
      Assert.fail("Caught unexpected ResourceHandlerException!");
    }
  }

  @Test
  public void testLifeCycle() {
    TrafficController trafficControllerSpy = spy(new TrafficController(conf,
        privilegedOperationExecutorMock));
    TrafficControlBandwidthHandlerImpl handlerImpl = new
        TrafficControlBandwidthHandlerImpl(privilegedOperationExecutorMock,
        cGroupsHandlerMock, trafficControllerSpy);

    try {
      handlerImpl.bootstrap(conf);
      testPreStart(trafficControllerSpy, handlerImpl);
      testPostComplete(trafficControllerSpy, handlerImpl);
    } catch (ResourceHandlerException e) {
      LOG.error("Unexpected exception: " + e);
      Assert.fail("Caught unexpected ResourceHandlerException!");
    }
  }

  private void testPreStart(TrafficController trafficControllerSpy,
      TrafficControlBandwidthHandlerImpl handlerImpl) throws
      ResourceHandlerException {
    reset(privilegedOperationExecutorMock);

    doReturn(TEST_CLASSID).when(trafficControllerSpy).getNextClassId();
    doReturn(TEST_CLASSID_STR).when(trafficControllerSpy)
        .getStringForNetClsClassId(TEST_CLASSID);
    when(cGroupsHandlerMock.getPathForCGroupTasks(CGroupsHandler
        .CGroupController.NET_CLS, TEST_CONTAINER_ID_STR)).thenReturn(
        TEST_TASKS_FILE);

    List<PrivilegedOperation> ops = handlerImpl.preStart(containerMock);

    verify(cGroupsHandlerMock).createCGroup(
        eq(CGroupsHandler.CGroupController.NET_CLS), eq(TEST_CONTAINER_ID_STR));
    verify(cGroupsHandlerMock).updateCGroupParam(
        eq(CGroupsHandler.CGroupController.NET_CLS), eq(TEST_CONTAINER_ID_STR),
        eq(CGroupsHandler.CGROUP_PARAM_CLASSID), eq(TEST_CLASSID_STR));

    Assert.assertEquals(2, ops.size());

    PrivilegedOperation addPidOp = ops.get(0);
    String expectedAddPidOpArg = PrivilegedOperation.CGROUP_ARG_PREFIX +
        TEST_TASKS_FILE;
    List<String> addPidOpArgs = addPidOp.getArguments();

    Assert.assertEquals(PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        addPidOp.getOperationType());
    Assert.assertEquals(1, addPidOpArgs.size());
    Assert.assertEquals(expectedAddPidOpArg, addPidOpArgs.get(0));

    PrivilegedOperation tcModifyOp = ops.get(1);
    List<String> tcModifyOpArgs = tcModifyOp.getArguments();

    Assert.assertEquals(PrivilegedOperation.OperationType.TC_MODIFY_STATE,
        tcModifyOp.getOperationType());
    Assert.assertEquals(1, tcModifyOpArgs.size());
    Assert.assertTrue(new File(tcModifyOpArgs.get(0)).exists());
  }

  private void testPostComplete(TrafficController trafficControllerSpy,
      TrafficControlBandwidthHandlerImpl handlerImpl) throws
      ResourceHandlerException {
    reset(privilegedOperationExecutorMock);

    List<PrivilegedOperation> ops = handlerImpl.postComplete(containerIdMock);

    verify(cGroupsHandlerMock).deleteCGroup(
        eq(CGroupsHandler.CGroupController.NET_CLS), eq(TEST_CONTAINER_ID_STR));

    try {
      ArgumentCaptor<PrivilegedOperation> opCaptor = ArgumentCaptor.forClass
          (PrivilegedOperation.class);

      verify(privilegedOperationExecutorMock)
          .executePrivilegedOperation(opCaptor.capture(), eq(false));

      List<String> args = opCaptor.getValue().getArguments();

      Assert.assertEquals(PrivilegedOperation.OperationType.TC_MODIFY_STATE,
          opCaptor.getValue().getOperationType());
      Assert.assertEquals(1, args.size());
      Assert.assertTrue(new File(args.get(0)).exists());

      verify(trafficControllerSpy).releaseClassId(TEST_CLASSID);
    } catch (PrivilegedOperationException e) {
      LOG.error("Caught exception: " + e);
      Assert.fail("Unexpected PrivilegedOperationException from mock!");
    }

    Assert.assertNull(ops);
  }

  @After
  public void teardown() {
    FileUtil.fullyDelete(new File(tmpPath));
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/TestTrafficController.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/TestTrafficController.java

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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestTrafficController {
  private static final Log LOG = LogFactory.getLog(TestTrafficController.class);
  private static final int ROOT_BANDWIDTH_MBIT = 100;
  private static final int YARN_BANDWIDTH_MBIT = 70;
  private static final int CONTAINER_BANDWIDTH_MBIT = 10;

  private static final String DEVICE = "eth0";
  private static final String WIPE_STATE_CMD = "qdisc del dev eth0 parent root";
  private static final String ADD_ROOT_QDISC_CMD =
      "qdisc add dev eth0 root handle 42: htb default 2";
  private static final String ADD_CGROUP_FILTER_CMD =
      "filter add dev eth0 parent 42: protocol ip prio 10 handle 1: cgroup";
  private static final String ADD_ROOT_CLASS_CMD =
      "class add dev eth0 parent 42:0 classid 42:1 htb rate 100mbit ceil 100mbit";
  private static final String ADD_DEFAULT_CLASS_CMD =
      "class add dev eth0 parent 42:1 classid 42:2 htb rate 30mbit ceil 100mbit";
  private static final String ADD_YARN_CLASS_CMD =
      "class add dev eth0 parent 42:1 classid 42:3 htb rate 70mbit ceil 70mbit";
  private static final String DEFAULT_TC_STATE_EXAMPLE =
      "qdisc pfifo_fast 0: root refcnt 2 bands 3 priomap  1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1";
  private static final String READ_QDISC_CMD = "qdisc show dev eth0";
  private static final String READ_FILTER_CMD = "filter show dev eth0";
  private static final String READ_CLASS_CMD = "class show dev eth0";
  private static final int MIN_CONTAINER_CLASS_ID = 4;
  private static final String FORMAT_CONTAINER_CLASS_STR = "0x0042%04d";
  private static final String FORMAT_ADD_CONTAINER_CLASS_TO_DEVICE =
      "class add dev eth0 parent 42:3 classid 42:%d htb rate 10mbit ceil %dmbit";
  private static final String FORAMT_DELETE_CONTAINER_CLASS_FROM_DEVICE =
      "class del dev eth0 classid 42:%d";

  private static final int TEST_CLASS_ID = 97;
  private static final String TEST_CLASS_ID_DECIMAL_STR = "4325527";

  private Configuration conf;
  private String tmpPath;

  private PrivilegedOperationExecutor privilegedOperationExecutorMock;

  @Before
  public void setup() {
    privilegedOperationExecutorMock = mock(PrivilegedOperationExecutor.class);
    conf = new YarnConfiguration();
    tmpPath = new StringBuffer(System.getProperty("test.build.data")).append
        ('/').append("hadoop.tmp.dir").toString();

    conf.set("hadoop.tmp.dir", tmpPath);
  }

  private void verifyTrafficControlOperation(PrivilegedOperation op,
      PrivilegedOperation.OperationType expectedOpType,
      List<String> expectedTcCmds)
      throws IOException {
    Assert.assertEquals(expectedOpType, op.getOperationType());

    List<String> args = op.getArguments();

    Assert.assertEquals(1, args.size());

    File tcCmdsFile = new File(args.get(0));

    Assert.assertTrue(tcCmdsFile.exists());

    List<String> tcCmds = Files.readAllLines(tcCmdsFile.toPath(),
        Charset.forName("UTF-8"));

    Assert.assertEquals(expectedTcCmds.size(), tcCmds.size());
    for (int i = 0; i < tcCmds.size(); ++i) {
      Assert.assertEquals(expectedTcCmds.get(i), tcCmds.get(i));
    }
  }

  @Test
  public void testBootstrapRecoveryDisabled() {
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, false);

    TrafficController trafficController = new TrafficController(conf,
        privilegedOperationExecutorMock);

    try {
      trafficController
          .bootstrap(DEVICE, ROOT_BANDWIDTH_MBIT, YARN_BANDWIDTH_MBIT);

      ArgumentCaptor<PrivilegedOperation> opCaptor = ArgumentCaptor.forClass
          (PrivilegedOperation.class);

      verify(privilegedOperationExecutorMock, times(2))
          .executePrivilegedOperation(opCaptor.capture(), eq(false));

      List<PrivilegedOperation> ops = opCaptor.getAllValues();

      verifyTrafficControlOperation(ops.get(0),
          PrivilegedOperation.OperationType.TC_MODIFY_STATE,
          Arrays.asList(WIPE_STATE_CMD));

      verifyTrafficControlOperation(ops.get(1),
          PrivilegedOperation.OperationType.TC_MODIFY_STATE,
          Arrays.asList(ADD_ROOT_QDISC_CMD, ADD_CGROUP_FILTER_CMD,
              ADD_ROOT_CLASS_CMD, ADD_DEFAULT_CLASS_CMD, ADD_YARN_CLASS_CMD));
    } catch (ResourceHandlerException | PrivilegedOperationException |
        IOException e) {
      LOG.error("Unexpected exception: " + e);
      Assert.fail("Caught unexpected exception: "
          + e.getClass().getSimpleName());
    }
  }

  @Test
  public void testBootstrapRecoveryEnabled() {
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);

    TrafficController trafficController = new TrafficController(conf,
        privilegedOperationExecutorMock);

    try {
      when(privilegedOperationExecutorMock.executePrivilegedOperation(
          any(PrivilegedOperation.class), eq(true)))
          .thenReturn(DEFAULT_TC_STATE_EXAMPLE);

      trafficController
          .bootstrap(DEVICE, ROOT_BANDWIDTH_MBIT, YARN_BANDWIDTH_MBIT);

      ArgumentCaptor<PrivilegedOperation> readOpCaptor = ArgumentCaptor.forClass
          (PrivilegedOperation.class);

      verify(privilegedOperationExecutorMock, times(1))
          .executePrivilegedOperation(readOpCaptor.capture(), eq(true));
      List<PrivilegedOperation> readOps = readOpCaptor.getAllValues();
      verifyTrafficControlOperation(readOps.get(0),
          PrivilegedOperation.OperationType.TC_READ_STATE,
          Arrays.asList(READ_QDISC_CMD, READ_FILTER_CMD, READ_CLASS_CMD));

      ArgumentCaptor<PrivilegedOperation> writeOpCaptor = ArgumentCaptor
          .forClass(PrivilegedOperation.class);
      verify(privilegedOperationExecutorMock, times(2))
          .executePrivilegedOperation(writeOpCaptor.capture(), eq(false));
      List<PrivilegedOperation> writeOps = writeOpCaptor.getAllValues();
      verifyTrafficControlOperation(writeOps.get(0),
          PrivilegedOperation.OperationType.TC_MODIFY_STATE,
          Arrays.asList(WIPE_STATE_CMD));

      verifyTrafficControlOperation(writeOps.get(1),
          PrivilegedOperation.OperationType.TC_MODIFY_STATE,
          Arrays.asList(ADD_ROOT_QDISC_CMD, ADD_CGROUP_FILTER_CMD,
              ADD_ROOT_CLASS_CMD, ADD_DEFAULT_CLASS_CMD, ADD_YARN_CLASS_CMD));
    } catch (ResourceHandlerException | PrivilegedOperationException |
        IOException e) {
      LOG.error("Unexpected exception: " + e);
      Assert.fail("Caught unexpected exception: "
          + e.getClass().getSimpleName());
    }
  }

  @Test
  public void testInvalidBuilder() {
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, false);

    TrafficController trafficController = new TrafficController(conf,
        privilegedOperationExecutorMock);
    try {
      trafficController
          .bootstrap(DEVICE, ROOT_BANDWIDTH_MBIT, YARN_BANDWIDTH_MBIT);

      try {
        TrafficController.BatchBuilder invalidBuilder = trafficController.
            new BatchBuilder(
            PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP);
        Assert.fail("Invalid builder check failed!");
      } catch (ResourceHandlerException e) {
      }
    } catch (ResourceHandlerException e) {
      LOG.error("Unexpected exception: " + e);
      Assert.fail("Caught unexpected exception: "
          + e.getClass().getSimpleName());
    }
  }

  @Test
  public void testClassIdFileContentParsing() {
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, false);

    TrafficController trafficController = new TrafficController(conf,
        privilegedOperationExecutorMock);

    int parsedClassId = trafficController.getClassIdFromFileContents
        (TEST_CLASS_ID_DECIMAL_STR);

    Assert.assertEquals(TEST_CLASS_ID, parsedClassId);
  }

  @Test
  public void testContainerOperations() {
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, false);

    TrafficController trafficController = new TrafficController(conf,
        privilegedOperationExecutorMock);
    try {
      trafficController
          .bootstrap(DEVICE, ROOT_BANDWIDTH_MBIT, YARN_BANDWIDTH_MBIT);

      int classId = trafficController.getNextClassId();

      Assert.assertTrue(classId >= MIN_CONTAINER_CLASS_ID);
      Assert.assertEquals(String.format(FORMAT_CONTAINER_CLASS_STR, classId),
          trafficController.getStringForNetClsClassId(classId));

      TrafficController.BatchBuilder builder = trafficController.
          new BatchBuilder(PrivilegedOperation.OperationType.TC_MODIFY_STATE)
          .addContainerClass(classId, CONTAINER_BANDWIDTH_MBIT, false);
      PrivilegedOperation addClassOp = builder.commitBatchToTempFile();

      String expectedAddClassCmd = String.format
          (FORMAT_ADD_CONTAINER_CLASS_TO_DEVICE, classId, YARN_BANDWIDTH_MBIT);
      verifyTrafficControlOperation(addClassOp,
          PrivilegedOperation.OperationType.TC_MODIFY_STATE,
          Arrays.asList(expectedAddClassCmd));

      TrafficController.BatchBuilder strictModeBuilder = trafficController.
          new BatchBuilder(PrivilegedOperation.OperationType.TC_MODIFY_STATE)
          .addContainerClass(classId, CONTAINER_BANDWIDTH_MBIT, true);
      PrivilegedOperation addClassStrictModeOp = strictModeBuilder
          .commitBatchToTempFile();

      String expectedAddClassStrictModeCmd = String.format
          (FORMAT_ADD_CONTAINER_CLASS_TO_DEVICE, classId,
              CONTAINER_BANDWIDTH_MBIT);
      verifyTrafficControlOperation(addClassStrictModeOp,
          PrivilegedOperation.OperationType.TC_MODIFY_STATE,
          Arrays.asList(expectedAddClassStrictModeCmd));

      TrafficController.BatchBuilder deleteBuilder = trafficController.new
          BatchBuilder(PrivilegedOperation.OperationType.TC_MODIFY_STATE)
          .deleteContainerClass(classId);
      PrivilegedOperation deleteClassOp = deleteBuilder.commitBatchToTempFile();

      String expectedDeleteClassCmd = String.format
          (FORAMT_DELETE_CONTAINER_CLASS_FROM_DEVICE, classId);
      verifyTrafficControlOperation(deleteClassOp,
          PrivilegedOperation.OperationType.TC_MODIFY_STATE,
          Arrays.asList(expectedDeleteClassCmd));
    } catch (ResourceHandlerException | IOException e) {
      LOG.error("Unexpected exception: " + e);
      Assert.fail("Caught unexpected exception: "
          + e.getClass().getSimpleName());
    }
  }

  @After
  public void teardown() {
    FileUtil.fullyDelete(new File(tmpPath));
  }
}

