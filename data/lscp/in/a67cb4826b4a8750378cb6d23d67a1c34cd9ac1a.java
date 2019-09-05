hadoop-tools/hadoop-gridmix/src/test/java/org/apache/hadoop/mapred/gridmix/DummyResourceCalculatorPlugin.java
    return getConf().getInt(NUM_PROCESSORS, -1);
  }

  @Override
  public int getNumCores() {
    return getNumProcessors();
  }

  @Override
  public long getCpuFrequency() {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
  public static final String YARN_TRACKING_URL_GENERATOR = 
      YARN_PREFIX + "tracking.url.generator";

  public static final String NM_PMEM_MB = NM_PREFIX + "resource.memory-mb";
  public static final int DEFAULT_NM_PMEM_MB = 8 * 1024;

  public static final String NM_SYSTEM_RESERVED_PMEM_MB = NM_PREFIX
      + "resource.system-reserved-memory-mb";

  public static final String NM_PMEM_CHECK_ENABLED = NM_PREFIX
      + "pmem-check-enabled";
  public static final String NM_VCORES = NM_PREFIX + "resource.cpu-vcores";
  public static final int DEFAULT_NM_VCORES = 8;

  public static final String NM_COUNT_LOGICAL_PROCESSORS_AS_CORES = NM_PREFIX
      + "resource.count-logical-processors-as-cores";
  public static final boolean DEFAULT_NM_COUNT_LOGICAL_PROCESSORS_AS_CORES =
      false;

  public static final String NM_PCORES_VCORES_MULTIPLIER = NM_PREFIX
      + "resource.pcores-vcores-multiplier";
  public static final float DEFAULT_NM_PCORES_VCORES_MULTIPLIER = 1.0f;

  public static final String NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT =
      NM_PREFIX + "resource.percentage-physical-cpu-limit";
  public static final int DEFAULT_NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT =
      100;

  public static final String NM_ENABLE_HARDWARE_CAPABILITY_DETECTION =
      NM_PREFIX + "resource.detect-hardware-capabilities";
  public static final boolean DEFAULT_NM_ENABLE_HARDWARE_CAPABILITY_DETECTION =
      false;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/LinuxResourceCalculatorPlugin.java
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
  private static final String INACTIVE_STRING = "Inactive";

  private static final String PROCFS_CPUINFO = "/proc/cpuinfo";
  private static final Pattern PROCESSOR_FORMAT =
      Pattern.compile("^processor[ \t]:[ \t]*([0-9]*)");
  private static final Pattern FREQUENCY_FORMAT =
      Pattern.compile("^cpu MHz[ \t]*:[ \t]*([0-9.]*)");
  private static final Pattern PHYSICAL_ID_FORMAT =
      Pattern.compile("^physical id[ \t]*:[ \t]*([0-9]*)");
  private static final Pattern CORE_ID_FORMAT =
      Pattern.compile("^core id[ \t]*:[ \t]*([0-9]*)");

  private static final String PROCFS_STAT = "/proc/stat";
  private static final Pattern CPU_TIME_FORMAT =
  private String procfsMemFile;
  private String procfsCpuFile;
  private String procfsStatFile;
  private long jiffyLengthInMillis;

  private long ramSize = 0;
  private long swapSize = 0;
  private long ramSizeFree = 0;  // free ram space on the machine (kB)
  private long swapSizeFree = 0; // free swap space on the machine (kB)
  private long inactiveSize = 0; // inactive cache memory (kB)
  private int numProcessors = 0;
  private int numCores = 0;
  private long cpuFrequency = 0L; // CPU frequency on the system (kHz)

  private boolean readMemInfoFile = false;
  private boolean readCpuInfoFile = false;

  long getCurrentTime() {

  }

  private void readProcMemInfoFile() {
    readProcMemInfoFile(false);
  }

  private void readProcMemInfoFile(boolean readAgain) {
    }

    BufferedReader in;
    InputStreamReader fReader;
    try {
      fReader = new InputStreamReader(
          new FileInputStream(procfsMemFile), Charset.forName("UTF-8"));
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      LOG.warn("Couldn't read " + procfsMemFile
          + "; can't determine memory settings");
      return;
    }

    Matcher mat;

    try {
      String str = in.readLine();
  }

  private void readProcCpuInfoFile() {
    if (readCpuInfoFile) {
      return;
    }
    HashSet<String> coreIdSet = new HashSet<>();
    BufferedReader in;
    InputStreamReader fReader;
    try {
      fReader = new InputStreamReader(
          new FileInputStream(procfsCpuFile), Charset.forName("UTF-8"));
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      LOG.warn("Couldn't read " + procfsCpuFile + "; can't determine cpu info");
      return;
    }
    Matcher mat;
    try {
      numProcessors = 0;
      numCores = 1;
      String currentPhysicalId = "";
      String str = in.readLine();
      while (str != null) {
        mat = PROCESSOR_FORMAT.matcher(str);
        if (mat.find()) {
          cpuFrequency = (long)(Double.parseDouble(mat.group(1)) * 1000); // kHz
        }
        mat = PHYSICAL_ID_FORMAT.matcher(str);
        if (mat.find()) {
          currentPhysicalId = str;
        }
        mat = CORE_ID_FORMAT.matcher(str);
        if (mat.find()) {
          coreIdSet.add(currentPhysicalId + " " + str);
          numCores = coreIdSet.size();
        }
        str = in.readLine();
      }
    } catch (IOException io) {
  }

  private void readProcStatFile() {
    BufferedReader in;
    InputStreamReader fReader;
    try {
      fReader = new InputStreamReader(
          new FileInputStream(procfsStatFile), Charset.forName("UTF-8"));
      return;
    }

    Matcher mat;
    try {
      String str = in.readLine();
      while (str != null) {
    return numProcessors;
  }

  @Override
  public int getNumCores() {
    readProcCpuInfoFile();
    return numCores;
  }

  @Override
  public long getCpuFrequency() {
  }

  public static void main(String[] args) {
    LinuxResourceCalculatorPlugin plugin = new LinuxResourceCalculatorPlugin();
    }
    System.out.println("CPU usage % : " + plugin.getCpuUsage());
  }

  @VisibleForTesting
  void setReadCpuInfoFile(boolean readCpuInfoFileValue) {
    this.readCpuInfoFile = readCpuInfoFileValue;
  }

  public long getJiffyLengthInMillis() {
    return this.jiffyLengthInMillis;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/ResourceCalculatorPlugin.java
  public abstract long getAvailablePhysicalMemorySize();

  public abstract int getNumProcessors();

  public abstract int getNumCores();


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/WindowsResourceCalculatorPlugin.java
    return numProcessors;
  }

  @Override
  public int getNumCores() {
    return getNumProcessors();
  }

  @Override
  public long getCpuFrequency() {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/util/TestLinuxResourceCalculatorPlugin.java
hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/ContainerExecutor.java
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReacquisitionContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
            YarnConfiguration.NM_WINDOWS_CONTAINER_CPU_LIMIT_ENABLED,
            YarnConfiguration.DEFAULT_NM_WINDOWS_CONTAINER_CPU_LIMIT_ENABLED)) {
          int containerVCores = resource.getVirtualCores();
          int nodeVCores = NodeManagerHardwareUtils.getVCores(conf);
          int nodeCpuPercentage =
              NodeManagerHardwareUtils.getNodeCpuPercentage(conf);

          float containerCpuPercentage =
              (float) (nodeCpuPercentage * containerVCores) / nodeVCores;

          cpuRate = Math.min(10000, (int) (containerCpuPercentage * 100));
        }
      }
      return new String[] { Shell.WINUTILS, "task", "create", "-m",

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeStatusUpdaterImpl.java
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeLabelsProvider;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

import com.google.common.annotations.VisibleForTesting;

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    int memoryMb = NodeManagerHardwareUtils.getContainerMemoryMB(conf);
    float vMemToPMem =
        conf.getFloat(
            YarnConfiguration.NM_VMEM_PMEM_RATIO, 
            YarnConfiguration.DEFAULT_NM_VMEM_PMEM_RATIO); 
    int virtualMemoryMb = (int)Math.ceil(memoryMb * vMemToPMem);
    
    int virtualCores = NodeManagerHardwareUtils.getVCores(conf);
    LOG.info("Nodemanager resources: memory set to " + memoryMb + "MB.");
    LOG.info("Nodemanager resources: vcores set to " + virtualCores + ".");

    this.totalResource = Resource.newInstance(memoryMb, virtualCores);
    metrics.addResource(totalResource);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/monitor/ContainersMonitorImpl.java
        conf.getLong(YarnConfiguration.NM_CONTAINER_METRICS_PERIOD_MS,
            YarnConfiguration.DEFAULT_NM_CONTAINER_METRICS_PERIOD_MS);

    long configuredPMemForContainers =
        NodeManagerHardwareUtils.getContainerMemoryMB(conf) * 1024 * 1024L;

    long configuredVCoresForContainers =
        NodeManagerHardwareUtils.getVCores(conf);


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/util/CgroupsLCEResourcesHandler.java
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
  Clock clock;

  private float yarnProcessors;
  int nodeVCores;

  public CgroupsLCEResourcesHandler() {
    this.controllerPaths = new HashMap<String, String>();

    initializeControllerPaths();

    nodeVCores = NodeManagerHardwareUtils.getVCores(plugin, conf);

    yarnProcessors = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    int systemProcessors = NodeManagerHardwareUtils.getNodeCPUs(plugin, conf);
    if (systemProcessors != (int) yarnProcessors) {
      LOG.info("YARN containers restricted to " + yarnProcessors + " cores");
      int[] limits = getOverallLimits(yarnProcessors);
      updateCgroup(CONTROLLER_CPU, containerName, "shares",
          String.valueOf(cpuShares));
      if (strictResourceUsageMode) {
        if (nodeVCores != containerVCores) {
          float containerCPU =
              (containerVCores * yarnProcessors) / (float) nodeVCores;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/util/NodeManagerHardwareUtils.java

package org.apache.hadoop.yarn.server.nodemanager.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NodeManagerHardwareUtils {

  private static final Log LOG = LogFactory
      .getLog(NodeManagerHardwareUtils.class);

  public static int getNodeCPUs(Configuration conf) {
    ResourceCalculatorPlugin plugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);
    return NodeManagerHardwareUtils.getNodeCPUs(plugin, conf);
  }

  public static int getNodeCPUs(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    int numProcessors = plugin.getNumProcessors();
    boolean countLogicalCores =
        conf.getBoolean(YarnConfiguration.NM_COUNT_LOGICAL_PROCESSORS_AS_CORES,
          YarnConfiguration.DEFAULT_NM_COUNT_LOGICAL_PROCESSORS_AS_CORES);
    if (!countLogicalCores) {
      numProcessors = plugin.getNumCores();
    }
    return numProcessors;
  }

  public static float getContainersCPUs(Configuration conf) {
    ResourceCalculatorPlugin plugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);
    return NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
  }

  public static float getContainersCPUs(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    int numProcessors = getNodeCPUs(plugin, conf);
    int nodeCpuPercentage = getNodeCpuPercentage(conf);

    return (nodeCpuPercentage * numProcessors) / 100.0f;
    }
    return nodeCpuPercentage;
  }

  public static int getVCores(Configuration conf) {
    ResourceCalculatorPlugin plugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);

    return NodeManagerHardwareUtils.getVCores(plugin, conf);
  }

  public static int getVCores(ResourceCalculatorPlugin plugin,
      Configuration conf) {

    int cores;
    boolean hardwareDetectionEnabled =
        conf.getBoolean(
          YarnConfiguration.NM_ENABLE_HARDWARE_CAPABILITY_DETECTION,
          YarnConfiguration.DEFAULT_NM_ENABLE_HARDWARE_CAPABILITY_DETECTION);

    String message;
    if (!hardwareDetectionEnabled || plugin == null) {
      cores =
          conf.getInt(YarnConfiguration.NM_VCORES,
            YarnConfiguration.DEFAULT_NM_VCORES);
      if (cores == -1) {
        cores = YarnConfiguration.DEFAULT_NM_VCORES;
      }
    } else {
      cores = conf.getInt(YarnConfiguration.NM_VCORES, -1);
      if (cores == -1) {
        float physicalCores =
            NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
        float multiplier =
            conf.getFloat(YarnConfiguration.NM_PCORES_VCORES_MULTIPLIER,
                YarnConfiguration.DEFAULT_NM_PCORES_VCORES_MULTIPLIER);
        if (multiplier > 0) {
          float tmp = physicalCores * multiplier;
          if (tmp > 0 && tmp < 1) {
            cores = 1;
          } else {
            cores = (int) tmp;
          }
        } else {
          message = "Illegal value for "
              + YarnConfiguration.NM_PCORES_VCORES_MULTIPLIER
              + ". Value must be greater than 0.";
          throw new IllegalArgumentException(message);
        }
      }
    }
    if(cores <= 0) {
      message = "Illegal value for " + YarnConfiguration.NM_VCORES
          + ". Value must be greater than 0.";
      throw new IllegalArgumentException(message);
    }

    return cores;
  }

  public static int getContainerMemoryMB(Configuration conf) {
    return NodeManagerHardwareUtils.getContainerMemoryMB(
      ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf), conf);
  }

  public static int getContainerMemoryMB(ResourceCalculatorPlugin plugin,
      Configuration conf) {

    int memoryMb;
    boolean hardwareDetectionEnabled = conf.getBoolean(
          YarnConfiguration.NM_ENABLE_HARDWARE_CAPABILITY_DETECTION,
          YarnConfiguration.DEFAULT_NM_ENABLE_HARDWARE_CAPABILITY_DETECTION);

    if (!hardwareDetectionEnabled || plugin == null) {
      memoryMb = conf.getInt(YarnConfiguration.NM_PMEM_MB,
            YarnConfiguration.DEFAULT_NM_PMEM_MB);
      if (memoryMb == -1) {
        memoryMb = YarnConfiguration.DEFAULT_NM_PMEM_MB;
      }
    } else {
      memoryMb = conf.getInt(YarnConfiguration.NM_PMEM_MB, -1);
      if (memoryMb == -1) {
        int physicalMemoryMB =
            (int) (plugin.getPhysicalMemorySize() / (1024 * 1024));
        int hadoopHeapSizeMB =
            (int) (Runtime.getRuntime().maxMemory() / (1024 * 1024));
        int containerPhysicalMemoryMB =
            (int) (0.8f * (physicalMemoryMB - (2 * hadoopHeapSizeMB)));
        int reservedMemoryMB =
            conf.getInt(YarnConfiguration.NM_SYSTEM_RESERVED_PMEM_MB, -1);
        if (reservedMemoryMB != -1) {
          containerPhysicalMemoryMB = physicalMemoryMB - reservedMemoryMB;
        }
        if(containerPhysicalMemoryMB <= 0) {
          LOG.error("Calculated memory for YARN containers is too low."
              + " Node memory is " + physicalMemoryMB
              + " MB, system reserved memory is "
              + reservedMemoryMB + " MB.");
        }
        containerPhysicalMemoryMB = Math.max(containerPhysicalMemoryMB, 0);
        memoryMb = containerPhysicalMemoryMB;
      }
    }
    if(memoryMb <= 0) {
      String message = "Illegal value for " + YarnConfiguration.NM_PMEM_MB
          + ". Value must be greater than 0.";
      throw new IllegalArgumentException(message);
    }
    return memoryMb;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestContainerExecutor.java
  public void testRunCommandWithCpuAndMemoryResources() {
    assumeTrue(Shell.WINDOWS);
    int containerCores = 1;
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_WINDOWS_CONTAINER_CPU_LIMIT_ENABLED, "true");
    conf.set(YarnConfiguration.NM_WINDOWS_CONTAINER_MEMORY_LIMIT_ENABLED, "true");

    String[] command =
        containerExecutor.getRunCommand("echo", "group1", null, null, conf,
          Resource.newInstance(1024, 1));
    int nodeVCores = NodeManagerHardwareUtils.getVCores(conf);
    Assert.assertEquals(YarnConfiguration.DEFAULT_NM_VCORES, nodeVCores);
    int cpuRate = Math.min(10000, (containerCores * 10000) / nodeVCores);

    String[] expected =
        {Shell.WINUTILS, "task", "create", "-m", "1024", "-c",
            String.valueOf(cpuRate), "group1", "cmd /c " + "echo" };
    Assert.assertEquals(Arrays.toString(expected), Arrays.toString(command));

    conf.setBoolean(YarnConfiguration.NM_ENABLE_HARDWARE_CAPABILITY_DETECTION,
        true);
    int nodeCPUs = NodeManagerHardwareUtils.getNodeCPUs(conf);
    float yarnCPUs = NodeManagerHardwareUtils.getContainersCPUs(conf);
    nodeVCores = NodeManagerHardwareUtils.getVCores(conf);
    Assert.assertEquals(nodeCPUs, (int) yarnCPUs);
    Assert.assertEquals(nodeCPUs, nodeVCores);
    command =
        containerExecutor.getRunCommand("echo", "group1", null, null, conf,
          Resource.newInstance(1024, 1));
    cpuRate = Math.min(10000, (containerCores * 10000) / nodeVCores);
    expected[6] = String.valueOf(cpuRate);
    Assert.assertEquals(Arrays.toString(expected), Arrays.toString(command));

    int yarnCpuLimit = 80;
    conf.setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT,
        yarnCpuLimit);
    yarnCPUs = NodeManagerHardwareUtils.getContainersCPUs(conf);
    nodeVCores = NodeManagerHardwareUtils.getVCores(conf);
    Assert.assertEquals(nodeCPUs * 0.8, yarnCPUs, 0.01);
    if (nodeCPUs == 1) {
      Assert.assertEquals(1, nodeVCores);
    } else {
      Assert.assertEquals((int) (nodeCPUs * 0.8), nodeVCores);
    }
    command =
        containerExecutor.getRunCommand("echo", "group1", null, null, conf,
          Resource.newInstance(1024, 1));
    int containerPerc = (yarnCpuLimit * containerCores) / nodeVCores;
    cpuRate = Math.min(10000, 100 * containerPerc);
    expected[6] = String.valueOf(cpuRate);
    Assert.assertEquals(Arrays.toString(expected), Arrays.toString(command));
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/util/TestCgroupsLCEResourcesHandler.java
    ResourceCalculatorPlugin plugin =
        Mockito.mock(ResourceCalculatorPlugin.class);
    Mockito.doReturn(numProcessors).when(plugin).getNumProcessors();
    Mockito.doReturn(numProcessors).when(plugin).getNumCores();
    handler.setConf(conf);
    handler.initConfig();

    ResourceCalculatorPlugin plugin =
        Mockito.mock(ResourceCalculatorPlugin.class);
    Mockito.doReturn(numProcessors).when(plugin).getNumProcessors();
    Mockito.doReturn(numProcessors).when(plugin).getNumCores();
    handler.setConf(conf);
    handler.initConfig();


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/util/TestNodeManagerHardwareUtils.java
import org.junit.Test;
import org.mockito.Mockito;

public class TestNodeManagerHardwareUtils {

  static class TestResourceCalculatorPlugin extends ResourceCalculatorPlugin {
    @Override
    public long getVirtualMemorySize() {
      return 0;
    }

    @Override
    public long getPhysicalMemorySize() {
      long ret = Runtime.getRuntime().maxMemory() * 2;
      ret = ret + (4L * 1024 * 1024 * 1024);
      return ret;
    }

    @Override
    public long getAvailableVirtualMemorySize() {
      return 0;
    }

    @Override
    public long getAvailablePhysicalMemorySize() {
      return 0;
    }

    @Override
    public int getNumProcessors() {
      return 8;
    }

    @Override
    public long getCpuFrequency() {
      return 0;
    }

    @Override
    public long getCumulativeCpuTime() {
      return 0;
    }

    @Override
    public float getCpuUsage() {
      return 0;
    }

    @Override
    public int getNumCores() {
      return 4;
    }
  }

  @Test
  public void testGetContainerCPU() {

    YarnConfiguration conf = new YarnConfiguration();
    float ret;
    final int numProcessors = 8;
    final int numCores = 4;
    ResourceCalculatorPlugin plugin =
        Mockito.mock(ResourceCalculatorPlugin.class);
    Mockito.doReturn(numProcessors).when(plugin).getNumProcessors();
    Mockito.doReturn(numCores).when(plugin).getNumCores();

    conf.setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 0);
    boolean catchFlag = false;
    try {
      NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
      Assert.fail("getContainerCores should have thrown exception");
    } catch (IllegalArgumentException ie) {
      catchFlag = true;
    }
    Assert.assertTrue(catchFlag);

    conf.setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT,
        100);
    ret = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    Assert.assertEquals(4, (int) ret);

    conf
      .setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 50);
    ret = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    Assert.assertEquals(2, (int) ret);

    conf
      .setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 75);
    ret = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    Assert.assertEquals(3, (int) ret);

    conf
      .setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 85);
    ret = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    Assert.assertEquals(3.4, ret, 0.1);

    conf.setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT,
        110);
    ret = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    Assert.assertEquals(4, (int) ret);
  }

  @Test
  public void testGetVCores() {

    ResourceCalculatorPlugin plugin = new TestResourceCalculatorPlugin();
    YarnConfiguration conf = new YarnConfiguration();

    conf.setFloat(YarnConfiguration.NM_PCORES_VCORES_MULTIPLIER, 1.25f);

    int ret = NodeManagerHardwareUtils.getVCores(plugin, conf);
    Assert.assertEquals(YarnConfiguration.DEFAULT_NM_VCORES, ret);

    conf.setBoolean(YarnConfiguration.NM_ENABLE_HARDWARE_CAPABILITY_DETECTION,
        true);
    ret = NodeManagerHardwareUtils.getVCores(plugin, conf);
    Assert.assertEquals(5, ret);

    conf.setBoolean(YarnConfiguration.NM_COUNT_LOGICAL_PROCESSORS_AS_CORES,
        true);
    ret = NodeManagerHardwareUtils.getVCores(plugin, conf);
    Assert.assertEquals(10, ret);

    conf.setInt(YarnConfiguration.NM_VCORES, 10);
    ret = NodeManagerHardwareUtils.getVCores(plugin, conf);
    Assert.assertEquals(10, ret);

    YarnConfiguration conf1 = new YarnConfiguration();
    conf1.setBoolean(YarnConfiguration.NM_ENABLE_HARDWARE_CAPABILITY_DETECTION,
        false);
    conf.setInt(YarnConfiguration.NM_VCORES, 10);
    ret = NodeManagerHardwareUtils.getVCores(plugin, conf);
    Assert.assertEquals(10, ret);
  }

  @Test
  public void testGetContainerMemoryMB() throws Exception {

    ResourceCalculatorPlugin plugin = new TestResourceCalculatorPlugin();
    long physicalMemMB = plugin.getPhysicalMemorySize() / (1024 * 1024);
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_ENABLE_HARDWARE_CAPABILITY_DETECTION,
        true);
    int mem = NodeManagerHardwareUtils.getContainerMemoryMB(null, conf);
    Assert.assertEquals(YarnConfiguration.DEFAULT_NM_PMEM_MB, mem);

    mem = NodeManagerHardwareUtils.getContainerMemoryMB(plugin, conf);
    int hadoopHeapSizeMB =
        (int) (Runtime.getRuntime().maxMemory() / (1024 * 1024));
    int calculatedMemMB =
        (int) (0.8 * (physicalMemMB - (2 * hadoopHeapSizeMB)));
    Assert.assertEquals(calculatedMemMB, mem);

    conf.setInt(YarnConfiguration.NM_PMEM_MB, 1024);
    mem = NodeManagerHardwareUtils.getContainerMemoryMB(conf);
    Assert.assertEquals(1024, mem);

    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_ENABLE_HARDWARE_CAPABILITY_DETECTION,
        false);
    mem = NodeManagerHardwareUtils.getContainerMemoryMB(conf);
    Assert.assertEquals(YarnConfiguration.DEFAULT_NM_PMEM_MB, mem);
    conf.setInt(YarnConfiguration.NM_PMEM_MB, 10 * 1024);
    mem = NodeManagerHardwareUtils.getContainerMemoryMB(conf);
    Assert.assertEquals(10 * 1024, mem);
  }
}

