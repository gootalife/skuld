hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
  public static final int DEFAULT_NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT =
      100;

  @Private
  public static final String NM_DISK_RESOURCE_PREFIX = NM_PREFIX
      + "resource.disk.";
  @Private
  public static final String NM_DISK_RESOURCE_ENABLED = NM_DISK_RESOURCE_PREFIX
      + "enabled";
  @Private
  public static final boolean DEFAULT_NM_DISK_RESOURCE_ENABLED = false;

  public static final String NM_NETWORK_RESOURCE_PREFIX = NM_PREFIX
      + "resource.network.";

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
  public static final int DEFAULT_NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_MBIT =
      1000;

  @Private
  public static final String NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_YARN_MBIT =
      NM_NETWORK_RESOURCE_PREFIX + "outbound-bandwidth-yarn-mbit";

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/CGroupsBlkioResourceHandlerImpl.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/CGroupsBlkioResourceHandlerImpl.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CGroupsBlkioResourceHandlerImpl implements DiskResourceHandler {

  static final Log LOG = LogFactory
      .getLog(CGroupsBlkioResourceHandlerImpl.class);

  private CGroupsHandler cGroupsHandler;
  @VisibleForTesting
  static final String DEFAULT_WEIGHT = "500";
  private static final String PARTITIONS_FILE = "/proc/partitions";

  CGroupsBlkioResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
    if(Shell.LINUX) {
      checkDiskScheduler();
    }
  }


  private void checkDiskScheduler() {
    String data;

    try {
      byte[] contents = Files.readAllBytes(Paths.get(PARTITIONS_FILE));
      data = new String(contents, "UTF-8").trim();
    } catch (IOException e) {
      String msg = "Couldn't read " + PARTITIONS_FILE +
          "; can't determine disk scheduler type";
      LOG.warn(msg, e);
      return;
    }
    String[] lines = data.split(System.lineSeparator());
    if (lines.length > 0) {
      for (String line : lines) {
        String[] columns = line.split("\\s+");
        if (columns.length > 4) {
          String partition = columns[4];
          if (partition.startsWith("sd") || partition.startsWith("hd")
              || partition.startsWith("vd") || partition.startsWith("xvd")) {
            String schedulerPath =
                "/sys/block/" + partition + "/queue/scheduler";
            File schedulerFile = new File(schedulerPath);
            if (schedulerFile.exists()) {
              try {
                byte[] contents = Files.readAllBytes(Paths.get(schedulerPath));
                String schedulerString = new String(contents, "UTF-8").trim();
                if (!schedulerString.contains("[cfq]")) {
                  LOG.warn("Device " + partition + " does not use the CFQ"
                      + " scheduler; disk isolation using "
                      + "CGroups will not work on this partition.");
                }
              } catch (IOException ie) {
                LOG.warn(
                    "Unable to determine disk scheduler type for partition "
                      + partition, ie);
              }
            }
          }
        }
      }
    }
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    this.cGroupsHandler
      .mountCGroupController(CGroupsHandler.CGroupController.BLKIO);
    return null;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {

    String cgroupId = container.getContainerId().toString();
    cGroupsHandler
      .createCGroup(CGroupsHandler.CGroupController.BLKIO, cgroupId);
    try {
      cGroupsHandler.updateCGroupParam(CGroupsHandler.CGroupController.BLKIO,
          cgroupId, CGroupsHandler.CGROUP_PARAM_BLKIO_WEIGHT, DEFAULT_WEIGHT);
    } catch (ResourceHandlerException re) {
      cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.BLKIO,
          cgroupId);
      LOG.warn("Could not update cgroup for container", re);
      throw re;
    }
    List<PrivilegedOperation> ret = new ArrayList<>();
    ret.add(new PrivilegedOperation(
      PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
      PrivilegedOperation.CGROUP_ARG_PREFIX
          + cGroupsHandler.getPathForCGroupTasks(
            CGroupsHandler.CGroupController.BLKIO, cgroupId)));
    return ret;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.BLKIO,
        containerId.toString());
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown() throws ResourceHandlerException {
    return null;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/CGroupsHandler.java
public interface CGroupsHandler {
  public enum CGroupController {
    CPU("cpu"),
    NET_CLS("net_cls"),
    BLKIO("blkio");

    private final String name;


  public static final String CGROUP_FILE_TASKS = "tasks";
  public static final String CGROUP_PARAM_CLASSID = "classid";
  public static final String CGROUP_PARAM_BLKIO_WEIGHT = "weight";


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/CGroupsHandlerImpl.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
  private final String cGroupMountPath;
  private final long deleteCGroupTimeout;
  private final long deleteCGroupDelay;
  private Map<CGroupController, String> controllerPaths;
  private final ReadWriteLock rwLock;
  private final PrivilegedOperationExecutor privilegedOperationExecutor;
  private final Clock clock;
    } else {

      Map<CGroupController, String> cPaths =
          initializeControllerPathsFromMtab(MTAB_FILE, this.cGroupPrefix);
      try {
        rwLock.writeLock().lock();
        controllerPaths = cPaths;
      } finally {
        rwLock.writeLock().unlock();
      }
    }
  }

  @VisibleForTesting
  static Map<CGroupController, String> initializeControllerPathsFromMtab(
      String mtab, String cGroupPrefix) throws ResourceHandlerException {
    try {
      Map<String, List<String>> parsedMtab = parseMtab(mtab);
      Map<CGroupController, String> ret = new HashMap<>();

      for (CGroupController controller : CGroupController.values()) {
        String name = controller.getName();
        String controllerPath = findControllerInMtab(name, parsedMtab);

        if (controllerPath != null) {
          File f = new File(controllerPath + "/" + cGroupPrefix);

          if (FileUtil.canWrite(f)) {
            ret.put(controller, controllerPath);
          } else {
            String error =
                new StringBuffer("Mount point Based on mtab file: ")
                  .append(mtab)
                  .append(". Controller mount point not writable for: ")
                  .append(name).toString();

            LOG.error(error);
          LOG.warn("Controller not mounted but automount disabled: " + name);
        }
      }
      return ret;
    } catch (IOException e) {
      LOG.warn("Failed to initialize controller paths! Exception: " + e);
      throw new ResourceHandlerException(
        "Failed to initialize controller paths!");
    }
  }

  private static Map<String, List<String>> parseMtab(String mtab)
      throws IOException {
    Map<String, List<String>> ret = new HashMap<String, List<String>>();
    BufferedReader in = null;

    try {
      FileInputStream fis = new FileInputStream(new File(mtab));
      in = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

      for (String str = in.readLine(); str != null;
        }
      }
    } catch (IOException e) {
      throw new IOException("Error while reading " + mtab, e);
    } finally {
      IOUtils.cleanup(LOG, in);
    }
    return ret;
  }

  private static String findControllerInMtab(String controller,
      Map<String, List<String>> entries) {
    for (Map.Entry<String, List<String>> e : entries.entrySet()) {
      if (e.getValue().contains(controller))
    return null;
  }

  @Override
  public void mountCGroupController(CGroupController controller)
      throws ResourceHandlerException {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/DiskResourceHandler.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/DiskResourceHandler.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface DiskResourceHandler extends ResourceHandler {
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/ResourceHandlerModule.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ResourceHandlerModule {
  private static volatile ResourceHandlerChain resourceHandlerChain;

  private static volatile TrafficControlBandwidthHandlerImpl
      trafficControlBandwidthHandler;
  private static volatile CGroupsHandler cGroupsHandler;
  private static volatile CGroupsBlkioResourceHandlerImpl
      cGroupsBlkioResourceHandler;

  public static CGroupsHandler getCGroupsHandler(Configuration conf)
      throws ResourceHandlerException {
    return getTrafficControlBandwidthHandler(conf);
  }

  public static DiskResourceHandler getDiskResourceHandler(Configuration conf)
      throws ResourceHandlerException {
    if (conf.getBoolean(YarnConfiguration.NM_DISK_RESOURCE_ENABLED,
        YarnConfiguration.DEFAULT_NM_DISK_RESOURCE_ENABLED)) {
      return getCgroupsBlkioResourceHandler(conf);
    }
    return null;
  }

  private static CGroupsBlkioResourceHandlerImpl getCgroupsBlkioResourceHandler(
      Configuration conf) throws ResourceHandlerException {
    if (cGroupsBlkioResourceHandler == null) {
      synchronized (DiskResourceHandler.class) {
        if (cGroupsBlkioResourceHandler == null) {
          cGroupsBlkioResourceHandler =
              new CGroupsBlkioResourceHandlerImpl(getCGroupsHandler(conf));
        }
      }
    }
    return cGroupsBlkioResourceHandler;
  }

  private static void addHandlerIfNotNull(List<ResourceHandler> handlerList,
      ResourceHandler handler) {
    if (handler != null) {
    ArrayList<ResourceHandler> handlerList = new ArrayList<>();

    addHandlerIfNotNull(handlerList, getOutboundBandwidthResourceHandler(conf));
    addHandlerIfNotNull(handlerList, getDiskResourceHandler(conf));
    resourceHandlerChain = new ResourceHandlerChain(handlerList);
  }

  public static ResourceHandlerChain getConfiguredResourceHandlerChain(
      Configuration conf) throws ResourceHandlerException {
    if (resourceHandlerChain == null) {
      synchronized (ResourceHandlerModule.class) {
        if (resourceHandlerChain == null) {
      return null;
    }
  }

  @VisibleForTesting
  static void nullifyResourceHandlerChain() throws ResourceHandlerException {
    resourceHandlerChain = null;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/util/CgroupsLCEResourcesHandler.java
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
  String getMtabFileName() {
    return MTAB_FILE;
  }

  @VisibleForTesting
  Map<String, String> getControllerPaths() {
    return Collections.unmodifiableMap(controllerPaths);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/TestCGroupsBlkioResourceHandlerImpl.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/TestCGroupsBlkioResourceHandlerImpl.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

public class TestCGroupsBlkioResourceHandlerImpl {

  private CGroupsHandler mockCGroupsHandler;
  private CGroupsBlkioResourceHandlerImpl cGroupsBlkioResourceHandlerImpl;

  @Before
  public void setup() {
    mockCGroupsHandler = mock(CGroupsHandler.class);
    cGroupsBlkioResourceHandlerImpl =
        new CGroupsBlkioResourceHandlerImpl(mockCGroupsHandler);
  }

  @Test
  public void testBootstrap() throws Exception {
    Configuration conf = new YarnConfiguration();
    List<PrivilegedOperation> ret =
        cGroupsBlkioResourceHandlerImpl.bootstrap(conf);
    verify(mockCGroupsHandler, times(1)).mountCGroupController(
        CGroupsHandler.CGroupController.BLKIO);
    Assert.assertNull(ret);
  }

  @Test
  public void testPreStart() throws Exception {
    String id = "container_01_01";
    String path = "test-path/" + id;
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    Container mockContainer = mock(Container.class);
    when(mockContainer.getContainerId()).thenReturn(mockContainerId);
    when(
      mockCGroupsHandler.getPathForCGroupTasks(
        CGroupsHandler.CGroupController.BLKIO, id)).thenReturn(path);

    List<PrivilegedOperation> ret =
        cGroupsBlkioResourceHandlerImpl.preStart(mockContainer);
    verify(mockCGroupsHandler, times(1)).createCGroup(
        CGroupsHandler.CGroupController.BLKIO, id);
    verify(mockCGroupsHandler, times(1)).updateCGroupParam(
        CGroupsHandler.CGroupController.BLKIO, id,
        CGroupsHandler.CGROUP_PARAM_BLKIO_WEIGHT,
        CGroupsBlkioResourceHandlerImpl.DEFAULT_WEIGHT);
    Assert.assertNotNull(ret);
    Assert.assertEquals(1, ret.size());
    PrivilegedOperation op = ret.get(0);
    Assert.assertEquals(PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        op.getOperationType());
    List<String> args = op.getArguments();
    Assert.assertEquals(1, args.size());
    Assert.assertEquals(PrivilegedOperation.CGROUP_ARG_PREFIX + path,
        args.get(0));
  }

  @Test
  public void testReacquireContainer() throws Exception {
    ContainerId containerIdMock = mock(ContainerId.class);
    Assert.assertNull(cGroupsBlkioResourceHandlerImpl
        .reacquireContainer(containerIdMock));
  }

  @Test
  public void testPostComplete() throws Exception {
    String id = "container_01_01";
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    Assert.assertNull(cGroupsBlkioResourceHandlerImpl
        .postComplete(mockContainerId));
    verify(mockCGroupsHandler, times(1)).deleteCGroup(
        CGroupsHandler.CGroupController.BLKIO, id);
  }

  @Test
  public void testTeardown() throws Exception {
    Assert.assertNull(cGroupsBlkioResourceHandlerImpl.teardown());
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/TestCGroupsHandlerImpl.java

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TestCGroupsHandlerImpl {
  private static final Log LOG =
      LogFactory.getLog(TestCGroupsHandlerImpl.class);
    try {
      cGroupsHandler = new CGroupsHandlerImpl(conf,
          privilegedOperationExecutorMock);
      PrivilegedOperation expectedOp = new PrivilegedOperation(
          PrivilegedOperation.OperationType.MOUNT_CGROUPS, (String) null);
      StringBuffer controllerKV = new StringBuffer(controller.getName())

      cGroupsHandler.mountCGroupController(controller);
      try {
        ArgumentCaptor<PrivilegedOperation> opCaptor = ArgumentCaptor.forClass(
            PrivilegedOperation.class);
        verify(privilegedOperationExecutorMock)
            .executePrivilegedOperation(opCaptor.capture(), eq(false));


      Assert.assertTrue(paramFile.exists());
      try {
        Assert.assertEquals(paramValue, new String(Files.readAllBytes(
            paramFile.toPath())));
      } catch (IOException e) {
        LOG.error("Caught exception: " + e);
        Assert.fail("Unexpected IOException trying to read cgroup param!");
      }

      Assert.assertEquals(paramValue,
          cGroupsHandler.getCGroupParam(controller, testCGroup, param));

    } catch (ResourceHandlerException e) {
      LOG.error("Caught exception: " + e);
      Assert
        .fail("Unexpected ResourceHandlerException during cgroup operations!");
    }
  }

  public static File createMockCgroupMount(File parentDir, String type)
      throws IOException {
    return createMockCgroupMount(parentDir, type, "hadoop-yarn");
  }

  public static File createMockCgroupMount(File parentDir, String type,
      String hierarchy) throws IOException {
    File cgroupMountDir =
        new File(parentDir.getAbsolutePath(), type + "/" + hierarchy);
    FileUtils.deleteQuietly(cgroupMountDir);
    if (!cgroupMountDir.mkdirs()) {
      String message =
          "Could not create dir " + cgroupMountDir.getAbsolutePath();
      throw new IOException(message);
    }
    return cgroupMountDir;
  }

  public static File createMockMTab(File parentDir) throws IOException {
    String cpuMtabContent =
        "none " + parentDir.getAbsolutePath()
            + "/cpu cgroup rw,relatime,cpu 0 0\n";
    String blkioMtabContent =
        "none " + parentDir.getAbsolutePath()
            + "/blkio cgroup rw,relatime,blkio 0 0\n";

    File mockMtab = new File(parentDir, UUID.randomUUID().toString());
    if (!mockMtab.exists()) {
      if (!mockMtab.createNewFile()) {
        String message = "Could not create file " + mockMtab.getAbsolutePath();
        throw new IOException(message);
      }
    }
    FileWriter mtabWriter = new FileWriter(mockMtab.getAbsoluteFile());
    mtabWriter.write(cpuMtabContent);
    mtabWriter.write(blkioMtabContent);
    mtabWriter.close();
    mockMtab.deleteOnExit();
    return mockMtab;
  }


  @Test
  public void testMtabParsing() throws Exception {
    File parentDir = new File(tmpPath);
    File cpuCgroupMountDir = createMockCgroupMount(parentDir, "cpu",
        hierarchy);
    Assert.assertTrue(cpuCgroupMountDir.exists());
    File blkioCgroupMountDir = createMockCgroupMount(parentDir,
        "blkio", hierarchy);
    Assert.assertTrue(blkioCgroupMountDir.exists());
    File mockMtabFile = createMockMTab(parentDir);
    Map<CGroupsHandler.CGroupController, String> controllerPaths =
        CGroupsHandlerImpl.initializeControllerPathsFromMtab(
          mockMtabFile.getAbsolutePath(), hierarchy);
    Assert.assertEquals(2, controllerPaths.size());
    Assert.assertTrue(controllerPaths
        .containsKey(CGroupsHandler.CGroupController.CPU));
    Assert.assertTrue(controllerPaths
        .containsKey(CGroupsHandler.CGroupController.BLKIO));
    String cpuDir = controllerPaths.get(CGroupsHandler.CGroupController.CPU);
    String blkioDir =
        controllerPaths.get(CGroupsHandler.CGroupController.BLKIO);
    Assert.assertEquals(parentDir.getAbsolutePath() + "/cpu", cpuDir);
    Assert.assertEquals(parentDir.getAbsolutePath() + "/blkio", blkioDir);
  }

  @After

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/TestResourceHandlerModule.java
  Configuration networkEnabledConf;

  @Before
  public void setup() throws Exception {
    emptyConf = new YarnConfiguration();
    networkEnabledConf = new YarnConfiguration();

    networkEnabledConf.setBoolean(YarnConfiguration
        .NM_LINUX_CONTAINER_CGROUPS_MOUNT, true);
    ResourceHandlerModule.nullifyResourceHandlerChain();
  }

  @Test
      Assert.fail("Unexpected ResourceHandlerException: " + e);
    }
  }

  @Test
  public void testDiskResourceHandler() throws Exception {

    DiskResourceHandler handler =
        ResourceHandlerModule.getDiskResourceHandler(emptyConf);
    Assert.assertNull(handler);

    Configuration diskConf = new YarnConfiguration();
    diskConf.setBoolean(YarnConfiguration.NM_DISK_RESOURCE_ENABLED, true);

    handler = ResourceHandlerModule.getDiskResourceHandler(diskConf);
    Assert.assertNotNull(handler);

    ResourceHandlerChain resourceHandlerChain =
        ResourceHandlerModule.getConfiguredResourceHandlerChain(diskConf);
    List<ResourceHandler> resourceHandlers =
        resourceHandlerChain.getResourceHandlerList();
    Assert.assertEquals(resourceHandlers.size(), 1);
    Assert.assertTrue(resourceHandlers.get(0) == handler);
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/util/TestCgroupsLCEResourcesHandler.java
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.TestCGroupsHandlerImpl;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.junit.Assert;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import java.io.*;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

public class TestCgroupsLCEResourcesHandler {

    @Override
    int[] getOverallLimits(float x) {
      if (generateLimitsMode) {
        return super.getOverallLimits(x);
      }
      return limits;
    handler.initConfig();

    File cpuCgroupMountDir = TestCGroupsHandlerImpl.createMockCgroupMount(
        cgroupDir, "cpu");

    File mockMtab = TestCGroupsHandlerImpl.createMockMTab(cgroupDir);

    handler.setMtabFile(mockMtab.getAbsolutePath());
    handler.init(mockLCE, plugin);
    File periodFile = new File(cpuCgroupMountDir, "cpu.cfs_period_us");
    File quotaFile = new File(cpuCgroupMountDir, "cpu.cfs_quota_us");
    Assert.assertFalse(periodFile.exists());
    Assert.assertFalse(quotaFile.exists());

    Assert.assertEquals(-1, ret[1]);
  }

  @Test
  public void testContainerLimits() throws IOException {
    LinuxContainerExecutor mockLCE = new MockLinuxContainerExecutor();
        new CustomCgroupsLCEResourceHandler();
    handler.generateLimitsMode = true;
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_DISK_RESOURCE_ENABLED, true);
    final int numProcessors = 4;
    ResourceCalculatorPlugin plugin =
        Mockito.mock(ResourceCalculatorPlugin.class);
    handler.initConfig();

    File cpuCgroupMountDir = TestCGroupsHandlerImpl.createMockCgroupMount(
        cgroupDir, "cpu");

    File mockMtab = TestCGroupsHandlerImpl.createMockMTab(cgroupDir);

    handler.setMtabFile(mockMtab.getAbsolutePath());
    handler.init(mockLCE, plugin);

    ContainerId id = ContainerId.fromString("container_1_1_1_1");
    handler.preExecute(id, Resource.newInstance(1024, 1));
    Assert.assertNotNull(handler.getControllerPaths());
    File containerCpuDir = new File(cpuCgroupMountDir, id.toString());
    Assert.assertTrue(containerCpuDir.exists());
    Assert.assertTrue(containerCpuDir.isDirectory());
    File periodFile = new File(containerCpuDir, "cpu.cfs_period_us");
    File quotaFile = new File(containerCpuDir, "cpu.cfs_quota_us");
    Assert.assertFalse(periodFile.exists());
    Assert.assertFalse(quotaFile.exists());

    FileUtils.deleteQuietly(containerCpuDir);
    conf.setBoolean(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE,
        true);
    handler.initConfig();
    handler.preExecute(id,
        Resource.newInstance(1024, YarnConfiguration.DEFAULT_NM_VCORES));
    Assert.assertTrue(containerCpuDir.exists());
    Assert.assertTrue(containerCpuDir.isDirectory());
    periodFile = new File(containerCpuDir, "cpu.cfs_period_us");
    quotaFile = new File(containerCpuDir, "cpu.cfs_quota_us");
    Assert.assertFalse(periodFile.exists());
    Assert.assertFalse(quotaFile.exists());

    FileUtils.deleteQuietly(containerCpuDir);
    conf.setBoolean(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE,
        true);
    handler.initConfig();
    handler.preExecute(id,
        Resource.newInstance(1024, YarnConfiguration.DEFAULT_NM_VCORES / 2));
    Assert.assertTrue(containerCpuDir.exists());
    Assert.assertTrue(containerCpuDir.isDirectory());
    periodFile = new File(containerCpuDir, "cpu.cfs_period_us");
    quotaFile = new File(containerCpuDir, "cpu.cfs_quota_us");
    Assert.assertTrue(periodFile.exists());
    Assert.assertTrue(quotaFile.exists());
    Assert.assertEquals(500 * 1000, readIntFromFile(periodFile));
    Assert.assertEquals(1000 * 1000, readIntFromFile(quotaFile));

    FileUtils.deleteQuietly(containerCpuDir);
    conf.setBoolean(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE,
        true);
    conf
      .setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 50);
    handler.initConfig();
    handler.init(mockLCE, plugin);
    handler.preExecute(id,
        Resource.newInstance(1024, YarnConfiguration.DEFAULT_NM_VCORES / 2));
    Assert.assertTrue(containerCpuDir.exists());
    Assert.assertTrue(containerCpuDir.isDirectory());
    periodFile = new File(containerCpuDir, "cpu.cfs_period_us");
    quotaFile = new File(containerCpuDir, "cpu.cfs_quota_us");
    Assert.assertTrue(periodFile.exists());
    Assert.assertTrue(quotaFile.exists());
    Assert.assertEquals(1000 * 1000, readIntFromFile(periodFile));

