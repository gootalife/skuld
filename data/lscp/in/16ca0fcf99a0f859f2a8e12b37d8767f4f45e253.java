hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/StringUtils.java
    return sb.toString();
  }

  public static String join(char separator, Iterable<?> strings) {
    return join(separator + "", strings);
  }

    return sb.toString();
  }

  public static String join(char separator, String[] strings) {
    return join(separator + "", strings);
  }


hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/util/TestStringUtils.java
    s.add("c");
    assertEquals("", StringUtils.join(":", s.subList(0, 0)));
    assertEquals("a", StringUtils.join(":", s.subList(0, 1)));
    assertEquals("", StringUtils.join(':', s.subList(0, 0)));
    assertEquals("a", StringUtils.join(':', s.subList(0, 1)));
    assertEquals("a:b", StringUtils.join(":", s.subList(0, 2)));
    assertEquals("a:b:c", StringUtils.join(":", s.subList(0, 3)));
    assertEquals("a:b", StringUtils.join(':', s.subList(0, 2)));
    assertEquals("a:b:c", StringUtils.join(':', s.subList(0, 3)));
  }
  
  @Test (timeout = 30000)

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/LinuxContainerExecutor.java
                   Integer.toString(Commands.INITIALIZE_CONTAINER.getValue()),
                   appId,
                   nmPrivateContainerTokensPath.toUri().getPath().toString(),
                   StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
                       localDirs),
                   StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
                       logDirs)));

    File jvm =                                  // use same jvm as parent
      new File(new File(System.getProperty("java.home"), "bin"), "java");
            nmPrivateContainerScriptPath.toUri().getPath().toString(),
            nmPrivateTokensPath.toUri().getPath().toString(),
            pidFilePath.toString(),
            StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
                localDirs),
            StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
                logDirs),
            resourcesOptions));

        if (tcCommandFile != null) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/privileged/PrivilegedOperation.java
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PrivilegedOperation {
  public final static char LINUX_FILE_PATH_SEPARATOR = '%';

  public enum OperationType {
    CHECK_SETUP("--checksetup"),

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/privileged/PrivilegedOperationExecutor.java

      if (noneArgsOnly == false) {
        finalOpArg.append(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR);
        finalOpArg.append(tasksFile);
      } else {
        finalOpArg.append(tasksFile);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/util/CgroupsLCEResourcesHandler.java
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.SystemClock;

    if (isCpuWeightEnabled()) {
      sb.append(pathForCgroup(CONTROLLER_CPU, containerName) + "/tasks");
      sb.append(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR);
    }

    if (sb.charAt(sb.length() - 1) ==
        PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR) {
      sb.deleteCharAt(sb.length() - 1);
    }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestLinuxContainerExecutorWithMocks.java
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
    assertEquals(Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
        appSubmitter, cmd, appId, containerId,
        workDir.toString(), "/bin/echo", "/dev/null", pidFile.toString(),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            dirsHandler.getLocalDirs()),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            dirsHandler.getLogDirs()), "cgroups=none"),
        readMockParams());
    
  }
    assertEquals(Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
        appSubmitter, cmd, appId, containerId,
        workDir.toString(), "/bin/echo", "/dev/null", pidFile.toString(),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            dirsHandler.getLocalDirs()),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            dirsHandler.getLogDirs()),
        "cgroups=none"), readMockParams());

  }

