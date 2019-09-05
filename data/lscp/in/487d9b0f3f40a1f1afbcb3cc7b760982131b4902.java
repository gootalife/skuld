hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-common/src/main/java/org/apache/hadoop/mapreduce/v2/util/MRApps.java
    boolean userClassesTakesPrecedence = 
      conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, false);

    if (userClassesTakesPrecedence) {
      conf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH_PREPEND_DISTCACHE,
        "true");
    }

    String classpathEnvVar =
      conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER, false)
        ? Environment.APP_CLASSPATH.name() : Environment.CLASSPATH.name();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
  public static final String YARN_APPLICATION_CLASSPATH = YARN_PREFIX
      + "application.classpath";

  public static final String YARN_APPLICATION_CLASSPATH_PREPEND_DISTCACHE =
    YARN_PREFIX + "application.classpath.prepend.distcache";

  public static final boolean
    DEFAULT_YARN_APPLICATION_CLASSPATH_PREPEND_DISTCACHE = false;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/launcher/ContainerLaunch.java
    if (Shell.WINDOWS) {
      
      String inputClassPath = environment.get(Environment.CLASSPATH.name());

      if (inputClassPath != null && !inputClassPath.isEmpty()) {


        boolean preferLocalizedJars = conf.getBoolean(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH_PREPEND_DISTCACHE,
          YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH_PREPEND_DISTCACHE
          );

        boolean needsSeparator = false;
        StringBuilder newClassPath = new StringBuilder();
        if (!preferLocalizedJars) {
          newClassPath.append(inputClassPath);
          needsSeparator = true;
        }


          for (String linkName : entry.getValue()) {
            if (needsSeparator) {
              newClassPath.append(File.pathSeparator);
            } else {
              needsSeparator = true;
            }
            newClassPath.append(pwd.toString())
              .append(Path.SEPARATOR).append(linkName);

            }
          }
        }
        if (preferLocalizedJars) {
          if (needsSeparator) {
            newClassPath.append(File.pathSeparator);
          }
          newClassPath.append(inputClassPath);
        }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/launcher/TestContainerLaunch.java
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch.ShellScriptBuilder;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;

public class TestContainerLaunch extends BaseContainerManagerTest {

  protected Context distContext = new NMContext(new NMContainerTokenSecretManager(
    conf), new NMTokenSecretManagerInNM(), null,
    new ApplicationACLsManager(conf), new NMNullStateStoreService()) {
    public int getHttpPort() {
      return HTTP_PORT;
    };
    public NodeId getNodeId() {
      return NodeId.newInstance("ahost", 1234);
    };
  };

  public TestContainerLaunch() throws UnsupportedFileSystemException {
    super();
  }
    }
  }

  @Test
  public void testPrependDistcache() throws Exception {

    Assume.assumeTrue(Shell.WINDOWS);

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    ApplicationId appId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);

    ContainerId cId = ContainerId.newContainerId(appAttemptId, 0);
    Map<String, String> userSetEnv = new HashMap<String, String>();
    userSetEnv.put(Environment.CONTAINER_ID.name(), "user_set_container_id");
    userSetEnv.put(Environment.NM_HOST.name(), "user_set_NM_HOST");
    userSetEnv.put(Environment.NM_PORT.name(), "user_set_NM_PORT");
    userSetEnv.put(Environment.NM_HTTP_PORT.name(), "user_set_NM_HTTP_PORT");
    userSetEnv.put(Environment.LOCAL_DIRS.name(), "user_set_LOCAL_DIR");
    userSetEnv.put(Environment.USER.key(), "user_set_" +
      Environment.USER.key());
    userSetEnv.put(Environment.LOGNAME.name(), "user_set_LOGNAME");
    userSetEnv.put(Environment.PWD.name(), "user_set_PWD");
    userSetEnv.put(Environment.HOME.name(), "user_set_HOME");
    userSetEnv.put(Environment.CLASSPATH.name(), "SYSTEM_CLPATH");
    containerLaunchContext.setEnvironment(userSetEnv);
    Container container = mock(Container.class);
    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(containerLaunchContext);
    when(container.getLocalizedResources()).thenReturn(null);
    Dispatcher dispatcher = mock(Dispatcher.class);
    EventHandler eventHandler = new EventHandler() {
      public void handle(Event event) {
        Assert.assertTrue(event instanceof ContainerExitEvent);
        ContainerExitEvent exitEvent = (ContainerExitEvent) event;
        Assert.assertEquals(ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
            exitEvent.getType());
      }
    };
    when(dispatcher.getEventHandler()).thenReturn(eventHandler);

    Configuration conf = new Configuration();

    ContainerLaunch launch = new ContainerLaunch(distContext, conf,
        dispatcher, exec, null, container, dirsHandler, containerManager);

    String testDir = System.getProperty("test.build.data",
        "target/test-dir");
    Path pwd = new Path(testDir);
    List<Path> appDirs = new ArrayList<Path>();
    List<String> containerLogs = new ArrayList<String>();

    Map<Path, List<String>> resources = new HashMap<Path, List<String>>();
    Path userjar = new Path("user.jar");
    List<String> lpaths = new ArrayList<String>();
    lpaths.add("userjarlink.jar");
    resources.put(userjar, lpaths);

    Path nmp = new Path(testDir);

    launch.sanitizeEnv(
      userSetEnv, pwd, appDirs, containerLogs, resources, nmp);

    List<String> result =
      getJarManifestClasspath(userSetEnv.get(Environment.CLASSPATH.name()));

    Assert.assertTrue(result.size() > 1);
    Assert.assertTrue(
      result.get(result.size() - 1).endsWith("userjarlink.jar"));


    cId = ContainerId.newContainerId(appAttemptId, 1);
    when(container.getContainerId()).thenReturn(cId);

    conf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH_PREPEND_DISTCACHE,
      "true");

    launch = new ContainerLaunch(distContext, conf,
        dispatcher, exec, null, container, dirsHandler, containerManager);

    launch.sanitizeEnv(
      userSetEnv, pwd, appDirs, containerLogs, resources, nmp);

    result =
      getJarManifestClasspath(userSetEnv.get(Environment.CLASSPATH.name()));

    Assert.assertTrue(result.size() > 1);
    Assert.assertTrue(
      result.get(0).endsWith("userjarlink.jar"));

  }

  private static List<String> getJarManifestClasspath(String path)
      throws Exception {
    List<String> classpath = new ArrayList<String>();
    JarFile jarFile = new JarFile(path);
    Manifest manifest = jarFile.getManifest();
    String cps = manifest.getMainAttributes().getValue("Class-Path");
    StringTokenizer cptok = new StringTokenizer(cps);
    while (cptok.hasMoreTokens()) {
      String cpentry = cptok.nextToken();
      classpath.add(cpentry);
    }
    return classpath;
  }


