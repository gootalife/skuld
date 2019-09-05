hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
  public static final String NM_HEALTH_CHECK_SCRIPT_OPTS = 
    NM_PREFIX + "health-checker.script.opts";

      by container executor. */
  public static final String NM_CONTAINER_LOCALIZER_JAVA_OPTS_KEY =
      NM_PREFIX + "container-localizer.java.opts";
  public static final String NM_CONTAINER_LOCALIZER_JAVA_OPTS_DEFAULT =
      "-Xmx256m";

  public static final String NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME =
    NM_PREFIX + "docker-container-executor.image-name";

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/LinuxContainerExecutor.java
    if (javaLibPath != null) {
      command.add("-Djava.library.path=" + javaLibPath);
    }
    command.addAll(ContainerLocalizer.getJavaOpts(getConf()));
    buildMainArgs(command, user, appId, locId, nmAddr, localDirs);
    String[] commandArray = command.toArray(new String[command.size()]);
    ShellCommandExecutor shExec = new ShellCommandExecutor(commandArray);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/WindowsSecureContainerExecutor.java
     if (javaLibPath != null) {
       command.add("-Djava.library.path=" + javaLibPath);
     }
     command.addAll(ContainerLocalizer.getJavaOpts(getConf()));

     ContainerLocalizer.buildMainArgs(command, user, appId, locId, nmAddr, 
         localDirs);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/ContainerLocalizer.java
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
    return status;
  }

  public static List<String> getJavaOpts(Configuration conf) {
    String opts = conf.get(YarnConfiguration.NM_CONTAINER_LOCALIZER_JAVA_OPTS_KEY,
        YarnConfiguration.NM_CONTAINER_LOCALIZER_JAVA_OPTS_DEFAULT);
    return Arrays.asList(opts.split(" "));
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestLinuxContainerExecutorWithMocks.java
    try {
      mockExec.startLocalizer(nmPrivateCTokensPath, address, "test", "application_0", "12345", dirsHandler);
      List<String> result=readMockParams();
      Assert.assertEquals(result.size(), 18);
      Assert.assertEquals(result.get(0), YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER);
      Assert.assertEquals(result.get(1), "test");
      Assert.assertEquals(result.get(2), "0" );
      Assert.assertEquals(result.get(3),"application_0" );
      Assert.assertEquals(result.get(4), "/bin/nmPrivateCTokensPath");
      Assert.assertEquals(result.get(8), "-classpath" );
      Assert.assertEquals(result.get(11), "-Xmx256m" );
      Assert.assertEquals(result.get(12),"org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer" );
      Assert.assertEquals(result.get(13), "test");
      Assert.assertEquals(result.get(14), "application_0");
      Assert.assertEquals(result.get(15),"12345" );
      Assert.assertEquals(result.get(16),"localhost" );
      Assert.assertEquals(result.get(17),"8040" );

    } catch (InterruptedException e) {
      LOG.error("Error:"+e.getMessage(),e);

