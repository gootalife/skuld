hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/job/impl/TaskAttemptImpl.java

    boolean userClassesTakesPrecedence =
      conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, false);

    Map<String, String> env = commonContainerSpec.getEnvironment();
    Map<String, String> myEnv = new HashMap<String, String>(env.size());
    myEnv.putAll(env);
    if (userClassesTakesPrecedence) {
      myEnv.put(Environment.CLASSPATH_PREPEND_DISTCACHE.name(), "true");
    }
    MapReduceChildJVM.setVMEnv(myEnv, remoteTask);


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-common/src/main/java/org/apache/hadoop/mapreduce/v2/util/MRApps.java
    boolean userClassesTakesPrecedence = 
      conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, false);

    String classpathEnvVar =
      conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER, false)
        ? Environment.APP_CLASSPATH.name() : Environment.CLASSPATH.name();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/ApplicationConstants.java
package org.apache.hadoop.yarn.api;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
    HADOOP_YARN_HOME("HADOOP_YARN_HOME"),

    @Private
    CLASSPATH_PREPEND_DISTCACHE("CLASSPATH_PREPEND_DISTCACHE"),


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
      ApplicationConstants.Environment.HADOOP_COMMON_HOME.key(),
      ApplicationConstants.Environment.HADOOP_HDFS_HOME.key(),
      ApplicationConstants.Environment.HADOOP_CONF_DIR.key(),
      ApplicationConstants.Environment.CLASSPATH_PREPEND_DISTCACHE.key(),
      ApplicationConstants.Environment.HADOOP_YARN_HOME.key()));
  
  public static final String YARN_APPLICATION_CLASSPATH = YARN_PREFIX
      + "application.classpath";


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/launcher/ContainerLaunch.java

        boolean preferLocalizedJars = Boolean.valueOf(
          environment.get(Environment.CLASSPATH_PREPEND_DISTCACHE.name())
          );

        boolean needsSeparator = false;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/launcher/TestContainerLaunch.java
    userSetEnv.put(Environment.LOGNAME.name(), "user_set_LOGNAME");
    userSetEnv.put(Environment.PWD.name(), "user_set_PWD");
    userSetEnv.put(Environment.HOME.name(), "user_set_HOME");
    userSetEnv.put(Environment.CLASSPATH.name(), "APATH");
    containerLaunchContext.setEnvironment(userSetEnv);
    Container container = mock(Container.class);
    when(container.getContainerId()).thenReturn(cId);
    Assert.assertTrue(
      result.get(result.size() - 1).endsWith("userjarlink.jar"));

    userSetEnv.put(Environment.CLASSPATH_PREPEND_DISTCACHE.name(), "true");

    cId = ContainerId.newContainerId(appAttemptId, 1);
    when(container.getContainerId()).thenReturn(cId);

    launch = new ContainerLaunch(distContext, conf,
        dispatcher, exec, null, container, dirsHandler, containerManager);


