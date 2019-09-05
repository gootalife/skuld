hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
    RM_PREFIX + "client.thread-count";
  public static final int DEFAULT_RM_CLIENT_THREAD_COUNT = 50;

  public static final String RM_AMLAUNCHER_THREAD_COUNT =
      RM_PREFIX + "amlauncher.thread-count";
  public static final int DEFAULT_RM_AMLAUNCHER_THREAD_COUNT = 50;

  public static final String RM_NODEMANAGER_CONNECT_RETIRES =
      RM_PREFIX + "nodemanager-connect-retries";
  public static final int DEFAULT_RM_NODEMANAGER_CONNECT_RETIRES = 10;

  public static final String RM_PRINCIPAL =
    RM_PREFIX + "principal";

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/amlauncher/ApplicationMasterLauncher.java
package org.apache.hadoop.yarn.server.resourcemanager.amlauncher;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
    EventHandler<AMLauncherEvent> {
  private static final Log LOG = LogFactory.getLog(
      ApplicationMasterLauncher.class);
  private ThreadPoolExecutor launcherPool;
  private LauncherThread launcherHandlingThread;
  
  private final BlockingQueue<Runnable> masterEvents
  public ApplicationMasterLauncher(RMContext context) {
    super(ApplicationMasterLauncher.class.getName());
    this.context = context;
    this.launcherHandlingThread = new LauncherThread();
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    int threadCount = conf.getInt(
        YarnConfiguration.RM_AMLAUNCHER_THREAD_COUNT,
        YarnConfiguration.DEFAULT_RM_AMLAUNCHER_THREAD_COUNT);
    ThreadFactory tf = new ThreadFactoryBuilder()
        .setNameFormat("ApplicationMasterLauncher #%d")
        .build();
    launcherPool = new ThreadPoolExecutor(threadCount, threadCount, 1,
        TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>());
    launcherPool.setThreadFactory(tf);

    Configuration newConf = new YarnConfiguration(conf);
    newConf.setInt(CommonConfigurationKeysPublic.
            IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        conf.getInt(YarnConfiguration.RM_NODEMANAGER_CONNECT_RETIRES,
            YarnConfiguration.DEFAULT_RM_NODEMANAGER_CONNECT_RETIRES));
    setConfig(newConf);
    super.serviceInit(newConf);
  }

  @Override
  protected void serviceStart() throws Exception {
    launcherHandlingThread.start();

