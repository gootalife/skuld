hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/JobHistoryServer.java
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
  private AggregatedLogDeletionService aggLogDelService;
  private HSAdminServer hsAdminServer;
  private HistoryServerStateStoreService stateStore;
  private JvmPauseMonitor pauseMonitor;

    addService(clientService);
    addService(aggLogDelService);
    addService(hsAdminServer);

    DefaultMetricsSystem.initialize("JobHistoryServer");
    JvmMetrics jm = JvmMetrics.initSingleton("JobHistoryServer", null);
    pauseMonitor = new JvmPauseMonitor(getConfig());
    jm.setPauseMonitor(pauseMonitor);

    super.serviceInit(config);
  }


  @Override
  protected void serviceStart() throws Exception {
    pauseMonitor.start();
    super.serviceStart();
  }
  
  @Override
  protected void serviceStop() throws Exception {
    DefaultMetricsSystem.shutdown();
    if (pauseMonitor != null) {
      pauseMonitor.stop();
    }
    super.serviceStop();
  }


