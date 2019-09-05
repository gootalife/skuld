hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeManager.java
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.NodeHealthScriptRunner;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
  private static final Log LOG = LogFactory.getLog(NodeManager.class);
  private static long nmStartupTime = System.currentTimeMillis();
  protected final NodeManagerMetrics metrics = NodeManagerMetrics.create();
  private JvmPauseMonitor pauseMonitor;
  private ApplicationACLsManager aclsManager;
  private NodeHealthCheckerService nodeHealthChecker;
  private NodeLabelsProvider nodeLabelsProvider;
    dispatcher.register(NodeManagerEventType.class, this);
    addService(dispatcher);

    pauseMonitor = new JvmPauseMonitor(conf);
    metrics.getJvmMetrics().setPauseMonitor(pauseMonitor);

    DefaultMetricsSystem.initialize("NodeManager");

    } catch (IOException e) {
      throw new YarnRuntimeException("Failed NodeManager login", e);
    }
    pauseMonitor.start();
    super.serviceStart();
  }

    try {
      super.serviceStop();
      DefaultMetricsSystem.shutdown();
      if (pauseMonitor != null) {
        pauseMonitor.stop();
      }
    } finally {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/metrics/NodeManagerMetrics.java
  @Metric("Disk utilization % on good log dirs")
      MutableGaugeInt goodLogDirsDiskUtilizationPerc;

  private JvmMetrics jvmMetrics = null;

  private long allocatedMB;
  private long availableMB;

  public NodeManagerMetrics(JvmMetrics jvmMetrics) {
    this.jvmMetrics = jvmMetrics;
  }

  public static NodeManagerMetrics create() {
    return create(DefaultMetricsSystem.instance());
  }

  static NodeManagerMetrics create(MetricsSystem ms) {
    JvmMetrics jm = JvmMetrics.create("NodeManager", null, ms);
    return ms.register(new NodeManagerMetrics(jm));
  }

  public JvmMetrics getJvmMetrics() {
    return jvmMetrics;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ResourceManager.java
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
  private WebApp webApp;
  private AppReportFetcher fetcher = null;
  protected ResourceTrackerService resourceTracker;
  private JvmPauseMonitor pauseMonitor;

  @VisibleForTesting
  protected String webAppAddress;
      rmContext.setResourceTrackerService(resourceTracker);

      DefaultMetricsSystem.initialize("ResourceManager");
      JvmMetrics jm = JvmMetrics.initSingleton("ResourceManager", null);
      pauseMonitor = new JvmPauseMonitor(conf);
      jm.setPauseMonitor(pauseMonitor);

      if (conf.getBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE,
      rmStore.start();

      pauseMonitor.start();

      if(recoveryEnabled) {
        try {
          LOG.info("Recovery started");
    protected void serviceStop() throws Exception {

      DefaultMetricsSystem.shutdown();
      if (pauseMonitor != null) {
        pauseMonitor.stop();
      }

      if (rmContext != null) {
        RMStateStore store = rmContext.getStateStore();

