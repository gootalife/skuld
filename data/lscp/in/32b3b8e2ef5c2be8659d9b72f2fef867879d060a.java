hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/container/ContainerImpl.java
  private int exitCode = ContainerExitStatus.INVALID;
  private final StringBuilder diagnostics;
  private boolean wasLaunched;
  private long containerLocalizationStartTime;
  private long containerLaunchStartTime;
  private static Clock clock = new SystemClock();

  @SuppressWarnings("unchecked") // dispatcher not typed
  private void sendContainerMonitorStartEvent() {
    long launchDuration = clock.getTime() - containerLaunchStartTime;
    metrics.addContainerLaunchDuration(launchDuration);

    long pmemBytes = getResource().getMemory() * 1024 * 1024L;
    float pmemRatio = daemonConf.getFloat(
        YarnConfiguration.NM_VMEM_PMEM_RATIO,
        YarnConfiguration.DEFAULT_NM_VMEM_PMEM_RATIO);
    long vmemBytes = (long) (pmemRatio * pmemBytes);
    int cpuVcores = getResource().getVirtualCores();
    long localizationDuration = containerLaunchStartTime -
        containerLocalizationStartTime;
    dispatcher.getEventHandler().handle(
        new ContainerStartMonitoringEvent(containerId,
        vmemBytes, pmemBytes, cpuVcores, launchDuration,
        localizationDuration));
  }

  private void addDiagnostics(String... diags) {
        }
      }

      container.containerLocalizationStartTime = clock.getTime();
      Map<String,LocalResource> cntrRsrc = ctxt.getLocalResources();
      if (!cntrRsrc.isEmpty()) {
      container.sendContainerMonitorStartEvent();
      container.metrics.runningContainer();
      container.wasLaunched  = true;

      if (container.recoveredAsKilled) {
        LOG.info("Killing " + container.containerId

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/monitor/ContainerMetrics.java
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.hadoop.yarn.api.records.ContainerId;

  public static final String VMEM_LIMIT_METRIC_NAME = "vMemLimitMBs";
  public static final String VCORE_LIMIT_METRIC_NAME = "vCoreLimit";
  public static final String PMEM_USAGE_METRIC_NAME = "pMemUsageMBs";
  public static final String LAUNCH_DURATION_METRIC_NAME = "launchDurationMs";
  public static final String LOCALIZATION_DURATION_METRIC_NAME =
      "localizationDurationMs";
  private static final String PHY_CPU_USAGE_METRIC_NAME = "pCpuUsagePercent";

  @Metric
  public MutableGaugeInt cpuVcoreLimit;

  @Metric
  public MutableGaugeLong launchDurationMs;

  @Metric
  public MutableGaugeLong localizationDurationMs;

  static final MetricsInfo RECORD_INFO =
      info("ContainerResource", "Resource limit and usage by container");

        VMEM_LIMIT_METRIC_NAME, "Virtual memory limit in MBs", 0);
    this.cpuVcoreLimit = registry.newGauge(
        VCORE_LIMIT_METRIC_NAME, "CPU limit in number of vcores", 0);
    this.launchDurationMs = registry.newGauge(
        LAUNCH_DURATION_METRIC_NAME, "Launch duration in MS", 0L);
    this.localizationDurationMs = registry.newGauge(
        LOCALIZATION_DURATION_METRIC_NAME, "Localization duration in MS", 0L);
  }

  ContainerMetrics tag(MetricsInfo info, ContainerId containerId) {
    this.cpuVcoreLimit.set(cpuVcores);
  }

  public void recordStateChangeDurations(long launchDuration,
      long localizationDuration) {
    this.launchDurationMs.set(launchDuration);
    this.localizationDurationMs.set(localizationDuration);
  }

  private synchronized void scheduleTimerTaskIfRequired() {
    if (flushPeriodMs > 0) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/monitor/ContainerStartMonitoringEvent.java
  private final long vmemLimit;
  private final long pmemLimit;
  private final int cpuVcores;
  private final long launchDuration;
  private final long localizationDuration;

  public ContainerStartMonitoringEvent(ContainerId containerId,
      long vmemLimit, long pmemLimit, int cpuVcores, long launchDuration,
      long localizationDuration) {
    super(containerId, ContainersMonitorEventType.START_MONITORING_CONTAINER);
    this.vmemLimit = vmemLimit;
    this.pmemLimit = pmemLimit;
    this.cpuVcores = cpuVcores;
    this.launchDuration = launchDuration;
    this.localizationDuration = localizationDuration;
  }

  public long getVmemLimit() {
  public int getCpuVcores() {
    return this.cpuVcores;
  }

  public long getLaunchDuration() {
    return this.launchDuration;
  }

  public long getLocalizationDuration() {
    return this.localizationDuration;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/monitor/ContainersMonitorImpl.java
    case START_MONITORING_CONTAINER:
      ContainerStartMonitoringEvent startEvent =
          (ContainerStartMonitoringEvent) monitoringEvent;

      if (containerMetricsEnabled) {
        ContainerMetrics usageMetrics = ContainerMetrics
            .forContainer(containerId, containerMetricsPeriodMs);
        usageMetrics.recordStateChangeDurations(
            startEvent.getLaunchDuration(),
            startEvent.getLocalizationDuration());
      }

      synchronized (this.containersToBeAdded) {
        ProcessTreeInfo processTreeInfo =
            new ProcessTreeInfo(containerId, null, null,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/monitor/TestContainerMetrics.java
    int anyPmemLimit = 1024;
    int anyVmemLimit = 2048;
    int anyVcores = 10;
    long anyLaunchDuration = 20L;
    long anyLocalizationDuration = 1000L;
    String anyProcessId = "1234";

    metrics.recordResourceLimit(anyVmemLimit, anyPmemLimit, anyVcores);
    metrics.recordProcessId(anyProcessId);
    metrics.recordStateChangeDurations(anyLaunchDuration,
        anyLocalizationDuration);

    Thread.sleep(110);
    metrics.getMetrics(collector, true);
    MetricsRecords.assertMetric(record, ContainerMetrics.VMEM_LIMIT_METRIC_NAME, anyVmemLimit);
    MetricsRecords.assertMetric(record, ContainerMetrics.VCORE_LIMIT_METRIC_NAME, anyVcores);

    MetricsRecords.assertMetric(record,
        ContainerMetrics.LAUNCH_DURATION_METRIC_NAME, anyLaunchDuration);
    MetricsRecords.assertMetric(record,
        ContainerMetrics.LOCALIZATION_DURATION_METRIC_NAME,
        anyLocalizationDuration);

    collector.clear();
  }
}

