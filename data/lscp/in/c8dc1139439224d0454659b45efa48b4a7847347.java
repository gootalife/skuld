hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/top/window/RollingWindowManager.java
  public TopWindow snapshot(long time) {
    TopWindow window = new TopWindow(windowLenMs);
    Set<String> metricNames = metricMap.keySet();
    LOG.debug("iterating in reported metrics, size={} values={}",
        metricNames.size(), metricNames);
    for (Map.Entry<String, RollingWindowMap> entry : metricMap.entrySet()) {
      String metricName = entry.getKey();
      RollingWindowMap rollingWindows = entry.getValue();

