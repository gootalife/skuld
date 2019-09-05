hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
  public static final String CLIENT_NM_CONNECT_MAX_WAIT_MS =
      YARN_PREFIX + "client.nodemanager-connect.max-wait-ms";
  public static final long DEFAULT_CLIENT_NM_CONNECT_MAX_WAIT_MS =
      3 * 60 * 1000;

  public static final String CLIENT_NM_CONNECT_RETRY_INTERVAL_MS =

