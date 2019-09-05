hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsClientConfigKeys.java
    public static final int     WINDOW_BASE_DEFAULT
        = 3000;
  }

  interface WebHdfsRetry {
    String  PREFIX = HdfsClientConfigKeys.PREFIX + "http.client.";
    String  RETRY_POLICY_ENABLED_KEY = PREFIX + "dfs.http.client.retry.policy.enabled";
    boolean RETRY_POLICY_ENABLED_DEFAULT = false;
    String  RETRY_POLICY_SPEC_KEY = PREFIX + "dfs.http.client.retry.policy.spec";
    String  RETRY_POLICY_SPEC_DEFAULT = "10000,6,60000,10"; //t1,n1,t2,n2,...
    String  FAILOVER_MAX_ATTEMPTS_KEY = PREFIX + "dfs.http.client.failover.max.attempts";
    int     FAILOVER_MAX_ATTEMPTS_DEFAULT =  15;
    String  RETRY_MAX_ATTEMPTS_KEY = PREFIX + "dfs.http.client.retry.max.attempts";
    int     RETRY_MAX_ATTEMPTS_DEFAULT = 10;
    String  FAILOVER_SLEEPTIME_BASE_KEY = PREFIX + "dfs.http.client.failover.sleep.base.millis";
    int     FAILOVER_SLEEPTIME_BASE_DEFAULT = 500;
    String  FAILOVER_SLEEPTIME_MAX_KEY = PREFIX + "dfs.http.client.failover.sleep.max.millis";
    int     FAILOVER_SLEEPTIME_MAX_DEFAULT =  15000;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  public static final long   DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_DEFAULT = 60000;

  @Deprecated
  public static final String  DFS_HTTP_CLIENT_RETRY_POLICY_ENABLED_KEY =
      HdfsClientConfigKeys.WebHdfsRetry.RETRY_POLICY_ENABLED_KEY;
  @Deprecated
  public static final boolean DFS_HTTP_CLIENT_RETRY_POLICY_ENABLED_DEFAULT =
      HdfsClientConfigKeys.WebHdfsRetry.RETRY_POLICY_ENABLED_DEFAULT;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_RETRY_POLICY_SPEC_KEY =
      HdfsClientConfigKeys.WebHdfsRetry.RETRY_POLICY_SPEC_KEY;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_RETRY_POLICY_SPEC_DEFAULT =
      HdfsClientConfigKeys.WebHdfsRetry.RETRY_POLICY_SPEC_DEFAULT;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY =
      HdfsClientConfigKeys.WebHdfsRetry.FAILOVER_MAX_ATTEMPTS_KEY;
  @Deprecated
  public static final int     DFS_HTTP_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT =
      HdfsClientConfigKeys.WebHdfsRetry.FAILOVER_MAX_ATTEMPTS_DEFAULT;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_RETRY_MAX_ATTEMPTS_KEY =
      HdfsClientConfigKeys.WebHdfsRetry.RETRY_MAX_ATTEMPTS_KEY;
  @Deprecated
  public static final int     DFS_HTTP_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT =
      HdfsClientConfigKeys.WebHdfsRetry.RETRY_MAX_ATTEMPTS_DEFAULT;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY =
      HdfsClientConfigKeys.WebHdfsRetry.FAILOVER_SLEEPTIME_BASE_KEY;
  @Deprecated
  public static final int     DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT =
      HdfsClientConfigKeys.WebHdfsRetry.FAILOVER_SLEEPTIME_BASE_DEFAULT;
  @Deprecated
  public static final String  DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY =
      HdfsClientConfigKeys.WebHdfsRetry.FAILOVER_SLEEPTIME_MAX_KEY;
  @Deprecated
  public static final int     DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT
      = HdfsClientConfigKeys.WebHdfsRetry.FAILOVER_SLEEPTIME_MAX_DEFAULT;

  public static final String  DFS_REJECT_UNRESOLVED_DN_TOPOLOGY_MAPPING_KEY = 

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsFileSystem.java
hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSClientRetries.java
    final Path dir = new Path("/testNamenodeRestart");

    if (isWebHDFS) {
      conf.setBoolean(HdfsClientConfigKeys.WebHdfsRetry.RETRY_POLICY_ENABLED_KEY, true);
    } else {
      conf.setBoolean(HdfsClientConfigKeys.Retry.POLICY_ENABLED_KEY, true);
    }

