hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ha/ZKFailoverController.java
  };
  
  protected static final String USAGE = 
      "Usage: hdfs zkfc [ -formatZK [-force] [-nonInteractive] ]";

  static final int ERR_CODE_FORMAT_DENIED = 2;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNode.java
    DFS_HA_AUTO_FAILOVER_ENABLED_KEY
  };
  
  private static final String USAGE = "Usage: hdfs namenode ["
      + StartupOption.BACKUP.getName() + "] | \n\t["
      + StartupOption.CHECKPOINT.getName() + "] | \n\t["
      + StartupOption.FORMAT.getName() + " ["

