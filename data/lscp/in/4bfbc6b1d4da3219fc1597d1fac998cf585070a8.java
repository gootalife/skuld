hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java
    LogFactory.getLog(DataNode.class.getName() + ".clienttrace");
  
  private static final String USAGE =
      "Usage: hdfs datanode [-regular | -rollback | -rollingupgrade rollback" +
      " ]\n" +
      "    -regular                 : Normal DataNode startup (default).\n" +
      "    -rollback                : Rollback a standard or rolling upgrade.\n" +
      "    -rollingupgrade rollback : Rollback a rolling upgrade operation.\n" +
      "  Refer to HDFS documentation for the difference between standard\n" +
      "  and rolling upgrades.";


