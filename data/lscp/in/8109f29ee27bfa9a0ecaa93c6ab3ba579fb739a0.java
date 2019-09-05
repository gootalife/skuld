hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DNConf.java
        DFS_BLOCKREPORT_INITIAL_DELAY_DEFAULT) * 1000L;
    if (initBRDelay >= blockReportInterval) {
      initBRDelay = 0;
      DataNode.LOG.info("dfs.blockreport.initialDelay is "
          + "greater than or equal to" + "dfs.blockreport.intervalMsec."
          + " Setting initial delay to 0 msec:");
    }
    initialBlockReportDelay = initBRDelay;
    

