hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/tools/TestDFSHAAdminMiniCluster.java
    tool.setConf(conf);
    assertEquals(0, runTool("-transitionToActive", "nn1"));
    assertEquals(0, runTool("-failover", "nn1", "nn2"));
    
    assertEquals(0, runTool("-ns", "minidfs-ns", "-failover", "nn2", "nn1"));

