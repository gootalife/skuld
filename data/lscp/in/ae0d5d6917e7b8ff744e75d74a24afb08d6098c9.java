hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFileTruncate.java
    assertEquals(newBlock.getBlock().getGenerationStamp(),
        oldBlock.getBlock().getGenerationStamp() + 1);

    Thread.sleep(2000);
    cluster.triggerBlockReports();
    DFSTestUtil.waitReplication(fs, p, REPLICATION);

