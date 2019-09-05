hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockStatsMXBean.java
  public void testStorageTypeStatsJMX() throws Exception {
    URL baseUrl = new URL (cluster.getHttpUri(0));
    String result = readOutput(new URL(baseUrl, "/jmx"));

    Map<String, Object> stat = (Map<String, Object>) JSON.parse(result);
    Object[] beans =(Object[]) stat.get("beans");

