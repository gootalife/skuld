hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsFileSystem.java
  }

  public static boolean isEnabled(final Configuration conf) {
    final boolean b = conf.getBoolean(
        HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY,
        HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_DEFAULT);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNodeHttpServer.java
  }

  private void initWebHdfs(Configuration conf) throws IOException {
    if (WebHdfsFileSystem.isEnabled(conf)) {
      UserParam.setUserPattern(conf.get(
          DFSConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY,

