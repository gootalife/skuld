hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSUtil.java
  @VisibleForTesting
  static String substituteForWildcardAddress(String configuredAddress,
    String defaultHost) {
    InetSocketAddress sockAddr = NetUtils.createSocketAddr(configuredAddress);
    final InetAddress addr = sockAddr.getAddress();
    if (addr != null && addr.isAnyLocalAddress()) {
      return defaultHost + ":" + sockAddr.getPort();
    } else {
      return configuredAddress;

