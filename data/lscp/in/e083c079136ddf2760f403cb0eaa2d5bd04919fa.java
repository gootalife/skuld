hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirDeleteOp.java
  static BlocksMapUpdateInfo delete(
      FSNamesystem fsn, String src, boolean recursive, boolean logRetryCache)
  static BlocksMapUpdateInfo deleteInternal(
      FSNamesystem fsn, String src, INodesInPath iip, boolean logRetryCache)

