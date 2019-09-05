hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlocksMap.java
  }

  int size() {
    if (blocks != null) {
      return blocks.size();
    } else {
      return 0;
    }
  }

  Iterable<BlockInfo> getBlocks() {

