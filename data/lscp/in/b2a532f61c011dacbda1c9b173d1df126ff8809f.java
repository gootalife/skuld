hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/UnderReplicatedBlocks.java
  synchronized void clear() {
    for (int i = 0; i < LEVEL; i++) {
      priorityQueues.get(i).clear();
    }
    corruptReplOneBlocks = 0;
    timestampsMap.clear();
  }


