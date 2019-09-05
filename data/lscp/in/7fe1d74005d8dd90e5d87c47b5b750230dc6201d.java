hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NamenodeFsck.java
        missing++;
        missize += block.getNumBytes();
      } else {
        report.append(" Live_repl=" + liveReplicas);
        if (showLocations || showRacks || showReplicaDetails) {
          StringBuilder sb = new StringBuilder("[");
          Iterable<DatanodeStorageInfo> storages = bm.getStorages(block.getLocalBlock());

