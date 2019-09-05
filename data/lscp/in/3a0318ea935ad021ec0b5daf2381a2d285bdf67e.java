hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
      String src, BlockInfo[] blocks) {
    for (BlockInfo b: blocks) {
      if (!b.isComplete()) {
        final int numNodes = b.numNodes();
        final int min = getMinStorageNum(b);
        final BlockUCState state = b.getBlockUCState();

