hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetImpl.java
  private static class VolumeInfo {
    final String directory;
    final long usedSpace; // size of space used by HDFS
    final long freeSpace; // size of free space excluding reserved space
    final long reservedSpace; // size of space reserved for non-HDFS and RBW

    VolumeInfo(FsVolumeImpl v, long usedSpace, long freeSpace) {
      this.directory = v.toString();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsVolumeImpl.java
  }
  
  @VisibleForTesting
  public long getCapacity() {
    this.configuredCapacity = capacity;
  }

  @Override
  public long getAvailable() throws IOException {
    long remaining = getCapacity() - getDfsUsed() - reservedForRbw.get();
    long available = usage.getAvailable() - reserved - reservedForRbw.get();
    if (remaining > available) {
      remaining = available;
    }

