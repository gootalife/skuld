hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSInputStream.java
  @Override
  public synchronized void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      DFSClient.LOG.debug("DFSInputStream has been closed already");
      return;
    }
    dfsClient.checkOpen();

