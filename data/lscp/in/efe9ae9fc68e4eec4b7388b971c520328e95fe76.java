hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLog.java
  }

  void logEdit(final FSEditLogOp op) {
    boolean needsSync = false;
    synchronized (this) {
      assert isOpenForWrite() :
        "bad state: " + state;
      endTransaction(start);
      
      needsSync = shouldForceSync();
      if (needsSync) {
        isAutoSyncScheduled = true;
      }
    }
    
    if (needsSync) {
      logSync();
    }
  }


  synchronized long rollEditLog(int layoutVersion) throws IOException {
    LOG.info("Rolling edit logs");

