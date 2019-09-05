hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockScanner.java
  synchronized void markSuspectBlock(String storageId, ExtendedBlock block) {
    if (!isEnabled()) {
      LOG.debug("Not scanning suspicious block {} on {}, because the block " +
          "scanner is disabled.", block, storageId);
      return;
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/VolumeScanner.java

  public synchronized void markSuspectBlock(ExtendedBlock block) {
    if (stopping) {
      LOG.debug("{}: Not scheduling suspect block {} for " +
          "rescanning, because this volume scanner is stopping.", this, block);
      return;
    }
    Boolean recent = recentSuspectBlocks.getIfPresent(block);
    if (recent != null) {
      LOG.debug("{}: Not scheduling suspect block {} for " +
          "rescanning, because we rescanned it recently.", this, block);
      return;
    }
    if (suspectBlocks.contains(block)) {
      LOG.debug("{}: suspect block {} is already queued for " +
          "rescanning.", this, block);
      return;
    }
    suspectBlocks.add(block);
    recentSuspectBlocks.put(block, true);
    LOG.debug("{}: Scheduling suspect block {} for rescanning.", this, block);
    notify(); // wake scanner thread.
  }


