hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapred/YarnOutputFiles.java
  public Path getSpillFileForWrite(int spillNumber, long size)
      throws IOException {
    return lDirAlloc.getLocalPathForWrite(
        String.format(SPILL_FILE_PATTERN,
            conf.get(JobContext.TASK_ATTEMPT_ID), spillNumber), size, conf);
  }


