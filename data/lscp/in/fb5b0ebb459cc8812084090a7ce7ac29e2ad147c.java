hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/task/reduce/ShuffleSchedulerImpl.java
      failureCounts.put(mapId, new IntWritable(1));
    }
    String hostname = host.getHostName();
    IntWritable hostFailedNum = hostFailures.get(hostname);
    if (hostFailedNum == null) {
      hostFailures.put(hostname, new IntWritable(1));
    }
    boolean hostFail = hostFailures.get(hostname).get() >
        getMaxHostFailures() ? true : false;

    if (failures >= abortFailureLimit) {
      try {

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/test/java/org/apache/hadoop/mapreduce/task/reduce/TestShuffleScheduler.java
    Assert.assertEquals(copyMessage(10, 1, 2), progress.toString());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public <K, V> void TestSucceedAndFailedCopyMap() throws Exception {
    JobConf job = new JobConf();
    job.setNumMapTasks(2);
    TaskUmbilicalProtocol mockUmbilical = mock(TaskUmbilicalProtocol.class);
    Reporter mockReporter = mock(Reporter.class);
    FileSystem mockFileSystem = mock(FileSystem.class);
    Class<? extends org.apache.hadoop.mapred.Reducer>  combinerClass = job.getCombinerClass();
    @SuppressWarnings("unchecked")  // needed for mock with generic
    CombineOutputCollector<K, V>  mockCombineOutputCollector =
        (CombineOutputCollector<K, V>) mock(CombineOutputCollector.class);
    org.apache.hadoop.mapreduce.TaskAttemptID mockTaskAttemptID =
        mock(org.apache.hadoop.mapreduce.TaskAttemptID.class);
    LocalDirAllocator mockLocalDirAllocator = mock(LocalDirAllocator.class);
    CompressionCodec mockCompressionCodec = mock(CompressionCodec.class);
    Counter mockCounter = mock(Counter.class);
    TaskStatus mockTaskStatus = mock(TaskStatus.class);
    Progress mockProgress = mock(Progress.class);
    MapOutputFile mockMapOutputFile = mock(MapOutputFile.class);
    Task mockTask = mock(Task.class);
    @SuppressWarnings("unchecked")
    MapOutput<K, V> output = mock(MapOutput.class);

    ShuffleConsumerPlugin.Context<K, V> context =
        new ShuffleConsumerPlugin.Context<K, V>(
            mockTaskAttemptID, job, mockFileSystem,
            mockUmbilical, mockLocalDirAllocator,
            mockReporter, mockCompressionCodec,
            combinerClass, mockCombineOutputCollector,
            mockCounter, mockCounter, mockCounter,
            mockCounter, mockCounter, mockCounter,
            mockTaskStatus, mockProgress, mockProgress,
            mockTask, mockMapOutputFile, null);
    TaskStatus status = new TaskStatus() {
      @Override
      public boolean getIsMap() {
        return false;
      }
      @Override
      public void addFetchFailedMap(TaskAttemptID mapTaskId) {
      }
    };
    Progress progress = new Progress();
    ShuffleSchedulerImpl<K, V> scheduler = new ShuffleSchedulerImpl<K, V>(job,
        status, null, null, progress, context.getShuffledMapsCounter(),
        context.getReduceShuffleBytes(), context.getFailedShuffleCounter());

    MapHost host1 = new MapHost("host1", null);
    TaskAttemptID failedAttemptID = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
        new JobID("test",0), TaskType.MAP, 0), 0);

    TaskAttemptID succeedAttemptID = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
        new JobID("test",0), TaskType.MAP, 1), 1);

    scheduler.hostFailed(host1.getHostName());

    long bytes = (long)500 * 1024 * 1024;
    scheduler.copySucceeded(succeedAttemptID, host1, bytes, 0, 500000, output);

    scheduler.copyFailed(failedAttemptID, host1, true, false);
  }

  private static String copyMessage(int attemptNo, double rate1, double rate2) {
    int attemptZero = attemptNo - 1;
    return String.format("copy task(attempt_test_0000_m_%06d_%d succeeded at %1.2f MB/s)"

