hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/jobhistory/JobHistoryEventHandler.java
  protected void setupEventWriter(JobId jobId, AMStartedEvent amStartedEvent)
      throws IOException {
    if (stagingDirPath == null) {
      LOG.error("Log Directory is null, returning");
    }

    MetaInfo fi = new MetaInfo(historyFile, logDirConfPath, writer,
        user, jobName, jobId, amStartedEvent.getForcedJobStateOnShutDown(),
        queueName);
    fi.getJobSummary().setJobId(jobId);
    fi.getJobSummary().setJobLaunchTime(amStartedEvent.getStartTime());
    fi.getJobSummary().setJobSubmitTime(amStartedEvent.getSubmitTime());
    fi.getJobIndexInfo().setJobStartTime(amStartedEvent.getStartTime());
    fi.getJobIndexInfo().setSubmitTime(amStartedEvent.getSubmitTime());
    fileMap.put(jobId, fi);
  }

        try {
          AMStartedEvent amStartedEvent =
              (AMStartedEvent) event.getHistoryEvent();
          setupEventWriter(event.getJobID(), amStartedEvent);
        } catch (IOException ioe) {
          LOG.error("Error JobHistoryEventHandler in handleEvent: " + event,
              ioe);
        tEvent.addEventInfo("NODE_MANAGER_HTTP_PORT",
                ase.getNodeManagerHttpPort());
        tEvent.addEventInfo("START_TIME", ase.getStartTime());
        tEvent.addEventInfo("SUBMIT_TIME", ase.getSubmitTime());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(jobId.toString());
        tEntity.setEntityType(MAPREDUCE_JOB_ENTITY_TYPE);

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/MRAppMaster.java
          new JobHistoryEvent(job.getID(), new AMStartedEvent(info
              .getAppAttemptId(), info.getStartTime(), info.getContainerId(),
              info.getNodeManagerHost(), info.getNodeManagerPort(), info
                  .getNodeManagerHttpPort(), appSubmitTime)));
    }

            .getAppAttemptId(), amInfo.getStartTime(), amInfo.getContainerId(),
            amInfo.getNodeManagerHost(), amInfo.getNodeManagerPort(), amInfo
                .getNodeManagerHttpPort(), this.forcedState == null ? null
                    : this.forcedState.toString(), appSubmitTime)));
    amInfos.add(amInfo);


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/jobhistory/TestJobHistoryEventHandler.java
    try {
      jheh.start();
      handleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(
          t.appAttemptId, 200, t.containerId, "nmhost", 3000, 4000, -1)));
      mockWriter = jheh.getEventWriter();
      verify(mockWriter).write(any(HistoryEvent.class));

    try {
      jheh.start();
      handleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(
          t.appAttemptId, 200, t.containerId, "nmhost", 3000, 4000, -1)));
      mockWriter = jheh.getEventWriter();
      verify(mockWriter).write(any(HistoryEvent.class));

    try {
      jheh.start();
      handleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(
          t.appAttemptId, 200, t.containerId, "nmhost", 3000, 4000, -1)));
      mockWriter = jheh.getEventWriter();
      verify(mockWriter).write(any(HistoryEvent.class));

    try {
      jheh.start();
      handleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(
          t.appAttemptId, 200, t.containerId, "nmhost", 3000, 4000, -1)));
      mockWriter = jheh.getEventWriter();
      verify(mockWriter).write(any(HistoryEvent.class));

    try {
      jheh.start();
      handleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(
        t.appAttemptId, 200, t.containerId, "nmhost", 3000, 4000, -1)));
      verify(jheh, times(0)).processDoneFiles(any(JobId.class));

      handleEvent(jheh, new JobHistoryEvent(t.jobId,
    try {
      jheh.start();
      handleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(
        t.appAttemptId, 200, t.containerId, "nmhost", 3000, 4000, -1)));
      verify(jheh, times(0)).processDoneFiles(t.jobId);

    try {
      jheh.start();
      handleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(
          t.appAttemptId, 200, t.containerId, "nmhost", 3000, 4000, -1)));

      handleEvent(jheh, new JobHistoryEvent(t.jobId, new JobFinishedEvent(
          TypeConverter.fromYarn(t.jobId), 0, 0, 0, 0, 0, new Counters(),
        pathStr);
  }

  @Test (timeout=50000)
  public void testAMStartedEvent() throws Exception {
    TestParams t = new TestParams();
    Configuration conf = new Configuration();

    JHEvenHandlerForTest realJheh =
        new JHEvenHandlerForTest(t.mockAppContext, 0);
    JHEvenHandlerForTest jheh = spy(realJheh);
    jheh.init(conf);

    EventWriter mockWriter = null;
    try {
      jheh.start();
      handleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(
          t.appAttemptId, 200, t.containerId, "nmhost", 3000, 4000, 100)));

      JobHistoryEventHandler.MetaInfo mi =
          JobHistoryEventHandler.fileMap.get(t.jobId);
      Assert.assertEquals(mi.getJobIndexInfo().getSubmitTime(), 100);
      Assert.assertEquals(mi.getJobIndexInfo().getJobStartTime(), 200);
      Assert.assertEquals(mi.getJobSummary().getJobSubmitTime(), 100);
      Assert.assertEquals(mi.getJobSummary().getJobLaunchTime(), 200);

      handleEvent(jheh, new JobHistoryEvent(t.jobId,
        new JobUnsuccessfulCompletionEvent(TypeConverter.fromYarn(t.jobId), 0,
          0, 0, JobStateInternal.FAILED.toString())));

      Assert.assertEquals(mi.getJobIndexInfo().getSubmitTime(), 100);
      Assert.assertEquals(mi.getJobIndexInfo().getJobStartTime(), 200);
      Assert.assertEquals(mi.getJobSummary().getJobSubmitTime(), 100);
      Assert.assertEquals(mi.getJobSummary().getJobLaunchTime(), 200);
      verify(jheh, times(1)).processDoneFiles(t.jobId);

      mockWriter = jheh.getEventWriter();
      verify(mockWriter, times(2)).write(any(HistoryEvent.class));
    } finally {
      jheh.stop();
    }
  }

  @Test (timeout=50000)
              .getTimelineStore();

      handleEvent(jheh, new JobHistoryEvent(t.jobId, new AMStartedEvent(
              t.appAttemptId, 200, t.containerId, "nmhost", 3000, 4000, -1),
              currentTime - 10));
      TimelineEntities entities = ts.getEntities("MAPREDUCE_JOB", null, null,
              null, null, null, null, null, null, null);

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/AMStartedEvent.java
public class AMStartedEvent implements HistoryEvent {
  private AMStarted datum = new AMStarted();
  private String forcedJobStateOnShutDown;
  private long submitTime;

  public AMStartedEvent(ApplicationAttemptId appAttemptId, long startTime,
      ContainerId containerId, String nodeManagerHost, int nodeManagerPort,
      int nodeManagerHttpPort, long submitTime) {
    this(appAttemptId, startTime, containerId, nodeManagerHost,
        nodeManagerPort, nodeManagerHttpPort, null, submitTime);
  }

  public AMStartedEvent(ApplicationAttemptId appAttemptId, long startTime,
      ContainerId containerId, String nodeManagerHost, int nodeManagerPort,
      int nodeManagerHttpPort, String forcedJobStateOnShutDown,
      long submitTime) {
    datum.applicationAttemptId = new Utf8(appAttemptId.toString());
    datum.startTime = startTime;
    datum.containerId = new Utf8(containerId.toString());
    datum.nodeManagerPort = nodeManagerPort;
    datum.nodeManagerHttpPort = nodeManagerHttpPort;
    this.forcedJobStateOnShutDown = forcedJobStateOnShutDown;
    this.submitTime = submitTime;
  }

  AMStartedEvent() {
    return this.forcedJobStateOnShutDown;
  }

  public long getSubmitTime() {
    return this.submitTime;
  }


  @Override

