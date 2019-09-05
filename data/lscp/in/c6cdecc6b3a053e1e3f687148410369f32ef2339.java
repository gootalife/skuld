hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/jobhistory/TestEvents.java
        new ByteArrayInputStream(getEvents())));
    HistoryEvent e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.JOB_PRIORITY_CHANGED));
    assertEquals("ID", ((JobPriorityChange) e.getDatum()).getJobid().toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.JOB_STATUS_CHANGED));
    assertEquals("ID", ((JobStatusChanged) e.getDatum()).getJobid().toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.TASK_UPDATED));
    assertEquals("ID", ((TaskUpdated) e.getDatum()).getTaskid().toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_KILLED));
    assertEquals(taskId,
        ((TaskAttemptUnsuccessfulCompletion) e.getDatum()).getTaskid().toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.JOB_KILLED));
    assertEquals("ID",
        ((JobUnsuccessfulCompletion) e.getDatum()).getJobid().toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_STARTED));
    assertEquals(taskId,
        ((TaskAttemptStarted) e.getDatum()).getTaskid().toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_FINISHED));
    assertEquals(taskId,
        ((TaskAttemptFinished) e.getDatum()).getTaskid().toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_KILLED));
    assertEquals(taskId,
        ((TaskAttemptUnsuccessfulCompletion) e.getDatum()).getTaskid().toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_KILLED));
    assertEquals(taskId,
        ((TaskAttemptUnsuccessfulCompletion) e.getDatum()).getTaskid().toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_STARTED));
    assertEquals(taskId,
        ((TaskAttemptStarted) e.getDatum()).getTaskid().toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_FINISHED));
    assertEquals(taskId,
        ((TaskAttemptFinished) e.getDatum()).getTaskid().toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_KILLED));
    assertEquals(taskId,
        ((TaskAttemptUnsuccessfulCompletion) e.getDatum()).getTaskid().toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_KILLED));
    assertEquals(taskId,
        ((TaskAttemptUnsuccessfulCompletion) e.getDatum()).getTaskid().toString());

    reader.close();
  }

  private TaskAttemptUnsuccessfulCompletion getTaskAttemptUnsuccessfulCompletion() {
    TaskAttemptUnsuccessfulCompletion datum = new TaskAttemptUnsuccessfulCompletion();
    datum.setAttemptId("attempt_1_2_r3_4_5");
    datum.setClockSplits(Arrays.asList(1, 2, 3));
    datum.setCpuUsages(Arrays.asList(100, 200, 300));
    datum.setError("Error");
    datum.setFinishTime(2L);
    datum.setHostname("hostname");
    datum.setRackname("rackname");
    datum.setPhysMemKbytes(Arrays.asList(1000, 2000, 3000));
    datum.setTaskid(taskId);
    datum.setPort(1000);
    datum.setTaskType("REDUCE");
    datum.setStatus("STATUS");
    datum.setCounters(getCounters());
    datum.setVMemKbytes(Arrays.asList(1000, 2000, 3000));
    return datum;
  }

  private JhCounters getCounters() {
    JhCounters counters = new JhCounters();
    counters.setGroups(new ArrayList<JhCounterGroup>(0));
    counters.setName("name");
    return counters;
  }

  private FakeEvent getCleanupAttemptFinishedEvent() {
    FakeEvent result = new FakeEvent(EventType.CLEANUP_ATTEMPT_FINISHED);
    TaskAttemptFinished datum = new TaskAttemptFinished();
    datum.setAttemptId("attempt_1_2_r3_4_5");

    datum.setCounters(getCounters());
    datum.setFinishTime(2L);
    datum.setHostname("hostname");
    datum.setRackname("rackName");
    datum.setState("state");
    datum.setTaskid(taskId);
    datum.setTaskStatus("taskStatus");
    datum.setTaskType("REDUCE");
    result.setDatum(datum);
    return result;
  }
    FakeEvent result = new FakeEvent(EventType.CLEANUP_ATTEMPT_STARTED);
    TaskAttemptStarted datum = new TaskAttemptStarted();

    datum.setAttemptId("attempt_1_2_r3_4_5");
    datum.setAvataar("avatar");
    datum.setContainerId("containerId");
    datum.setHttpPort(10000);
    datum.setLocality("locality");
    datum.setShufflePort(10001);
    datum.setStartTime(1L);
    datum.setTaskid(taskId);
    datum.setTaskType("taskType");
    datum.setTrackerName("trackerName");
    result.setDatum(datum);
    return result;
  }
    FakeEvent result = new FakeEvent(EventType.SETUP_ATTEMPT_FINISHED);
    TaskAttemptFinished datum = new TaskAttemptFinished();

    datum.setAttemptId("attempt_1_2_r3_4_5");
    datum.setCounters(getCounters());
    datum.setFinishTime(2L);
    datum.setHostname("hostname");
    datum.setRackname("rackname");
    datum.setState("state");
    datum.setTaskid(taskId);
    datum.setTaskStatus("taskStatus");
    datum.setTaskType("REDUCE");
    result.setDatum(datum);
    return result;
  }
  private FakeEvent getSetupAttemptStartedEvent() {
    FakeEvent result = new FakeEvent(EventType.SETUP_ATTEMPT_STARTED);
    TaskAttemptStarted datum = new TaskAttemptStarted();
    datum.setAttemptId("ID");
    datum.setAvataar("avataar");
    datum.setContainerId("containerId");
    datum.setHttpPort(10000);
    datum.setLocality("locality");
    datum.setShufflePort(10001);
    datum.setStartTime(1L);
    datum.setTaskid(taskId);
    datum.setTaskType("taskType");
    datum.setTrackerName("trackerName");
    result.setDatum(datum);
    return result;
  }
  private FakeEvent getJobPriorityChangedEvent() {
    FakeEvent result = new FakeEvent(EventType.JOB_PRIORITY_CHANGED);
    JobPriorityChange datum = new JobPriorityChange();
    datum.setJobid("ID");
    datum.setPriority("priority");
    result.setDatum(datum);
    return result;
  }
  private FakeEvent getJobStatusChangedEvent() {
    FakeEvent result = new FakeEvent(EventType.JOB_STATUS_CHANGED);
    JobStatusChanged datum = new JobStatusChanged();
    datum.setJobid("ID");
    datum.setJobStatus("newStatus");
    result.setDatum(datum);
    return result;
  }
  private FakeEvent getTaskUpdatedEvent() {
    FakeEvent result = new FakeEvent(EventType.TASK_UPDATED);
    TaskUpdated datum = new TaskUpdated();
    datum.setFinishTime(2L);
    datum.setTaskid("ID");
    result.setDatum(datum);
    return result;
  }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/jobhistory/TestJobSummary.java
    JobId mockJobId = mock(JobId.class);
    when(mockJobId.toString()).thenReturn("testJobId");
    summary.setJobId(mockJobId);
    summary.setJobSubmitTime(2L);
    summary.setJobLaunchTime(3L);
    summary.setFirstMapTaskLaunchTime(4L);
    summary.setFirstReduceTaskLaunchTime(5L);
    summary.setJobFinishTime(6L);
    summary.setNumFinishedMaps(1);
    summary.setNumFailedMaps(0);
    summary.setNumFinishedReduces(1);

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/AMStartedEvent.java
      ContainerId containerId, String nodeManagerHost, int nodeManagerPort,
      int nodeManagerHttpPort, String forcedJobStateOnShutDown,
      long submitTime) {
    datum.setApplicationAttemptId(new Utf8(appAttemptId.toString()));
    datum.setStartTime(startTime);
    datum.setContainerId(new Utf8(containerId.toString()));
    datum.setNodeManagerHost(new Utf8(nodeManagerHost));
    datum.setNodeManagerPort(nodeManagerPort);
    datum.setNodeManagerHttpPort(nodeManagerHttpPort);
    this.forcedJobStateOnShutDown = forcedJobStateOnShutDown;
    this.submitTime = submitTime;
  }
  public ApplicationAttemptId getAppAttemptId() {
    return ConverterUtils.toApplicationAttemptId(datum.getApplicationAttemptId()
        .toString());
  }

  public long getStartTime() {
    return datum.getStartTime();
  }

  public ContainerId getContainerId() {
    return ConverterUtils.toContainerId(datum.getContainerId().toString());
  }

  public String getNodeManagerHost() {
    return datum.getNodeManagerHost().toString();
  }

  public int getNodeManagerPort() {
    return datum.getNodeManagerPort();
  }
  
  public int getNodeManagerHttpPort() {
    return datum.getNodeManagerHttpPort();
  }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/AvroArrayUtils.java
  }

  public static int[] fromAvro(List<Integer> avro) {
    int[] result = new int[avro.size()];

    int i = 0;
      

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/EventReader.java
    }

    Schema myschema = new SpecificData(Event.class.getClassLoader()).getSchema(Event.class);
    Schema.Parser parser = new Schema.Parser();
    this.schema = parser.parse(in.readLine());
    this.reader = new SpecificDatumReader(schema, myschema);
    this.decoder = DecoderFactory.get().jsonDecoder(schema, in);
  }
      return null;
    }
    HistoryEvent result;
    switch (wrapper.getType()) {
    case JOB_SUBMITTED:
      result = new JobSubmittedEvent(); break;
    case JOB_INITED:
    case AM_STARTED:
      result = new AMStartedEvent(); break;
    default:
      throw new RuntimeException("unexpected event type: " + wrapper.getType());
    }
    result.setDatum(wrapper.getEvent());
    return result;
  }

  static Counters fromAvro(JhCounters counters) {
    Counters result = new Counters();
    if(counters != null) {
      for (JhCounterGroup g : counters.getGroups()) {
        CounterGroup group =
            result.addGroup(StringInterner.weakIntern(g.getName().toString()),
                StringInterner.weakIntern(g.getDisplayName().toString()));
        for (JhCounter c : g.getCounts()) {
          group.addCounter(StringInterner.weakIntern(c.getName().toString()),
              StringInterner.weakIntern(c.getDisplayName().toString()),
                  c.getValue());
        }
      }
    }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/EventWriter.java
  
  synchronized void write(HistoryEvent event) throws IOException { 
    Event wrapper = new Event();
    wrapper.setType(event.getEventType());
    wrapper.setEvent(event.getDatum());
    writer.write(wrapper, encoder);
    encoder.flush();
    out.writeBytes("\n");
  }
  static JhCounters toAvro(Counters counters, String name) {
    JhCounters result = new JhCounters();
    result.setName(new Utf8(name));
    result.setGroups(new ArrayList<JhCounterGroup>(0));
    if (counters == null) return result;
    for (CounterGroup group : counters) {
      JhCounterGroup g = new JhCounterGroup();
      g.setName(new Utf8(group.getName()));
      g.setDisplayName(new Utf8(group.getDisplayName()));
      g.setCounts(new ArrayList<JhCounter>(group.size()));
      for (Counter counter : group) {
        JhCounter c = new JhCounter();
        c.setName(new Utf8(counter.getName()));
        c.setDisplayName(new Utf8(counter.getDisplayName()));
        c.setValue(counter.getValue());
        g.getCounts().add(c);
      }
      result.getGroups().add(g);
    }
    return result;
  }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/JobFinishedEvent.java
  public Object getDatum() {
    if (datum == null) {
      datum = new JobFinished();
      datum.setJobid(new Utf8(jobId.toString()));
      datum.setFinishTime(finishTime);
      datum.setFinishedMaps(finishedMaps);
      datum.setFinishedReduces(finishedReduces);
      datum.setFailedMaps(failedMaps);
      datum.setFailedReduces(failedReduces);
      datum.setMapCounters(EventWriter.toAvro(mapCounters, "MAP_COUNTERS"));
      datum.setReduceCounters(EventWriter.toAvro(reduceCounters,
          "REDUCE_COUNTERS"));
      datum.setTotalCounters(EventWriter.toAvro(totalCounters,
          "TOTAL_COUNTERS"));
    }
    return datum;
  }

  public void setDatum(Object oDatum) {
    this.datum = (JobFinished) oDatum;
    this.jobId = JobID.forName(datum.getJobid().toString());
    this.finishTime = datum.getFinishTime();
    this.finishedMaps = datum.getFinishedMaps();
    this.finishedReduces = datum.getFinishedReduces();
    this.failedMaps = datum.getFailedMaps();
    this.failedReduces = datum.getFailedReduces();
    this.mapCounters = EventReader.fromAvro(datum.getMapCounters());
    this.reduceCounters = EventReader.fromAvro(datum.getReduceCounters());
    this.totalCounters = EventReader.fromAvro(datum.getTotalCounters());
  }

  public EventType getEventType() {

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/JobHistoryParser.java
    public List<AMInfo> getAMInfos() { return amInfos; }
    public AMInfo getLatestAMInfo() { return latestAmInfo; }
  }
  

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/JobInfoChangeEvent.java
  public JobInfoChangeEvent(JobID id, long submitTime, long launchTime) {
    datum.setJobid(new Utf8(id.toString()));
    datum.setSubmitTime(submitTime);
    datum.setLaunchTime(launchTime);
  }

  JobInfoChangeEvent() { }
  }

  public JobID getJobId() { return JobID.forName(datum.getJobid().toString()); }
  public long getSubmitTime() { return datum.getSubmitTime(); }
  public long getLaunchTime() { return datum.getLaunchTime(); }

  public EventType getEventType() {
    return EventType.JOB_INFO_CHANGED;

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/JobInitedEvent.java
  public JobInitedEvent(JobID id, long launchTime, int totalMaps,
                        int totalReduces, String jobStatus, boolean uberized) {
    datum.setJobid(new Utf8(id.toString()));
    datum.setLaunchTime(launchTime);
    datum.setTotalMaps(totalMaps);
    datum.setTotalReduces(totalReduces);
    datum.setJobStatus(new Utf8(jobStatus));
    datum.setUberized(uberized);
  }

  JobInitedEvent() { }
  public void setDatum(Object datum) { this.datum = (JobInited)datum; }

  public JobID getJobId() { return JobID.forName(datum.getJobid().toString()); }
  public long getLaunchTime() { return datum.getLaunchTime(); }
  public int getTotalMaps() { return datum.getTotalMaps(); }
  public int getTotalReduces() { return datum.getTotalReduces(); }
  public String getStatus() { return datum.getJobStatus().toString(); }
  public EventType getEventType() {
    return EventType.JOB_INITED;
  }
  public boolean getUberized() { return datum.getUberized(); }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/JobPriorityChangeEvent.java
  public JobPriorityChangeEvent(JobID id, JobPriority priority) {
    datum.setJobid(new Utf8(id.toString()));
    datum.setPriority(new Utf8(priority.name()));
  }

  JobPriorityChangeEvent() { }
  }

  public JobID getJobId() {
    return JobID.forName(datum.getJobid().toString());
  }
  public JobPriority getPriority() {
    return JobPriority.valueOf(datum.getPriority().toString());
  }
  public EventType getEventType() {

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/JobStatusChangedEvent.java
  public JobStatusChangedEvent(JobID id, String jobStatus) {
    datum.setJobid(new Utf8(id.toString()));
    datum.setJobStatus(new Utf8(jobStatus));
  }

  JobStatusChangedEvent() {}
  }

  public JobID getJobId() { return JobID.forName(datum.getJobid().toString()); }
  public String getStatus() { return datum.getJobStatus().toString(); }
  public EventType getEventType() {
    return EventType.JOB_STATUS_CHANGED;

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/JobSubmittedEvent.java
        Map<JobACL, AccessControlList> jobACLs, String jobQueueName,
        String workflowId, String workflowName, String workflowNodeName,
        String workflowAdjacencies, String workflowTags) {
    datum.setJobid(new Utf8(id.toString()));
    datum.setJobName(new Utf8(jobName));
    datum.setUserName(new Utf8(userName));
    datum.setSubmitTime(submitTime);
    datum.setJobConfPath(new Utf8(jobConfPath));
    Map<CharSequence, CharSequence> jobAcls = new HashMap<CharSequence, CharSequence>();
    for (Entry<JobACL, AccessControlList> entry : jobACLs.entrySet()) {
      jobAcls.put(new Utf8(entry.getKey().getAclName()), new Utf8(
          entry.getValue().getAclString()));
    }
    datum.setAcls(jobAcls);
    if (jobQueueName != null) {
      datum.setJobQueueName(new Utf8(jobQueueName));
    }
    if (workflowId != null) {
      datum.setWorkflowId(new Utf8(workflowId));
    }
    if (workflowName != null) {
      datum.setWorkflowName(new Utf8(workflowName));
    }
    if (workflowNodeName != null) {
      datum.setWorkflowNodeName(new Utf8(workflowNodeName));
    }
    if (workflowAdjacencies != null) {
      datum.setWorkflowAdjacencies(new Utf8(workflowAdjacencies));
    }
    if (workflowTags != null) {
      datum.setWorkflowTags(new Utf8(workflowTags));
    }
  }

  }

  public JobID getJobId() { return JobID.forName(datum.getJobid().toString()); }
  public String getJobName() { return datum.getJobName().toString(); }
  public String getJobQueueName() {
    if (datum.getJobQueueName() != null) {
      return datum.getJobQueueName().toString();
    }
    return null;
  }
  public String getUserName() { return datum.getUserName().toString(); }
  public long getSubmitTime() { return datum.getSubmitTime(); }
  public String getJobConfPath() { return datum.getJobConfPath().toString(); }
  public Map<JobACL, AccessControlList> getJobAcls() {
    Map<JobACL, AccessControlList> jobAcls =
        new HashMap<JobACL, AccessControlList>();
    for (JobACL jobACL : JobACL.values()) {
      Utf8 jobACLsUtf8 = new Utf8(jobACL.getAclName());
      if (datum.getAcls().containsKey(jobACLsUtf8)) {
        jobAcls.put(jobACL, new AccessControlList(datum.getAcls().get(
            jobACLsUtf8).toString()));
      }
    }
  }
  public String getWorkflowId() {
    if (datum.getWorkflowId() != null) {
      return datum.getWorkflowId().toString();
    }
    return null;
  }
  public String getWorkflowName() {
    if (datum.getWorkflowName() != null) {
      return datum.getWorkflowName().toString();
    }
    return null;
  }
  public String getWorkflowNodeName() {
    if (datum.getWorkflowNodeName() != null) {
      return datum.getWorkflowNodeName().toString();
    }
    return null;
  }
  public String getWorkflowAdjacencies() {
    if (datum.getWorkflowAdjacencies() != null) {
      return datum.getWorkflowAdjacencies().toString();
    }
    return null;
  }
  public String getWorkflowTags() {
    if (datum.getWorkflowTags() != null) {
      return datum.getWorkflowTags().toString();
    }
    return null;
  }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/JobUnsuccessfulCompletionEvent.java
  }

  public JobID getJobId() {
    return JobID.forName(datum.getJobid().toString());
  }
  public long getFinishTime() { return datum.getFinishTime(); }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/MapAttemptFinishedEvent.java
  public Object getDatum() {
    if (datum == null) {
      datum = new MapAttemptFinished();
      datum.setTaskid(new Utf8(attemptId.getTaskID().toString()));
      datum.setAttemptId(new Utf8(attemptId.toString()));
      datum.setTaskType(new Utf8(taskType.name()));
      datum.setTaskStatus(new Utf8(taskStatus));
      datum.setMapFinishTime(mapFinishTime);
      datum.setFinishTime(finishTime);
      datum.setHostname(new Utf8(hostname));
      datum.setPort(port);
      if (rackName != null) {
        datum.setRackname(new Utf8(rackName));
      }
      datum.setState(new Utf8(state));
      datum.setCounters(EventWriter.toAvro(counters));

      datum.setClockSplits(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetWallclockTime(allSplits)));
      datum.setCpuUsages(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetCPUTime(allSplits)));
      datum.setVMemKbytes(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetVMemKbytes(allSplits)));
      datum.setPhysMemKbytes(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetPhysMemKbytes(allSplits)));
    }
    return datum;
  }

  public void setDatum(Object oDatum) {
    this.datum = (MapAttemptFinished)oDatum;
    this.attemptId = TaskAttemptID.forName(datum.getAttemptId().toString());
    this.taskType = TaskType.valueOf(datum.getTaskType().toString());
    this.taskStatus = datum.getTaskStatus().toString();
    this.mapFinishTime = datum.getMapFinishTime();
    this.finishTime = datum.getFinishTime();
    this.hostname = datum.getHostname().toString();
    this.rackName = datum.getRackname().toString();
    this.port = datum.getPort();
    this.state = datum.getState().toString();
    this.counters = EventReader.fromAvro(datum.getCounters());
    this.clockSplits = AvroArrayUtils.fromAvro(datum.getClockSplits());
    this.cpuUsages = AvroArrayUtils.fromAvro(datum.getCpuUsages());
    this.vMemKbytes = AvroArrayUtils.fromAvro(datum.getVMemKbytes());
    this.physMemKbytes = AvroArrayUtils.fromAvro(datum.getPhysMemKbytes());
  }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/ReduceAttemptFinishedEvent.java
  public Object getDatum() {
    if (datum == null) {
      datum = new ReduceAttemptFinished();
      datum.setTaskid(new Utf8(attemptId.getTaskID().toString()));
      datum.setAttemptId(new Utf8(attemptId.toString()));
      datum.setTaskType(new Utf8(taskType.name()));
      datum.setTaskStatus(new Utf8(taskStatus));
      datum.setShuffleFinishTime(shuffleFinishTime);
      datum.setSortFinishTime(sortFinishTime);
      datum.setFinishTime(finishTime);
      datum.setHostname(new Utf8(hostname));
      datum.setPort(port);
      if (rackName != null) {
        datum.setRackname(new Utf8(rackName));
      }
      datum.setState(new Utf8(state));
      datum.setCounters(EventWriter.toAvro(counters));

      datum.setClockSplits(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetWallclockTime(allSplits)));
      datum.setCpuUsages(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetCPUTime(allSplits)));
      datum.setVMemKbytes(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetVMemKbytes(allSplits)));
      datum.setPhysMemKbytes(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetPhysMemKbytes(allSplits)));
    }
    return datum;
  }

  public void setDatum(Object oDatum) {
    this.datum = (ReduceAttemptFinished)oDatum;
    this.attemptId = TaskAttemptID.forName(datum.getAttemptId().toString());
    this.taskType = TaskType.valueOf(datum.getTaskType().toString());
    this.taskStatus = datum.getTaskStatus().toString();
    this.shuffleFinishTime = datum.getShuffleFinishTime();
    this.sortFinishTime = datum.getSortFinishTime();
    this.finishTime = datum.getFinishTime();
    this.hostname = datum.getHostname().toString();
    this.rackName = datum.getRackname().toString();
    this.port = datum.getPort();
    this.state = datum.getState().toString();
    this.counters = EventReader.fromAvro(datum.getCounters());
    this.clockSplits = AvroArrayUtils.fromAvro(datum.getClockSplits());
    this.cpuUsages = AvroArrayUtils.fromAvro(datum.getCpuUsages());
    this.vMemKbytes = AvroArrayUtils.fromAvro(datum.getVMemKbytes());
    this.physMemKbytes = AvroArrayUtils.fromAvro(datum.getPhysMemKbytes());
  }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/TaskAttemptFinishedEvent.java
  public Object getDatum() {
    if (datum == null) {
      datum = new TaskAttemptFinished();
      datum.setTaskid(new Utf8(attemptId.getTaskID().toString()));
      datum.setAttemptId(new Utf8(attemptId.toString()));
      datum.setTaskType(new Utf8(taskType.name()));
      datum.setTaskStatus(new Utf8(taskStatus));
      datum.setFinishTime(finishTime);
      if (rackName != null) {
        datum.setRackname(new Utf8(rackName));
      }
      datum.setHostname(new Utf8(hostname));
      datum.setState(new Utf8(state));
      datum.setCounters(EventWriter.toAvro(counters));
    }
    return datum;
  }
  public void setDatum(Object oDatum) {
    this.datum = (TaskAttemptFinished)oDatum;
    this.attemptId = TaskAttemptID.forName(datum.getAttemptId().toString());
    this.taskType = TaskType.valueOf(datum.getTaskType().toString());
    this.taskStatus = datum.getTaskStatus().toString();
    this.finishTime = datum.getFinishTime();
    this.rackName = datum.getRackname().toString();
    this.hostname = datum.getHostname().toString();
    this.state = datum.getState().toString();
    this.counters = EventReader.fromAvro(datum.getCounters());
  }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/TaskAttemptStartedEvent.java
      TaskType taskType, long startTime, String trackerName,
      int httpPort, int shufflePort, ContainerId containerId,
      String locality, String avataar) {
    datum.setAttemptId(new Utf8(attemptId.toString()));
    datum.setTaskid(new Utf8(attemptId.getTaskID().toString()));
    datum.setStartTime(startTime);
    datum.setTaskType(new Utf8(taskType.name()));
    datum.setTrackerName(new Utf8(trackerName));
    datum.setHttpPort(httpPort);
    datum.setShufflePort(shufflePort);
    datum.setContainerId(new Utf8(containerId.toString()));
    if (locality != null) {
      datum.setLocality(new Utf8(locality));
    }
    if (avataar != null) {
      datum.setAvataar(new Utf8(avataar));
    }
  }

      long startTime, String trackerName, int httpPort, int shufflePort,
      String locality, String avataar) {
    this(attemptId, taskType, startTime, trackerName, httpPort, shufflePort,
        ConverterUtils.toContainerId("container_-1_-1_-1_-1"), locality,
            avataar);
  }

  TaskAttemptStartedEvent() {}
  }

  public TaskID getTaskId() {
    return TaskID.forName(datum.getTaskid().toString());
  }
  public String getTrackerName() { return datum.getTrackerName().toString(); }
  public long getStartTime() { return datum.getStartTime(); }
  public TaskType getTaskType() {
    return TaskType.valueOf(datum.getTaskType().toString());
  }
  public int getHttpPort() { return datum.getHttpPort(); }
  public int getShufflePort() { return datum.getShufflePort(); }
  public TaskAttemptID getTaskAttemptId() {
    return TaskAttemptID.forName(datum.getAttemptId().toString());
  }
  public EventType getEventType() {
  }
  public ContainerId getContainerId() {
    return ConverterUtils.toContainerId(datum.getContainerId().toString());
  }
  public String getLocality() {
    if (datum.getLocality() != null) {
      return datum.getLocality().toString();
    }
    return null;
  }
  public String getAvataar() {
    if (datum.getAvataar() != null) {
      return datum.getAvataar().toString();
    }
    return null;
  }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/TaskAttemptUnsuccessfulCompletionEvent.java
  public Object getDatum() {
    if(datum == null) {
      datum = new TaskAttemptUnsuccessfulCompletion();
      datum.setTaskid(new Utf8(attemptId.getTaskID().toString()));
      datum.setTaskType(new Utf8(taskType.name()));
      datum.setAttemptId(new Utf8(attemptId.toString()));
      datum.setFinishTime(finishTime);
      datum.setHostname(new Utf8(hostname));
      if (rackName != null) {
        datum.setRackname(new Utf8(rackName));
      }
      datum.setPort(port);
      datum.setError(new Utf8(error));
      datum.setStatus(new Utf8(status));

      datum.setCounters(EventWriter.toAvro(counters));

      datum.setClockSplits(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetWallclockTime(allSplits)));
      datum.setCpuUsages(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetCPUTime(allSplits)));
      datum.setVMemKbytes(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetVMemKbytes(allSplits)));
      datum.setPhysMemKbytes(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetPhysMemKbytes(allSplits)));
    }
    return datum;
  }
    this.datum =
        (TaskAttemptUnsuccessfulCompletion)odatum;
    this.attemptId =
        TaskAttemptID.forName(datum.getAttemptId().toString());
    this.taskType =
        TaskType.valueOf(datum.getTaskType().toString());
    this.finishTime = datum.getFinishTime();
    this.hostname = datum.getHostname().toString();
    this.rackName = datum.getRackname().toString();
    this.port = datum.getPort();
    this.status = datum.getStatus().toString();
    this.error = datum.getError().toString();
    this.counters =
        EventReader.fromAvro(datum.getCounters());
    this.clockSplits =
        AvroArrayUtils.fromAvro(datum.getClockSplits());
    this.cpuUsages =
        AvroArrayUtils.fromAvro(datum.getCpuUsages());
    this.vMemKbytes =
        AvroArrayUtils.fromAvro(datum.getVMemKbytes());
    this.physMemKbytes =
        AvroArrayUtils.fromAvro(datum.getPhysMemKbytes());
  }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/TaskFailedEvent.java
  public Object getDatum() {
    if(datum == null) {
      datum = new TaskFailed();
      datum.setTaskid(new Utf8(id.toString()));
      datum.setError(new Utf8(error));
      datum.setFinishTime(finishTime);
      datum.setTaskType(new Utf8(taskType.name()));
      datum.setFailedDueToAttempt(
          failedDueToAttempt == null
          ? null
          : new Utf8(failedDueToAttempt.toString()));
      datum.setStatus(new Utf8(status));
      datum.setCounters(EventWriter.toAvro(counters));
    }
    return datum;
  }
  public void setDatum(Object odatum) {
    this.datum = (TaskFailed)odatum;
    this.id =
        TaskID.forName(datum.getTaskid().toString());
    this.taskType =
        TaskType.valueOf(datum.getTaskType().toString());
    this.finishTime = datum.getFinishTime();
    this.error = datum.getError().toString();
    this.failedDueToAttempt =
        datum.getFailedDueToAttempt() == null
        ? null
        : TaskAttemptID.forName(
            datum.getFailedDueToAttempt().toString());
    this.status = datum.getStatus().toString();
    this.counters =
        EventReader.fromAvro(datum.getCounters());
  }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/TaskFinishedEvent.java
  public Object getDatum() {
    if (datum == null) {
      datum = new TaskFinished();
      datum.setTaskid(new Utf8(taskid.toString()));
      if(successfulAttemptId != null)
      {
        datum.setSuccessfulAttemptId(new Utf8(successfulAttemptId.toString()));
      }
      datum.setFinishTime(finishTime);
      datum.setCounters(EventWriter.toAvro(counters));
      datum.setTaskType(new Utf8(taskType.name()));
      datum.setStatus(new Utf8(status));
    }
    return datum;
  }

  public void setDatum(Object oDatum) {
    this.datum = (TaskFinished)oDatum;
    this.taskid = TaskID.forName(datum.getTaskid().toString());
    if (datum.getSuccessfulAttemptId() != null) {
      this.successfulAttemptId = TaskAttemptID
          .forName(datum.getSuccessfulAttemptId().toString());
    }
    this.finishTime = datum.getFinishTime();
    this.taskType = TaskType.valueOf(datum.getTaskType().toString());
    this.status = datum.getStatus().toString();
    this.counters = EventReader.fromAvro(datum.getCounters());
  }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/TaskStartedEvent.java
  public TaskStartedEvent(TaskID id, long startTime, 
      TaskType taskType, String splitLocations) {
    datum.setTaskid(new Utf8(id.toString()));
    datum.setSplitLocations(new Utf8(splitLocations));
    datum.setStartTime(startTime);
    datum.setTaskType(new Utf8(taskType.name()));
  }

  TaskStartedEvent() {}
  public void setDatum(Object datum) { this.datum = (TaskStarted)datum; }

  public TaskID getTaskId() {
    return TaskID.forName(datum.getTaskid().toString());
  }
  public String getSplitLocations() {
    return datum.getSplitLocations().toString();
  }
  public long getStartTime() { return datum.getStartTime(); }
  public TaskType getTaskType() {
    return TaskType.valueOf(datum.getTaskType().toString());
  }
  public EventType getEventType() {

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/TaskUpdatedEvent.java
  public TaskUpdatedEvent(TaskID id, long finishTime) {
    datum.setTaskid(new Utf8(id.toString()));
    datum.setFinishTime(finishTime);
  }

  TaskUpdatedEvent() {}
  public void setDatum(Object datum) { this.datum = (TaskUpdated)datum; }

  public TaskID getTaskId() {
    return TaskID.forName(datum.getTaskid().toString());
  }
  public long getFinishTime() { return datum.getFinishTime(); }
  public EventType getEventType() {
    return EventType.TASK_UPDATED;

