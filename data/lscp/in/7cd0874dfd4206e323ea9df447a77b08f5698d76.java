hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/rm/RMContainerAllocator.java

  private ScheduleStats scheduleStats = new ScheduleStats();

  private String mapNodeLabelExpression;

  private String reduceNodeLabelExpression;

  public RMContainerAllocator(ClientService clientService, AppContext context) {
    super(clientService, context);
    this.stopped = new AtomicBoolean(false);
    RackResolver.init(conf);
    retryInterval = getConfig().getLong(MRJobConfig.MR_AM_TO_RM_WAIT_INTERVAL_MS,
                                MRJobConfig.DEFAULT_MR_AM_TO_RM_WAIT_INTERVAL_MS);
    mapNodeLabelExpression = conf.get(MRJobConfig.MAP_NODE_LABEL_EXP);
    reduceNodeLabelExpression = conf.get(MRJobConfig.REDUCE_NODE_LABEL_EXP);
    retrystartTime = System.currentTimeMillis();
          reduceResourceRequest.getVirtualCores());
        if (reqEvent.getEarlierAttemptFailed()) {
          pendingReduces.addFirst(new ContainerRequest(reqEvent,
              PRIORITY_REDUCE, reduceNodeLabelExpression));
        } else {
          pendingReduces.add(new ContainerRequest(reqEvent, PRIORITY_REDUCE,
              reduceNodeLabelExpression));
        }
      }
      
      if (event.getEarlierAttemptFailed()) {
        earlierFailedMaps.add(event.getAttemptID());
        request =
            new ContainerRequest(event, PRIORITY_FAST_FAIL_MAP,
                mapNodeLabelExpression);
        LOG.info("Added "+event.getAttemptID()+" to list of failed maps");
      } else {
        for (String host : event.getHosts()) {
            LOG.debug("Added attempt req to rack " + rack);
         }
       }
        request =
            new ContainerRequest(event, PRIORITY_MAP, mapNodeLabelExpression);
      }
      maps.put(event.getAttemptID(), request);
      addContainerReq(request);

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/rm/RMContainerRequestor.java
    final String[] racks;
    final Priority priority;
    final String nodeLabelExpression;

    final long requestTimeMs;

    public ContainerRequest(ContainerRequestEvent event, Priority priority,
        String nodeLabelExpression) {
      this(event.getAttemptID(), event.getCapability(), event.getHosts(),
          event.getRacks(), priority, nodeLabelExpression);
    }

    public ContainerRequest(ContainerRequestEvent event, Priority priority,
                            long requestTimeMs) {
      this(event.getAttemptID(), event.getCapability(), event.getHosts(),
          event.getRacks(), priority, requestTimeMs,null);
    }

    public ContainerRequest(TaskAttemptId attemptID,
                            Resource capability, String[] hosts, String[] racks,
                            Priority priority, String nodeLabelExpression) {
      this(attemptID, capability, hosts, racks, priority,
          System.currentTimeMillis(), nodeLabelExpression);
    }

    public ContainerRequest(TaskAttemptId attemptID,
        Resource capability, String[] hosts, String[] racks,
        Priority priority, long requestTimeMs,String nodeLabelExpression) {
      this.attemptID = attemptID;
      this.capability = capability;
      this.hosts = hosts;
      this.racks = racks;
      this.priority = priority;
      this.requestTimeMs = requestTimeMs;
      this.nodeLabelExpression = nodeLabelExpression;
    }
    
    public String toString() {
    for (String host : req.hosts) {
      if (!isNodeBlacklisted(host)) {
        addResourceRequest(req.priority, host, req.capability,
            null);
      }
    }

    for (String rack : req.racks) {
      addResourceRequest(req.priority, rack, req.capability,
          null);
    }

    addResourceRequest(req.priority, ResourceRequest.ANY, req.capability,
        req.nodeLabelExpression);
  }

  protected void decContainerReq(ContainerRequest req) {
  }

  private void addResourceRequest(Priority priority, String resourceName,
      Resource capability, String nodeLabelExpression) {
    Map<String, Map<Resource, ResourceRequest>> remoteRequests =
      this.remoteRequestsTable.get(priority);
    if (remoteRequests == null) {
      remoteRequest.setResourceName(resourceName);
      remoteRequest.setCapability(capability);
      remoteRequest.setNumContainers(0);
      remoteRequest.setNodeLabelExpression(nodeLabelExpression);
      reqMap.put(capability, remoteRequest);
    }
    remoteRequest.setNumContainers(remoteRequest.getNumContainers() + 1);
    }
    String[] hosts = newHosts.toArray(new String[newHosts.size()]);
    ContainerRequest newReq = new ContainerRequest(orig.attemptID, orig.capability,
        hosts, orig.racks, orig.priority, orig.nodeLabelExpression);
    return newReq;
  }
  

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/rm/TestRMContainerAllocator.java
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
    ContainerRequestEvent event1 =
        createReq(jobId, 1, 2048, new String[] { "h1" }, false, false);
    scheduledRequests.maps.put(mock(TaskAttemptId.class),
        new RMContainerRequestor.ContainerRequest(event1, null,null));
    assignedRequests.reduces.put(mock(TaskAttemptId.class),
        mock(Container.class));

        assignedRequests.preemptionWaitingReduces.size());
  }

  @Test
  public void testMapReduceAllocationWithNodeLabelExpression() throws Exception {

    LOG.info("Running testMapReduceAllocationWithNodeLabelExpression");
    Configuration conf = new Configuration();
    conf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 1.0f);
    conf.set(MRJobConfig.MAP_NODE_LABEL_EXP, "MapNodes");
    conf.set(MRJobConfig.REDUCE_NODE_LABEL_EXP, "ReduceNodes");
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    final MockScheduler mockScheduler = new MockScheduler(appAttemptId);
    MyContainerAllocator allocator =
        new MyContainerAllocator(null, conf, appAttemptId, mockJob) {
          @Override
          protected void register() {
          }

          @Override
          protected ApplicationMasterProtocol createSchedulerProxy() {
            return mockScheduler;
          }
        };

    ContainerRequestEvent reqMapEvents;
    reqMapEvents = createReq(jobId, 0, 1024, new String[] { "map" });
    allocator.sendRequests(Arrays.asList(reqMapEvents));

    ContainerRequestEvent reqReduceEvents;
    reqReduceEvents =
        createReq(jobId, 0, 2048, new String[] { "reduce" }, false, true);
    allocator.sendRequests(Arrays.asList(reqReduceEvents));
    allocator.schedule();
    Assert.assertEquals(3, mockScheduler.lastAsk.size());
    validateLabelsRequests(mockScheduler.lastAsk.get(0), false);
    validateLabelsRequests(mockScheduler.lastAsk.get(1), false);
    validateLabelsRequests(mockScheduler.lastAsk.get(2), false);

    ContainerId cid0 = mockScheduler.assignContainer("map", false);
    allocator.schedule();
    Assert.assertEquals(3, mockScheduler.lastAsk.size());
    validateLabelsRequests(mockScheduler.lastAsk.get(0), true);
    validateLabelsRequests(mockScheduler.lastAsk.get(1), true);
    validateLabelsRequests(mockScheduler.lastAsk.get(2), true);

    allocator.close();
  }

  private void validateLabelsRequests(ResourceRequest resourceRequest,
      boolean isReduce) {
    switch (resourceRequest.getResourceName()) {
    case "map":
    case "reduce":
    case NetworkTopology.DEFAULT_RACK:
      Assert.assertNull(resourceRequest.getNodeLabelExpression());
      break;
    case "*":
      Assert.assertEquals(isReduce ? "ReduceNodes" : "MapNodes",
          resourceRequest.getNodeLabelExpression());
      break;
    default:
      Assert.fail("Invalid resource location "
          + resourceRequest.getResourceName());
    }
  }

  @Test
  public void testMapReduceScheduling() throws Exception {

            .getNumContainers(), req.getRelaxLocality());
        askCopy.add(reqCopy);
      }
      SecurityUtil.setTokenServiceUseIp(false);
      lastAsk = ask;
      lastRelease = release;
      lastBlacklistAdditions = blacklistAdditions;

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/MRJobConfig.java

  public static final String QUEUE_NAME = "mapreduce.job.queuename";

  public static final String JOB_NODE_LABEL_EXP = "mapreduce.job.node-label-expression";

  public static final String AM_NODE_LABEL_EXP = "mapreduce.job.am.node-label-expression";

  public static final String MAP_NODE_LABEL_EXP = "mapreduce.map.node-label-expression";

  public static final String REDUCE_NODE_LABEL_EXP = "mapreduce.reduce.node-label-expression";

  public static final String RESERVATION_ID = "mapreduce.job.reservation.id";

  public static final String JOB_TAGS = "mapreduce.job.tags";

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/main/java/org/apache/hadoop/mapred/YARNRunner.java
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

  private static final Log LOG = LogFactory.getLog(YARNRunner.class);

  private final static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  public final static Priority AM_CONTAINER_PRIORITY = recordFactory
      .newRecordInstance(Priority.class);
  static {
    AM_CONTAINER_PRIORITY.setPriority(0);
  }

  private ResourceMgrDelegate resMgrDelegate;
  private ClientCache clientCache;
  private Configuration conf;
        conf.getInt(MRJobConfig.MR_AM_MAX_ATTEMPTS,
            MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS));
    appContext.setResource(capability);

    String amNodelabelExpression = conf.get(MRJobConfig.AM_NODE_LABEL_EXP);
    if (null != amNodelabelExpression
        && amNodelabelExpression.trim().length() != 0) {
      ResourceRequest amResourceRequest =
          recordFactory.newRecordInstance(ResourceRequest.class);
      amResourceRequest.setPriority(AM_CONTAINER_PRIORITY);
      amResourceRequest.setResourceName(ResourceRequest.ANY);
      amResourceRequest.setCapability(capability);
      amResourceRequest.setNumContainers(1);
      amResourceRequest.setNodeLabelExpression(amNodelabelExpression.trim());
      appContext.setAMContainerResourceRequest(amResourceRequest);
    }
    appContext.setNodeLabelExpression(jobConf
        .get(JobContext.JOB_NODE_LABEL_EXP));

    appContext.setApplicationType(MRJobConfig.MR_APPLICATION_TYPE);
    if (tagsFromConf != null && !tagsFromConf.isEmpty()) {
      appContext.setApplicationTags(new HashSet<String>(tagsFromConf));

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapred/TestYARNRunner.java
    throw new IllegalStateException("Profiler opts not found!");
  }

  @Test
  public void testNodeLabelExp() throws Exception {
    JobConf jobConf = new JobConf();

    jobConf.set(MRJobConfig.JOB_NODE_LABEL_EXP, "GPU");
    jobConf.set(MRJobConfig.AM_NODE_LABEL_EXP, "highMem");

    YARNRunner yarnRunner = new YARNRunner(jobConf);
    ApplicationSubmissionContext appSubCtx =
        buildSubmitContext(yarnRunner, jobConf);

    assertEquals(appSubCtx.getNodeLabelExpression(), "GPU");
    assertEquals(appSubCtx.getAMContainerResourceRequest()
        .getNodeLabelExpression(), "highMem");
  }

  @Test
  public void testAMStandardEnv() throws Exception {
    final String ADMIN_LIB_PATH = "foo";

