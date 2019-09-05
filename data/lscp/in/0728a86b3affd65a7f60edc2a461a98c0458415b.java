hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/QueueInfoPBImpl.java
  public String getDefaultNodeLabelExpression() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasDefaultNodeLabelExpression()) ? p
        .getDefaultNodeLabelExpression().trim() : null;
  }

  @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/ResourceRequestPBImpl.java
    if (!p.hasNodeLabelExpression()) {
      return null;
    }
    return (p.getNodeLabelExpression().trim());
  }

  @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacitySchedulerConfiguration.java
  }
  
  public String getDefaultNodeLabelExpression(String queue) {
    String defaultLabelExpression = get(getQueuePrefix(queue)
        + DEFAULT_NODE_LABEL_EXPRESSION);
    if (defaultLabelExpression == null) {
      return null;
    }
    return defaultLabelExpression.trim();
  }
  
  public void setDefaultNodeLabelExpression(String queue, String exp) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/TestSchedulerUtils.java
    Assert.assertEquals(ContainerExitStatus.PREEMPTED, cd.getExitStatus());
  }
  
  @Test (timeout = 30000)
  public void testNormalizeNodeLabelExpression()
      throws IOException {
    YarnScheduler scheduler = mock(YarnScheduler.class);
    Set<String> queueAccessibleNodeLabels = Sets.newHashSet();
    QueueInfo queueInfo = mock(QueueInfo.class);
    when(queueInfo.getQueueName()).thenReturn("queue");
    when(queueInfo.getAccessibleNodeLabels()).thenReturn(queueAccessibleNodeLabels);
    when(queueInfo.getDefaultNodeLabelExpression()).thenReturn(" x ");
    when(scheduler.getQueueInfo(any(String.class), anyBoolean(), anyBoolean()))
        .thenReturn(queueInfo);
    
    Resource maxResource = Resources.createResource(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    try {
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays.asList("x", "y"));
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
          ImmutableSet.of(NodeLabel.newInstance("x"),
              NodeLabel.newInstance("y")));
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      Assert.assertTrue(resReq.getNodeLabelExpression().equals("x"));
      
      resReq.setNodeLabelExpression(" y ");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      Assert.assertTrue(resReq.getNodeLabelExpression().equals("y"));
    } catch (InvalidResourceRequestException e) {
      e.printStackTrace();
      fail("Should be valid when request labels is a subset of queue labels");
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
          Arrays.asList("x", "y"));
    }
  }

  public static SchedulerApplication<SchedulerApplicationAttempt>
      verifyAppAddedAndRemovedFromScheduler(
          Map<ApplicationId, SchedulerApplication<SchedulerApplicationAttempt>> applications,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestCapacityScheduler.java
    rm.stop();
  }
  
  @Test
  public void testDefaultNodeLabelExpressionQueueConfig() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setDefaultNodeLabelExpression("root.a", " x");
    conf.setDefaultNodeLabelExpression("root.b", " y ");
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    cs.init(conf);
    cs.start();

    QueueInfo queueInfoA = cs.getQueueInfo("a", true, false);
    Assert.assertEquals(queueInfoA.getQueueName(), "a");
    Assert.assertEquals(queueInfoA.getDefaultNodeLabelExpression(), "x");

    QueueInfo queueInfoB = cs.getQueueInfo("b", true, false);
    Assert.assertEquals(queueInfoB.getQueueName(), "b");
    Assert.assertEquals(queueInfoB.getDefaultNodeLabelExpression(), "y");
  }

  private void setMaxAllocMb(Configuration conf, int maxAllocMb) {
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        maxAllocMb);

