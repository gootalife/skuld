hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ApplicationMasterService.java
              
      try {
        RMServerUtils.normalizeAndValidateRequests(ask,
            rScheduler.getMaximumResourceCapability(), app.getQueue(),
            rScheduler);
      } catch (InvalidResourceRequestException e) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/RMAppManager.java
    ApplicationId applicationId = submissionContext.getApplicationId();

    RMAppImpl application =
        createAndPopulateNewRMApp(submissionContext, submitTime, user, false);
    ApplicationId appId = submissionContext.getApplicationId();

    if (UserGroupInformation.isSecurityEnabled()) {
    RMAppImpl application =
        createAndPopulateNewRMApp(appContext, appState.getSubmitTime(),
            appState.getUser(), true);

    application.handle(new RMAppRecoverEvent(appId, rmState));
  }

  private RMAppImpl createAndPopulateNewRMApp(
      ApplicationSubmissionContext submissionContext, long submitTime,
      String user, boolean isRecovery) throws YarnException {
    ApplicationId applicationId = submissionContext.getApplicationId();
    ResourceRequest amReq =
        validateAndCreateResourceRequest(submissionContext, isRecovery);

    RMAppImpl application =
        new RMAppImpl(applicationId, rmContext, this.conf,
      String message = "Application with id " + applicationId
          + " is already present! Cannot add a duplicate!";
      LOG.warn(message);
      throw new YarnException(message);
    }
    this.applicationACLsManager.addApplication(applicationId,
  }

  private ResourceRequest validateAndCreateResourceRequest(
      ApplicationSubmissionContext submissionContext, boolean isRecovery)
      throws InvalidResourceRequestException {

    if (!submissionContext.getUnmanagedAM()) {
      ResourceRequest amReq = submissionContext.getAMContainerResourceRequest();
      if (amReq == null) {
        amReq = BuilderUtils
            .newResourceRequest(RMAppAttemptImpl.AM_CONTAINER_PRIORITY,
                ResourceRequest.ANY, submissionContext.getResource(), 1);
      }

      }

      try {
        SchedulerUtils.normalizeAndValidateRequest(amReq,
            scheduler.getMaximumResourceCapability(),
            submissionContext.getQueue(), scheduler, isRecovery);
      } catch (InvalidResourceRequestException e) {
        LOG.warn("RM app submission failed in validating AM resource request"
            + " for application " + submissionContext.getApplicationId(), e);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/RMServerUtils.java
  public static void normalizeAndValidateRequests(List<ResourceRequest> ask,
      Resource maximumResource, String queueName, YarnScheduler scheduler)
      throws InvalidResourceRequestException {
    for (ResourceRequest resReq : ask) {
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maximumResource,
          queueName, scheduler);
    }
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/attempt/RMAppAttemptImpl.java
        appAttempt.amReq.setResourceName(ResourceRequest.ANY);
        appAttempt.amReq.setRelaxLocality(true);
        
        Allocation amContainerAllocation =
            appAttempt.scheduler.allocate(appAttempt.applicationAttemptId,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/SchedulerUtils.java
    ask.setCapability(normalized);
  }

  private static void normalizeNodeLabelExpressionInRequest(
      ResourceRequest resReq, QueueInfo queueInfo) {

    String labelExp = resReq.getNodeLabelExpression();

    if (labelExp == null && queueInfo != null && ResourceRequest.ANY
        .equals(resReq.getResourceName())) {
      labelExp = queueInfo.getDefaultNodeLabelExpression();
    }

    if (labelExp == null) {
      labelExp = RMNodeLabelsManager.NO_LABEL;
    }
    resReq.setNodeLabelExpression(labelExp);
  }

  public static void normalizeAndValidateRequest(ResourceRequest resReq,
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      boolean isRecovery)
      throws InvalidResourceRequestException {

    QueueInfo queueInfo = null;
    try {
      queueInfo = scheduler.getQueueInfo(queueName, false, false);
    } catch (IOException e) {
    }
    SchedulerUtils.normalizeNodeLabelExpressionInRequest(resReq, queueInfo);
    if (!isRecovery) {
      validateResourceRequest(resReq, maximumResource, queueInfo);
    }
  }

  public static void normalizeAndvalidateRequest(ResourceRequest resReq,
      Resource maximumResource, String queueName, YarnScheduler scheduler)
      throws InvalidResourceRequestException {
    normalizeAndValidateRequest(resReq, maximumResource, queueName, scheduler,
        false);
  }

  public static void validateResourceRequest(ResourceRequest resReq,
      Resource maximumResource, QueueInfo queueInfo)
      throws InvalidResourceRequestException {
    if (resReq.getCapability().getMemory() < 0 ||
        resReq.getCapability().getMemory() > maximumResource.getMemory()) {
          + resReq.getCapability().getVirtualCores()
          + ", maxVirtualCores=" + maximumResource.getVirtualCores());
    }
    String labelExp = resReq.getNodeLabelExpression();
    if (!ResourceRequest.ANY.equals(resReq.getResourceName())
        && labelExp != null && !labelExp.trim().isEmpty()) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestWorkPreservingRMRestart.java
    rm2.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    rm2.waitForState(app1.getApplicationId(), RMAppState.FAILED);
  }

  @Test (timeout = 30000)
  public void testAppFailToValidateResourceRequestOnRecovery() throws Exception{
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = rm1.submitApp(200);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 50);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 100);

    rm2 = new MockRM(conf, memStore);
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    rm2.start();
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/TestSchedulerUtils.java
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.TestAMAuthorization.MockRMWithAMS;
import org.apache.hadoop.yarn.server.resourcemanager.TestAMAuthorization.MyContainerManager;
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler);

      resReq.setNodeLabelExpression("y");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler);
      
      resReq.setNodeLabelExpression("");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler);
      
      resReq.setNodeLabelExpression(" ");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler);
    } catch (InvalidResourceRequestException e) {
      e.printStackTrace();
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("z");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x && y");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler);
      
      resReq.setNodeLabelExpression("");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler);
      
      resReq.setNodeLabelExpression("  ");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler);
    } catch (InvalidResourceRequestException e) {
      e.printStackTrace();
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler);
      
      resReq.setNodeLabelExpression("y");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler);
      
      resReq.setNodeLabelExpression("z");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler);
    } catch (InvalidResourceRequestException e) {
      e.printStackTrace();
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), "rack", resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), "rack", resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
      ResourceRequest resReq =
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler);
    } catch (InvalidResourceRequestException e) {
      fail("Zero memory should be accepted");
      ResourceRequest resReq =
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler);
    } catch (InvalidResourceRequestException e) {
      fail("Zero vcores should be accepted");
      ResourceRequest resReq =
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler);
    } catch (InvalidResourceRequestException e) {
      fail("Max memory should be accepted");
      ResourceRequest resReq =
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler);
    } catch (InvalidResourceRequestException e) {
      fail("Max vcores should not be accepted");
      ResourceRequest resReq =
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler);
      fail("Negative memory should not be accepted");
    } catch (InvalidResourceRequestException e) {
      ResourceRequest resReq =
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler);
      fail("Negative vcores should not be accepted");
    } catch (InvalidResourceRequestException e) {
      ResourceRequest resReq =
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler);
      fail("More than max memory should not be accepted");
    } catch (InvalidResourceRequestException e) {
      ResourceRequest resReq =
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler);
      fail("More than max vcores should not be accepted");
    } catch (InvalidResourceRequestException e) {

