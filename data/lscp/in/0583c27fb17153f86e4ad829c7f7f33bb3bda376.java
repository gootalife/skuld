hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/RMAppImpl.java
        ((RMAppAttemptImpl) app.currentAttempt)
          .transferStateFromAttempt(oldAttempt);
        return initialState;
      } else {
        if (numberOfFailure >= app.maxAppAttempts) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/attempt/RMAppAttemptImpl.java
        attemptState.getMemorySeconds(),attemptState.getVcoreSeconds());
  }

  public void transferStateFromAttempt(RMAppAttempt attempt) {
    this.justFinishedContainers = attempt.getJustFinishedContainersReference();
    this.finishedContainersSentToAM =
        attempt.getFinishedContainersSentToAMReference();
        appAttempt.progress = 1.0f;
        RMApp rmApp =appAttempt.rmContext.getRMApps().get(
            appAttempt.getAppAttemptId().getApplicationId());

        if (appAttempt.submissionContext
            .getKeepContainersAcrossApplicationAttempts()
            && !appAttempt.submissionContext.getUnmanagedAM()
            && rmApp.getCurrentAppAttempt() != appAttempt) {
          appAttempt.transferStateFromAttempt(rmApp.getCurrentAppAttempt());
        }
        if (rmApp.getCurrentAppAttempt() == appAttempt

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestWorkPreservingRMRestart.java
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    rm2.start();
  }

  @Test(timeout = 20000)
  public void testContainerCompleteMsgNotLostAfterAMFailedAndRMRestart() throws Exception {
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    rm1 = new MockRM(conf, memStore);
    rm1.start();

    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();

    RMApp app0 = rm1.submitApp(200, "", UserGroupInformation.getCurrentUser()
        .getShortUserName(), null, false, null, YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS, 
        null, null, true, true, false, null, 0, null, true);
    MockAM am0 = MockRM.launchAndRegisterAM(app0, rm1, nm1);

    am0.allocate("127.0.0.1", 1000, 2, new ArrayList<ContainerId>());
    nm1.nodeHeartbeat(true);
    List<Container> conts = am0.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    while (conts.size() == 0) {
      nm1.nodeHeartbeat(true);
      conts.addAll(am0.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(500);
    }

    nm1.nodeHeartbeat(am0.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm1.waitForState(app0.getApplicationId(), RMAppState.ACCEPTED);
    MockAM am1 = MockRM.launchAndRegisterAM(app0, rm1, nm1);

    rm2 = new MockRM(conf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());

    NMContainerStatus amContainer =
        TestRMRestart.createNMContainerStatus(am0.getApplicationAttemptId(), 1,
          ContainerState.RUNNING);
    NMContainerStatus completedContainer=
        TestRMRestart.createNMContainerStatus(am0.getApplicationAttemptId(), 2,
          ContainerState.COMPLETE);
    NMContainerStatus runningContainer =
        TestRMRestart.createNMContainerStatus(am0.getApplicationAttemptId(), 3,
          ContainerState.RUNNING);
    nm1.registerNode(Arrays.asList(amContainer, runningContainer,
        completedContainer), null);
    Thread.sleep(200);

    RMApp recoveredApp0 =
        rm2.getRMContext().getRMApps().get(app0.getApplicationId());
    RMAppAttempt loadedAttempt1 = recoveredApp0.getCurrentAppAttempt();
    assertEquals(1,loadedAttempt1.getJustFinishedContainers().size());
  }

}

