hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FairScheduler.java
    }
  }

  @VisibleForTesting
  synchronized void attemptScheduling(FSSchedulerNode node) {
    if (rmContext.isWorkPreservingRecoveryEnabled()
        && !rmContext.isSchedulerReadyForAllocatingContainers()) {
      return;
    }

    final NodeId nodeID = node.getNodeID();
    if (!nodes.containsKey(nodeID)) {
      LOG.info("Skipping scheduling as the node " + nodeID +
          " has been removed");
      return;
    }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/TestFairScheduler.java
    }
  }

  @Test
  public void testSchedulingOnRemovedNode() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    Assert.assertTrue("Continuous scheduling should be disabled.",
        !scheduler.isContinuousSchedulingEnabled());

    ApplicationAttemptId id11 = createAppAttemptId(1, 1);
    createMockRMApp(id11);

    scheduler.addApplication(id11.getApplicationId(), "root.queue1", "user1",
        false);
    scheduler.addApplicationAttempt(id11, false, false);

    List<ResourceRequest> ask1 = new ArrayList<>();
    ResourceRequest request1 =
        createResourceRequest(1024, 8, ResourceRequest.ANY, 1, 1, true);

    ask1.add(request1);
    scheduler.allocate(id11, ask1, new ArrayList<ContainerId>(), null,
        null);

    String hostName = "127.0.0.1";
    RMNode node1 = MockNodes.newNodeInfo(1,
      Resources.createResource(8 * 1024, 8), 1, hostName);
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    FSSchedulerNode node = (FSSchedulerNode)scheduler.getSchedulerNode(
      node1.getNodeID());

    NodeRemovedSchedulerEvent removeNode1 =
        new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(removeNode1);

    scheduler.attemptScheduling(node);

    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(id11,
            RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent1);
  }

  @Test
  public void testDefaultRuleInitializesProperlyWhenPolicyNotConfigured()
      throws IOException {

