hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FSAppAttempt.java
      }

      return container.getResource();
    }


    return FairScheduler.CONTAINER_RESERVED;
  }

  private boolean hasNodeOrRackLocalRequests(Priority priority) {
    return getResourceRequests(priority).size() > 1;
  }

  private boolean isOverAMShareLimit() {
    if (!isAmRunning() && !getUnmanagedAM()) {
      List<ResourceRequest> ask = appSchedulingInfo.getAllResourceRequests();
      if (ask.isEmpty() || !getQueue().canRunAppAM(
          ask.get(0).getCapability())) {
        return true;
      }
    }
    return false;
  }

  private Resource assignContainer(FSSchedulerNode node, boolean reserved) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Node offered to app: " + getName() + " reserved: " + reserved);
    }

    Collection<Priority> prioritiesToTry = (reserved) ?
    synchronized (this) {
      for (Priority priority : prioritiesToTry) {
        if (!reserved && !hasContainerForNode(priority, node)) {
          continue;
        }

    return Resources.none();
  }

  private boolean hasContainerForNode(Priority prio, FSSchedulerNode node) {
    ResourceRequest anyRequest = getResourceRequest(prio, ResourceRequest.ANY);
    ResourceRequest rackRequest = getResourceRequest(prio, node.getRackName());
    ResourceRequest nodeRequest = getResourceRequest(prio, node.getNodeName());
                (nodeRequest != null && nodeRequest.getNumContainers() > 0)) &&
            Resources.lessThanOrEqual(RESOURCE_CALCULATOR, null,
                anyRequest.getCapability(),
                node.getRMNode().getTotalCapability()) &&
            getQueue().fitsInMaxShare(anyRequest.getCapability());
  }

  private boolean isValidReservation(FSSchedulerNode node) {
    Priority reservedPriority = node.getReservedContainer().
        getReservedPriority();
    return hasContainerForNode(reservedPriority, node) &&
        !isOverAMShareLimit();
  }

  public boolean assignReservedContainer(FSSchedulerNode node) {
    RMContainer rmContainer = node.getReservedContainer();
    Priority reservedPriority = rmContainer.getReservedPriority();

    if (!isValidReservation(node)) {
      LOG.info("Releasing reservation that cannot be satisfied for " +
          "application " + getApplicationAttemptId() + " on node " + node);
      unreserve(reservedPriority, node);
      return false;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to fulfill reservation for application "
          + getApplicationAttemptId() + " on node: " + node);
    }

    if (Resources.fitsIn(node.getReservedContainer().getReservedResource(),
        node.getAvailableResource())) {
      assignContainer(node, true);
    }
    return true;
  }

  static class RMContainerComparator implements Comparator<RMContainer>,
      Serializable {

  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    if (isOverAMShareLimit()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping allocation because maxAMShare limit would " +
            "be exceeded");
      }
      return Resources.none();
    }
    return assignContainer(node, false);
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FSQueue.java
  @Override
  public void decPendingResource(String nodeLabel, Resource resourceToDec) {
  }

  public boolean fitsInMaxShare(Resource additionalResource) {
    Resource usagePlusAddition =
        Resources.add(getResourceUsage(), additionalResource);

    if (!Resources.fitsIn(usagePlusAddition, getMaxShare())) {
      return false;
    }

    FSQueue parentQueue = getParent();
    if (parentQueue != null) {
      return parentQueue.fitsInMaxShare(additionalResource);
    }
    return true;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FairScheduler.java
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;

    boolean validReservation = false;
    FSAppAttempt reservedAppSchedulable = node.getReservedAppSchedulable();
    if (reservedAppSchedulable != null) {
      validReservation = reservedAppSchedulable.assignReservedContainer(node);
    }
    if (!validReservation) {
      int assignedContainers = 0;
      while (node.getReservedContainer() == null) {
    updateRootQueueMetrics();
  }

  public FSAppAttempt getSchedulerApp(ApplicationAttemptId appAttemptId) {
    return super.getApplicationAttempt(appAttemptId);
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/TestFairScheduler.java
        0, queue2.getAmResourceUsage().getMemory());
  }

  @Test
  public void testQueueMaxAMShareWithContainerReservation() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queue1\">");
    out.println("<maxAMShare>0.5</maxAMShare>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(10240, 10),
            1, "127.0.0.1");
    RMNode node2 =
        MockNodes.newNodeInfo(1, Resources.createResource(10240, 10),
            2, "127.0.0.2");
    RMNode node3 =
        MockNodes.newNodeInfo(1, Resources.createResource(5120, 5),
            3, "127.0.0.3");
    NodeAddedSchedulerEvent nodeE1 = new NodeAddedSchedulerEvent(node1);
    NodeUpdateSchedulerEvent updateE1 = new NodeUpdateSchedulerEvent(node1);
    NodeAddedSchedulerEvent nodeE2 = new NodeAddedSchedulerEvent(node2);
    NodeUpdateSchedulerEvent updateE2 = new NodeUpdateSchedulerEvent(node2);
    NodeAddedSchedulerEvent nodeE3 = new NodeAddedSchedulerEvent(node3);
    NodeUpdateSchedulerEvent updateE3 = new NodeUpdateSchedulerEvent(node3);
    scheduler.handle(nodeE1);
    scheduler.handle(nodeE2);
    scheduler.handle(nodeE3);
    scheduler.update();
    FSLeafQueue queue1 = scheduler.getQueueManager().getLeafQueue("queue1",
        true);
    Resource amResource1 = Resource.newInstance(1024, 1);
    Resource amResource2 = Resource.newInstance(1024, 1);
    Resource amResource3 = Resource.newInstance(10240, 1);
    Resource amResource4 = Resource.newInstance(5120, 1);
    Resource amResource5 = Resource.newInstance(1024, 1);
    Resource amResource6 = Resource.newInstance(10240, 1);
    Resource amResource7 = Resource.newInstance(1024, 1);
    Resource amResource8 = Resource.newInstance(1024, 1);
    int amPriority = RMAppAttemptImpl.AM_CONTAINER_PRIORITY.getPriority();
    ApplicationAttemptId attId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(attId1, "queue1", "user1", amResource1);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId1);
    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    scheduler.update();
    scheduler.handle(updateE1);
    assertEquals("Application1's AM requests 1024 MB memory",
        1024, app1.getAMResource().getMemory());
    assertEquals("Application1's AM should be running",
        1, app1.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 1024 MB memory",
        1024, queue1.getAmResourceUsage().getMemory());

    ApplicationAttemptId attId2 = createAppAttemptId(2, 1);
    createApplicationWithAMResource(attId2, "queue1", "user1", amResource2);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    scheduler.update();
    scheduler.handle(updateE2);
    assertEquals("Application2's AM requests 1024 MB memory",
        1024, app2.getAMResource().getMemory());
    assertEquals("Application2's AM should be running",
        1, app2.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemory());

    ApplicationAttemptId attId3 = createAppAttemptId(3, 1);
    createApplicationWithAMResource(attId3, "queue1", "user1", amResource3);
    createSchedulingRequestExistingApplication(10240, 1, amPriority, attId3);
    FSAppAttempt app3 = scheduler.getSchedulerApp(attId3);
    scheduler.update();
    scheduler.handle(updateE1);
    scheduler.handle(updateE2);
    assertEquals("Application3's AM resource shouldn't be updated",
        0, app3.getAMResource().getMemory());
    assertEquals("Application3's AM should not be running",
        0, app3.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemory());

    ApplicationAttemptId attId4 = createAppAttemptId(4, 1);
    createApplicationWithAMResource(attId4, "queue1", "user1", amResource4);
    createSchedulingRequestExistingApplication(5120, 1, amPriority, attId4);
    FSAppAttempt app4 = scheduler.getSchedulerApp(attId4);
    scheduler.update();
    scheduler.handle(updateE1);
    assertEquals("Application4's AM resource shouldn't be updated",
        0, app4.getAMResource().getMemory());
    assertEquals("Application4's AM should not be running",
        0, app4.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemory());

    scheduler.update();
    scheduler.handle(updateE3);
    assertEquals("Application4's AM requests 5120 MB memory",
        5120, app4.getAMResource().getMemory());
    assertEquals("Application4's AM should be running",
        1, app4.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 7168 MB memory",
        7168, queue1.getAmResourceUsage().getMemory());

    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(attId1,
            RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent1);
    assertEquals("Queue1's AM resource usage should be 6144 MB memory",
        6144, queue1.getAmResourceUsage().getMemory());

    ApplicationAttemptId attId5 = createAppAttemptId(5, 1);
    createApplicationWithAMResource(attId5, "queue1", "user1", amResource5);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId5);
    FSAppAttempt app5 = scheduler.getSchedulerApp(attId5);
    scheduler.update();
    scheduler.handle(updateE1);
    assertEquals("Application5's AM requests 1024 MB memory",
        1024, app5.getAMResource().getMemory());
    assertEquals("Application5's AM should be running",
        1, app5.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 7168 MB memory",
        7168, queue1.getAmResourceUsage().getMemory());

    AppAttemptRemovedSchedulerEvent appRemovedEvent3 =
        new AppAttemptRemovedSchedulerEvent(attId3,
            RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent3);
    assertEquals("Queue1's AM resource usage should be 7168 MB memory",
        7168, queue1.getAmResourceUsage().getMemory());

    ApplicationAttemptId attId6 = createAppAttemptId(6, 1);
    createApplicationWithAMResource(attId6, "queue1", "user1", amResource6);
    createSchedulingRequestExistingApplication(10240, 1, amPriority, attId6);
    FSAppAttempt app6 = scheduler.getSchedulerApp(attId6);
    scheduler.update();
    scheduler.handle(updateE1);
    assertEquals("Application6's AM resource shouldn't be updated",
        0, app6.getAMResource().getMemory());
    assertEquals("Application6's AM should not be running",
        0, app6.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 7168 MB memory",
        7168, queue1.getAmResourceUsage().getMemory());

    ApplicationAttemptId attId7 = createAppAttemptId(7, 1);
    createApplicationWithAMResource(attId7, "queue1", "user1", amResource7);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId7);
    FSAppAttempt app7 = scheduler.getSchedulerApp(attId7);
    scheduler.update();
    scheduler.handle(updateE1);
    assertEquals("Application7's AM requests 1024 MB memory",
        1024, app7.getAMResource().getMemory());
    assertEquals("Application7's AM should be running",
        1, app7.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 8192 MB memory",
        8192, queue1.getAmResourceUsage().getMemory());

    AppAttemptRemovedSchedulerEvent appRemovedEvent4 =
        new AppAttemptRemovedSchedulerEvent(attId4,
            RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent4);
    assertEquals("Queue1's AM resource usage should be 3072 MB memory",
        3072, queue1.getAmResourceUsage().getMemory());

    AppAttemptRemovedSchedulerEvent appRemovedEvent5 =
        new AppAttemptRemovedSchedulerEvent(attId5,
            RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent5);
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
              2048, queue1.getAmResourceUsage().getMemory());

    scheduler.update();
    scheduler.handle(updateE1);
    scheduler.handle(updateE2);

    ApplicationAttemptId attId8 = createAppAttemptId(8, 1);
    createApplicationWithAMResource(attId8, "queue1", "user1", amResource8);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId8);
    FSAppAttempt app8 = scheduler.getSchedulerApp(attId8);
    scheduler.update();
    scheduler.handle(updateE1);
    assertEquals("Application8's AM resource shouldn't be updated",
        0, app8.getAMResource().getMemory());
    assertEquals("Application8's AM should not be running",
        0, app8.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemory());
    scheduler.update();
    scheduler.handle(updateE2);
    assertEquals("Application8's AM resource shouldn't be updated",
        0, app8.getAMResource().getMemory());
    assertEquals("Application8's AM should not be running",
        0, app8.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemory());

    AppAttemptRemovedSchedulerEvent appRemovedEvent2 =
        new AppAttemptRemovedSchedulerEvent(attId2,
            RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent2);
    assertEquals("Queue1's AM resource usage should be 1024 MB memory",
        1024, queue1.getAmResourceUsage().getMemory());

    scheduler.update();
    scheduler.handle(updateE2);
    assertEquals("Application6's AM requests 10240 MB memory",
        10240, app6.getAMResource().getMemory());
    assertEquals("Application6's AM should be running",
        1, app6.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 11264 MB memory",
        11264, queue1.getAmResourceUsage().getMemory());

    scheduler.update();
    scheduler.handle(updateE1);
    assertEquals("Application8's AM requests 1024 MB memory",
        1024, app8.getAMResource().getMemory());
    assertEquals("Application8's AM should be running",
        1, app8.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 12288 MB memory",
        12288, queue1.getAmResourceUsage().getMemory());
  }

  @Test
  public void testMaxRunningAppsHierarchicalQueues() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

