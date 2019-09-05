hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/ResourceLimits.java
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

public class ResourceLimits {
  volatile Resource limit;

  private volatile Resource amountNeededUnreserve;

  public ResourceLimits(Resource limit) {
    this.amountNeededUnreserve = Resources.none();
    this.limit = limit;
  }

  public ResourceLimits(Resource limit, Resource amountNeededUnreserve) {
    this.amountNeededUnreserve = amountNeededUnreserve;
    this.limit = limit;
  }

  public Resource getLimit() {
    return limit;
  }

  public Resource getAmountNeededUnreserve() {
    return amountNeededUnreserve;
  }

  public void setLimit(Resource limit) {
    this.limit = limit;
  }

  public void setAmountNeededUnreserve(Resource amountNeededUnreserve) {
    this.amountNeededUnreserve = amountNeededUnreserve;
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/AbstractCSQueue.java
        getCurrentLimitResource(nodePartition, clusterResource,
            currentResourceLimits, schedulingMode);

    if (Resources.greaterThan(resourceCalculator, clusterResource,
        newTotalResource, currentLimitResource)) {

                + newTotalWithoutReservedResource + ", maxLimitCapacity: "
                + currentLimitResource);
          }
          currentResourceLimits.setAmountNeededUnreserve(Resources.subtract(newTotalResource,
            currentLimitResource));
          return true;
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(getQueueName()
            + "Check assign to queue, nodePartition="
            + " max-capacity: "
            + queueCapacities.getAbsoluteMaximumCapacity(nodePartition) + ")");
      }
      return false;
    }
    return true;
  }
  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/LeafQueue.java
  private final QueueResourceLimitsInfo queueResourceLimitsInfo =
      new QueueResourceLimitsInfo();
  
  private volatile ResourceLimits cachedResourceLimitsForHeadroom = null;

  private OrderingPolicy<FiCaSchedulerApp> 
    orderingPolicy = new FifoOrderingPolicy<FiCaSchedulerApp>();
    this.lastClusterResource = clusterResource;
    updateAbsoluteCapacityResource(clusterResource);
    
    this.cachedResourceLimitsForHeadroom = new ResourceLimits(clusterResource);
    

          if (!super.canAssignToThisQueue(clusterResource, node.getPartition(),
              currentResourceLimits, required,
              application.getCurrentReservation(), schedulingMode)) {
            return NULL_ASSIGNMENT;
          }

          if (!canAssignToUser(clusterResource, application.getUser(), userLimit,
              application, node.getPartition(), currentResourceLimits)) {
            break;
          }

          CSAssignment assignment =
            assignContainersOnNode(clusterResource, node, application, priority,
                null, schedulingMode, currentResourceLimits);

          if (assignment.getSkipped()) {
    CSAssignment tmp =
        assignContainersOnNode(clusterResource, node, application, priority,
          rmContainer, schedulingMode, new ResourceLimits(Resources.none()));
    
  private void setQueueResourceLimitsInfo(
      Resource clusterResource) {
    synchronized (queueResourceLimitsInfo) {
      queueResourceLimitsInfo.setQueueCurrentLimit(cachedResourceLimitsForHeadroom
          .getLimit());
      queueResourceLimitsInfo.setClusterResource(clusterResource);
    }
    setQueueResourceLimitsInfo(clusterResource);
    
    Resource headroom =
        getHeadroom(queueUser, cachedResourceLimitsForHeadroom.getLimit(),
            clusterResource, userLimit);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Headroom calculation for user " + user + ": " + 
          " userLimit=" + userLimit + 
          " queueMaxAvailRes=" + cachedResourceLimitsForHeadroom.getLimit() +
          " consumed=" + queueUser.getUsed() + 
          " headroom=" + headroom);
    }
  @Private
  protected synchronized boolean canAssignToUser(Resource clusterResource,
      String userName, Resource limit, FiCaSchedulerApp application,
      String nodePartition, ResourceLimits currentResoureLimits) {
    User user = getUser(userName);

            limit)) {
      if (this.reservationsContinueLooking &&
          nodePartition.equals(CommonNodeLabelsManager.NO_LABEL)) {
        if (Resources.lessThanOrEqual(
            resourceCalculator,
            clusterResource,
            Resources.subtract(user.getUsed(),application.getCurrentReservation()),
            limit)) {

          if (LOG.isDebugEnabled()) {
            LOG.debug("User " + userName + " in queue " + getQueueName()
                + user.getUsed() + " reserved: "
                + application.getCurrentReservation() + " limit: " + limit);
          }
          Resource amountNeededToUnreserve = Resources.subtract(user.getUsed(nodePartition), limit);
          currentResoureLimits.setAmountNeededUnreserve(
              Resources.max(resourceCalculator, clusterResource,
                  currentResoureLimits.getAmountNeededUnreserve(), amountNeededToUnreserve));
          return true;
        }
      }

  private CSAssignment assignContainersOnNode(Resource clusterResource,
      FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, SchedulingMode schedulingMode,
      ResourceLimits currentResoureLimits) {

    CSAssignment assigned;

      assigned =
          assignNodeLocalContainers(clusterResource, nodeLocalResourceRequest, 
            node, application, priority, reservedContainer,
            allocatedContainer, schedulingMode, currentResoureLimits);
      if (Resources.greaterThan(resourceCalculator, clusterResource,
        assigned.getResource(), Resources.none())) {

      assigned = 
          assignRackLocalContainers(clusterResource, rackLocalResourceRequest, 
            node, application, priority, reservedContainer,
            allocatedContainer, schedulingMode, currentResoureLimits);
      if (Resources.greaterThan(resourceCalculator, clusterResource,
        assigned.getResource(), Resources.none())) {

      assigned =
          assignOffSwitchContainers(clusterResource, offSwitchResourceRequest,
            node, application, priority, reservedContainer,
            allocatedContainer, schedulingMode, currentResoureLimits);

      if (allocatedContainer.getValue() != null) {
    return SKIP_ASSIGNMENT;
  }

  @Private
  protected boolean findNodeToUnreserve(Resource clusterResource,
      FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority,
      Resource minimumUnreservedResource) {
    NodeId idToUnreserve =
        application.getNodeIdToUnreserve(priority, minimumUnreservedResource,
      LOG.debug("unreserving for app: " + application.getApplicationId()
        + " on nodeId: " + idToUnreserve
        + " in order to replace reserved application and place it on node: "
        + node.getNodeID() + " needing: " + minimumUnreservedResource);
    }

    return true;
  }

  private CSAssignment assignNodeLocalContainers(Resource clusterResource,
      ResourceRequest nodeLocalResourceRequest, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResoureLimits) {
    if (canAssign(application, priority, node, NodeType.NODE_LOCAL, 
        reservedContainer)) {
      return assignContainer(clusterResource, node, application, priority,
          nodeLocalResourceRequest, NodeType.NODE_LOCAL, reservedContainer,
          allocatedContainer, schedulingMode, currentResoureLimits);
    }

    return new CSAssignment(Resources.none(), NodeType.NODE_LOCAL);
      ResourceRequest rackLocalResourceRequest, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResoureLimits) {
    if (canAssign(application, priority, node, NodeType.RACK_LOCAL,
        reservedContainer)) {
      return assignContainer(clusterResource, node, application, priority,
          rackLocalResourceRequest, NodeType.RACK_LOCAL, reservedContainer,
          allocatedContainer, schedulingMode, currentResoureLimits);
    }

    return new CSAssignment(Resources.none(), NodeType.RACK_LOCAL);
      ResourceRequest offSwitchResourceRequest, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResoureLimits) {
    if (canAssign(application, priority, node, NodeType.OFF_SWITCH,
        reservedContainer)) {
      return assignContainer(clusterResource, node, application, priority,
          offSwitchResourceRequest, NodeType.OFF_SWITCH, reservedContainer,
          allocatedContainer, schedulingMode, currentResoureLimits);
    }
    
    return new CSAssignment(Resources.none(), NodeType.OFF_SWITCH);
  private CSAssignment assignContainer(Resource clusterResource, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority, 
      ResourceRequest request, NodeType type, RMContainer rmContainer,
      MutableObject createdContainer, SchedulingMode schedulingMode,
      ResourceLimits currentResoureLimits) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("assignContainers: node=" + node.getNodeName()
        + " application=" + application.getApplicationId()
    int availableContainers = 
        resourceCalculator.computeAvailableContainers(available, capability);

    boolean needToUnreserve = Resources.greaterThan(resourceCalculator,clusterResource,
        currentResoureLimits.getAmountNeededUnreserve(), Resources.none());

    if (availableContainers > 0) {

        unreserve(application, priority, node, rmContainer);
      } else if (this.reservationsContinueLooking && node.getLabels().isEmpty()) {
        if (!shouldAllocOrReserveNewContainer || needToUnreserve) {
          Resource amountToUnreserve = capability;
          if (needToUnreserve) {
            amountToUnreserve = currentResoureLimits.getAmountNeededUnreserve();
          }
          boolean containerUnreserved =
              findNodeToUnreserve(clusterResource, node, application, priority,
                  amountToUnreserve);
          if (!containerUnreserved) {
            return new CSAssignment(Resources.none(), type);
          }
      if (shouldAllocOrReserveNewContainer || rmContainer != null) {

        if (reservationsContinueLooking && rmContainer == null) {
          if (needToUnreserve) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("we needed to unreserve to be able to allocate");
            }
            return new CSAssignment(Resources.none(), type);
          }
        }
    this.cachedResourceLimitsForHeadroom = new ResourceLimits(currentResourceLimits.getLimit());
    Resource queueMaxResource =
        Resources.multiplyAndNormalizeDown(resourceCalculator, labelManager
            .getResourceByLabel(RMNodeLabelsManager.NO_LABEL, clusterResource),
            queueCapacities
                .getAbsoluteMaximumCapacity(RMNodeLabelsManager.NO_LABEL),
            minimumAllocation);
    this.cachedResourceLimitsForHeadroom.setLimit(Resources.min(resourceCalculator,
        clusterResource, queueMaxResource, currentResourceLimits.getLimit()));
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestReservations.java

    boolean res = a.findNodeToUnreserve(csContext.getClusterResource(),
        node_1, app_0, priorityMap, capability);
    assertFalse(res);

    app_0.reserve(node_1, priorityMap, rmContainer, container);
    node_1.reserveResource(app_0, priorityMap, rmContainer);
    res = a.findNodeToUnreserve(csContext.getClusterResource(), node_1, app_0,
        priorityMap, capability);
    assertFalse(res);
  }

    Resource capability = Resources.createResource(32 * GB, 0);
    ResourceLimits limits = new ResourceLimits(clusterResource);
    boolean res =
        a.canAssignToThisQueue(clusterResource,
            RMNodeLabelsManager.NO_LABEL, limits, capability, Resources.none(),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertFalse(res);
    assertEquals(limits.getAmountNeededUnreserve(), Resources.none());

    assertEquals(3 * GB, node_1.getUsedResource().getMemory());

    capability = Resources.createResource(5 * GB, 0);
    limits = new ResourceLimits(clusterResource);
    res =
        a.canAssignToThisQueue(clusterResource,
            RMNodeLabelsManager.NO_LABEL, limits, capability, Resources.createResource(5 * GB),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertTrue(res);
    assertEquals(Resources.createResource(2 * GB, 3), limits.getAmountNeededUnreserve());

    limits = new ResourceLimits(clusterResource);
    res =
        a.canAssignToThisQueue(clusterResource,
            RMNodeLabelsManager.NO_LABEL,limits, capability, Resources.none(),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertFalse(res);
    assertEquals(Resources.none(), limits.getAmountNeededUnreserve());

    refreshQueuesTurnOffReservationsContLook(a, csConf);

    limits = new ResourceLimits(clusterResource);
    res =
        a.canAssignToThisQueue(clusterResource,
            RMNodeLabelsManager.NO_LABEL, limits, capability, Resources.none(),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertFalse(res);
    assertEquals(limits.getAmountNeededUnreserve(), Resources.none());
    limits = new ResourceLimits(clusterResource);
    res =
        a.canAssignToThisQueue(clusterResource,
            RMNodeLabelsManager.NO_LABEL, limits, capability, Resources.createResource(5 * GB),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertFalse(res);
    assertEquals(Resources.none(), limits.getAmountNeededUnreserve());
  }

  public void refreshQueuesTurnOffReservationsContLook(LeafQueue a,
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());

    Resource limit = Resources.createResource(14 * GB, 0);
    ResourceLimits userResourceLimits = new ResourceLimits(clusterResource);
    boolean res = a.canAssignToUser(clusterResource, user_0, limit, app_0, "", userResourceLimits);
    assertTrue(res);
    assertEquals(Resources.none(), userResourceLimits.getAmountNeededUnreserve());


    limit = Resources.createResource(12 * GB, 0);
    userResourceLimits = new ResourceLimits(clusterResource);
    res = a.canAssignToUser(clusterResource, user_0, limit, app_0,
             "", userResourceLimits);
    assertTrue(res);
    assertEquals(Resources.createResource(1 * GB, 4),
        userResourceLimits.getAmountNeededUnreserve());

    refreshQueuesTurnOffReservationsContLook(a, csConf);
    userResourceLimits = new ResourceLimits(clusterResource);

    res = a.canAssignToUser(clusterResource, user_0, limit, app_0, "", userResourceLimits);
    assertFalse(res);
    assertEquals(Resources.none(), userResourceLimits.getAmountNeededUnreserve());
  }

  @Test

