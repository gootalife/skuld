hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/QueueMetrics.java
  public MetricsSystem getMetricsSystem() {
    return metricsSystem;
  }

  public long getAggregateAllocatedContainers() {
    return aggregateContainersAllocated.value();
  }

  public long getAggegatedReleasedContainers() {
    return aggregateContainersReleased.value();
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/SchedulerHealth.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/SchedulerHealth.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SchedulerHealth {

  static public class DetailedInformation {
    long timestamp;
    NodeId nodeId;
    ContainerId containerId;
    String queue;

    public DetailedInformation(long timestamp, NodeId nodeId,
        ContainerId containerId, String queue) {
      this.timestamp = timestamp;
      this.nodeId = nodeId;
      this.containerId = containerId;
      this.queue = queue;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public NodeId getNodeId() {
      return nodeId;
    }

    public ContainerId getContainerId() {
      return containerId;
    }

    public String getQueue() {
      return queue;
    }
  }

  enum Operation {
    ALLOCATION, RELEASE, PREEMPTION, RESERVATION, FULFILLED_RESERVATION
  }

  long lastSchedulerRunTime;
  Map<Operation, Resource> lastSchedulerRunDetails;
  Map<Operation, DetailedInformation> schedulerHealthDetails;
  Map<Operation, Long> schedulerOperationCounts;
  Map<Operation, Long> schedulerOperationAggregateCounts;

  public SchedulerHealth() {
    lastSchedulerRunDetails = new ConcurrentHashMap<>();
    schedulerHealthDetails = new ConcurrentHashMap<>();
    schedulerOperationCounts = new ConcurrentHashMap<>();
    schedulerOperationAggregateCounts = new ConcurrentHashMap<>();
    for (Operation op : Operation.values()) {
      lastSchedulerRunDetails.put(op, Resource.newInstance(0, 0));
      schedulerOperationCounts.put(op, 0L);
      schedulerHealthDetails.put(op, new DetailedInformation(0, null, null,
        null));
      schedulerOperationAggregateCounts.put(op, 0L);
    }

  }

  public void updateAllocation(long timestamp, NodeId nodeId,
      ContainerId containerId, String queue) {
    DetailedInformation di =
        new DetailedInformation(timestamp, nodeId, containerId, queue);
    schedulerHealthDetails.put(Operation.ALLOCATION, di);
  }

  public void updateRelease(long timestamp, NodeId nodeId,
      ContainerId containerId, String queue) {
    DetailedInformation di =
        new DetailedInformation(timestamp, nodeId, containerId, queue);
    schedulerHealthDetails.put(Operation.RELEASE, di);
  }

  public void updatePreemption(long timestamp, NodeId nodeId,
      ContainerId containerId, String queue) {
    DetailedInformation di =
        new DetailedInformation(timestamp, nodeId, containerId, queue);
    schedulerHealthDetails.put(Operation.PREEMPTION, di);
  }

  public void updateReservation(long timestamp, NodeId nodeId,
      ContainerId containerId, String queue) {
    DetailedInformation di =
        new DetailedInformation(timestamp, nodeId, containerId, queue);
    schedulerHealthDetails.put(Operation.RESERVATION, di);
  }

  public void updateSchedulerRunDetails(long timestamp, Resource allocated,
      Resource reserved) {
    lastSchedulerRunTime = timestamp;
    lastSchedulerRunDetails.put(Operation.ALLOCATION, allocated);
    lastSchedulerRunDetails.put(Operation.RESERVATION, reserved);
  }

  public void updateSchedulerReleaseDetails(long timestamp, Resource released) {
    lastSchedulerRunTime = timestamp;
    lastSchedulerRunDetails.put(Operation.RELEASE, released);
  }

  public void updateSchedulerReleaseCounts(long count) {
    updateCounts(Operation.RELEASE, count);
  }

  public void updateSchedulerAllocationCounts(long count) {
    updateCounts(Operation.ALLOCATION, count);
  }

  public void updateSchedulerReservationCounts(long count) {
    updateCounts(Operation.RESERVATION, count);
  }

  public void updateSchedulerFulfilledReservationCounts(long count) {
    updateCounts(Operation.FULFILLED_RESERVATION, count);
  }

  public void updateSchedulerPreemptionCounts(long count) {
    updateCounts(Operation.PREEMPTION, count);
  }

  private void updateCounts(Operation op, long count) {
    schedulerOperationCounts.put(op, count);
    Long tmp = schedulerOperationAggregateCounts.get(op);
    schedulerOperationAggregateCounts.put(op, tmp + count);
  }

  public long getLastSchedulerRunTime() {
    return lastSchedulerRunTime;
  }

  private Resource getResourceDetails(Operation op) {
    return lastSchedulerRunDetails.get(op);
  }

  public Resource getResourcesAllocated() {
    return getResourceDetails(Operation.ALLOCATION);
  }

  public Resource getResourcesReserved() {
    return getResourceDetails(Operation.RESERVATION);
  }

  public Resource getResourcesReleased() {
    return getResourceDetails(Operation.RELEASE);
  }

  private DetailedInformation getDetailedInformation(Operation op) {
    return schedulerHealthDetails.get(op);
  }

  public DetailedInformation getLastAllocationDetails() {
    return getDetailedInformation(Operation.ALLOCATION);
  }

  public DetailedInformation getLastReleaseDetails() {
    return getDetailedInformation(Operation.RELEASE);
  }

  public DetailedInformation getLastReservationDetails() {
    return getDetailedInformation(Operation.RESERVATION);
  }

  public DetailedInformation getLastPreemptionDetails() {
    return getDetailedInformation(Operation.PREEMPTION);
  }

  private Long getOperationCount(Operation op) {
    return schedulerOperationCounts.get(op);
  }

  public Long getAllocationCount() {
    return getOperationCount(Operation.ALLOCATION);
  }

  public Long getReleaseCount() {
    return getOperationCount(Operation.RELEASE);
  }

  public Long getReservationCount() {
    return getOperationCount(Operation.RESERVATION);
  }

  public Long getPreemptionCount() {
    return getOperationCount(Operation.PREEMPTION);
  }

  private Long getAggregateOperationCount(Operation op) {
    return schedulerOperationAggregateCounts.get(op);
  }

  public Long getAggregateAllocationCount() {
    return getAggregateOperationCount(Operation.ALLOCATION);
  }

  public Long getAggregateReleaseCount() {
    return getAggregateOperationCount(Operation.RELEASE);
  }

  public Long getAggregateReservationCount() {
    return getAggregateOperationCount(Operation.RESERVATION);
  }

  public Long getAggregatePreemptionCount() {
    return getAggregateOperationCount(Operation.PREEMPTION);
  }

  public Long getAggregateFulFilledReservationsCount() {
    return getAggregateOperationCount(Operation.FULFILLED_RESERVATION);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CSAssignment.java
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.AssignmentInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;

@Private
@Unstable
  private final RMContainer excessReservation;
  private final FiCaSchedulerApp application;
  private final boolean skipped;
  private boolean fulfilledReservation;
  private final AssignmentInformation assignmentInformation;

  public CSAssignment(Resource resource, NodeType type) {
    this(resource, type, null, null, false, false);
  }

  public CSAssignment(FiCaSchedulerApp application,
      RMContainer excessReservation) {
    this(excessReservation.getContainer().getResource(), NodeType.NODE_LOCAL,
      excessReservation, application, false, false);
  }

  public CSAssignment(boolean skipped) {
    this(Resource.newInstance(0, 0), NodeType.NODE_LOCAL, null, null, skipped,
      false);
  }

  public CSAssignment(Resource resource, NodeType type,
      RMContainer excessReservation, FiCaSchedulerApp application,
      boolean skipped, boolean fulfilledReservation) {
    this.resource = resource;
    this.type = type;
    this.excessReservation = excessReservation;
    this.application = application;
    this.skipped = skipped;
    this.fulfilledReservation = fulfilledReservation;
    this.assignmentInformation = new AssignmentInformation();
  }

  public Resource getResource() {
  
  @Override
  public String toString() {
    String ret = "resource:" + resource.toString();
    ret += "; type:" + type;
    ret += "; excessReservation:" + excessReservation;
    ret +=
        "; applicationid:"
            + (application != null ? application.getApplicationId().toString()
                : "null");
    ret += "; skipped:" + skipped;
    ret += "; fulfilled reservation:" + fulfilledReservation;
    ret +=
        "; allocations(count/resource):"
            + assignmentInformation.getNumAllocations() + "/"
            + assignmentInformation.getAllocated().toString();
    ret +=
        "; reservations(count/resource):"
            + assignmentInformation.getNumReservations() + "/"
            + assignmentInformation.getReserved().toString();
    return ret;
  }
  
  public void setFulfilledReservation(boolean fulfilledReservation) {
    this.fulfilledReservation = fulfilledReservation;
  }

  public boolean isFulfilledReservation() {
    return this.fulfilledReservation;
  }
  
  public AssignmentInformation getAssignmentInformation() {
    return this.assignmentInformation;
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacityScheduler.java
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.QueueMapping;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.QueueMapping.MappingType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.AssignmentInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
  private boolean scheduleAsynchronously;
  private AsyncScheduleThread asyncSchedulerThread;
  private RMNodeLabelsManager labelManager;
  private SchedulerHealth schedulerHealth = new SchedulerHealth();
  long lastNodeUpdateTime;
      LOG.debug("nodeUpdate: " + nm + " clusterResources: " + clusterResource);
    }

    Resource releaseResources = Resource.newInstance(0, 0);

    FiCaSchedulerNode node = getNode(nm.getNodeID());
    
    List<UpdatedContainerInfo> containerInfoList = nm.pullContainerUpdates();
    }

    int releasedContainers = 0;
    for (ContainerStatus completedContainer : completedContainers) {
      ContainerId containerId = completedContainer.getContainerId();
      RMContainer container = getRMContainer(containerId);
      LOG.debug("Container FINISHED: " + containerId);
      completedContainer(container, completedContainer,
        RMContainerEventType.FINISHED);
      if (container != null) {
        releasedContainers++;
        Resource rs = container.getAllocatedResource();
        if (rs != null) {
          Resources.addTo(releaseResources, rs);
        }
        rs = container.getReservedResource();
        if (rs != null) {
          Resources.addTo(releaseResources, rs);
        }
      }
    }

    schedulerHealth.updateSchedulerReleaseDetails(lastNodeUpdateTime,
      releaseResources);
    schedulerHealth.updateSchedulerReleaseCounts(releasedContainers);

    if(LOG.isDebugEnabled()) {
    node.updateLabels(newLabels);
  }

  private void updateSchedulerHealth(long now, FiCaSchedulerNode node,
      CSAssignment assignment) {

    NodeId nodeId = node.getNodeID();
    List<AssignmentInformation.AssignmentDetails> allocations =
        assignment.getAssignmentInformation().getAllocationDetails();
    List<AssignmentInformation.AssignmentDetails> reservations =
        assignment.getAssignmentInformation().getReservationDetails();
    if (!allocations.isEmpty()) {
      ContainerId allocatedContainerId =
          allocations.get(allocations.size() - 1).containerId;
      String allocatedQueue = allocations.get(allocations.size() - 1).queue;
      schedulerHealth.updateAllocation(now, nodeId, allocatedContainerId,
        allocatedQueue);
    }
    if (!reservations.isEmpty()) {
      ContainerId reservedContainerId =
          reservations.get(reservations.size() - 1).containerId;
      String reservedQueue = reservations.get(reservations.size() - 1).queue;
      schedulerHealth.updateReservation(now, nodeId, reservedContainerId,
        reservedQueue);
    }
    schedulerHealth.updateSchedulerReservationCounts(assignment
      .getAssignmentInformation().getNumReservations());
    schedulerHealth.updateSchedulerAllocationCounts(assignment
      .getAssignmentInformation().getNumAllocations());
    schedulerHealth.updateSchedulerRunDetails(now, assignment
      .getAssignmentInformation().getAllocated(), assignment
      .getAssignmentInformation().getReserved());
 }

  private synchronized void allocateContainersToNode(FiCaSchedulerNode node) {
    if (rmContext.isWorkPreservingRecoveryEnabled()
        && !rmContext.isSchedulerReadyForAllocatingContainers()) {
      return;
    }
    updateSchedulerHealth(lastNodeUpdateTime, node,
      new CSAssignment(Resources.none(), NodeType.NODE_LOCAL));

    CSAssignment assignment;

          node.getNodeID());
      
      LeafQueue queue = ((LeafQueue)reservedApplication.getQueue());
      assignment = queue.assignContainers(
              clusterResource,
              node,
              new ResourceLimits(labelManager.getResourceByLabel(
                  RMNodeLabelsManager.NO_LABEL, clusterResource)));
      if (assignment.isFulfilledReservation()) {
        CSAssignment tmp =
            new CSAssignment(reservedContainer.getReservedResource(),
              assignment.getType());
        Resources.addTo(assignment.getAssignmentInformation().getAllocated(),
          reservedContainer.getReservedResource());
        tmp.getAssignmentInformation().addAllocationDetails(
          reservedContainer.getContainerId(), queue.getQueuePath());
        tmp.getAssignmentInformation().incrAllocations();
        updateSchedulerHealth(lastNodeUpdateTime, node, tmp);
        schedulerHealth.updateSchedulerFulfilledReservationCounts(1);
      }

      RMContainer excessReservation = assignment.getExcessReservation();
      if (excessReservation != null) {
          LOG.debug("Trying to schedule on node: " + node.getNodeName() +
              ", available: " + node.getAvailableResource());
        }
        assignment = root.assignContainers(
            clusterResource,
            node,
            new ResourceLimits(labelManager.getResourceByLabel(
                RMNodeLabelsManager.NO_LABEL, clusterResource)));
        updateSchedulerHealth(lastNodeUpdateTime, node, assignment);
      }
    } else {
      LOG.info("Skipping scheduling since node " + node.getNodeID() + 
    {
      NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)event;
      RMNode node = nodeUpdatedEvent.getRMNode();
      setLastNodeUpdateTime(Time.now());
      nodeUpdate(node);
      if (!scheduleAsynchronously) {
        allocateContainersToNode(getNode(node.getNodeID()));
    LOG.info("Application attempt " + application.getApplicationAttemptId()
        + " released container " + container.getId() + " on node: " + node
        + " with event: " + event);
    if (containerStatus.getExitStatus() == ContainerExitStatus.PREEMPTED) {
      schedulerHealth.updatePreemption(Time.now(), container.getNodeId(),
        container.getId(), queue.getQueuePath());
      schedulerHealth.updateSchedulerPreemptionCounts(1);
    } else {
      schedulerHealth.updateRelease(lastNodeUpdateTime, container.getNodeId(),
        container.getId(), queue.getQueuePath());
    }
  }

  @Lock(Lock.NoLock.class)
    }
    return ret;
  }

  public SchedulerHealth getSchedulerHealth() {
    return this.schedulerHealth;
  }

  private synchronized void setLastNodeUpdateTime(long time) {
    this.lastNodeUpdateTime = time;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/LeafQueue.java
    }

    CSAssignment tmp =
        assignContainersOnNode(clusterResource, node, application, priority,
          rmContainer);
    
    CSAssignment ret = new CSAssignment(Resources.none(), NodeType.NODE_LOCAL);
    if (tmp.getAssignmentInformation().getNumAllocations() > 0) {
      ret.setFulfilledReservation(true);
    }
    return ret;
  }
  
  protected Resource getHeadroom(User user, Resource queueCurrentLimit,
  private CSAssignment assignContainersOnNode(Resource clusterResource,
      FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer) {

    CSAssignment assigned;

    NodeType requestType = null;
    MutableObject allocatedContainer = new MutableObject();
            node, application, priority, reservedContainer,
            allocatedContainer);
      if (Resources.greaterThan(resourceCalculator, clusterResource,
        assigned.getResource(), Resources.none())) {

        if (allocatedContainer.getValue() != null) {
          application.incNumAllocatedContainers(NodeType.NODE_LOCAL,
            requestType);
        }
        assigned.setType(NodeType.NODE_LOCAL);
        return assigned;
      }
    }

            node, application, priority, reservedContainer,
            allocatedContainer);
      if (Resources.greaterThan(resourceCalculator, clusterResource,
        assigned.getResource(), Resources.none())) {

        if (allocatedContainer.getValue() != null) {
          application.incNumAllocatedContainers(NodeType.RACK_LOCAL,
            requestType);
        }
        assigned.setType(NodeType.RACK_LOCAL);
        return assigned;
      }
    }
    
      if (allocatedContainer.getValue() != null) {
        application.incNumAllocatedContainers(NodeType.OFF_SWITCH, requestType);
      }
      assigned.setType(NodeType.OFF_SWITCH);
      return assigned;
    }
    
    return SKIP_ASSIGNMENT;
  private Resource getMinimumResourceNeedUnreserved(Resource askedResource) {
    return Resources.subtract(
      Resources.add(queueUsage.getUsed(), askedResource),
      currentResourceLimits.getLimit());
  }

  @Private
  }


  private CSAssignment assignNodeLocalContainers(Resource clusterResource,
      ResourceRequest nodeLocalResourceRequest, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer) {
          allocatedContainer);
    }

    return new CSAssignment(Resources.none(), NodeType.NODE_LOCAL);
  }

  private CSAssignment assignRackLocalContainers(Resource clusterResource,
      ResourceRequest rackLocalResourceRequest, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer) {
          allocatedContainer);
    }

    return new CSAssignment(Resources.none(), NodeType.RACK_LOCAL);
  }

  private CSAssignment assignOffSwitchContainers(Resource clusterResource,
      ResourceRequest offSwitchResourceRequest, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer) {
          allocatedContainer);
    }
    
    return new CSAssignment(Resources.none(), NodeType.OFF_SWITCH);
  }

  boolean canAssign(FiCaSchedulerApp application, Priority priority, 
        .getApplicationAttemptId(), application.getNewContainerId());
  
    return BuilderUtils.newContainer(containerId, nodeId, node.getRMNode()
      .getHttpAddress(), capability, priority, null);

  }


  private CSAssignment assignContainer(Resource clusterResource, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority, 
      ResourceRequest request, NodeType type, RMContainer rmContainer,
      MutableObject createdContainer) {
      if (rmContainer != null) {
        unreserve(application, priority, node, rmContainer);
      }
      return new CSAssignment(Resources.none(), type);
    }
    
    Resource capability = request.getCapability();
      LOG.warn("Node : " + node.getNodeID()
          + " does not have sufficient resource for request : " + request
          + " node total capability : " + node.getTotalResource());
      return new CSAssignment(Resources.none(), type);
    }

    assert Resources.greaterThan(
    if (container == null) {
      LOG.warn("Couldn't get container for allocation!");
      return new CSAssignment(Resources.none(), type);
    }
    
    boolean shouldAllocOrReserveNewContainer = shouldAllocOrReserveNewContainer(
          if (!containerUnreserved) {
            return new CSAssignment(Resources.none(), type);
          }
        }
      }

      if (allocatedContainer == null) {
        return new CSAssignment(Resources.none(), type);
      }

          " queue=" + this + 
          " clusterResource=" + clusterResource);
      createdContainer.setValue(allocatedContainer);
      CSAssignment assignment = new CSAssignment(container.getResource(), type);
      assignment.getAssignmentInformation().addAllocationDetails(
        container.getId(), getQueuePath());
      assignment.getAssignmentInformation().incrAllocations();
      Resources.addTo(assignment.getAssignmentInformation().getAllocated(),
        container.getResource());
      return assignment;
    } else {
          if (!checkLimitsToReserve(clusterResource, 
              application, capability)) {
            return new CSAssignment(Resources.none(), type);
          }
        }

            " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() + 
            " used=" + queueUsage.getUsed() +
            " cluster=" + clusterResource);
        CSAssignment assignment =
            new CSAssignment(request.getCapability(), type);
        assignment.getAssignmentInformation().addReservationDetails(
          container.getId(), getQueuePath());
        assignment.getAssignmentInformation().incrReservations();
        Resources.addTo(assignment.getAssignmentInformation().getReserved(),
          request.getCapability());
        return assignment;
      }
      return new CSAssignment(Resources.none(), type);
    }
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/ParentQueue.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.AssignmentInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;
            nodeLabels);
        
        Resources
          .addTo(assignment.getResource(), assignedToChild.getResource());
        Resources.addTo(assignment.getAssignmentInformation().getAllocated(),
          assignedToChild.getAssignmentInformation().getAllocated());
        Resources.addTo(assignment.getAssignmentInformation().getReserved(),
            assignedToChild.getAssignmentInformation().getReserved());
        assignment.getAssignmentInformation().incrAllocations(
          assignedToChild.getAssignmentInformation().getNumAllocations());
        assignment.getAssignmentInformation().incrReservations(
          assignedToChild.getAssignmentInformation().getNumReservations());
        assignment
          .getAssignmentInformation()
          .getAllocationDetails()
          .addAll(
              assignedToChild.getAssignmentInformation().getAllocationDetails());
        assignment
          .getAssignmentInformation()
          .getReservationDetails()
          .addAll(
              assignedToChild.getAssignmentInformation()
                  .getReservationDetails());
        
        LOG.info("assignedContainer" +
            " queue=" + getQueueName() + 

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/common/AssignmentInformation.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/common/AssignmentInformation.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AssignmentInformation {

  public enum Operation {
    ALLOCATION, RESERVATION
  }

  public static class AssignmentDetails {
    public ContainerId containerId;
    public String queue;

    public AssignmentDetails(ContainerId containerId, String queue) {
      this.containerId = containerId;
      this.queue = queue;
    }
  }

  private final Map<Operation, Integer> operationCounts;
  private final Map<Operation, Resource> operationResources;
  private final Map<Operation, List<AssignmentDetails>> operationDetails;

  public AssignmentInformation() {
    this.operationCounts = new HashMap<>();
    this.operationResources = new HashMap<>();
    this.operationDetails = new HashMap<>();
    for (Operation op : Operation.values()) {
      operationCounts.put(op, 0);
      operationResources.put(op, Resource.newInstance(0, 0));
      operationDetails.put(op, new ArrayList<AssignmentDetails>());
    }
  }

  public int getNumAllocations() {
    return operationCounts.get(Operation.ALLOCATION);
  }

  public void incrAllocations() {
    increment(Operation.ALLOCATION, 1);
  }

  public void incrAllocations(int by) {
    increment(Operation.ALLOCATION, by);
  }

  public int getNumReservations() {
    return operationCounts.get(Operation.RESERVATION);
  }

  public void incrReservations() {
    increment(Operation.RESERVATION, 1);
  }

  public void incrReservations(int by) {
    increment(Operation.RESERVATION, by);
  }

  private void increment(Operation op, int by) {
    operationCounts.put(op, operationCounts.get(op) + by);
  }

  public Resource getAllocated() {
    return operationResources.get(Operation.ALLOCATION);
  }

  public Resource getReserved() {
    return operationResources.get(Operation.RESERVATION);
  }

  private void addAssignmentDetails(Operation op, ContainerId containerId,
      String queue) {
    operationDetails.get(op).add(new AssignmentDetails(containerId, queue));
  }

  public void addAllocationDetails(ContainerId containerId, String queue) {
    addAssignmentDetails(Operation.ALLOCATION, containerId, queue);
  }

  public void addReservationDetails(ContainerId containerId, String queue) {
    addAssignmentDetails(Operation.RESERVATION, containerId, queue);
  }

  public List<AssignmentDetails> getAllocationDetails() {
    return operationDetails.get(Operation.ALLOCATION);
  }

  public List<AssignmentDetails> getReservationDetails() {
    return operationDetails.get(Operation.RESERVATION);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/CapacitySchedulerPage.java
import static org.apache.hadoop.yarn.util.StringHelper.join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
              span(".q", "default")._()._();
      } else {
        CSQueue root = cs.getRootQueue();
        CapacitySchedulerInfo sinfo = new CapacitySchedulerInfo(root, cs);
        csqinfo.csinfo = sinfo;
        csqinfo.qinfo = null;

      script().$type("text/javascript").
          _("$('#cs').hide();")._()._().
      _(RMAppsBlock.class);
      html._(HealthBlock.class);
    }
  }

  public static class HealthBlock extends HtmlBlock {

    final CapacityScheduler cs;

    @Inject
    HealthBlock(ResourceManager rm) {
      cs = (CapacityScheduler) rm.getResourceScheduler();
    }

    @Override
    public void render(HtmlBlock.Block html) {
      SchedulerHealth healthInfo = cs.getSchedulerHealth();
      DIV<Hamlet> div = html.div("#health");
      div.h4("Aggregate scheduler counts");
      TBODY<TABLE<DIV<Hamlet>>> tbody =
          div.table("#lastrun").thead().$class("ui-widget-header").tr().th()
            .$class("ui-state-default")._("Total Container Allocations(count)")
            ._().th().$class("ui-state-default")
            ._("Total Container Releases(count)")._().th()
            .$class("ui-state-default")
            ._("Total Fulfilled Reservations(count)")._().th()
            .$class("ui-state-default")._("Total Container Preemptions(count)")
            ._()._()._().tbody();
      tbody
        .$class("ui-widget-content")
        .tr()
        .td(
          String.valueOf(cs.getRootQueueMetrics()
            .getAggregateAllocatedContainers()))
        .td(
          String.valueOf(cs.getRootQueueMetrics()
            .getAggegatedReleasedContainers()))
        .td(healthInfo.getAggregateFulFilledReservationsCount().toString())
        .td(healthInfo.getAggregatePreemptionCount().toString())._()._()._();
      div.h4("Last scheduler run");
      tbody =
          div.table("#lastrun").thead().$class("ui-widget-header").tr().th()
            .$class("ui-state-default")._("Time")._().th()
            .$class("ui-state-default")._("Allocations(count - resources)")._()
            .th().$class("ui-state-default")._("Reservations(count - resources)")
            ._().th().$class("ui-state-default")._("Releases(count - resources)")
            ._()._()._().tbody();
      tbody
        .$class("ui-widget-content")
        .tr()
        .td(Times.format(healthInfo.getLastSchedulerRunTime()))
        .td(
          healthInfo.getAllocationCount().toString() + " - "
              + healthInfo.getResourcesAllocated().toString())
        .td(
          healthInfo.getReservationCount().toString() + " - "
              + healthInfo.getResourcesReserved().toString())
        .td(
          healthInfo.getReleaseCount().toString() + " - "
              + healthInfo.getResourcesReleased().toString())._()._()._();
      Map<String, SchedulerHealth.DetailedInformation> info = new HashMap<>();
      info.put("Allocation", healthInfo.getLastAllocationDetails());
      info.put("Reservation", healthInfo.getLastReservationDetails());
      info.put("Release", healthInfo.getLastReleaseDetails());
      info.put("Preemption", healthInfo.getLastPreemptionDetails());

      for (Map.Entry<String, SchedulerHealth.DetailedInformation> entry : info
        .entrySet()) {
        String containerId = "N/A";
        String nodeId = "N/A";
        String queue = "N/A";
        String table = "#" + entry.getKey();
        div.h4("Last " + entry.getKey());
        tbody =
            div.table(table).thead().$class("ui-widget-header").tr().th()
              .$class("ui-state-default")._("Time")._().th()
              .$class("ui-state-default")._("Container Id")._().th()
              .$class("ui-state-default")._("Node Id")._().th()
              .$class("ui-state-default")._("Queue")._()._()._().tbody();
        SchedulerHealth.DetailedInformation di = entry.getValue();
        if (di.getTimestamp() != 0) {
          containerId = di.getContainerId().toString();
          nodeId = di.getNodeId().toString();
          queue = di.getQueue();
        }
        tbody.$class("ui-widget-content").tr()
          .td(Times.format(di.getTimestamp())).td(containerId).td(nodeId)
          .td(queue)._()._()._();
      }
      div._();
    }
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/JAXBContextResolver.java
            NodesInfo.class, RemoteExceptionData.class,
            CapacitySchedulerQueueInfoList.class, ResourceInfo.class,
            UsersInfo.class, UserInfo.class, ApplicationStatisticsInfo.class,
            StatisticsItemInfo.class, CapacitySchedulerHealthInfo.class };
    final Class[] rootUnwrappedTypes =
        { NewApplication.class, ApplicationSubmissionContextInfo.class,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMWebServices.java
    if (rs instanceof CapacityScheduler) {
      CapacityScheduler cs = (CapacityScheduler) rs;
      CSQueue root = cs.getRootQueue();
      sinfo = new CapacitySchedulerInfo(root, cs);
    } else if (rs instanceof FairScheduler) {
      FairScheduler fs = (FairScheduler) rs;
      sinfo = new FairSchedulerInfo(fs);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/CapacitySchedulerHealthInfo.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/CapacitySchedulerHealthInfo.java

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@XmlAccessorType(XmlAccessType.FIELD)
public class CapacitySchedulerHealthInfo {

  @XmlAccessorType(XmlAccessType.FIELD)
  public static class OperationInformation {
    String nodeId;
    String containerId;
    String queue;

    OperationInformation() {
    }

    OperationInformation(SchedulerHealth.DetailedInformation di) {
      this.nodeId = di.getNodeId() == null ? "N/A" : di.getNodeId().toString();
      this.containerId =
          di.getContainerId() == null ? "N/A" : di.getContainerId().toString();
      this.queue = di.getQueue() == null ? "N/A" : di.getQueue();
    }

    public String getNodeId() {
      return nodeId;
    }

    public String getContainerId() {
      return containerId;
    }

    public String getQueue() {
      return queue;
    }
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  public static class LastRunDetails {
    String operation;
    long count;
    ResourceInfo resources;

    LastRunDetails() {
    }

    LastRunDetails(String operation, long count, Resource resource) {
      this.operation = operation;
      this.count = count;
      this.resources = new ResourceInfo(resource);
    }

    public String getOperation() {
      return operation;
    }

    public long getCount() {
      return count;
    }

    public ResourceInfo getResources() {
      return resources;
    }
  }

  long lastrun;
  Map<String, OperationInformation> operationsInfo;
  List<LastRunDetails> lastRunDetails;

  CapacitySchedulerHealthInfo() {
  }

  public long getLastrun() {
    return lastrun;
  }

  CapacitySchedulerHealthInfo(CapacityScheduler cs) {
    SchedulerHealth ht = cs.getSchedulerHealth();
    lastrun = ht.getLastSchedulerRunTime();
    operationsInfo = new HashMap<>();
    operationsInfo.put("last-allocation",
      new OperationInformation(ht.getLastAllocationDetails()));
    operationsInfo.put("last-release",
      new OperationInformation(ht.getLastReleaseDetails()));
    operationsInfo.put("last-preemption",
      new OperationInformation(ht.getLastPreemptionDetails()));
    operationsInfo.put("last-reservation",
      new OperationInformation(ht.getLastReservationDetails()));

    lastRunDetails = new ArrayList<>();
    lastRunDetails.add(new LastRunDetails("releases", ht.getReleaseCount(), ht
      .getResourcesReleased()));
    lastRunDetails.add(new LastRunDetails("allocations", ht
      .getAllocationCount(), ht.getResourcesAllocated()));
    lastRunDetails.add(new LastRunDetails("reservations", ht
      .getReservationCount(), ht.getResourcesReserved()));

  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/CapacitySchedulerInfo.java
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;

@XmlRootElement(name = "capacityScheduler")
  protected float maxCapacity;
  protected String queueName;
  protected CapacitySchedulerQueueInfoList queues;
  protected CapacitySchedulerHealthInfo health;

  @XmlTransient
  static final float EPSILON = 1e-8f;
  public CapacitySchedulerInfo() {
  } // JAXB needs this

  public CapacitySchedulerInfo(CSQueue parent, CapacityScheduler cs) {
    this.queueName = parent.getQueueName();
    this.usedCapacity = parent.getUsedCapacity() * 100;
    this.capacity = parent.getCapacity() * 100;
    this.maxCapacity = max * 100;

    queues = getQueues(parent);
    health = new CapacitySchedulerHealthInfo(cs);
  }

  public float getCapacity() {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/TestSchedulerHealth.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/TestSchedulerHealth.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.Application;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.Task;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assume.assumeTrue;

public class TestSchedulerHealth {

  private ResourceManager resourceManager;

  public void setup() {
    resourceManager = new ResourceManager() {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
        mgr.init(getConfig());
        return mgr;
      }
    };

    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
      ResourceScheduler.class);
    resourceManager.init(conf);
    resourceManager.getRMContext().getContainerTokenSecretManager()
      .rollMasterKey();
    resourceManager.getRMContext().getNMTokenSecretManager().rollMasterKey();
    ((AsyncDispatcher) resourceManager.getRMContext().getDispatcher()).start();
  }

  @Test
  public void testCounts() {

    SchedulerHealth sh = new SchedulerHealth();
    int value = 1;
    for (int i = 0; i < 2; ++i) {
      sh.updateSchedulerPreemptionCounts(value);
      sh.updateSchedulerAllocationCounts(value);
      sh.updateSchedulerReservationCounts(value);
      sh.updateSchedulerReleaseCounts(value);

      Assert.assertEquals(value, sh.getAllocationCount().longValue());
      Assert.assertEquals(value, sh.getReleaseCount().longValue());
      Assert.assertEquals(value, sh.getReservationCount().longValue());
      Assert.assertEquals(value, sh.getPreemptionCount().longValue());

      Assert.assertEquals(value * (i + 1), sh.getAggregateAllocationCount()
        .longValue());
      Assert.assertEquals(value * (i + 1), sh.getAggregateReleaseCount()
        .longValue());
      Assert.assertEquals(value * (i + 1), sh.getAggregateReservationCount()
        .longValue());
      Assert.assertEquals(value * (i + 1), sh.getAggregatePreemptionCount()
        .longValue());

    }
  }

  @Test
  public void testOperationDetails() {

    SchedulerHealth sh = new SchedulerHealth();
    long now = Time.now();
    sh.updateRelease(now, NodeId.newInstance("testhost", 1234),
      ContainerId.fromString("container_1427562107907_0002_01_000001"),
      "testqueue");
    Assert.assertEquals("container_1427562107907_0002_01_000001", sh
      .getLastReleaseDetails().getContainerId().toString());
    Assert.assertEquals("testhost:1234", sh.getLastReleaseDetails().getNodeId()
      .toString());
    Assert.assertEquals("testqueue", sh.getLastReleaseDetails().getQueue());
    Assert.assertEquals(now, sh.getLastReleaseDetails().getTimestamp());
    Assert.assertEquals(0, sh.getLastSchedulerRunTime());

    now = Time.now();
    sh.updateReservation(now, NodeId.newInstance("testhost1", 1234),
      ContainerId.fromString("container_1427562107907_0003_01_000001"),
      "testqueue1");
    Assert.assertEquals("container_1427562107907_0003_01_000001", sh
      .getLastReservationDetails().getContainerId().toString());
    Assert.assertEquals("testhost1:1234", sh.getLastReservationDetails()
      .getNodeId().toString());
    Assert
      .assertEquals("testqueue1", sh.getLastReservationDetails().getQueue());
    Assert.assertEquals(now, sh.getLastReservationDetails().getTimestamp());
    Assert.assertEquals(0, sh.getLastSchedulerRunTime());

    now = Time.now();
    sh.updateAllocation(now, NodeId.newInstance("testhost2", 1234),
      ContainerId.fromString("container_1427562107907_0004_01_000001"),
      "testqueue2");
    Assert.assertEquals("container_1427562107907_0004_01_000001", sh
      .getLastAllocationDetails().getContainerId().toString());
    Assert.assertEquals("testhost2:1234", sh.getLastAllocationDetails()
      .getNodeId().toString());
    Assert.assertEquals("testqueue2", sh.getLastAllocationDetails().getQueue());
    Assert.assertEquals(now, sh.getLastAllocationDetails().getTimestamp());
    Assert.assertEquals(0, sh.getLastSchedulerRunTime());

    now = Time.now();
    sh.updatePreemption(now, NodeId.newInstance("testhost3", 1234),
      ContainerId.fromString("container_1427562107907_0005_01_000001"),
      "testqueue3");
    Assert.assertEquals("container_1427562107907_0005_01_000001", sh
      .getLastPreemptionDetails().getContainerId().toString());
    Assert.assertEquals("testhost3:1234", sh.getLastPreemptionDetails()
      .getNodeId().toString());
    Assert.assertEquals("testqueue3", sh.getLastPreemptionDetails().getQueue());
    Assert.assertEquals(now, sh.getLastPreemptionDetails().getTimestamp());
    Assert.assertEquals(0, sh.getLastSchedulerRunTime());
  }

  @Test
  public void testResourceUpdate() {
    SchedulerHealth sh = new SchedulerHealth();
    long now = Time.now();
    sh.updateSchedulerRunDetails(now, Resource.newInstance(1024, 1),
      Resource.newInstance(2048, 1));
    Assert.assertEquals(now, sh.getLastSchedulerRunTime());
    Assert.assertEquals(Resource.newInstance(1024, 1),
      sh.getResourcesAllocated());
    Assert.assertEquals(Resource.newInstance(2048, 1),
      sh.getResourcesReserved());
    now = Time.now();
    sh.updateSchedulerReleaseDetails(now, Resource.newInstance(3072, 1));
    Assert.assertEquals(now, sh.getLastSchedulerRunTime());
    Assert.assertEquals(Resource.newInstance(3072, 1),
      sh.getResourcesReleased());
  }

  private NodeManager registerNode(String hostName, int containerManagerPort,
      int httpPort, String rackName, Resource capability) throws IOException,
      YarnException {
    NodeManager nm =
        new NodeManager(hostName, containerManagerPort, httpPort, rackName,
          capability, resourceManager);
    NodeAddedSchedulerEvent nodeAddEvent1 =
        new NodeAddedSchedulerEvent(resourceManager.getRMContext().getRMNodes()
          .get(nm.getNodeId()));
    resourceManager.getResourceScheduler().handle(nodeAddEvent1);
    return nm;
  }

  private void nodeUpdate(NodeManager nm) {
    RMNode node =
        resourceManager.getRMContext().getRMNodes().get(nm.getNodeId());
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    resourceManager.getResourceScheduler().handle(nodeUpdate);
  }

  @Test
  public void testCapacitySchedulerAllocation() throws Exception {

    setup();

    boolean isCapacityScheduler =
        resourceManager.getResourceScheduler() instanceof CapacityScheduler;
    assumeTrue("This test is only supported on Capacity Scheduler",
      isCapacityScheduler);

    String host_0 = "host_0";
    NodeManager nm_0 =
        registerNode(host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
          Resources.createResource(5 * 1024, 1));

    Priority priority_0 =
        org.apache.hadoop.yarn.server.resourcemanager.resource.Priority
          .create(0);
    Priority priority_1 =
        org.apache.hadoop.yarn.server.resourcemanager.resource.Priority
          .create(1);

    Application application_0 =
        new Application("user_0", "default", resourceManager);
    application_0.submit();

    application_0.addNodeManager(host_0, 1234, nm_0);

    Resource capability_0_0 = Resources.createResource(1024, 1);
    application_0.addResourceRequestSpec(priority_1, capability_0_0);

    Resource capability_0_1 = Resources.createResource(2 * 1024, 1);
    application_0.addResourceRequestSpec(priority_0, capability_0_1);

    Task task_0_0 =
        new Task(application_0, priority_1, new String[] { host_0 });
    application_0.addTask(task_0_0);
    Task task_0_1 =
        new Task(application_0, priority_0, new String[] { host_0 });
    application_0.addTask(task_0_1);

    application_0.schedule();

    nodeUpdate(nm_0);
    SchedulerHealth sh =
        ((CapacityScheduler) resourceManager.getResourceScheduler())
          .getSchedulerHealth();
    Assert.assertEquals(2, sh.getAllocationCount().longValue());
    Assert.assertEquals(Resource.newInstance(3 * 1024, 2),
      sh.getResourcesAllocated());
    Assert.assertEquals(2, sh.getAggregateAllocationCount().longValue());
    Assert.assertEquals("host_0:1234", sh.getLastAllocationDetails()
      .getNodeId().toString());
    Assert.assertEquals("root.default", sh.getLastAllocationDetails()
      .getQueue());

    Task task_0_2 =
        new Task(application_0, priority_0, new String[] { host_0 });
    application_0.addTask(task_0_2);
    application_0.schedule();

    nodeUpdate(nm_0);
    Assert.assertEquals(1, sh.getAllocationCount().longValue());
    Assert.assertEquals(Resource.newInstance(2 * 1024, 1),
      sh.getResourcesAllocated());
    Assert.assertEquals(3, sh.getAggregateAllocationCount().longValue());
    Assert.assertEquals("host_0:1234", sh.getLastAllocationDetails()
      .getNodeId().toString());
    Assert.assertEquals("root.default", sh.getLastAllocationDetails()
      .getQueue());
  }

  @Test
  public void testCapacitySchedulerReservation() throws Exception {

    setup();

    boolean isCapacityScheduler =
        resourceManager.getResourceScheduler() instanceof CapacityScheduler;
    assumeTrue("This test is only supported on Capacity Scheduler",
      isCapacityScheduler);

    String host_0 = "host_0";
    NodeManager nm_0 =
        registerNode(host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
          Resources.createResource(2 * 1024, 1));
    String host_1 = "host_1";
    NodeManager nm_1 =
        registerNode(host_1, 1234, 2345, NetworkTopology.DEFAULT_RACK,
          Resources.createResource(5 * 1024, 1));
    nodeUpdate(nm_0);
    nodeUpdate(nm_1);

    Priority priority_0 =
        org.apache.hadoop.yarn.server.resourcemanager.resource.Priority
          .create(0);
    Priority priority_1 =
        org.apache.hadoop.yarn.server.resourcemanager.resource.Priority
          .create(1);

    Application application_0 =
        new Application("user_0", "default", resourceManager);
    application_0.submit();

    application_0.addNodeManager(host_0, 1234, nm_0);
    application_0.addNodeManager(host_1, 1234, nm_1);

    Resource capability_0_0 = Resources.createResource(1024, 1);
    application_0.addResourceRequestSpec(priority_1, capability_0_0);

    Resource capability_0_1 = Resources.createResource(2 * 1024, 1);
    application_0.addResourceRequestSpec(priority_0, capability_0_1);

    Task task_0_0 =
        new Task(application_0, priority_1, new String[] { host_0 });
    application_0.addTask(task_0_0);

    application_0.schedule();

    nodeUpdate(nm_0);
    SchedulerHealth sh =
        ((CapacityScheduler) resourceManager.getResourceScheduler())
          .getSchedulerHealth();
    Assert.assertEquals(1, sh.getAllocationCount().longValue());
    Assert.assertEquals(Resource.newInstance(1024, 1),
      sh.getResourcesAllocated());
    Assert.assertEquals(1, sh.getAggregateAllocationCount().longValue());
    Assert.assertEquals("host_0:1234", sh.getLastAllocationDetails()
      .getNodeId().toString());
    Assert.assertEquals("root.default", sh.getLastAllocationDetails()
      .getQueue());

    Task task_0_1 =
        new Task(application_0, priority_0, new String[] { host_0 });
    application_0.addTask(task_0_1);
    application_0.schedule();

    nodeUpdate(nm_0);
    Assert.assertEquals(0, sh.getAllocationCount().longValue());
    Assert.assertEquals(1, sh.getReservationCount().longValue());
    Assert.assertEquals(Resource.newInstance(2 * 1024, 1),
      sh.getResourcesReserved());
    Assert.assertEquals(1, sh.getAggregateAllocationCount().longValue());
    Assert.assertEquals("host_0:1234", sh.getLastAllocationDetails()
      .getNodeId().toString());
    Assert.assertEquals("root.default", sh.getLastAllocationDetails()
      .getQueue());
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestCapacityScheduler.java
    CapacityScheduler cs =
        (CapacityScheduler) resourceManager.getResourceScheduler();
    CSQueue origRootQ = cs.getRootQueue();
    CapacitySchedulerInfo oldInfo = new CapacitySchedulerInfo(origRootQ, cs);
    int origNumAppsA = getNumAppsInQueue("a", origRootQ.getChildQueues());
    int origNumAppsRoot = origRootQ.getNumApplications();

    CSQueue newRootQ = cs.getRootQueue();
    int newNumAppsA = getNumAppsInQueue("a", newRootQ.getChildQueues());
    int newNumAppsRoot = newRootQ.getNumApplications();
    CapacitySchedulerInfo newInfo = new CapacitySchedulerInfo(newRootQ, cs);
    CapacitySchedulerLeafQueueInfo origOldA1 =
        (CapacitySchedulerLeafQueueInfo) getQueueInfo("a1", oldInfo.getQueues());
    CapacitySchedulerLeafQueueInfo origNewA1 =

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServicesCapacitySched.java
package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
    JSONObject info = json.getJSONObject("scheduler");
    assertEquals("incorrect number of elements", 1, info.length());
    info = info.getJSONObject("schedulerInfo");
    assertEquals("incorrect number of elements", 7, info.length());
    verifyClusterSchedulerGeneric(info.getString("type"),
        (float) info.getDouble("usedCapacity"),
        (float) info.getDouble("capacity"),
        (float) info.getDouble("maxCapacity"), info.getString("queueName"));
    JSONObject health = info.getJSONObject("health");
    assertNotNull(health);
    assertEquals("incorrect number of elements", 3, health.length());

    JSONArray arr = info.getJSONObject("queues").getJSONArray("queue");
    assertEquals("incorrect number of elements", 2, arr.length());

