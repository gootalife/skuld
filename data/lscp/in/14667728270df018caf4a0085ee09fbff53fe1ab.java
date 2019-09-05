hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/AbstractCSQueue.java

public abstract class AbstractCSQueue implements CSQueue {
  private static final Log LOG = LogFactory.getLog(AbstractCSQueue.class);  
  CSQueue parent;
  final String queueName;
  volatile int numContainers;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CSAssignment.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.AssignmentInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.resource.Resources;

@Private
@Unstable
public class CSAssignment {
  public static final CSAssignment NULL_ASSIGNMENT =
      new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);

  public static final CSAssignment SKIP_ASSIGNMENT = new CSAssignment(true);

  private Resource resource;
  private NodeType type;
  private RMContainer excessReservation;
  private FiCaSchedulerApp application;
    return resource;
  }
  
  public void setResource(Resource resource) {
    this.resource = resource;
  }

  public NodeType getType() {
    return type;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/LeafQueue.java
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY
        && !accessibleToPartition(node.getPartition())) {
      return CSAssignment.NULL_ASSIGNMENT;
    }

            + ", because it doesn't need more resource, schedulingMode="
            + schedulingMode.name() + " node-partition=" + node.getPartition());
      }
      return CSAssignment.NULL_ASSIGNMENT;
    }

    for (Iterator<FiCaSchedulerApp> assignmentIterator =
      if (!super.canAssignToThisQueue(clusterResource, node.getPartition(),
          currentResourceLimits, application.getCurrentReservation(),
          schedulingMode)) {
        return CSAssignment.NULL_ASSIGNMENT;
      }
      
      Resource userLimit =
      } else if (!assignment.getSkipped()) {
        return CSAssignment.NULL_ASSIGNMENT;
      }
    }

    return CSAssignment.NULL_ASSIGNMENT;
  }

  protected Resource getHeadroom(User user, Resource queueCurrentLimit,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/ParentQueue.java
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY
        && !accessibleToPartition(node.getPartition())) {
      return CSAssignment.NULL_ASSIGNMENT;
    }
    
            + ", because it doesn't need more resource, schedulingMode="
            + schedulingMode.name() + " node-partition=" + node.getPartition());
      }
      return CSAssignment.NULL_ASSIGNMENT;
    }
    
    CSAssignment assignment = 

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/allocator/AllocationState.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/allocator/AllocationState.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator;

public enum AllocationState {
  APP_SKIPPED,
  PRIORITY_SKIPPED,
  LOCALITY_SKIPPED,
  QUEUE_SKIPPED,
  ALLOCATED,
  RESERVED
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/allocator/ContainerAllocation.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/allocator/ContainerAllocation.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.util.resource.Resources;

public class ContainerAllocation {
  public static final ContainerAllocation PRIORITY_SKIPPED =
      new ContainerAllocation(null, null, AllocationState.PRIORITY_SKIPPED);
  
  public static final ContainerAllocation APP_SKIPPED =
      new ContainerAllocation(null, null, AllocationState.APP_SKIPPED);

  public static final ContainerAllocation QUEUE_SKIPPED =
      new ContainerAllocation(null, null, AllocationState.QUEUE_SKIPPED);
  
  public static final ContainerAllocation LOCALITY_SKIPPED =
      new ContainerAllocation(null, null, AllocationState.LOCALITY_SKIPPED);
  
  RMContainer containerToBeUnreserved;
  private Resource resourceToBeAllocated = Resources.none();
  AllocationState state;
  NodeType containerNodeType = NodeType.NODE_LOCAL;
  NodeType requestNodeType = NodeType.NODE_LOCAL;
  Container updatedContainer;

  public ContainerAllocation(RMContainer containerToBeUnreserved,
      Resource resourceToBeAllocated, AllocationState state) {
    this.containerToBeUnreserved = containerToBeUnreserved;
    this.resourceToBeAllocated = resourceToBeAllocated;
    this.state = state;
  }
  
  public RMContainer getContainerToBeUnreserved() {
    return containerToBeUnreserved;
  }
  
  public Resource getResourceToBeAllocated() {
    if (resourceToBeAllocated == null) {
      return Resources.none();
    }
    return resourceToBeAllocated;
  }
  
  public AllocationState getAllocationState() {
    return state;
  }
  
  public NodeType getContainerNodeType() {
    return containerNodeType;
  }
  
  public Container getUpdatedContainer() {
    return updatedContainer;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/allocator/ContainerAllocator.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/allocator/ContainerAllocator.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

public abstract class ContainerAllocator {
  FiCaSchedulerApp application;
  final ResourceCalculator rc;
  final RMContext rmContext;
  
  public ContainerAllocator(FiCaSchedulerApp application,
      ResourceCalculator rc, RMContext rmContext) {
    this.application = application;
    this.rc = rc;
    this.rmContext = rmContext;
  }
  
  abstract ContainerAllocation preAllocation(
      Resource clusterResource, FiCaSchedulerNode node,
      SchedulingMode schedulingMode, ResourceLimits resourceLimits,
      Priority priority, RMContainer reservedContainer);
  
  abstract ContainerAllocation doAllocation(
      ContainerAllocation allocationResult, Resource clusterResource,
      FiCaSchedulerNode node, SchedulingMode schedulingMode, Priority priority,
      RMContainer reservedContainer);
  
  boolean checkHeadroom(Resource clusterResource,
      ResourceLimits currentResourceLimits, Resource required,
      FiCaSchedulerNode node) {
    Resource resourceCouldBeUnReserved = application.getCurrentReservation();
    if (!application.getCSLeafQueue().getReservationContinueLooking()
        || !node.getPartition().equals(RMNodeLabelsManager.NO_LABEL)) {
      resourceCouldBeUnReserved = Resources.none();
    }
    return Resources.greaterThanOrEqual(rc, clusterResource, Resources.add(
        currentResourceLimits.getHeadroom(), resourceCouldBeUnReserved),
        required);
  }
  
  public ContainerAllocation allocate(Resource clusterResource,
      FiCaSchedulerNode node, SchedulingMode schedulingMode,
      ResourceLimits resourceLimits, Priority priority,
      RMContainer reservedContainer) {
    ContainerAllocation result =
        preAllocation(clusterResource, node, schedulingMode,
            resourceLimits, priority, reservedContainer);
    
    if (AllocationState.ALLOCATED == result.state
        || AllocationState.RESERVED == result.state) {
      result = doAllocation(result, clusterResource, node,
          schedulingMode, priority, reservedContainer);
    }
    
    return result;
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/allocator/RegularContainerAllocator.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/allocator/RegularContainerAllocator.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

public class RegularContainerAllocator extends ContainerAllocator {
  private static final Log LOG = LogFactory.getLog(RegularContainerAllocator.class);
  
  private ResourceRequest lastResourceRequest = null;
  
  public RegularContainerAllocator(FiCaSchedulerApp application,
      ResourceCalculator rc, RMContext rmContext) {
    super(application, rc, rmContext);
  }
  
  private ContainerAllocation preCheckForNewContainer(Resource clusterResource,
      FiCaSchedulerNode node, SchedulingMode schedulingMode,
      ResourceLimits resourceLimits, Priority priority) {
    if (SchedulerAppUtils.isBlacklisted(application, node, LOG)) {
      return ContainerAllocation.APP_SKIPPED;
    }

    ResourceRequest anyRequest =
        application.getResourceRequest(priority, ResourceRequest.ANY);
    if (null == anyRequest) {
      return ContainerAllocation.PRIORITY_SKIPPED;
    }

    Resource required = anyRequest.getCapability();

    if (application.getTotalRequiredResources(priority) <= 0) {
      return ContainerAllocation.PRIORITY_SKIPPED;
    }

    if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      RMAppAttempt rmAppAttempt =
          rmContext.getRMApps().get(application.getApplicationId())
              .getCurrentAppAttempt();
      if (rmAppAttempt.getSubmissionContext().getUnmanagedAM() == false
          && null == rmAppAttempt.getMasterContainer()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip allocating AM container to app_attempt="
              + application.getApplicationAttemptId()
              + ", don't allow to allocate AM container in non-exclusive mode");
        }
        return ContainerAllocation.APP_SKIPPED;
      }
    }

    if (!SchedulerUtils.checkResourceRequestMatchingNodePartition(anyRequest,
        node.getPartition(), schedulingMode)) {
      return ContainerAllocation.PRIORITY_SKIPPED;
    }

    if (!application.getCSLeafQueue().getReservationContinueLooking()) {
      if (!shouldAllocOrReserveNewContainer(priority, required)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("doesn't need containers based on reservation algo!");
        }
        return ContainerAllocation.PRIORITY_SKIPPED;
      }
    }

    if (!checkHeadroom(clusterResource, resourceLimits, required, node)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("cannot allocate required resource=" + required
            + " because of headroom");
      }
      return ContainerAllocation.QUEUE_SKIPPED;
    }

    application.addSchedulingOpportunity(priority);

    int missedNonPartitionedRequestSchedulingOpportunity = 0;
    if (anyRequest.getNodeLabelExpression()
        .equals(RMNodeLabelsManager.NO_LABEL)) {
      missedNonPartitionedRequestSchedulingOpportunity =
          application
              .addMissedNonPartitionedRequestSchedulingOpportunity(priority);
    }

    if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      if (missedNonPartitionedRequestSchedulingOpportunity < rmContext
          .getScheduler().getNumClusterNodes()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip app_attempt=" + application.getApplicationAttemptId()
              + " priority=" + priority
              + " because missed-non-partitioned-resource-request"
              + " opportunity under requred:" + " Now="
              + missedNonPartitionedRequestSchedulingOpportunity + " required="
              + rmContext.getScheduler().getNumClusterNodes());
        }

        return ContainerAllocation.APP_SKIPPED;
      }
    }
    
    return null;
  }

  @Override
  ContainerAllocation preAllocation(Resource clusterResource,
      FiCaSchedulerNode node, SchedulingMode schedulingMode,
      ResourceLimits resourceLimits, Priority priority,
      RMContainer reservedContainer) {
    ContainerAllocation result;
    if (null == reservedContainer) {
      result =
          preCheckForNewContainer(clusterResource, node, schedulingMode,
              resourceLimits, priority);
      if (null != result) {
        return result;
      }
    } else {
      if (application.getTotalRequiredResources(priority) == 0) {
        return new ContainerAllocation(reservedContainer, null,
            AllocationState.QUEUE_SKIPPED);
      }
    }

    result =
        assignContainersOnNode(clusterResource, node, priority,
            reservedContainer, schedulingMode, resourceLimits);
    
    if (null == reservedContainer) {
      if (result.state == AllocationState.PRIORITY_SKIPPED) {
        application.subtractSchedulingOpportunity(priority);
      }
    }
    
    return result;
  }
  
  public synchronized float getLocalityWaitFactor(
      Priority priority, int clusterNodes) {
    int requiredResources = 
        Math.max(application.getResourceRequests(priority).size() - 1, 0);
    
    return Math.min(((float)requiredResources / clusterNodes), 1.0f);
  }
  
  private int getActualNodeLocalityDelay() {
    return Math.min(rmContext.getScheduler().getNumClusterNodes(), application
        .getCSLeafQueue().getNodeLocalityDelay());
  }

  private boolean canAssign(Priority priority, FiCaSchedulerNode node,
      NodeType type, RMContainer reservedContainer) {

    if (type == NodeType.OFF_SWITCH) {
      if (reservedContainer != null) {
        return true;
      }

      ResourceRequest offSwitchRequest =
          application.getResourceRequest(priority, ResourceRequest.ANY);
      long missedOpportunities = application.getSchedulingOpportunities(priority);
      long requiredContainers = offSwitchRequest.getNumContainers();

      float localityWaitFactor =
          getLocalityWaitFactor(priority, rmContext.getScheduler()
              .getNumClusterNodes());

      return ((requiredContainers * localityWaitFactor) < missedOpportunities);
    }

    ResourceRequest rackLocalRequest =
        application.getResourceRequest(priority, node.getRackName());
    if (rackLocalRequest == null || rackLocalRequest.getNumContainers() <= 0) {
      return false;
    }

    if (type == NodeType.RACK_LOCAL) {
      long missedOpportunities = application.getSchedulingOpportunities(priority);
      return getActualNodeLocalityDelay() < missedOpportunities;
    }

    if (type == NodeType.NODE_LOCAL) {
      ResourceRequest nodeLocalRequest =
          application.getResourceRequest(priority, node.getNodeName());
      if (nodeLocalRequest != null) {
        return nodeLocalRequest.getNumContainers() > 0;
      }
    }

    return false;
  }

  private ContainerAllocation assignNodeLocalContainers(
      Resource clusterResource, ResourceRequest nodeLocalResourceRequest,
      FiCaSchedulerNode node, Priority priority, RMContainer reservedContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResoureLimits) {
    if (canAssign(priority, node, NodeType.NODE_LOCAL, reservedContainer)) {
      return assignContainer(clusterResource, node, priority,
          nodeLocalResourceRequest, NodeType.NODE_LOCAL, reservedContainer,
          schedulingMode, currentResoureLimits);
    }

    return ContainerAllocation.LOCALITY_SKIPPED;
  }

  private ContainerAllocation assignRackLocalContainers(
      Resource clusterResource, ResourceRequest rackLocalResourceRequest,
      FiCaSchedulerNode node, Priority priority, RMContainer reservedContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResoureLimits) {
    if (canAssign(priority, node, NodeType.RACK_LOCAL, reservedContainer)) {
      return assignContainer(clusterResource, node, priority,
          rackLocalResourceRequest, NodeType.RACK_LOCAL, reservedContainer,
          schedulingMode, currentResoureLimits);
    }

    return ContainerAllocation.LOCALITY_SKIPPED;
  }

  private ContainerAllocation assignOffSwitchContainers(
      Resource clusterResource, ResourceRequest offSwitchResourceRequest,
      FiCaSchedulerNode node, Priority priority, RMContainer reservedContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResoureLimits) {
    if (canAssign(priority, node, NodeType.OFF_SWITCH, reservedContainer)) {
      return assignContainer(clusterResource, node, priority,
          offSwitchResourceRequest, NodeType.OFF_SWITCH, reservedContainer,
          schedulingMode, currentResoureLimits);
    }

    return ContainerAllocation.QUEUE_SKIPPED;
  }

  private ContainerAllocation assignContainersOnNode(Resource clusterResource,
      FiCaSchedulerNode node, Priority priority, RMContainer reservedContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResoureLimits) {

    ContainerAllocation assigned;

    NodeType requestType = null;
    ResourceRequest nodeLocalResourceRequest =
        application.getResourceRequest(priority, node.getNodeName());
    if (nodeLocalResourceRequest != null) {
      requestType = NodeType.NODE_LOCAL;
      assigned =
          assignNodeLocalContainers(clusterResource, nodeLocalResourceRequest,
              node, priority, reservedContainer, schedulingMode,
              currentResoureLimits);
      if (Resources.greaterThan(rc, clusterResource,
          assigned.getResourceToBeAllocated(), Resources.none())) {
        assigned.requestNodeType = requestType;
        return assigned;
      }
    }

    ResourceRequest rackLocalResourceRequest =
        application.getResourceRequest(priority, node.getRackName());
    if (rackLocalResourceRequest != null) {
      if (!rackLocalResourceRequest.getRelaxLocality()) {
        return ContainerAllocation.PRIORITY_SKIPPED;
      }

      if (requestType != NodeType.NODE_LOCAL) {
        requestType = NodeType.RACK_LOCAL;
      }

      assigned =
          assignRackLocalContainers(clusterResource, rackLocalResourceRequest,
              node, priority, reservedContainer, schedulingMode,
              currentResoureLimits);
      if (Resources.greaterThan(rc, clusterResource,
          assigned.getResourceToBeAllocated(), Resources.none())) {
        assigned.requestNodeType = requestType;
        return assigned;
      }
    }

    ResourceRequest offSwitchResourceRequest =
        application.getResourceRequest(priority, ResourceRequest.ANY);
    if (offSwitchResourceRequest != null) {
      if (!offSwitchResourceRequest.getRelaxLocality()) {
        return ContainerAllocation.PRIORITY_SKIPPED;
      }
      if (requestType != NodeType.NODE_LOCAL
          && requestType != NodeType.RACK_LOCAL) {
        requestType = NodeType.OFF_SWITCH;
      }

      assigned =
          assignOffSwitchContainers(clusterResource, offSwitchResourceRequest,
              node, priority, reservedContainer, schedulingMode,
              currentResoureLimits);
      assigned.requestNodeType = requestType;

      return assigned;
    }

    return ContainerAllocation.PRIORITY_SKIPPED;
  }

  private ContainerAllocation assignContainer(Resource clusterResource,
      FiCaSchedulerNode node, Priority priority, ResourceRequest request,
      NodeType type, RMContainer rmContainer, SchedulingMode schedulingMode,
      ResourceLimits currentResoureLimits) {
    lastResourceRequest = request;
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("assignContainers: node=" + node.getNodeName()
        + " application=" + application.getApplicationId()
        + " priority=" + priority.getPriority()
        + " request=" + request + " type=" + type);
    }

    if (!SchedulerUtils.checkResourceRequestMatchingNodePartition(request,
        node.getPartition(), schedulingMode)) {
      return new ContainerAllocation(rmContainer, null,
          AllocationState.QUEUE_SKIPPED);
    }

    Resource capability = request.getCapability();
    Resource available = node.getAvailableResource();
    Resource totalResource = node.getTotalResource();

    if (!Resources.lessThanOrEqual(rc, clusterResource,
        capability, totalResource)) {
      LOG.warn("Node : " + node.getNodeID()
          + " does not have sufficient resource for request : " + request
          + " node total capability : " + node.getTotalResource());
      return ContainerAllocation.QUEUE_SKIPPED;
    }

    assert Resources.greaterThan(
        rc, clusterResource, available, Resources.none());

    boolean shouldAllocOrReserveNewContainer = shouldAllocOrReserveNewContainer(
        priority, capability);

    int availableContainers =
        rc.computeAvailableContainers(available, capability);

    Resource resourceNeedToUnReserve =
        Resources.max(rc, clusterResource,
            Resources.subtract(capability, currentResoureLimits.getHeadroom()),
            currentResoureLimits.getAmountNeededUnreserve());

    boolean needToUnreserve =
        Resources.greaterThan(rc, clusterResource,
            resourceNeedToUnReserve, Resources.none());

    RMContainer unreservedContainer = null;
    boolean reservationsContinueLooking =
        application.getCSLeafQueue().getReservationContinueLooking();

    if (availableContainers > 0) {
      if (rmContainer == null && reservationsContinueLooking
          && node.getLabels().isEmpty()) {
        if (!shouldAllocOrReserveNewContainer || needToUnreserve) {
          if (!needToUnreserve) {
            resourceNeedToUnReserve = capability;
          }
          unreservedContainer =
              application.findNodeToUnreserve(clusterResource, node, priority,
                  resourceNeedToUnReserve);
          if (null == unreservedContainer) {
            return ContainerAllocation.QUEUE_SKIPPED;
          }
        }
      }

      ContainerAllocation result =
          new ContainerAllocation(unreservedContainer, request.getCapability(),
              AllocationState.ALLOCATED);
      result.containerNodeType = type;
      return result;
    } else {
      if (shouldAllocOrReserveNewContainer || rmContainer != null) {

        if (reservationsContinueLooking && rmContainer == null) {
          if (needToUnreserve) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("we needed to unreserve to be able to allocate");
            }
            return ContainerAllocation.QUEUE_SKIPPED;
          }
        }

        ContainerAllocation result =
            new ContainerAllocation(null, request.getCapability(),
                AllocationState.RESERVED);
        result.containerNodeType = type;
        return result;
      }
      return ContainerAllocation.QUEUE_SKIPPED;
    }
  }

  boolean
      shouldAllocOrReserveNewContainer(Priority priority, Resource required) {
    int requiredContainers = application.getTotalRequiredResources(priority);
    int reservedContainers = application.getNumReservedContainers(priority);
    int starvation = 0;
    if (reservedContainers > 0) {
      float nodeFactor =
          Resources
              .ratio(rc, required, application.getCSLeafQueue().getMaximumAllocation());

      starvation =
          (int) ((application.getReReservations(priority) / 
              (float) reservedContainers) * (1.0f - (Math.min(
                  nodeFactor, application.getCSLeafQueue()
                  .getMinimumAllocationFactor()))));

      if (LOG.isDebugEnabled()) {
        LOG.debug("needsContainers:" + " app.#re-reserve="
            + application.getReReservations(priority) + " reserved="
            + reservedContainers + " nodeFactor=" + nodeFactor
            + " minAllocFactor="
            + application.getCSLeafQueue().getMinimumAllocationFactor()
            + " starvation=" + starvation);
      }
    }
    return (((starvation + requiredContainers) - reservedContainers) > 0);
  }
  
  private Container getContainer(RMContainer rmContainer,
      FiCaSchedulerNode node, Resource capability, Priority priority) {
    return (rmContainer != null) ? rmContainer.getContainer()
        : createContainer(node, capability, priority);
  }

  private Container createContainer(FiCaSchedulerNode node, Resource capability,
      Priority priority) {

    NodeId nodeId = node.getRMNode().getNodeID();
    ContainerId containerId =
        BuilderUtils.newContainerId(application.getApplicationAttemptId(),
            application.getNewContainerId());

    return BuilderUtils.newContainer(containerId, nodeId, node.getRMNode()
        .getHttpAddress(), capability, priority, null);
  }
  
  private ContainerAllocation handleNewContainerAllocation(
      ContainerAllocation allocationResult, FiCaSchedulerNode node,
      Priority priority, RMContainer reservedContainer, Container container) {
    if (reservedContainer != null) {
      application.unreserve(priority, node, reservedContainer);
    }
    
    RMContainer allocatedContainer =
        application.allocate(allocationResult.containerNodeType, node,
            priority, lastResourceRequest, container);

    if (allocatedContainer == null) {
      ContainerAllocation ret =
          new ContainerAllocation(allocationResult.containerToBeUnreserved,
              null, AllocationState.QUEUE_SKIPPED);
      ret.state = AllocationState.APP_SKIPPED;
      return ret;
    }

    node.allocateContainer(allocatedContainer);
    
    application.incNumAllocatedContainers(allocationResult.containerNodeType,
        allocationResult.requestNodeType);
    
    return allocationResult;    
  }

  @Override
  ContainerAllocation doAllocation(ContainerAllocation allocationResult,
      Resource clusterResource, FiCaSchedulerNode node,
      SchedulingMode schedulingMode, Priority priority,
      RMContainer reservedContainer) {
    Container container =
        getContainer(reservedContainer, node,
            allocationResult.getResourceToBeAllocated(), priority);

    if (container == null) {
      LOG.warn("Couldn't get container for allocation!");
      return ContainerAllocation.QUEUE_SKIPPED;
    }

    if (allocationResult.getAllocationState() == AllocationState.ALLOCATED) {
      allocationResult =
          handleNewContainerAllocation(allocationResult, node, priority,
              reservedContainer, container);
    } else {
      application.reserve(priority, node, reservedContainer, container);
    }
    allocationResult.updatedContainer = container;

    if (reservedContainer == null) {
      if (allocationResult.containerNodeType != NodeType.OFF_SWITCH) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Resetting scheduling opportunities");
        }
        application.resetSchedulingOpportunities(priority);
      }
      
      application.resetMissedNonPartitionedRequestSchedulingOpportunity(priority);
    }

    return allocationResult;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/common/fica/FiCaSchedulerApp.java
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAssignment;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityHeadroomProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator.AllocationState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator.ContainerAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator.RegularContainerAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator.ContainerAllocation;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
public class FiCaSchedulerApp extends SchedulerApplicationAttempt {
  private static final Log LOG = LogFactory.getLog(FiCaSchedulerApp.class);

  private final Set<ContainerId> containersToPreempt =
    new HashSet<ContainerId>();
    

  private ResourceScheduler scheduler;
  
  private ContainerAllocator containerAllocator;

  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId, 
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext) {
    if (scheduler.getResourceCalculator() != null) {
      rc = scheduler.getResourceCalculator();
    }
    
    containerAllocator = new RegularContainerAllocator(this, rc, rmContext);
  }

  synchronized public boolean containerCompleted(RMContainer rmContainer,
      ((FiCaSchedulerApp) appAttempt).getHeadroomProvider();
  }

  public void reserve(Priority priority,
      FiCaSchedulerNode node, RMContainer rmContainer, Container container) {
    node.reserveResource(this, priority, rmContainer);
  }

  @VisibleForTesting
  public RMContainer findNodeToUnreserve(Resource clusterResource,
      FiCaSchedulerNode node, Priority priority,
    return nodeToUnreserve.getReservedContainer();
  }

  public LeafQueue getCSLeafQueue() {
    return (LeafQueue)queue;
  }

  private CSAssignment getCSAssignmentFromAllocateResult(
      Resource clusterResource, ContainerAllocation result) {
    boolean skipped =
        (result.getAllocationState() == AllocationState.APP_SKIPPED);
    CSAssignment assignment = new CSAssignment(skipped);
    assignment.setApplication(this);
    
    assignment.setExcessReservation(result.getContainerToBeUnreserved());

    if (Resources.greaterThan(rc, clusterResource,
        result.getResourceToBeAllocated(), Resources.none())) {
      Resource allocatedResource = result.getResourceToBeAllocated();
      Container updatedContainer = result.getUpdatedContainer();
      
      assignment.setResource(allocatedResource);
      assignment.setType(result.getContainerNodeType());

      if (result.getAllocationState() == AllocationState.RESERVED) {
        LOG.info("Reserved container " + " application=" + getApplicationId()
            + " resource=" + allocatedResource + " queue="
            + this.toString() + " cluster=" + clusterResource);
        assignment.getAssignmentInformation().addReservationDetails(
            updatedContainer.getId(), getCSLeafQueue().getQueuePath());
        assignment.getAssignmentInformation().incrReservations();
        Resources.addTo(assignment.getAssignmentInformation().getReserved(),
            allocatedResource);
        assignment.setFulfilledReservation(true);
      } else {
        LOG.info("assignedContainer" + " application attempt="
            + getApplicationAttemptId() + " container="
            + updatedContainer.getId() + " queue=" + this + " clusterResource="
            + clusterResource);

        getCSLeafQueue().getOrderingPolicy().containerAllocated(this,
            getRMContainer(updatedContainer.getId()));

        assignment.getAssignmentInformation().addAllocationDetails(
            updatedContainer.getId(), getCSLeafQueue().getQueuePath());
        assignment.getAssignmentInformation().incrAllocations();
        Resources.addTo(assignment.getAssignmentInformation().getAllocated(),
            allocatedResource);
      }
    }
    
    return assignment;
  }
  
  public CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits currentResourceLimits,
            + ", because it doesn't need more resource, schedulingMode="
            + schedulingMode.name() + " node-label=" + node.getPartition());
      }
      return CSAssignment.SKIP_ASSIGNMENT;
    }

    synchronized (this) {
      for (Priority priority : getPriorities()) {
        ContainerAllocation allocationResult =
            containerAllocator.allocate(clusterResource, node,
                schedulingMode, currentResourceLimits, priority, null);

        AllocationState allocationState = allocationResult.getAllocationState();

        if (allocationState == AllocationState.PRIORITY_SKIPPED) {
          continue;
        }
        return getCSAssignmentFromAllocateResult(clusterResource,
            allocationResult);
      }
    }

    return CSAssignment.SKIP_ASSIGNMENT;
  }


  public synchronized CSAssignment assignReservedContainer(
      FiCaSchedulerNode node, RMContainer rmContainer,
      Resource clusterResource, SchedulingMode schedulingMode) {
    ContainerAllocation result =
        containerAllocator.allocate(clusterResource, node,
            schedulingMode, new ResourceLimits(Resources.none()),
            rmContainer.getReservedPriority(), rmContainer);

    return getCSAssignmentFromAllocateResult(clusterResource, result);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestLeafQueue.java
            ContainerState.COMPLETE, "",
            ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
        RMContainerEventType.KILL, null, true);
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(4*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, node_0.getUsedResource().getMemory());
  }
  
  private void verifyContainerAllocated(CSAssignment assignment, NodeType nodeType) {
    Assert.assertTrue(Resources.greaterThan(resourceCalculator, null,
        assignment.getResource(), Resources.none()));
    Assert
        .assertTrue(assignment.getAssignmentInformation().getNumAllocations() > 0);
    Assert.assertEquals(nodeType, assignment.getType());
  }

  private void verifyNoContainerAllocated(CSAssignment assignment) {
    Assert.assertTrue(Resources.equals(assignment.getResource(),
        Resources.none()));
    Assert
        .assertTrue(assignment.getAssignmentInformation().getNumAllocations() == 0);
  }

  @Test
  public void testLocalityScheduling() throws Exception {
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 =
        new FiCaSchedulerApp(appAttemptId_0, user_0, a,
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);

    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyNoContainerAllocated(assignment);
    assertEquals(1, app_0.getSchedulingOpportunities(priority));
    assertEquals(3, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType()); // None->NODE_LOCAL
    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyNoContainerAllocated(assignment);
    assertEquals(2, app_0.getSchedulingOpportunities(priority));
    assertEquals(3, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType()); // None->NODE_LOCAL
    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyNoContainerAllocated(assignment);
    assertEquals(3, app_0.getSchedulingOpportunities(priority));
    assertEquals(3, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType()); // None->NODE_LOCAL
    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyContainerAllocated(assignment, NodeType.OFF_SWITCH);
    assertEquals(4, app_0.getSchedulingOpportunities(priority)); // should NOT reset
    assertEquals(2, app_0.getTotalRequiredResources(priority));
    
    assignment = a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyContainerAllocated(assignment, NodeType.NODE_LOCAL);
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(1, app_0.getTotalRequiredResources(priority));
    
    assignment = a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyContainerAllocated(assignment, NodeType.NODE_LOCAL);
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(0, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType());
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(1, app_0.getSchedulingOpportunities(priority));
    assertEquals(2, app_0.getTotalRequiredResources(priority));

    assignment = a.assignContainers(clusterResource, node_3,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyContainerAllocated(assignment, NodeType.RACK_LOCAL);
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(1, app_0.getTotalRequiredResources(priority));
  }
  
  @Test
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 =
        new FiCaSchedulerApp(appAttemptId_0, user_0, a,
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);
    
    
    CSAssignment assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyNoContainerAllocated(assignment);
    assertEquals(1, app_0.getSchedulingOpportunities(priority_1));
    assertEquals(2, app_0.getTotalRequiredResources(priority_1));
    assertEquals(0, app_0.getSchedulingOpportunities(priority_2));
    assertEquals(1, app_0.getTotalRequiredResources(priority_2));

    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyNoContainerAllocated(assignment);
    assertEquals(2, app_0.getSchedulingOpportunities(priority_1));
    assertEquals(2, app_0.getTotalRequiredResources(priority_1));
    assertEquals(0, app_0.getSchedulingOpportunities(priority_2));
    assertEquals(1, app_0.getTotalRequiredResources(priority_2));

    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyContainerAllocated(assignment, NodeType.OFF_SWITCH);
    assertEquals(3, app_0.getSchedulingOpportunities(priority_1));
    assertEquals(1, app_0.getTotalRequiredResources(priority_1));
    assertEquals(0, app_0.getSchedulingOpportunities(priority_2));
    assertEquals(1, app_0.getTotalRequiredResources(priority_2));

    assignment = a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyContainerAllocated(assignment, NodeType.NODE_LOCAL);
    assertEquals(0, app_0.getSchedulingOpportunities(priority_1));
    assertEquals(0, app_0.getTotalRequiredResources(priority_1));
    assertEquals(0, app_0.getSchedulingOpportunities(priority_2));
    assertEquals(1, app_0.getTotalRequiredResources(priority_2));

    assignment = a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyContainerAllocated(assignment, NodeType.OFF_SWITCH);
    assertEquals(0, app_0.getSchedulingOpportunities(priority_1));
    assertEquals(0, app_0.getTotalRequiredResources(priority_1));
    assertEquals(1, app_0.getSchedulingOpportunities(priority_2));
    assertEquals(0, app_0.getTotalRequiredResources(priority_2));

    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 = 
        new FiCaSchedulerApp(appAttemptId_0, user_0, a, 
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);
    
    app_0.updateResourceRequests(app_0_requests_0);
    
    CSAssignment assignment = a.assignContainers(clusterResource, node_0_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyContainerAllocated(assignment, NodeType.NODE_LOCAL);
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(0, app_0.getTotalRequiredResources(priority));

    assignment = a.assignContainers(clusterResource, node_1_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyNoContainerAllocated(assignment);
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // Still zero
    assertEquals(0, app_0.getTotalRequiredResources(priority));

    assignment = a.assignContainers(clusterResource, node_0_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyNoContainerAllocated(assignment);
    assertEquals(1, app_0.getSchedulingOpportunities(priority)); 
    assertEquals(1, app_0.getTotalRequiredResources(priority));
    
    assignment = a.assignContainers(clusterResource, node_1_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyContainerAllocated(assignment, NodeType.NODE_LOCAL);
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(0, app_0.getTotalRequiredResources(priority));
  }

  @Test (timeout = 30000)
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 =
        new FiCaSchedulerApp(appAttemptId_0, user_0, a,
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_1 =
        new FiCaSchedulerApp(appAttemptId_1, user_0, a,
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_1, user_0);

    
    CSAssignment assignment =
        a.assignContainers(clusterResource, node_0_1, new ResourceLimits(
            clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyNoContainerAllocated(assignment);
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should be 0
    

    assignment = a.assignContainers(clusterResource, node_1_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyNoContainerAllocated(assignment);
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should be 0
    

    assignment = a.assignContainers(clusterResource, node_1_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyNoContainerAllocated(assignment);
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should be 0


    assignment = a.assignContainers(clusterResource, node_1_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyNoContainerAllocated(assignment);
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should be 0
    

    assignment = a.assignContainers(clusterResource, node_1_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyNoContainerAllocated(assignment);
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); 
    assertEquals(1, app_0.getTotalRequiredResources(priority));


    assignment = a.assignContainers(clusterResource, node_1_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyContainerAllocated(assignment, NodeType.NODE_LOCAL);
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); 
    assertEquals(0, app_0.getTotalRequiredResources(priority));


