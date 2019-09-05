hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/RMContextImpl.java
    activeServiceContext.setNMTokenSecretManager(nmTokenSecretManager);
  }

  @VisibleForTesting
  public void setScheduler(ResourceScheduler scheduler) {
    activeServiceContext.setScheduler(scheduler);
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/ResourceLimits.java
public class ResourceLimits {
  private volatile Resource limit;

  private volatile Resource amountNeededUnreserve;

  private volatile Resource headroom;

  public ResourceLimits(Resource limit) {
    this(limit, Resources.none());
  }

  public ResourceLimits(Resource limit, Resource amountNeededUnreserve) {
    this.amountNeededUnreserve = amountNeededUnreserve;
    this.headroom = limit;
    this.limit = limit;
  }

    return limit;
  }

  public Resource getHeadroom() {
    return headroom;
  }

  public void setHeadroom(Resource headroom) {
    this.headroom = headroom;
  }

  public Resource getAmountNeededUnreserve() {
    return amountNeededUnreserve;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/AbstractCSQueue.java
  volatile int numContainers;
  
  final Resource minimumAllocation;
  volatile Resource maximumAllocation;
  QueueState state;
  final CSQueueMetrics metrics;
  protected final PrivilegedEntity queueEntity;
  
  Map<AccessType, AccessControlList> acls = 
      new HashMap<AccessType, AccessControlList>();
  volatile boolean reservationsContinueLooking;
  private boolean preemptionDisabled;

  }
  
  @Private
  public Resource getMaximumAllocation() {
    return maximumAllocation;
  }
  
  }
  
  synchronized boolean canAssignToThisQueue(Resource clusterResource,
      String nodePartition, ResourceLimits currentResourceLimits, Resource resourceCouldBeUnreserved,
      SchedulingMode schedulingMode) {
        getCurrentLimitResource(nodePartition, clusterResource,
            currentResourceLimits, schedulingMode);

    Resource nowTotalUsed = queueUsage.getUsed(nodePartition);

    currentResourceLimits.setHeadroom(Resources.subtract(currentLimitResource,
        nowTotalUsed));

    if (Resources.greaterThanOrEqual(resourceCalculator, clusterResource,
        nowTotalUsed, currentLimitResource)) {

              resourceCouldBeUnreserved, Resources.none())) {
        Resource newTotalWithoutReservedResource =
            Resources.subtract(nowTotalUsed, resourceCouldBeUnreserved);

                + newTotalWithoutReservedResource + ", maxLimitCapacity: "
                + currentLimitResource);
          }
          return true;
        }
      }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CSAssignment.java

  final private Resource resource;
  private NodeType type;
  private RMContainer excessReservation;
  private FiCaSchedulerApp application;
  private final boolean skipped;
  private boolean fulfilledReservation;
  private final AssignmentInformation assignmentInformation;
    return application;
  }

  public void setApplication(FiCaSchedulerApp application) {
    this.application = application;
  }

  public RMContainer getExcessReservation() {
    return excessReservation;
  }

  public void setExcessReservation(RMContainer rmContainer) {
    excessReservation = rmContainer;
  }

  public boolean getSkipped() {
    return skipped;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacityHeadroomProvider.java
  LeafQueue.User user;
  LeafQueue queue;
  FiCaSchedulerApp application;
  LeafQueue.QueueResourceLimitsInfo queueResourceLimitsInfo;
  
  public CapacityHeadroomProvider(LeafQueue.User user, LeafQueue queue,
      FiCaSchedulerApp application,
      LeafQueue.QueueResourceLimitsInfo queueResourceLimitsInfo) {

    this.user = user;
    this.queue = queue;
    this.application = application;
    this.queueResourceLimitsInfo = queueResourceLimitsInfo;
  }
  
  public Resource getHeadroom() {
      clusterResource = queueResourceLimitsInfo.getClusterResource();
    }
    Resource headroom = queue.getHeadroom(user, queueCurrentLimit, 
      clusterResource, application);
    
    if (headroom.getMemory() < 0) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacityScheduler.java
        updateSchedulerHealth(lastNodeUpdateTime, node, tmp);
        schedulerHealth.updateSchedulerFulfilledReservationCounts(1);
      }
    }

                RMNodeLabelsManager.NO_LABEL, clusterResource)),
            SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY);
        updateSchedulerHealth(lastNodeUpdateTime, node, assignment);
      }
    } else {
      LOG.info("Skipping scheduling since node "

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/LeafQueue.java
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
  
  private float maxAMResourcePerQueuePercent;
  
  private volatile int nodeLocalityDelay;

  Map<ApplicationAttemptId, FiCaSchedulerApp> applicationAttemptMap =
      new HashMap<ApplicationAttemptId, FiCaSchedulerApp>();

  Set<FiCaSchedulerApp> pendingApplications;
  
  private volatile float minimumAllocationFactor;

  private Map<String, User> users = new HashMap<String, User>();

    return Collections.singletonList(userAclInfo);
  }

  public String toString() {
    return queueName + ": " + 
        "capacity=" + queueCapacities.getCapacity() + ", " + 
    return applicationAttemptMap.get(applicationAttemptId);
  }
  
  private void handleExcessReservedContainer(Resource clusterResource,
      CSAssignment assignment) {
    if (assignment.getExcessReservation() != null) {
      RMContainer excessReservedContainer = assignment.getExcessReservation();

      completedContainer(clusterResource, assignment.getApplication(),
          scheduler.getNode(excessReservedContainer.getAllocatedNode()),
          excessReservedContainer,
          SchedulerUtils.createAbnormalContainerStatus(
              excessReservedContainer.getContainerId(),
              SchedulerUtils.UNRESERVED_CONTAINER),
          RMContainerEventType.RELEASED, null, false);

      assignment.setExcessReservation(null);
    }
  }
  
  @Override
  public synchronized CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits currentResourceLimits,

    if (LOG.isDebugEnabled()) {
      LOG.debug("assignContainers: node=" + node.getNodeName()
          + " #applications=" + orderingPolicy.getNumSchedulableEntities());
    }

      FiCaSchedulerApp application =
          getApplication(reservedContainer.getApplicationAttemptId());
      synchronized (application) {
        CSAssignment assignment = application.assignReservedContainer(node, reservedContainer,
            clusterResource, schedulingMode);
        handleExcessReservedContainer(clusterResource, assignment);
        return assignment;
      }
    }


    if (!hasPendingResourceRequest(node.getPartition(), clusterResource,
        schedulingMode)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip this queue=" + getQueuePath()
            + ", because it doesn't need more resource, schedulingMode="
    }

    for (Iterator<FiCaSchedulerApp> assignmentIterator =
        orderingPolicy.getAssignmentIterator(); assignmentIterator.hasNext();) {
      FiCaSchedulerApp application = assignmentIterator.next();

      if (!super.canAssignToThisQueue(clusterResource, node.getPartition(),
          currentResourceLimits, application.getCurrentReservation(),
          schedulingMode)) {
        return NULL_ASSIGNMENT;
      }
      
      Resource userLimit =
          computeUserLimitAndSetHeadroom(application, clusterResource,
              node.getPartition(), schedulingMode);

      if (!canAssignToUser(clusterResource, application.getUser(), userLimit,
          application, node.getPartition(), currentResourceLimits)) {
        continue;
      }

      CSAssignment assignment =
          application.assignContainers(clusterResource, node,
              currentResourceLimits, schedulingMode);

      if (LOG.isDebugEnabled()) {
        LOG.debug("post-assignContainers for application "
            + application.getApplicationId());
        application.showRequests();
      }

      Resource assigned = assignment.getResource();
      
      handleExcessReservedContainer(clusterResource, assignment);

      if (Resources.greaterThan(resourceCalculator, clusterResource, assigned,
          Resources.none())) {
        RMContainer reservedOrAllocatedRMContainer =
            application.getRMContainer(assignment.getAssignmentInformation()
                .getFirstAllocatedOrReservedContainerId());

        allocateResource(clusterResource, application, assigned,
            node.getPartition(), reservedOrAllocatedRMContainer);

        return assignment;
      } else if (!assignment.getSkipped()) {
        return NULL_ASSIGNMENT;
      }
    }

    return NULL_ASSIGNMENT;
  }

  protected Resource getHeadroom(User user, Resource queueCurrentLimit,
      Resource clusterResource, FiCaSchedulerApp application) {
    return getHeadroom(user, queueCurrentLimit, clusterResource,
        computeUserLimit(application, clusterResource, user,
            RMNodeLabelsManager.NO_LABEL,
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY));
  }
  
  private Resource getHeadroom(User user, Resource currentResourceLimit,

  @Lock({LeafQueue.class, FiCaSchedulerApp.class})
  Resource computeUserLimitAndSetHeadroom(FiCaSchedulerApp application,
      Resource clusterResource, String nodePartition,
      SchedulingMode schedulingMode) {
    String user = application.getUser();
    User queueUser = getUser(user);
    Resource userLimit =
        computeUserLimit(application, clusterResource, queueUser,
            nodePartition, schedulingMode);

    setQueueResourceLimitsInfo(clusterResource);
    
    }
    
    CapacityHeadroomProvider headroomProvider = new CapacityHeadroomProvider(
      queueUser, this, application, queueResourceLimitsInfo);
    
    application.setHeadroomProvider(headroomProvider);

    return userLimit;
  }
  
  @Lock(NoLock.class)
  public int getNodeLocalityDelay() {
    return nodeLocalityDelay;
  }

  @Lock(NoLock.class)
  private Resource computeUserLimit(FiCaSchedulerApp application,
      Resource clusterResource, User user,
      String nodePartition, SchedulingMode schedulingMode) {
            queueCapacities.getAbsoluteCapacity(nodePartition),
            minimumAllocation);

    Resource required = minimumAllocation;

    queueCapacity =
        Resources.max(
        if (Resources.lessThanOrEqual(
            resourceCalculator,
            clusterResource,
            Resources.subtract(user.getUsed(),
                application.getCurrentReservation()), limit)) {

          if (LOG.isDebugEnabled()) {
            LOG.debug("User " + userName + " in queue " + getQueueName()
                + user.getUsed() + " reserved: "
                + application.getCurrentReservation() + " limit: " + limit);
          }
          Resource amountNeededToUnreserve =
              Resources.subtract(user.getUsed(nodePartition), limit);
          currentResoureLimits.setAmountNeededUnreserve(amountNeededToUnreserve);
          return true;
        }
      }
    return true;
  }

  @Override
  public void completedContainer(Resource clusterResource, 
      FiCaSchedulerApp application, FiCaSchedulerNode node, RMContainer rmContainer, 
        if (rmContainer.getState() == RMContainerState.RESERVED) {
          removed = application.unreserve(rmContainer.getReservedPriority(),
              node, rmContainer);
        } else {
          removed =
    this.cachedResourceLimitsForHeadroom =
        new ResourceLimits(currentResourceLimits.getLimit());
    Resource queueMaxResource =
        Resources.multiplyAndNormalizeDown(resourceCalculator, labelManager
            .getResourceByLabel(RMNodeLabelsManager.NO_LABEL, clusterResource),
            queueCapacities
                .getAbsoluteMaximumCapacity(RMNodeLabelsManager.NO_LABEL),
            minimumAllocation);
    this.cachedResourceLimitsForHeadroom.setLimit(Resources.min(
        resourceCalculator, clusterResource, queueMaxResource,
        currentResourceLimits.getLimit()));
  }

  @Override
      orderingPolicy.getSchedulableEntities()) {
      synchronized (application) {
        computeUserLimitAndSetHeadroom(application, clusterResource,
            RMNodeLabelsManager.NO_LABEL,
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
      }
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/ParentQueue.java
  final PartitionedQueueComparator partitionQueueComparator;
  volatile int numApplications;
  private final CapacitySchedulerContext scheduler;
  private boolean needToResortQueuesAtNextAllocation = false;

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);
      if (!super.canAssignToThisQueue(clusterResource, node.getPartition(),
          resourceLimits, Resources.createResource(
              getMetrics().getReservedMB(), getMetrics()
                  .getReservedVirtualCores()), schedulingMode)) {
        break;
  
  private Iterator<CSQueue> sortAndGetChildrenAllocationIterator(FiCaSchedulerNode node) {
    if (node.getPartition().equals(RMNodeLabelsManager.NO_LABEL)) {
      if (needToResortQueuesAtNextAllocation) {
        List<CSQueue> childrenList = new ArrayList<>(childQueues);
        childQueues.clear();
        childQueues.addAll(childrenList);
        needToResortQueuesAtNextAllocation = false;
      }
      return childQueues.iterator();
    }

            }
          }
        }
        
        needToResortQueuesAtNextAllocation = !sortQueues;
      }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/common/fica/FiCaSchedulerApp.java
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.mutable.MutableObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAssignment;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityHeadroomProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
public class FiCaSchedulerApp extends SchedulerApplicationAttempt {
  private static final Log LOG = LogFactory.getLog(FiCaSchedulerApp.class);

  static final CSAssignment NULL_ASSIGNMENT =
      new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);

  static final CSAssignment SKIP_ASSIGNMENT = new CSAssignment(true);

  private final Set<ContainerId> containersToPreempt =
    new HashSet<ContainerId>();
    
  private CapacityHeadroomProvider headroomProvider;

  private ResourceCalculator rc = new DefaultResourceCalculator();

  private ResourceScheduler scheduler;

  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId, 
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext) {
    
    setAMResource(amResource);
    setPriority(appPriority);

    scheduler = rmContext.getScheduler();

    if (scheduler.getResourceCalculator() != null) {
      rc = scheduler.getResourceCalculator();
    }
  }

  synchronized public boolean containerCompleted(RMContainer rmContainer,
    return rmContainer;
  }

  public boolean unreserve(Priority priority,
      FiCaSchedulerNode node, RMContainer rmContainer) {
    if (unreserve(node, priority)) {
      node.unreserveResource(this);

      queue.getMetrics().unreserveResource(getUser(),
          rmContainer.getContainer().getResource());
      return true;
    }
    return false;
  }

  @VisibleForTesting
  public synchronized boolean unreserve(FiCaSchedulerNode node, Priority priority) {
    Map<NodeId, RMContainer> reservedContainers =
      this.reservedContainers.get(priority);
      ((FiCaSchedulerApp) appAttempt).getHeadroomProvider();
  }

  private int getActualNodeLocalityDelay() {
    return Math.min(scheduler.getNumClusterNodes(), getCSLeafQueue()
        .getNodeLocalityDelay());
  }

  private boolean canAssign(Priority priority, FiCaSchedulerNode node,
      NodeType type, RMContainer reservedContainer) {

    if (type == NodeType.OFF_SWITCH) {
      if (reservedContainer != null) {
        return true;
      }

      ResourceRequest offSwitchRequest =
          getResourceRequest(priority, ResourceRequest.ANY);
      long missedOpportunities = getSchedulingOpportunities(priority);
      long requiredContainers = offSwitchRequest.getNumContainers();

      float localityWaitFactor =
          getLocalityWaitFactor(priority, scheduler.getNumClusterNodes());

      return ((requiredContainers * localityWaitFactor) < missedOpportunities);
    }

    ResourceRequest rackLocalRequest =
        getResourceRequest(priority, node.getRackName());
    if (rackLocalRequest == null || rackLocalRequest.getNumContainers() <= 0) {
      return false;
    }

    if (type == NodeType.RACK_LOCAL) {
      long missedOpportunities = getSchedulingOpportunities(priority);
      return getActualNodeLocalityDelay() < missedOpportunities;
    }

    if (type == NodeType.NODE_LOCAL) {
      ResourceRequest nodeLocalRequest =
          getResourceRequest(priority, node.getNodeName());
      if (nodeLocalRequest != null) {
        return nodeLocalRequest.getNumContainers() > 0;
      }
    }

    return false;
  }

  boolean
      shouldAllocOrReserveNewContainer(Priority priority, Resource required) {
    int requiredContainers = getTotalRequiredResources(priority);
    int reservedContainers = getNumReservedContainers(priority);
    int starvation = 0;
    if (reservedContainers > 0) {
      float nodeFactor =
          Resources.ratio(
              rc, required, getCSLeafQueue().getMaximumAllocation()
              );

      starvation =
          (int)((getReReservations(priority) / (float)reservedContainers) *
                (1.0f - (Math.min(nodeFactor, getCSLeafQueue().getMinimumAllocationFactor())))
               );

      if (LOG.isDebugEnabled()) {
        LOG.debug("needsContainers:" +
            " app.#re-reserve=" + getReReservations(priority) +
            " reserved=" + reservedContainers +
            " nodeFactor=" + nodeFactor +
            " minAllocFactor=" + getCSLeafQueue().getMinimumAllocationFactor() +
            " starvation=" + starvation);
      }
    }
    return (((starvation + requiredContainers) - reservedContainers) > 0);
  }

  private CSAssignment assignNodeLocalContainers(Resource clusterResource,
      ResourceRequest nodeLocalResourceRequest, FiCaSchedulerNode node,
      Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResoureLimits) {
    if (canAssign(priority, node, NodeType.NODE_LOCAL,
        reservedContainer)) {
      return assignContainer(clusterResource, node, priority,
          nodeLocalResourceRequest, NodeType.NODE_LOCAL, reservedContainer,
          allocatedContainer, schedulingMode, currentResoureLimits);
    }

    return new CSAssignment(Resources.none(), NodeType.NODE_LOCAL);
  }

  private CSAssignment assignRackLocalContainers(Resource clusterResource,
      ResourceRequest rackLocalResourceRequest, FiCaSchedulerNode node,
      Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResoureLimits) {
    if (canAssign(priority, node, NodeType.RACK_LOCAL,
        reservedContainer)) {
      return assignContainer(clusterResource, node, priority,
          rackLocalResourceRequest, NodeType.RACK_LOCAL, reservedContainer,
          allocatedContainer, schedulingMode, currentResoureLimits);
    }

    return new CSAssignment(Resources.none(), NodeType.RACK_LOCAL);
  }

  private CSAssignment assignOffSwitchContainers(Resource clusterResource,
      ResourceRequest offSwitchResourceRequest, FiCaSchedulerNode node,
      Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResoureLimits) {
    if (canAssign(priority, node, NodeType.OFF_SWITCH,
        reservedContainer)) {
      return assignContainer(clusterResource, node, priority,
          offSwitchResourceRequest, NodeType.OFF_SWITCH, reservedContainer,
          allocatedContainer, schedulingMode, currentResoureLimits);
    }

    return new CSAssignment(Resources.none(), NodeType.OFF_SWITCH);
  }

  private CSAssignment assignContainersOnNode(Resource clusterResource,
      FiCaSchedulerNode node, Priority priority,
      RMContainer reservedContainer, SchedulingMode schedulingMode,
      ResourceLimits currentResoureLimits) {

    CSAssignment assigned;

    NodeType requestType = null;
    MutableObject allocatedContainer = new MutableObject();
    ResourceRequest nodeLocalResourceRequest =
        getResourceRequest(priority, node.getNodeName());
    if (nodeLocalResourceRequest != null) {
      requestType = NodeType.NODE_LOCAL;
      assigned =
          assignNodeLocalContainers(clusterResource, nodeLocalResourceRequest,
            node, priority, reservedContainer,
            allocatedContainer, schedulingMode, currentResoureLimits);
      if (Resources.greaterThan(rc, clusterResource,
        assigned.getResource(), Resources.none())) {

        if (allocatedContainer.getValue() != null) {
          incNumAllocatedContainers(NodeType.NODE_LOCAL,
            requestType);
        }
        assigned.setType(NodeType.NODE_LOCAL);
        return assigned;
      }
    }

    ResourceRequest rackLocalResourceRequest =
        getResourceRequest(priority, node.getRackName());
    if (rackLocalResourceRequest != null) {
      if (!rackLocalResourceRequest.getRelaxLocality()) {
        return SKIP_ASSIGNMENT;
      }

      if (requestType != NodeType.NODE_LOCAL) {
        requestType = NodeType.RACK_LOCAL;
      }

      assigned =
          assignRackLocalContainers(clusterResource, rackLocalResourceRequest,
            node, priority, reservedContainer,
            allocatedContainer, schedulingMode, currentResoureLimits);
      if (Resources.greaterThan(rc, clusterResource,
        assigned.getResource(), Resources.none())) {

        if (allocatedContainer.getValue() != null) {
          incNumAllocatedContainers(NodeType.RACK_LOCAL,
            requestType);
        }
        assigned.setType(NodeType.RACK_LOCAL);
        return assigned;
      }
    }

    ResourceRequest offSwitchResourceRequest =
        getResourceRequest(priority, ResourceRequest.ANY);
    if (offSwitchResourceRequest != null) {
      if (!offSwitchResourceRequest.getRelaxLocality()) {
        return SKIP_ASSIGNMENT;
      }
      if (requestType != NodeType.NODE_LOCAL
          && requestType != NodeType.RACK_LOCAL) {
        requestType = NodeType.OFF_SWITCH;
      }

      assigned =
          assignOffSwitchContainers(clusterResource, offSwitchResourceRequest,
            node, priority, reservedContainer,
            allocatedContainer, schedulingMode, currentResoureLimits);

      if (allocatedContainer.getValue() != null) {
        incNumAllocatedContainers(NodeType.OFF_SWITCH, requestType);
      }
      assigned.setType(NodeType.OFF_SWITCH);
      return assigned;
    }

    return SKIP_ASSIGNMENT;
  }

  public void reserve(Priority priority,
      FiCaSchedulerNode node, RMContainer rmContainer, Container container) {
    if (rmContainer == null) {
      queue.getMetrics().reserveResource(
          getUser(), container.getResource());
    }

    rmContainer = super.reserve(node, priority, rmContainer, container);

    node.reserveResource(this, priority, rmContainer);
  }

  private Container getContainer(RMContainer rmContainer,
      FiCaSchedulerNode node, Resource capability, Priority priority) {
    return (rmContainer != null) ? rmContainer.getContainer()
        : createContainer(node, capability, priority);
  }

  Container createContainer(FiCaSchedulerNode node, Resource capability,
      Priority priority) {

    NodeId nodeId = node.getRMNode().getNodeID();
    ContainerId containerId =
        BuilderUtils.newContainerId(getApplicationAttemptId(),
            getNewContainerId());

    return BuilderUtils.newContainer(containerId, nodeId, node.getRMNode()
        .getHttpAddress(), capability, priority, null);
  }

  @VisibleForTesting
  public RMContainer findNodeToUnreserve(Resource clusterResource,
      FiCaSchedulerNode node, Priority priority,
      Resource minimumUnreservedResource) {
    NodeId idToUnreserve =
        getNodeIdToUnreserve(priority, minimumUnreservedResource,
            rc, clusterResource);
    if (idToUnreserve == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("checked to see if could unreserve for app but nothing "
            + "reserved that matches for this app");
      }
      return null;
    }
    FiCaSchedulerNode nodeToUnreserve =
        ((CapacityScheduler) scheduler).getNode(idToUnreserve);
    if (nodeToUnreserve == null) {
      LOG.error("node to unreserve doesn't exist, nodeid: " + idToUnreserve);
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("unreserving for app: " + getApplicationId()
        + " on nodeId: " + idToUnreserve
        + " in order to replace reserved application and place it on node: "
        + node.getNodeID() + " needing: " + minimumUnreservedResource);
    }

    Resources.addTo(getHeadroom(), nodeToUnreserve
        .getReservedContainer().getReservedResource());

    return nodeToUnreserve.getReservedContainer();
  }

  private LeafQueue getCSLeafQueue() {
    return (LeafQueue)queue;
  }

  private CSAssignment assignContainer(Resource clusterResource, FiCaSchedulerNode node,
      Priority priority,
      ResourceRequest request, NodeType type, RMContainer rmContainer,
      MutableObject createdContainer, SchedulingMode schedulingMode,
      ResourceLimits currentResoureLimits) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("assignContainers: node=" + node.getNodeName()
        + " application=" + getApplicationId()
        + " priority=" + priority.getPriority()
        + " request=" + request + " type=" + type);
    }

    if (!SchedulerUtils.checkResourceRequestMatchingNodePartition(request,
        node.getPartition(), schedulingMode)) {
      if (rmContainer != null) {
        unreserve(priority, node, rmContainer);
      }
      return new CSAssignment(Resources.none(), type);
    }

    Resource capability = request.getCapability();
    Resource available = node.getAvailableResource();
    Resource totalResource = node.getTotalResource();

    if (!Resources.lessThanOrEqual(rc, clusterResource,
        capability, totalResource)) {
      LOG.warn("Node : " + node.getNodeID()
          + " does not have sufficient resource for request : " + request
          + " node total capability : " + node.getTotalResource());
      return new CSAssignment(Resources.none(), type);
    }

    assert Resources.greaterThan(
        rc, clusterResource, available, Resources.none());

    Container container =
        getContainer(rmContainer, node, capability, priority);

    if (container == null) {
      LOG.warn("Couldn't get container for allocation!");
      return new CSAssignment(Resources.none(), type);
    }

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
        getCSLeafQueue().getReservationContinueLooking();

    if (availableContainers > 0) {

      if (rmContainer != null) {
        unreserve(priority, node, rmContainer);
      } else if (reservationsContinueLooking && node.getLabels().isEmpty()) {
        if (!shouldAllocOrReserveNewContainer || needToUnreserve) {
          if (!needToUnreserve) {
            resourceNeedToUnReserve = capability;
          }
          unreservedContainer =
              findNodeToUnreserve(clusterResource, node, priority,
                  resourceNeedToUnReserve);
          if (null == unreservedContainer) {
            return new CSAssignment(Resources.none(), type);
          }
        }
      }

      RMContainer allocatedContainer =
          allocate(type, node, priority, request, container);

      if (allocatedContainer == null) {
        CSAssignment csAssignment =  new CSAssignment(Resources.none(), type);
        csAssignment.setApplication(this);
        csAssignment.setExcessReservation(unreservedContainer);
        return csAssignment;
      }

      node.allocateContainer(allocatedContainer);

      getCSLeafQueue().getOrderingPolicy().containerAllocated(this,
          allocatedContainer);

      LOG.info("assignedContainer" +
          " application attempt=" + getApplicationAttemptId() +
          " container=" + container +
          " queue=" + this +
          " clusterResource=" + clusterResource);
      createdContainer.setValue(allocatedContainer);
      CSAssignment assignment = new CSAssignment(container.getResource(), type);
      assignment.getAssignmentInformation().addAllocationDetails(
        container.getId(), getCSLeafQueue().getQueuePath());
      assignment.getAssignmentInformation().incrAllocations();
      assignment.setApplication(this);
      Resources.addTo(assignment.getAssignmentInformation().getAllocated(),
        container.getResource());

      assignment.setExcessReservation(unreservedContainer);
      return assignment;
    } else {
      if (shouldAllocOrReserveNewContainer || rmContainer != null) {

        if (reservationsContinueLooking && rmContainer == null) {
          if (needToUnreserve) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("we needed to unreserve to be able to allocate");
            }
            return new CSAssignment(Resources.none(), type);
          }
        }

        reserve(priority, node, rmContainer, container);

        LOG.info("Reserved container " +
            " application=" + getApplicationId() +
            " resource=" + request.getCapability() +
            " queue=" + this.toString() +
            " cluster=" + clusterResource);
        CSAssignment assignment =
            new CSAssignment(request.getCapability(), type);
        assignment.getAssignmentInformation().addReservationDetails(
          container.getId(), getCSLeafQueue().getQueuePath());
        assignment.getAssignmentInformation().incrReservations();
        Resources.addTo(assignment.getAssignmentInformation().getReserved(),
          request.getCapability());
        return assignment;
      }
      return new CSAssignment(Resources.none(), type);
    }
  }

  private boolean checkHeadroom(Resource clusterResource,
      ResourceLimits currentResourceLimits, Resource required, FiCaSchedulerNode node) {
    Resource resourceCouldBeUnReserved = getCurrentReservation();
    if (!getCSLeafQueue().getReservationContinueLooking() || !node.getPartition().equals(RMNodeLabelsManager.NO_LABEL)) {
      resourceCouldBeUnReserved = Resources.none();
    }
    return Resources
        .greaterThanOrEqual(rc, clusterResource, Resources.add(
            currentResourceLimits.getHeadroom(), resourceCouldBeUnReserved),
            required);
  }

  public CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits currentResourceLimits,
      SchedulingMode schedulingMode) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("pre-assignContainers for application "
          + getApplicationId());
      showRequests();
    }

    if (!hasPendingResourceRequest(rc,
        node.getPartition(), clusterResource, schedulingMode)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip app_attempt=" + getApplicationAttemptId()
            + ", because it doesn't need more resource, schedulingMode="
            + schedulingMode.name() + " node-label=" + node.getPartition());
      }
      return SKIP_ASSIGNMENT;
    }

    synchronized (this) {
      if (SchedulerAppUtils.isBlacklisted(this, node, LOG)) {
        return SKIP_ASSIGNMENT;
      }

      for (Priority priority : getPriorities()) {
        ResourceRequest anyRequest =
            getResourceRequest(priority, ResourceRequest.ANY);
        if (null == anyRequest) {
          continue;
        }

        Resource required = anyRequest.getCapability();

        if (getTotalRequiredResources(priority) <= 0) {
          continue;
        }

        if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {

          RMAppAttempt rmAppAttempt =
              rmContext.getRMApps()
                  .get(getApplicationId()).getCurrentAppAttempt();
          if (rmAppAttempt.getSubmissionContext().getUnmanagedAM() == false
              && null == rmAppAttempt.getMasterContainer()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Skip allocating AM container to app_attempt="
                  + getApplicationAttemptId()
                  + ", don't allow to allocate AM container in non-exclusive mode");
            }
            break;
          }
        }

        if (!SchedulerUtils.checkResourceRequestMatchingNodePartition(
            anyRequest, node.getPartition(), schedulingMode)) {
          continue;
        }

        if (!getCSLeafQueue().getReservationContinueLooking()) {
          if (!shouldAllocOrReserveNewContainer(priority, required)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("doesn't need containers based on reservation algo!");
            }
            continue;
          }
        }

        if (!checkHeadroom(clusterResource, currentResourceLimits, required,
            node)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("cannot allocate required resource=" + required
                + " because of headroom");
          }
          return NULL_ASSIGNMENT;
        }

        addSchedulingOpportunity(priority);

        int missedNonPartitionedRequestSchedulingOpportunity = 0;
        if (anyRequest.getNodeLabelExpression().equals(
            RMNodeLabelsManager.NO_LABEL)) {
          missedNonPartitionedRequestSchedulingOpportunity =
              addMissedNonPartitionedRequestSchedulingOpportunity(priority);
        }

        if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
          if (missedNonPartitionedRequestSchedulingOpportunity < rmContext
              .getScheduler().getNumClusterNodes()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Skip app_attempt="
                  + getApplicationAttemptId() + " priority="
                  + priority
                  + " because missed-non-partitioned-resource-request"
                  + " opportunity under requred:" + " Now="
                  + missedNonPartitionedRequestSchedulingOpportunity
                  + " required="
                  + rmContext.getScheduler().getNumClusterNodes());
            }

            return SKIP_ASSIGNMENT;
          }
        }

        CSAssignment assignment =
            assignContainersOnNode(clusterResource, node,
                priority, null, schedulingMode, currentResourceLimits);

        if (assignment.getSkipped()) {
          subtractSchedulingOpportunity(priority);
          continue;
        }

        Resource assigned = assignment.getResource();
        if (Resources.greaterThan(rc, clusterResource,
            assigned, Resources.none())) {
          if (assignment.getType() != NodeType.OFF_SWITCH) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Resetting scheduling opportunities");
            }
            resetSchedulingOpportunities(priority);
          }
          resetMissedNonPartitionedRequestSchedulingOpportunity(priority);

          return assignment;
        } else {
          return SKIP_ASSIGNMENT;
        }
      }
    }

    return SKIP_ASSIGNMENT;
  }


  public synchronized CSAssignment assignReservedContainer(
      FiCaSchedulerNode node, RMContainer rmContainer,
      Resource clusterResource, SchedulingMode schedulingMode) {
    Priority priority = rmContainer.getReservedPriority();
    if (getTotalRequiredResources(priority) == 0) {
      return new CSAssignment(this, rmContainer);
    }

    CSAssignment tmp =
        assignContainersOnNode(clusterResource, node, priority,
          rmContainer, schedulingMode, new ResourceLimits(Resources.none()));

    CSAssignment ret = new CSAssignment(Resources.none(), NodeType.NODE_LOCAL);
    if (tmp.getAssignmentInformation().getNumAllocations() > 0) {
      ret.setFulfilledReservation(true);
    }
    return ret;
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestApplicationLimits.java

    LeafQueue queue = TestLeafQueue.stubLeafQueue((LeafQueue)queues.get(A));
    queue.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));
    
    String host_0 = "host_0";
    String rack_0 = "rack_0";
    queue.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY); // Schedule to compute
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());
    
    final ApplicationAttemptId appAttemptId_1_0 = 
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY); // Schedule to compute
    expectedHeadroom = Resources.createResource(10*16*GB / 2, 1); // changes
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());

    clusterResource = Resources.createResource(90*16*GB);
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY); // Schedule to compute
    expectedHeadroom = Resources.createResource(9*16*GB / 2, 1); // changes
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());
  }
  


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestCapacityScheduler.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestContainerAllocation.java

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtilTestHelper;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMSecretManagerService;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


public class TestContainerAllocation {
    SecurityUtilTestHelper.setTokenServiceUseIp(false);
    MockRM.launchAndRegisterAM(app1, rm1, nm1);
  }
  
  @Test(timeout = 60000)
  public void testExcessReservationWillBeUnreserved() throws Exception {
    MockRM rm1 = new MockRM();

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 8 * GB);

    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    
    RMApp app2 = rm1.submitApp(1 * GB, "app", "user", null, "default");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);
  
    am1.allocate("*", 4 * GB, 1, new ArrayList<ContainerId>());
    am2.allocate("*", 4 * GB, 1, new ArrayList<ContainerId>());
    
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    
    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 =
        cs.getApplicationAttempt(am2.getApplicationAttemptId());

    Assert.assertEquals(2, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());
    Assert.assertTrue(schedulerApp2.getReservedContainers().size() > 0);
    
    Assert.assertEquals(2 * GB, cs.getNode(nm1.getNodeId())
        .getAvailableResource().getMemory());
    Assert.assertNotNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    Assert.assertEquals(10 * GB, cs.getRootQueue().getQueueResourceUsage()
        .getUsed().getMemory());
    
    am2.allocate("*", 4 * GB, 0, new ArrayList<ContainerId>());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    
    Assert.assertTrue(schedulerApp2.getReservedContainers().size() == 0);
    Assert.assertEquals(2 * GB, cs.getNode(nm1.getNodeId())
        .getAvailableResource().getMemory());
    Assert.assertNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    Assert.assertEquals(6 * GB, cs.getRootQueue().getQueueResourceUsage()
        .getUsed().getMemory());

    rm1.close();
  }
  
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestLeafQueue.java
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class TestLeafQueue {  
  private final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);

    cs.setRMContext(spyRMContext);
    cs.init(csConf);
    cs.start();

    when(spyRMContext.getScheduler()).thenReturn(cs);
    when(cs.getNumClusterNodes()).thenReturn(3);
  }
  
  private static final String A = "a";
  }

  static LeafQueue stubLeafQueue(LeafQueue queue) {
    
    CSQueue parent = queue.getParent();
    doNothing().when(parent).completedContainer(
        any(Resource.class), any(FiCaSchedulerApp.class), any(FiCaSchedulerNode.class), 
    qb.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    qb.computeUserLimitAndSetHeadroom(app_0, clusterResource,
        "", SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);

    qb.submitApplicationAttempt(app_2, user_1);
    qb.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    qb.computeUserLimitAndSetHeadroom(app_0, clusterResource,
        "", SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);

    assertEquals(8*GB, qb.getUsedResources().getMemory());
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    qb.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    qb.computeUserLimitAndSetHeadroom(app_3, clusterResource,
        "", SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(4*GB, qb.getUsedResources().getMemory());
                      u0Priority, recordFactory)));
    qb.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    qb.computeUserLimitAndSetHeadroom(app_4, clusterResource,
        "", SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    qb.computeUserLimitAndSetHeadroom(app_3, clusterResource,
        "", SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    
    
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_0.getHeadroom().getMemory());
    assertEquals(0*GB, app_1.getHeadroom().getMemory()); 
    assertEquals(4*GB, a.getMetrics().getAllocatedMB());
  }

  @Test
  public void testReservationExchange() throws Exception {

    String host_1 = "127.0.0.2";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0, 4*GB);
    
    when(csContext.getNode(node_0.getNodeID())).thenReturn(node_0);
    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);
    
    final int numNodes = 3;
    Resource clusterResource = 
        Resources.createResource(numNodes * (4*GB), numNodes * 16);
        RMContainerEventType.KILL, null, true);
    CSAssignment assignment = a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(4*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentReservation().getMemory());
    assertEquals(0*GB, node_0.getUsedResource().getMemory());
  }
  
  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestReservations.java
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;

public class TestReservations {

    cs.setRMContext(spyRMContext);
    cs.init(csConf);
    cs.start();

    when(cs.getNumClusterNodes()).thenReturn(3);
  }

  private static final String A = "a";
  }

  static LeafQueue stubLeafQueue(LeafQueue queue) {
    return queue;
  }

    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);
    when(csContext.getNode(node_2.getNodeID())).thenReturn(node_2);

    cs.getAllNodes().put(node_0.getNodeID(), node_0);
    cs.getAllNodes().put(node_1.getNodeID(), node_1);
    cs.getAllNodes().put(node_2.getNodeID(), node_2);

    final int numNodes = 3;
    Resource clusterResource = Resources.createResource(numNodes * (8 * GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);

    cs.getAllNodes().put(node_0.getNodeID(), node_0);
    cs.getAllNodes().put(node_1.getNodeID(), node_1);

    when(csContext.getNode(node_0.getNodeID())).thenReturn(node_0);
    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);

    assertEquals(2, app_0.getTotalRequiredResources(priorityReduce));

    CSAssignment csAssignment = a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(13 * GB, a.getUsedResources().getMemory());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemory());
        node_1.getNodeID(), "user", rmContext);

    RMContainer toUnreserveContainer =
        app_0.findNodeToUnreserve(csContext.getClusterResource(), node_1,
            priorityMap, capability);
    assertTrue(toUnreserveContainer == null);

    app_0.reserve(node_1, priorityMap, rmContainer, container);
    node_1.reserveResource(app_0, priorityMap, rmContainer);
    toUnreserveContainer =
        app_0.findNodeToUnreserve(csContext.getClusterResource(), node_1,
            priorityMap, capability);
    assertTrue(toUnreserveContainer == null);
  }

  @Test
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());

    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());

    ResourceLimits limits =
        new ResourceLimits(Resources.createResource(13 * GB));
    boolean res =
        a.canAssignToThisQueue(Resources.createResource(13 * GB),
            RMNodeLabelsManager.NO_LABEL, limits,
            Resources.createResource(3 * GB),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertTrue(res);
    assertEquals(0, limits.getHeadroom().getMemory());

    refreshQueuesTurnOffReservationsContLook(a, csConf);

    limits =
        new ResourceLimits(Resources.createResource(13 * GB));
    res =
        a.canAssignToThisQueue(Resources.createResource(13 * GB),
            RMNodeLabelsManager.NO_LABEL, limits,
            Resources.createResource(3 * GB),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertFalse(res);
  }

  public void refreshQueuesTurnOffReservationsContLook(LeafQueue a,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestUtils.java
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMActiveServiceContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
    
    rmContext.setNodeLabelManager(nlm);
    rmContext.setSystemMetricsPublisher(mock(SystemMetricsPublisher.class));

    ResourceScheduler mockScheduler = mock(ResourceScheduler.class);
    when(mockScheduler.getResourceCalculator()).thenReturn(
        new DefaultResourceCalculator());
    rmContext.setScheduler(mockScheduler);

    return rmContext;
  }
  
  }
  
  public static ApplicationId getMockApplicationId(int appId) {
    return ApplicationId.newInstance(0L, appId);
  }
  
  public static ApplicationAttemptId 
  getMockApplicationAttemptId(int appId, int attemptId) {
    ApplicationId applicationId = BuilderUtils.newApplicationId(0l, appId);
    return ApplicationAttemptId.newInstance(applicationId, attemptId);
  }
  
  public static FiCaSchedulerNode getMockNode(
      String host, String rack, int port, int capability) {
    NodeId nodeId = NodeId.newInstance(host, port);
    RMNode rmNode = mock(RMNode.class);
    when(rmNode.getNodeID()).thenReturn(nodeId);
    when(rmNode.getTotalCapability()).thenReturn(
    
    FiCaSchedulerNode node = spy(new FiCaSchedulerNode(rmNode, false));
    LOG.info("node = " + host + " avail=" + node.getAvailableResource());
    
    when(node.getNodeID()).thenReturn(nodeId);
    return node;
  }


