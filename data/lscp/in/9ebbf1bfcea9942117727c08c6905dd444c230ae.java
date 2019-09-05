hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/utils/BuilderUtils.java
    request.setResourceName(r.getResourceName());
    request.setCapability(r.getCapability());
    request.setNumContainers(r.getNumContainers());
    request.setNodeLabelExpression(r.getNodeLabelExpression());
    return request;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/attempt/RMAppAttemptImpl.java
  private ConcurrentMap<NodeId, List<ContainerStatus>>
      finishedContainersSentToAM =
      new ConcurrentHashMap<NodeId, List<ContainerStatus>>();
  private volatile Container masterContainer;

  private float progress = 0;
  private String host = "N/A";

  @Override
  public Container getMasterContainer() {
    return this.masterContainer;
  }

  @InterfaceAudience.Private

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/AppSchedulingInfo.java
  boolean pending = true; // for app metrics
  
  private ResourceUsage appResourceUsage;
 
  public AppSchedulingInfo(ApplicationAttemptId appAttemptId,
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      long epoch, ResourceUsage appResourceUsage) {
    this.applicationAttemptId = appAttemptId;
    this.applicationId = appAttemptId.getApplicationId();
    this.queue = queue;
    this.user = user;
    this.activeUsersManager = activeUsersManager;
    this.containerIdCounter = new AtomicLong(epoch << EPOCH_BIT_SHIFT);
    this.appResourceUsage = appResourceUsage;
  }

  public ApplicationId getApplicationId() {
            lastRequestCapability);
        
        Resource increasedResource = Resources.multiply(request.getCapability(),
            request.getNumContainers());
        queue.incPendingResource(
            request.getNodeLabelExpression(),
            increasedResource);
        appResourceUsage.incPending(request.getNodeLabelExpression(), increasedResource);
        if (lastRequest != null) {
          Resource decreasedResource =
              Resources.multiply(lastRequestCapability, lastRequestContainers);
          queue.decPendingResource(lastRequest.getNodeLabelExpression(),
              decreasedResource);
          appResourceUsage.decPending(lastRequest.getNodeLabelExpression(),
              decreasedResource);
        }
      }
    }
      checkForDeactivation();
    }
    
    appResourceUsage.decPending(offSwitchRequest.getNodeLabelExpression(),
        offSwitchRequest.getCapability());
    queue.decPendingResource(offSwitchRequest.getNodeLabelExpression(),
        offSwitchRequest.getCapability());
  }
  }
  
  public ResourceRequest cloneResourceRequest(ResourceRequest request) {
    ResourceRequest newRequest =
        ResourceRequest.newInstance(request.getPriority(),
            request.getResourceName(), request.getCapability(), 1,
            request.getRelaxLocality(), request.getNodeLabelExpression());
    return newRequest;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/ResourceUsage.java

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.util.resource.Resources;

  }

  private Resource _get(String label, ResourceType type) {
    if (label == null) {
      label = RMNodeLabelsManager.NO_LABEL;
    }
    
    try {
      readLock.lock();
      UsageByLabel usage = usages.get(label);
  }

  private UsageByLabel getAndAddIfMissing(String label) {
    if (label == null) {
      label = RMNodeLabelsManager.NO_LABEL;
    }
    if (!usages.containsKey(label)) {
      UsageByLabel u = new UsageByLabel(label);
      usages.put(label, u);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/SchedulerApplicationAttempt.java
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerReservedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.base.Preconditions;
  private Set<ContainerId> pendingRelease = null;

  Multiset<Priority> schedulingOpportunities = HashMultiset.create();
  
  Multiset<Priority> missedNonPartitionedRequestSchedulingOpportunity =
      HashMultiset.create();
  
  protected Map<Priority, Long> lastScheduledContainer =
      new HashMap<Priority, Long>();
    this.rmContext = rmContext;
    this.appSchedulingInfo = 
        new AppSchedulingInfo(applicationAttemptId, user, queue,  
            activeUsersManager, rmContext.getEpoch(), attemptResourceUsage);
    this.queue = queue;
    this.pendingRelease = new HashSet<ContainerId>();
    this.attemptId = applicationAttemptId;
    return this.appSchedulingInfo.isBlacklisted(resourceName);
  }

  public synchronized int addMissedNonPartitionedRequestSchedulingOpportunity(
      Priority priority) {
    missedNonPartitionedRequestSchedulingOpportunity.add(priority);
    return missedNonPartitionedRequestSchedulingOpportunity.count(priority);
  }

  public synchronized void
      resetMissedNonPartitionedRequestSchedulingOpportunity(Priority priority) {
    missedNonPartitionedRequestSchedulingOpportunity.setCount(priority, 0);
  }

  
  public synchronized void addSchedulingOpportunity(Priority priority) {
    schedulingOpportunities.setCount(priority,
        schedulingOpportunities.count(priority) + 1);
  public Set<String> getBlacklistedNodes() {
    return this.appSchedulingInfo.getBlackListCopy();
  }
  
  @Private
  public boolean hasPendingResourceRequest(ResourceCalculator rc,
      String nodePartition, Resource cluster,
      SchedulingMode schedulingMode) {
    return SchedulerUtils.hasPendingResourceRequest(rc,
        this.attemptResourceUsage, nodePartition, cluster,
        schedulingMode);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/SchedulerUtils.java
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

    if (labelExp == null && queueInfo != null
        && ResourceRequest.ANY.equals(resReq.getResourceName())) {
      labelExp = queueInfo.getDefaultNodeLabelExpression();
    }
    
    resReq
        .setNodeLabelExpression(labelExp == null ? RMNodeLabelsManager.NO_LABEL
            : labelExp);
    
    if (!ResourceRequest.ANY.equals(resReq.getResourceName())
        && labelExp != null && !labelExp.trim().isEmpty()) {
    }
  }
  
  public static void checkIfLabelInClusterNodeLabels(RMNodeLabelsManager mgr,
      Set<String> labels) throws IOException {
    if (mgr == null) {
    }
  }

  public static boolean checkQueueLabelExpression(Set<String> queueLabels,
      String labelExpression) {
    if (queueLabels != null && queueLabels.contains(RMNodeLabelsManager.ANY)) {
    }
    return null;
  }
  
  public static boolean checkResourceRequestMatchingNodePartition(
      ResourceRequest offswitchResourceRequest, String nodePartition,
      SchedulingMode schedulingMode) {
    String nodePartitionToLookAt = null;
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY) {
      nodePartitionToLookAt = nodePartition;
    } else {
      nodePartitionToLookAt = RMNodeLabelsManager.NO_LABEL;
    }
    
    String askedNodePartition = offswitchResourceRequest.getNodeLabelExpression();
    if (null == askedNodePartition) {
      askedNodePartition = RMNodeLabelsManager.NO_LABEL;
    }
    return askedNodePartition.equals(nodePartitionToLookAt);
  }
  
  private static boolean hasPendingResourceRequest(ResourceCalculator rc,
      ResourceUsage usage, String partitionToLookAt, Resource cluster) {
    if (Resources.greaterThan(rc, cluster,
        usage.getPending(partitionToLookAt), Resources.none())) {
      return true;
    }
    return false;
  }

  @Private
  public static boolean hasPendingResourceRequest(ResourceCalculator rc,
      ResourceUsage usage, String nodePartition, Resource cluster,
      SchedulingMode schedulingMode) {
    String partitionToLookAt = nodePartition;
    if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      partitionToLookAt = RMNodeLabelsManager.NO_LABEL;
    }
    return hasPendingResourceRequest(rc, usage, partitionToLookAt, cluster);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/AbstractCSQueue.java

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.security.PrivilegedEntity.EntityType;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
public abstract class AbstractCSQueue implements CSQueue {
  private static final Log LOG = LogFactory.getLog(AbstractCSQueue.class);
  
  static final CSAssignment NULL_ASSIGNMENT =
      new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);
  
  static final CSAssignment SKIP_ASSIGNMENT = new CSAssignment(true);
  
  CSQueue parent;
  final String queueName;
  volatile int numContainers;
  }
  
  synchronized void allocateResource(Resource clusterResource, 
      Resource resource, String nodePartition) {
    queueUsage.incUsed(nodePartition, resource);

    ++numContainers;
    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
  }
  
  protected synchronized void releaseResource(Resource clusterResource,
      Resource resource, String nodePartition) {
    queueUsage.decUsed(nodePartition, resource);

    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
        clusterResource, minimumAllocation);
                                        parentQ.getPreemptionDisabled());
  }
  
  private Resource getCurrentLimitResource(String nodePartition,
      Resource clusterResource, ResourceLimits currentResourceLimits,
      SchedulingMode schedulingMode) {
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY) {
      Resource queueMaxResource =
          Resources.multiplyAndNormalizeDown(resourceCalculator,
              labelManager.getResourceByLabel(nodePartition, clusterResource),
              queueCapacities.getAbsoluteMaximumCapacity(nodePartition), minimumAllocation);
      if (nodePartition.equals(RMNodeLabelsManager.NO_LABEL)) {
        return Resources.min(resourceCalculator, clusterResource,
            queueMaxResource, currentResourceLimits.getLimit());
      }
      return queueMaxResource;  
    } else if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      return labelManager.getResourceByLabel(nodePartition, clusterResource);
    }
    
    return Resources.none();
  }
  
  synchronized boolean canAssignToThisQueue(Resource clusterResource,
      String nodePartition, ResourceLimits currentResourceLimits,
      Resource nowRequired, Resource resourceCouldBeUnreserved,
      SchedulingMode schedulingMode) {
    Resource newTotalResource =
        Resources.add(queueUsage.getUsed(nodePartition), nowRequired);

    Resource currentLimitResource =
        getCurrentLimitResource(nodePartition, clusterResource,
            currentResourceLimits, schedulingMode);

    if (this.reservationsContinueLooking
        && nodePartition.equals(RMNodeLabelsManager.NO_LABEL)
        && Resources.greaterThan(resourceCalculator, clusterResource,
            resourceCouldBeUnreserved, Resources.none())) {
      }
    }

    if (Resources.greaterThan(resourceCalculator, clusterResource,
        newTotalResource, currentLimitResource)) {
      return false;

    if (LOG.isDebugEnabled()) {
      LOG.debug(getQueueName()
          + "Check assign to queue, nodePartition="
          + nodePartition
          + " usedResources: "
          + queueUsage.getUsed(nodePartition)
          + " clusterResources: "
          + clusterResource
          + " currentUsedCapacity "
          + Resources.divide(resourceCalculator, clusterResource,
              queueUsage.getUsed(nodePartition),
              labelManager.getResourceByLabel(nodePartition, clusterResource))
          + " max-capacity: "
          + queueCapacities.getAbsoluteMaximumCapacity(nodePartition) + ")");
    }
    return true;
  }
  
  @Override
  public void incPendingResource(String nodeLabel, Resource resourceToInc) {
    if (nodeLabel == null) {
      parent.decPendingResource(nodeLabel, resourceToDec);
    }
  }
  
  boolean hasPendingResourceRequest(String nodePartition, 
      Resource cluster, SchedulingMode schedulingMode) {
    return SchedulerUtils.hasPendingResourceRequest(resourceCalculator,
        queueUsage, nodePartition, cluster, schedulingMode);
  }
  
  boolean accessibleToPartition(String nodePartition) {
    if (accessibleLabels != null
        && accessibleLabels.contains(RMNodeLabelsManager.ANY)) {
      return true;
    }
    if (nodePartition == null
        || nodePartition.equals(RMNodeLabelsManager.NO_LABEL)) {
      return true;
    }
    if (accessibleLabels != null && accessibleLabels.contains(nodePartition)) {
      return true;
    }
    return false;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CSQueue.java
  public CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits resourceLimits,
      SchedulingMode schedulingMode);
  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacityScheduler.java
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
          getCurrentAttemptForContainer(reservedContainer.getContainerId());

      LOG.info("Trying to fulfill reservation for application "
          + reservedApplication.getApplicationId() + " on node: "
          + node.getNodeID());

      LeafQueue queue = ((LeafQueue) reservedApplication.getQueue());
      assignment =
          queue.assignContainers(
              clusterResource,
              node,
              new ResourceLimits(labelManager.getResourceByLabel(
                  RMNodeLabelsManager.NO_LABEL, clusterResource)),
              SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
      if (assignment.isFulfilledReservation()) {
        CSAssignment tmp =
            new CSAssignment(reservedContainer.getReservedResource(),
      RMContainer excessReservation = assignment.getExcessReservation();
      if (excessReservation != null) {
        Container container = excessReservation.getContainer();
        queue.completedContainer(clusterResource, assignment.getApplication(),
            node, excessReservation, SchedulerUtils
                .createAbnormalContainerStatus(container.getId(),
                    SchedulerUtils.UNRESERVED_CONTAINER),
            RMContainerEventType.RELEASED, null, true);
      }
            new ResourceLimits(labelManager.getResourceByLabel(
                RMNodeLabelsManager.NO_LABEL, clusterResource)),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
        if (Resources.greaterThan(calculator, clusterResource,
            assignment.getResource(), Resources.none())) {
          updateSchedulerHealth(lastNodeUpdateTime, node, assignment);
          return;
        }
        
        if (StringUtils.equals(node.getPartition(),
            RMNodeLabelsManager.NO_LABEL)) {
          return;
        }
        
        try {
          if (rmContext.getNodeLabelManager().isExclusiveNodeLabel(
              node.getPartition())) {
            return;
          }
        } catch (IOException e) {
          LOG.warn("Exception when trying to get exclusivity of node label="
              + node.getPartition(), e);
          return;
        }
        
        assignment = root.assignContainers(
            clusterResource,
            node,
            new ResourceLimits(labelManager.getResourceByLabel(
                RMNodeLabelsManager.NO_LABEL, clusterResource)),
            SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY);
        updateSchedulerHealth(lastNodeUpdateTime, node, assignment);
        if (Resources.greaterThan(calculator, clusterResource,
            assignment.getResource(), Resources.none())) {
          return;
        }
      }
    } else {
      LOG.info("Skipping scheduling since node "
          + node.getNodeID()
          + " is reserved by application "
          + node.getReservedContainer().getContainerId()
              .getApplicationAttemptId());
    }
  }

  @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacitySchedulerConfiguration.java
    		getMaximumApplicationMasterResourcePercent());
  }
  
  public void setMaximumApplicationMasterResourcePerQueuePercent(String queue,
      float percent) {
    setFloat(getQueuePrefix(queue) + MAXIMUM_AM_RESOURCE_SUFFIX, percent);
  }
  
  public float getNonLabeledQueueCapacity(String queue) {
    float capacity = queue.equals("root") ? 100.0f : getFloat(
        getQueuePrefix(queue) + CAPACITY, UNDEFINED);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/LeafQueue.java
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
    return applicationAttemptMap.get(applicationAttemptId);
  }
  
  @Override
  public synchronized CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits currentResourceLimits,
      SchedulingMode schedulingMode) {
    updateCurrentResourceLimits(currentResourceLimits, clusterResource);
    
    if(LOG.isDebugEnabled()) {
        + " #applications=" + activeApplications.size());
    }
    
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
          getApplication(reservedContainer.getApplicationAttemptId());
      synchronized (application) {
        return assignReservedContainer(application, node, reservedContainer,
            clusterResource, schedulingMode);
      }
    }
    
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY
        && !accessibleToPartition(node.getPartition())) {
      return NULL_ASSIGNMENT;
    }
    
    if (!hasPendingResourceRequest(node.getPartition(),
        clusterResource, schedulingMode)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip this queue=" + getQueuePath()
            + ", because it doesn't need more resource, schedulingMode="
            + schedulingMode.name() + " node-partition=" + node.getPartition());
      }
      return NULL_ASSIGNMENT;
    }
    
        application.showRequests();
      }
      
      if (!application.hasPendingResourceRequest(resourceCalculator,
          node.getPartition(), clusterResource, schedulingMode)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip app_attempt=" + application.getApplicationAttemptId()
              + ", because it doesn't need more resource, schedulingMode="
              + schedulingMode.name() + " node-label=" + node.getPartition());
        }
        continue;
      }

      synchronized (application) {
        if (SchedulerAppUtils.isBlacklisted(application, node, LOG)) {
            continue;
          }
          
          if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
            RMAppAttempt rmAppAttempt =
                csContext.getRMContext().getRMApps()
                    .get(application.getApplicationId()).getCurrentAppAttempt();
            if (null == rmAppAttempt.getMasterContainer()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Skip allocating AM container to app_attempt="
                    + application.getApplicationAttemptId()
                    + ", don't allow to allocate AM container in non-exclusive mode");
              }
              break;
            }
          }
          
          if (!SchedulerUtils.checkResourceRequestMatchingNodePartition(
              anyRequest, node.getPartition(), schedulingMode)) {
            continue;
          }
          
            }
          }
          
          Resource userLimit = 
              computeUserLimitAndSetHeadroom(application, clusterResource, 
                  required, node.getPartition(), schedulingMode);          
          
          if (!super.canAssignToThisQueue(clusterResource, node.getPartition(),
              this.currentResourceLimits, required,
              application.getCurrentReservation(), schedulingMode)) {
            return NULL_ASSIGNMENT;
          }

          if (!canAssignToUser(clusterResource, application.getUser(), userLimit,
              application, true, node.getPartition())) {
            break;
          }

          application.addSchedulingOpportunity(priority);
          
          int missedNonPartitionedRequestSchedulingOpportunity = 0;
          if (anyRequest.getNodeLabelExpression().equals(
              RMNodeLabelsManager.NO_LABEL)) {
            missedNonPartitionedRequestSchedulingOpportunity =
                application
                    .addMissedNonPartitionedRequestSchedulingOpportunity(priority);
          }
          
          if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
            if (missedNonPartitionedRequestSchedulingOpportunity < scheduler
                .getNumClusterNodes()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Skip app_attempt="
                    + application.getApplicationAttemptId()
                    + " priority="
                    + priority
                    + " because missed-non-partitioned-resource-request"
                    + " opportunity under requred:"
                    + " Now=" + missedNonPartitionedRequestSchedulingOpportunity
                    + " required="
                    + scheduler.getNumClusterNodes());
              }

              break;
            }
          }
          
          CSAssignment assignment =  
            assignContainersOnNode(clusterResource, node, application, priority, 
                null, schedulingMode);

          if (assignment.getSkipped()) {
            allocateResource(clusterResource, application, assigned,
                node.getPartition());
            
            if (assignment.getType() != NodeType.OFF_SWITCH) {
              }
              application.resetSchedulingOpportunities(priority);
            }
            application.resetMissedNonPartitionedRequestSchedulingOpportunity(priority);
            
            return assignment;

  private synchronized CSAssignment assignReservedContainer(
      FiCaSchedulerApp application, FiCaSchedulerNode node,
      RMContainer rmContainer, Resource clusterResource,
      SchedulingMode schedulingMode) {
    Priority priority = rmContainer.getReservedPriority();
    if (application.getTotalRequiredResources(priority) == 0) {
    CSAssignment tmp =
        assignContainersOnNode(clusterResource, node, application, priority,
          rmContainer, schedulingMode);
    
  protected Resource getHeadroom(User user, Resource queueCurrentLimit,
      Resource clusterResource, FiCaSchedulerApp application, Resource required) {
    return getHeadroom(user, queueCurrentLimit, clusterResource,
        computeUserLimit(application, clusterResource, required, user,
            RMNodeLabelsManager.NO_LABEL, SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY));
  }
  
  private Resource getHeadroom(User user, Resource currentResourceLimit,

  @Lock({LeafQueue.class, FiCaSchedulerApp.class})
  Resource computeUserLimitAndSetHeadroom(FiCaSchedulerApp application,
      Resource clusterResource, Resource required, String nodePartition,
      SchedulingMode schedulingMode) {
    String user = application.getUser();
    User queueUser = getUser(user);

    Resource userLimit =
        computeUserLimit(application, clusterResource, required,
            queueUser, nodePartition, schedulingMode);

    setQueueResourceLimitsInfo(clusterResource);
    
  @Lock(NoLock.class)
  private Resource computeUserLimit(FiCaSchedulerApp application,
      Resource clusterResource, Resource required, User user,
      String nodePartition, SchedulingMode schedulingMode) {
    Resource queueCapacity =
        Resources.multiplyAndNormalizeUp(resourceCalculator,
            labelManager.getResourceByLabel(nodePartition, clusterResource),
            queueCapacities.getAbsoluteCapacity(nodePartition),
            minimumAllocation);

    queueCapacity =

    Resource currentCapacity =
        Resources.lessThan(resourceCalculator, clusterResource,
            queueUsage.getUsed(nodePartition), queueCapacity) ? queueCapacity
            : Resources.add(queueUsage.getUsed(nodePartition), required);
    
    
    final int activeUsers = activeUsersManager.getNumActiveUsers();
    
    Resource userLimitResource = Resources.max(
        resourceCalculator, clusterResource, 
        Resources.divideAndCeil(
            resourceCalculator, currentCapacity, activeUsers),
            Resources.multiplyAndRoundDown(
                currentCapacity, userLimit), 
            100)
        );
    
    Resource maxUserLimit = Resources.none();
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY) {
      maxUserLimit =
          Resources.multiplyAndRoundDown(queueCapacity, userLimitFactor);
    } else if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      maxUserLimit =
          labelManager.getResourceByLabel(nodePartition, clusterResource);
    }
    
    userLimitResource =
        Resources.roundUp(
            resourceCalculator, 
            Resources.min(
                resourceCalculator, clusterResource,   
                  userLimitResource,
                  maxUserLimit
                ), 
            minimumAllocation);

      String userName = application.getUser();
      LOG.debug("User limit computation for " + userName + 
          " in queue " + getQueueName() +
          " userLimitPercent=" + userLimit +
          " userLimitFactor=" + userLimitFactor +
          " required: " + required + 
          " consumed: " + user.getUsed() + 
          " user-limit-resource: " + userLimitResource +
          " queueCapacity: " + queueCapacity + 
          " qconsumed: " + queueUsage.getUsed() +
          " currentCapacity: " + currentCapacity +
          " clusterCapacity: " + clusterResource
      );
    }
    user.setUserResourceLimit(userLimitResource);
    return userLimitResource;
  }
  
  @Private
  protected synchronized boolean canAssignToUser(Resource clusterResource,
      String userName, Resource limit, FiCaSchedulerApp application,
      boolean checkReservations, String nodePartition) {
    User user = getUser(userName);

    if (Resources
        .greaterThan(resourceCalculator, clusterResource,
            user.getUsed(nodePartition),
            limit)) {
      if (this.reservationsContinueLooking && checkReservations
          && nodePartition.equals(CommonNodeLabelsManager.NO_LABEL)) {
        if (Resources.lessThanOrEqual(
            resourceCalculator,
            clusterResource,
      if (LOG.isDebugEnabled()) {
        LOG.debug("User " + userName + " in queue " + getQueueName()
            + " will exceed limit - " + " consumed: "
            + user.getUsed(nodePartition) + " limit: " + limit);
      }
      return false;
    }

  private CSAssignment assignContainersOnNode(Resource clusterResource,
      FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, SchedulingMode schedulingMode) {

    CSAssignment assigned;

      assigned =
          assignNodeLocalContainers(clusterResource, nodeLocalResourceRequest, 
            node, application, priority, reservedContainer,
            allocatedContainer, schedulingMode);
      if (Resources.greaterThan(resourceCalculator, clusterResource,
        assigned.getResource(), Resources.none())) {

      assigned = 
          assignRackLocalContainers(clusterResource, rackLocalResourceRequest, 
            node, application, priority, reservedContainer,
            allocatedContainer, schedulingMode);
      if (Resources.greaterThan(resourceCalculator, clusterResource,
        assigned.getResource(), Resources.none())) {

      assigned =
          assignOffSwitchContainers(clusterResource, offSwitchResourceRequest,
            node, application, priority, reservedContainer,
            allocatedContainer, schedulingMode);

      if (allocatedContainer.getValue() != null) {

  @Private
  protected boolean checkLimitsToReserve(Resource clusterResource,
      FiCaSchedulerApp application, Resource capability, String nodePartition,
      SchedulingMode schedulingMode) {
    Resource userLimit = computeUserLimitAndSetHeadroom(application,
        clusterResource, capability, nodePartition, schedulingMode);

    if (!canAssignToThisQueue(clusterResource, RMNodeLabelsManager.NO_LABEL,
        this.currentResourceLimits, capability, Resources.none(), schedulingMode)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("was going to reserve but hit queue limit");
      }

    if (!canAssignToUser(clusterResource, application.getUser(), userLimit,
        application, false, nodePartition)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("was going to reserve but hit user limit");
      }
  private CSAssignment assignNodeLocalContainers(Resource clusterResource,
      ResourceRequest nodeLocalResourceRequest, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer,
      SchedulingMode schedulingMode) {
    if (canAssign(application, priority, node, NodeType.NODE_LOCAL, 
        reservedContainer)) {
      return assignContainer(clusterResource, node, application, priority,
          nodeLocalResourceRequest, NodeType.NODE_LOCAL, reservedContainer,
          allocatedContainer, schedulingMode);
    }

    return new CSAssignment(Resources.none(), NodeType.NODE_LOCAL);
  private CSAssignment assignRackLocalContainers(Resource clusterResource,
      ResourceRequest rackLocalResourceRequest, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer,
      SchedulingMode schedulingMode) {
    if (canAssign(application, priority, node, NodeType.RACK_LOCAL,
        reservedContainer)) {
      return assignContainer(clusterResource, node, application, priority,
          rackLocalResourceRequest, NodeType.RACK_LOCAL, reservedContainer,
          allocatedContainer, schedulingMode);
    }

    return new CSAssignment(Resources.none(), NodeType.RACK_LOCAL);
  private CSAssignment assignOffSwitchContainers(Resource clusterResource,
      ResourceRequest offSwitchResourceRequest, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer,
      SchedulingMode schedulingMode) {
    if (canAssign(application, priority, node, NodeType.OFF_SWITCH,
        reservedContainer)) {
      return assignContainer(clusterResource, node, application, priority,
          offSwitchResourceRequest, NodeType.OFF_SWITCH, reservedContainer,
          allocatedContainer, schedulingMode);
    }
    
    return new CSAssignment(Resources.none(), NodeType.OFF_SWITCH);
  }
  
  private int getActualNodeLocalityDelay() {
    return Math.min(scheduler.getNumClusterNodes(), getNodeLocalityDelay());
  }

  boolean canAssign(FiCaSchedulerApp application, Priority priority, 
      FiCaSchedulerNode node, NodeType type, RMContainer reservedContainer) {

    if (type == NodeType.RACK_LOCAL) {
      long missedOpportunities = application.getSchedulingOpportunities(priority);
      return getActualNodeLocalityDelay() < missedOpportunities;
    }

  private CSAssignment assignContainer(Resource clusterResource, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority, 
      ResourceRequest request, NodeType type, RMContainer rmContainer,
      MutableObject createdContainer, SchedulingMode schedulingMode) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("assignContainers: node=" + node.getNodeName()
        + " application=" + application.getApplicationId()
    }
    
    if (!SchedulerUtils.checkResourceRequestMatchingNodePartition(request,
        node.getPartition(), schedulingMode)) {
          if (!checkLimitsToReserve(clusterResource,
              application, capability, node.getPartition(), schedulingMode)) {
            return new CSAssignment(Resources.none(), type);
          }
        }
        if (removed) {
          releaseResource(clusterResource, application,
              container.getResource(), node.getPartition());
          LOG.info("completedContainer" +
              " container=" + container +
              " queue=" + this +

  synchronized void allocateResource(Resource clusterResource,
      SchedulerApplicationAttempt application, Resource resource,
      String nodePartition) {
    super.allocateResource(clusterResource, resource, nodePartition);
    
    String userName = application.getUser();
    User user = getUser(userName);
    user.assignContainer(resource, nodePartition);
    Resources.subtractFrom(application.getHeadroom(), resource); // headroom
  }

  synchronized void releaseResource(Resource clusterResource, 
      FiCaSchedulerApp application, Resource resource, String nodePartition) {
    super.releaseResource(clusterResource, resource, nodePartition);
    
    String userName = application.getUser();
    User user = getUser(userName);
    user.releaseContainer(resource, nodePartition);
    metrics.setAvailableResourcesToUser(userName, application.getHeadroom());
      
    LOG.info(getQueueName() + 
  
  private void updateAbsoluteCapacityResource(Resource clusterResource) {
    absoluteCapacityResource =
        Resources.multiplyAndNormalizeUp(resourceCalculator, labelManager
            .getResourceByLabel(RMNodeLabelsManager.NO_LABEL, clusterResource),
            queueCapacities.getAbsoluteCapacity(), minimumAllocation);
  }
  
    for (FiCaSchedulerApp application : activeApplications) {
      synchronized (application) {
        computeUserLimitAndSetHeadroom(application, clusterResource,
            Resources.none(), RMNodeLabelsManager.NO_LABEL,
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
      }
    }
  }
      }
    }

    public void assignContainer(Resource resource, String nodePartition) {
      userResourceUsage.incUsed(nodePartition, resource);
    }

    public void releaseContainer(Resource resource, String nodePartition) {
      userResourceUsage.decUsed(nodePartition, resource);
    }

    public Resource getUserResourceLimit() {
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      allocateResource(clusterResource, attempt, rmContainer.getContainer()
          .getResource(), node.getPartition());
    }
    getParent().recoverContainer(clusterResource, attempt, rmContainer);
  }
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      allocateResource(clusterResource, application, rmContainer.getContainer()
          .getResource(), node.getPartition());
      LOG.info("movedContainer" + " container=" + rmContainer.getContainer()
          + " resource=" + rmContainer.getContainer().getResource()
          + " queueMoveIn=" + this + " usedCapacity=" + getUsedCapacity()
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      releaseResource(clusterResource, application, rmContainer.getContainer()
          .getResource(), node.getPartition());
      LOG.info("movedContainer" + " container=" + rmContainer.getContainer()
          + " resource=" + rmContainer.getContainer().getResource()
          + " queueMoveOut=" + this + " usedCapacity=" + getUsedCapacity()

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/ParentQueue.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

  @Override
  public synchronized CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits resourceLimits,
      SchedulingMode schedulingMode) {
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY
        && !accessibleToPartition(node.getPartition())) {
      return NULL_ASSIGNMENT;
    }
    
    if (!super.hasPendingResourceRequest(node.getPartition(),
        clusterResource, schedulingMode)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip this queue=" + getQueuePath()
            + ", because it doesn't need more resource, schedulingMode="
            + schedulingMode.name() + " node-partition=" + node.getPartition());
      }
      return NULL_ASSIGNMENT;
    }
    
    CSAssignment assignment = 
        new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);
    
    while (canAssign(clusterResource, node)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to assign containers to child-queue of "
      if (!super.canAssignToThisQueue(clusterResource, node.getPartition(),
          resourceLimits, minimumAllocation, Resources.createResource(
              getMetrics().getReservedMB(), getMetrics()
                  .getReservedVirtualCores()), schedulingMode)) {
        break;
      }
      
      CSAssignment assignedToChild =
          assignContainersToChildQueues(clusterResource, node, resourceLimits,
              schedulingMode);
      assignment.setType(assignedToChild.getType());
      
              assignedToChild.getResource(), Resources.none())) {
        super.allocateResource(clusterResource, assignedToChild.getResource(),
            node.getPartition());
        
        Resources
  }
  
  private synchronized CSAssignment assignContainersToChildQueues(
      Resource cluster, FiCaSchedulerNode node, ResourceLimits limits,
      SchedulingMode schedulingMode) {
    CSAssignment assignment = 
        new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);
    
      ResourceLimits childLimits =
          getResourceLimitsOfChild(childQueue, cluster, limits);
      
      assignment = childQueue.assignContainers(cluster, node, 
          childLimits, schedulingMode);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Assigned to queue: " + childQueue.getQueuePath() +
          " stats: " + childQueue + " --> " + 
      synchronized (this) {
        super.releaseResource(clusterResource, rmContainer.getContainer()
            .getResource(), node.getPartition());

        LOG.info("completedContainer" +
            " queue=" + getQueueName() + 
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      super.allocateResource(clusterResource, rmContainer.getContainer()
          .getResource(), node.getPartition());
    }
    if (parent != null) {
      parent.recoverContainer(clusterResource, attempt, rmContainer);
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      super.allocateResource(clusterResource, rmContainer.getContainer()
          .getResource(), node.getPartition());
      LOG.info("movedContainer" + " queueMoveIn=" + getQueueName()
          + " usedCapacity=" + getUsedCapacity() + " absoluteUsedCapacity="
          + getAbsoluteUsedCapacity() + " used=" + queueUsage.getUsed() + " cluster="
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      super.releaseResource(clusterResource,
          rmContainer.getContainer().getResource(),
          node.getPartition());
      LOG.info("movedContainer" + " queueMoveOut=" + getQueueName()
          + " usedCapacity=" + getUsedCapacity() + " absoluteUsedCapacity="
          + getAbsoluteUsedCapacity() + " used=" + queueUsage.getUsed() + " cluster="

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/SchedulingMode.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/SchedulingMode.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

public enum SchedulingMode {
  RESPECT_PARTITION_EXCLUSIVITY,
  
  IGNORE_PARTITION_EXCLUSIVITY
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/Application.java
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.Task.State;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
    } else {
      request.setNumContainers(request.getNumContainers() + 1);
    }
    if (request.getNodeLabelExpression() == null) {
      request.setNodeLabelExpression(RMNodeLabelsManager.NO_LABEL);
    }
    
    ask.remove(request);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/MockAM.java
  public AllocateResponse allocate(
      String host, int memory, int numContainers,
      List<ContainerId> releases, String labelExpression) throws Exception {
    return allocate(host, memory, numContainers, 1, releases, labelExpression);
  }
  
  public AllocateResponse allocate(
      String host, int memory, int numContainers, int priority,
      List<ContainerId> releases, String labelExpression) throws Exception {
    List<ResourceRequest> reqs =
        createReq(new String[] { host }, memory, priority, numContainers,
            labelExpression);
    return allocate(reqs, releases);
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/MockRM.java
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

  
  public boolean waitForState(MockNM nm, ContainerId containerId,
      RMContainerState containerState, int timeoutMillisecs) throws Exception {
    return waitForState(Arrays.asList(nm), containerId, containerState,
        timeoutMillisecs);
  }
  
  public boolean waitForState(Collection<MockNM> nms, ContainerId containerId,
      RMContainerState containerState, int timeoutMillisecs) throws Exception {
    RMContainer container = getResourceScheduler().getRMContainer(containerId);
    int timeoutSecs = 0;
    while(container == null && timeoutSecs++ < timeoutMillisecs / 100) {
      for (MockNM nm : nms) {
        nm.nodeHeartbeat(true);
      }
      container = getResourceScheduler().getRMContainer(containerId);
      System.out.println("Waiting for container " + containerId + " to be allocated.");
      Thread.sleep(100);
        && timeoutSecs++ < timeoutMillisecs / 100) {
      System.out.println("Container : " + containerId + " State is : "
          + container.getState() + " Waiting for state : " + containerState);
      for (MockNM nm : nms) {
        nm.nodeHeartbeat(true);
      }
      Thread.sleep(100);

      if (timeoutMillisecs <= timeoutSecs * 100) {
    rm.waitForState(rmApp.getApplicationId(), RMAppState.FINISHED);
  }
  
  @SuppressWarnings("rawtypes")
  private static void waitForSchedulerAppAttemptAdded(
      ApplicationAttemptId attemptId, MockRM rm) throws InterruptedException {
    int tick = 0;
    while (null == ((AbstractYarnScheduler) rm.getResourceScheduler())
        .getApplicationAttempt(attemptId) && tick < 50) {
      Thread.sleep(100);
      if (tick % 10 == 0) {
        System.out.println("waiting for SchedulerApplicationAttempt="
            + attemptId + " added.");
      }
      tick++;
    }
  }

  public static MockAM launchAM(RMApp app, MockRM rm, MockNM nm)
      throws Exception {
    rm.waitForState(app.getApplicationId(), RMAppState.ACCEPTED);
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    waitForSchedulerAppAttemptAdded(attempt.getAppAttemptId(), rm);
    System.out.println("Launch AM " + attempt.getAppAttemptId());
    nm.nodeHeartbeat(true);
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestApplicationLimits.java

    queue.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    Resource expectedHeadroom = Resources.createResource(10*16*GB, 1);
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());


    queue.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY); // Schedule to compute
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());
    assertEquals(expectedHeadroom, app_0_1.getHeadroom());// no change
    
    
    queue.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY); // Schedule to compute
    expectedHeadroom = Resources.createResource(10*16*GB / 2, 1); // changes
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());
    assertEquals(expectedHeadroom, app_0_1.getHeadroom());
    clusterResource = Resources.createResource(90*16*GB);
    queue.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY); // Schedule to compute
    expectedHeadroom = Resources.createResource(9*16*GB / 2, 1); // changes
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());
    assertEquals(expectedHeadroom, app_0_1.getHeadroom());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestChildQueueOrder.java
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
        final Resource allocatedResource = Resources.createResource(allocation);
        if (queue instanceof ParentQueue) {
          ((ParentQueue)queue).allocateResource(clusterResource, 
              allocatedResource, RMNodeLabelsManager.NO_LABEL);
        } else {
          FiCaSchedulerApp app1 = getMockApplication(0, "");
          ((LeafQueue)queue).allocateResource(clusterResource, app1, 
          doReturn(new CSAssignment(Resources.none(), type)).
          when(queue)
              .assignContainers(eq(clusterResource), eq(node),
                  any(ResourceLimits.class), any(SchedulingMode.class));

          Resource available = node.getAvailableResource();
      }
    }).
    when(queue).assignContainers(eq(clusterResource), eq(node), 
        any(ResourceLimits.class), any(SchedulingMode.class));
    doNothing().when(node).releaseContainer(any(Container.class));
  }

    CSQueue c = queues.get(C);
    CSQueue d = queues.get(D);
    
    queues.get(CapacitySchedulerConfiguration.ROOT).getQueueResourceUsage()
        .incPending(Resources.createResource(1 * GB));
    a.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    b.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    c.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    d.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));

    final String user_0 = "user_0";

    stubQueueAllocation(c, clusterResource, node_0, 0*GB);
    stubQueueAllocation(d, clusterResource, node_0, 0*GB);
    root.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    for(int i=0; i < 2; i++)
    {
      stubQueueAllocation(a, clusterResource, node_0, 0*GB);
      stubQueueAllocation(c, clusterResource, node_0, 0*GB);
      stubQueueAllocation(d, clusterResource, node_0, 0*GB);
      root.assignContainers(clusterResource, node_0, new ResourceLimits(
          clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    } 
    for(int i=0; i < 3; i++)
    {
      stubQueueAllocation(c, clusterResource, node_0, 1*GB);
      stubQueueAllocation(d, clusterResource, node_0, 0*GB);
      root.assignContainers(clusterResource, node_0, new ResourceLimits(
          clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    }  
    for(int i=0; i < 4; i++)
    {
      stubQueueAllocation(c, clusterResource, node_0, 0*GB);
      stubQueueAllocation(d, clusterResource, node_0, 1*GB);
      root.assignContainers(clusterResource, node_0, new ResourceLimits(
          clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    }    
    verifyQueueMetrics(a, 1*GB, clusterResource);
    verifyQueueMetrics(b, 2*GB, clusterResource);
      stubQueueAllocation(c, clusterResource, node_0, 0*GB);
      stubQueueAllocation(d, clusterResource, node_0, 0*GB);
      root.assignContainers(clusterResource, node_0, new ResourceLimits(
          clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    }
    verifyQueueMetrics(a, 3*GB, clusterResource);
    verifyQueueMetrics(b, 2*GB, clusterResource);
    stubQueueAllocation(c, clusterResource, node_0, 0*GB);
    stubQueueAllocation(d, clusterResource, node_0, 0*GB);
    root.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyQueueMetrics(a, 2*GB, clusterResource);
    verifyQueueMetrics(b, 3*GB, clusterResource);
    verifyQueueMetrics(c, 3*GB, clusterResource);
    stubQueueAllocation(c, clusterResource, node_0, 0*GB);
    stubQueueAllocation(d, clusterResource, node_0, 0*GB);
    root.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyQueueMetrics(a, 3*GB, clusterResource);
    verifyQueueMetrics(b, 2*GB, clusterResource);
    verifyQueueMetrics(c, 3*GB, clusterResource);
    stubQueueAllocation(c, clusterResource, node_0, 0*GB);
    stubQueueAllocation(d, clusterResource, node_0, 1*GB);
    root.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    InOrder allocationOrder = inOrder(d,b);
    allocationOrder.verify(d).assignContainers(eq(clusterResource),
        any(FiCaSchedulerNode.class), any(ResourceLimits.class),
        any(SchedulingMode.class));
    allocationOrder.verify(b).assignContainers(eq(clusterResource),
        any(FiCaSchedulerNode.class), any(ResourceLimits.class),
        any(SchedulingMode.class));
    verifyQueueMetrics(a, 3*GB, clusterResource);
    verifyQueueMetrics(b, 2*GB, clusterResource);
    verifyQueueMetrics(c, 3*GB, clusterResource);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestContainerAllocation.java
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
    rm1.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.ALLOCATED);
    MockRM.launchAndRegisterAM(app1, rm1, nm1);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestLeafQueue.java
    
    a.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(
        (int)(node_0.getTotalResource().getMemory() * a.getCapacity()) - (1*GB),
        a.getMetrics().getAvailableMB());
    
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(1*GB, a.getUsedResources().getMemory());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    a.setUserLimitFactor(10);
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(3*GB, a.getUsedResources().getMemory());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(4*GB, a.getUsedResources().getMemory());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());
    a.setMaxCapacity(0.5f);
    a.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(4*GB, a.getUsedResources().getMemory());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(3*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());

    a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(4*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(2*GB, app_1.getCurrentConsumption().getMemory());
        1, qb.getActiveUsersManager().getNumActiveUsers());
    qb.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    qb.computeUserLimitAndSetHeadroom(app_0, clusterResource, app_0
        .getResourceRequest(u0Priority, ResourceRequest.ANY).getCapability(),
        "", SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);

    assertEquals(9*GB,app_0.getHeadroom().getMemory());
            u1Priority, recordFactory)));
    qb.submitApplicationAttempt(app_2, user_1);
    qb.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    qb.computeUserLimitAndSetHeadroom(app_0, clusterResource, app_0
        .getResourceRequest(u0Priority, ResourceRequest.ANY).getCapability(),
        "", SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);

    assertEquals(8*GB, qb.getUsedResources().getMemory());
    assertEquals(4*GB, app_0.getCurrentConsumption().getMemory());
    qb.submitApplicationAttempt(app_1, user_0);
    qb.submitApplicationAttempt(app_3, user_1);
    qb.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    qb.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    qb.computeUserLimitAndSetHeadroom(app_3, clusterResource, app_3
        .getResourceRequest(u1Priority, ResourceRequest.ANY).getCapability(),
        "", SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(4*GB, qb.getUsedResources().getMemory());
    assertEquals(5*GB, app_3.getHeadroom().getMemory());
              TestUtils.createResourceRequest(ResourceRequest.ANY, 6*GB, 1, true,
                      u0Priority, recordFactory)));
    qb.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    qb.computeUserLimitAndSetHeadroom(app_4, clusterResource, app_4
        .getResourceRequest(u0Priority, ResourceRequest.ANY).getCapability(),
        "", SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    qb.computeUserLimitAndSetHeadroom(app_3, clusterResource, app_3
        .getResourceRequest(u1Priority, ResourceRequest.ANY).getCapability(),
        "", SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    
    
                priority, recordFactory)));

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(1*GB, a.getUsedResources().getMemory());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
            priority, recordFactory)));

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(3*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());
    a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(3*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());
            priority, recordFactory)));
    assertEquals(1, a.getActiveUsersManager().getNumActiveUsers());
    a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(0*GB, app_2.getHeadroom().getMemory());   // hit queue max-cap 
  }

    
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(1*GB, a.getUsedResources().getMemory());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    a.setUserLimit(25);
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    a.setUserLimitFactor(10);
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(5*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(6*GB, a.getUsedResources().getMemory());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    a.setMaxCapacity(0.5f);
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(6*GB, a.getUsedResources().getMemory());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    a.setMaxCapacity(1.0f);
    a.setUserLimitFactor(1);
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(7*GB, a.getUsedResources().getMemory()); 
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(8*GB, a.getUsedResources().getMemory()); 
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(1*GB, a.getUsedResources().getMemory());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(6*GB, a.getUsedResources().getMemory()); 
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
            ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
        RMContainerEventType.KILL, null, true);
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(5*GB, a.getUsedResources().getMemory()); 
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
            ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
        RMContainerEventType.KILL, null, true);
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(4*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentConsumption().getMemory());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(6*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    doReturn(-1).when(a).getNodeLocalityDelay();
    
    a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(10*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentConsumption().getMemory());
            ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
        RMContainerEventType.KILL, null, true);
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(8*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(8*GB, app_1.getCurrentConsumption().getMemory());
    
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(1*GB, a.getUsedResources().getMemory());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(6*GB, a.getUsedResources().getMemory()); 
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
            ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
        RMContainerEventType.KILL, null, true);
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(5*GB, a.getUsedResources().getMemory()); 
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(5*GB, a.getUsedResources().getMemory()); 
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    
    a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(9*GB, a.getUsedResources().getMemory()); 
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentConsumption().getMemory());
            ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
        RMContainerEventType.KILL, null, true);
    CSAssignment assignment = a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(8*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentConsumption().getMemory());
    
    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_2), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(1, app_0.getSchedulingOpportunities(priority));

    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_2), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(2, app_0.getSchedulingOpportunities(priority));
    
    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_2), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(3, app_0.getSchedulingOpportunities(priority));
    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0).allocate(eq(NodeType.OFF_SWITCH), eq(node_2), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(4, app_0.getSchedulingOpportunities(priority)); // should NOT reset
    
    assignment = a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0).allocate(eq(NodeType.NODE_LOCAL), eq(node_0), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    
    assignment = a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0).allocate(eq(NodeType.NODE_LOCAL), eq(node_1), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    
    assignment = a.assignContainers(clusterResource, node_3,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(1, app_0.getSchedulingOpportunities(priority));
    assertEquals(2, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType()); // None->NODE_LOCAL

    assignment = a.assignContainers(clusterResource, node_3,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0).allocate(eq(NodeType.RACK_LOCAL), eq(node_3), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_2), 
        eq(priority_1), any(ResourceRequest.class), any(Container.class));
    assertEquals(1, app_0.getSchedulingOpportunities(priority_1));
    a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_2), 
        eq(priority_1), any(ResourceRequest.class), any(Container.class));
    assertEquals(2, app_0.getSchedulingOpportunities(priority_1));

    a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0).allocate(eq(NodeType.OFF_SWITCH), eq(node_2), 
        eq(priority_1), any(ResourceRequest.class), any(Container.class));
    assertEquals(3, app_0.getSchedulingOpportunities(priority_1));

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0).allocate(eq(NodeType.NODE_LOCAL), eq(node_0), 
        eq(priority_1), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority_1));

    a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_1), 
        eq(priority_1), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority_1));
    
    a.assignContainers(clusterResource, node_0_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0).allocate(eq(NodeType.NODE_LOCAL), eq(node_0_0), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    a.assignContainers(clusterResource, node_1_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_1_0), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // Still zero
    a.assignContainers(clusterResource, node_0_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_1_0), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(1, app_0.getSchedulingOpportunities(priority)); 
    
    a.assignContainers(clusterResource, node_1_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0).allocate(eq(NodeType.NODE_LOCAL), eq(node_1_0), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    a.assignContainers(clusterResource, node_0_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_0_1), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should be 0
    a.assignContainers(clusterResource, node_1_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_0_1), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should be 0
    a.assignContainers(clusterResource, node_1_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_1_1), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should be 0
    a.assignContainers(clusterResource, node_1_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_1_1), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should be 0

    a.assignContainers(clusterResource, node_1_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0,never()).allocate(eq(NodeType.RACK_LOCAL), eq(node_1_1), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); 

    a.assignContainers(clusterResource, node_1_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verify(app_0).allocate(eq(NodeType.NODE_LOCAL), eq(node_1_0), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); 

    try {
      a.assignContainers(clusterResource, node_0, 
          new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    } catch (NullPointerException e) {
      Assert.fail("NPE when allocating container on node but "
          + "forget to set off-switch request should be handled");

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestNodeLabelContainerAllocation.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestNodeLabelContainerAllocation.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class TestNodeLabelContainerAllocation {
  private final int GB = 1024;

  private YarnConfiguration conf;
  
  RMNodeLabelsManager mgr;

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
      ResourceScheduler.class);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }
  
  private Configuration getConfigurationWithQueueLabels(Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);
    
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b", "c"});
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "y", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setAccessibleNodeLabels(A, toSet("x"));
    conf.setCapacityByLabel(A, "x", 100);
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 20);
    conf.setAccessibleNodeLabels(B, toSet("y"));
    conf.setCapacityByLabel(B, "y", 100);
    
    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    conf.setCapacity(C, 70);
    conf.setMaximumCapacity(C, 70);
    conf.setAccessibleNodeLabels(C, RMNodeLabelsManager.EMPTY_STRING_SET);
    
    final String A1 = A + ".a1";
    conf.setQueues(A, new String[] {"a1"});
    conf.setCapacity(A1, 100);
    conf.setMaximumCapacity(A1, 100);
    conf.setCapacityByLabel(A1, "x", 100);
    
    final String B1 = B + ".b1";
    conf.setQueues(B, new String[] {"b1"});
    conf.setCapacity(B1, 100);
    conf.setMaximumCapacity(B1, 100);
    conf.setCapacityByLabel(B1, "y", 100);

    final String C1 = C + ".c1";
    conf.setQueues(C, new String[] {"c1"});
    conf.setCapacity(C1, 100);
    conf.setMaximumCapacity(C1, 100);
    
    return conf;
  }
  
  private void checkTaskContainersHost(ApplicationAttemptId attemptId,
      ContainerId containerId, ResourceManager rm, String host) {
    YarnScheduler scheduler = rm.getRMContext().getScheduler();
    SchedulerAppReport appReport = scheduler.getSchedulerAppInfo(attemptId);

    Assert.assertTrue(appReport.getLiveContainers().size() > 0);
    for (RMContainer c : appReport.getLiveContainers()) {
      if (c.getContainerId().equals(containerId)) {
        Assert.assertEquals(host, c.getAllocatedNode().getHost());
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  private <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
    return set;
  }
  
  
  @Test (timeout = 300000)
  public void testContainerAllocationWithSingleUserLimits() throws Exception {
    final RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);

    mgr.addToCluserNodeLabels(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x"),
        NodeId.newInstance("h2", 0), toSet("y")));

    MockRM rm1 = new MockRM(TestUtils.getConfigurationWithDefaultQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8000); // label = x
    rm1.registerNode("h2:1234", 8000); // label = y
    MockNM nm3 = rm1.registerNode("h3:1234", 8000); // label = <empty>

    RMApp app1 = rm1.submitApp(200, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    
    ContainerId containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "");
    Assert.assertTrue(rm1.waitForState(nm3, containerId,
          RMContainerState.ALLOCATED, 10 * 1000));
    containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 3);
    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "");
    Assert.assertFalse(rm1.waitForState(nm3, containerId,
          RMContainerState.ALLOCATED, 10 * 1000));

    for (int id = 3; id <= 8; id++) {
      containerId =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), id);
      am1.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "x");
      Assert.assertTrue(rm1.waitForState(nm1, containerId,
          RMContainerState.ALLOCATED, 10 * 1000));
    }
    rm1.close();
  }
  
  @Test(timeout = 300000)
  public void testContainerAllocateWithComplexLabels() throws Exception {

    mgr.addToCluserNodeLabels(ImmutableSet.of("x", "y", "z"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0),
        toSet("x"), NodeId.newInstance("h2", 0), toSet("y"),
        NodeId.newInstance("h3", 0), toSet("y"), NodeId.newInstance("h4", 0),
        toSet("z"), NodeId.newInstance("h5", 0),
        RMNodeLabelsManager.EMPTY_STRING_SET));

    MockRM rm1 = new MockRM(TestUtils.getComplexConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 2048);
    MockNM nm2 = rm1.registerNode("h2:1234", 2048);
    MockNM nm3 = rm1.registerNode("h3:1234", 2048);
    MockNM nm4 = rm1.registerNode("h4:1234", 2048);
    MockNM nm5 = rm1.registerNode("h5:1234", 2048);
    
    ContainerId containerId;

    RMApp app1 = rm1.submitApp(1024, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "y");
    containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2L);
    Assert.assertTrue(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am1.getApplicationAttemptId(), containerId, rm1,
        "h2");

    RMApp app2 = rm1.submitApp(1024, "app", "user", null, "b1");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm5);

    am2.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newContainerId(am2.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm4, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertFalse(rm1.waitForState(nm5, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    
    RMApp app3 = rm1.submitApp(1024, "app", "user", null, "b2");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm5);

    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "y");
    containerId = ContainerId.newContainerId(am3.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertTrue(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am3.getApplicationAttemptId(), containerId, rm1,
        "h3");
    
    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "z");
    containerId = ContainerId.newContainerId(am3.getApplicationAttemptId(), 3L);
    Assert.assertTrue(rm1.waitForState(nm4, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am3.getApplicationAttemptId(), containerId, rm1,
        "h4");

    rm1.close();
  }

  @Test (timeout = 120000)
  public void testContainerAllocateWithLabels() throws Exception {
    mgr.addToCluserNodeLabels(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x"),
        NodeId.newInstance("h2", 0), toSet("y")));

    MockRM rm1 = new MockRM(getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8000); // label = x
    MockNM nm2 = rm1.registerNode("h2:1234", 8000); // label = y
    MockNM nm3 = rm1.registerNode("h3:1234", 8000); // label = <empty>
    
    ContainerId containerId;

    RMApp app1 = rm1.submitApp(200, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm3);

    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "x");
    containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertTrue(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am1.getApplicationAttemptId(), containerId, rm1,
        "h1");

    RMApp app2 = rm1.submitApp(200, "app", "user", null, "b1");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm3);

    am2.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "y");
    containerId = ContainerId.newContainerId(am2.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertTrue(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am2.getApplicationAttemptId(), containerId, rm1,
        "h2");
    
    RMApp app3 = rm1.submitApp(200, "app", "user", null, "c1");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm3);

    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newContainerId(am3.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertTrue(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am3.getApplicationAttemptId(), containerId, rm1,
        "h3");

    rm1.close();
  }
  
  @Test (timeout = 120000)
  public void testContainerAllocateWithDefaultQueueLabels() throws Exception {

    mgr.addToCluserNodeLabels(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x"),
        NodeId.newInstance("h2", 0), toSet("y")));

    MockRM rm1 = new MockRM(TestUtils.getConfigurationWithDefaultQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8000); // label = x
    MockNM nm2 = rm1.registerNode("h2:1234", 8000); // label = y
    MockNM nm3 = rm1.registerNode("h3:1234", 8000); // label = <empty>
    
    ContainerId containerId;

    RMApp app1 = rm1.submitApp(200, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertTrue(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am1.getApplicationAttemptId(), containerId, rm1,
        "h1");

    RMApp app2 = rm1.submitApp(200, "app", "user", null, "b1");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    am2.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newContainerId(am2.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertTrue(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am2.getApplicationAttemptId(), containerId, rm1,
        "h2");
    
    RMApp app3 = rm1.submitApp(200, "app", "user", null, "c1");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm3);

    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newContainerId(am3.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    Assert.assertTrue(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkTaskContainersHost(am3.getApplicationAttemptId(), containerId, rm1,
        "h3");

    rm1.close();
  }
  
  private void checkPendingResource(MockRM rm, int priority,
      ApplicationAttemptId attemptId, int memory) {
    CapacityScheduler cs = (CapacityScheduler) rm.getRMContext().getScheduler();
    FiCaSchedulerApp app = cs.getApplicationAttempt(attemptId);
    ResourceRequest rr =
        app.getAppSchedulingInfo().getResourceRequest(
            Priority.newInstance(priority), "*");
    Assert.assertEquals(memory,
        rr.getCapability().getMemory() * rr.getNumContainers());
  }
  
  private void checkLaunchedContainerNumOnNode(MockRM rm, NodeId nodeId,
      int numContainers) {
    CapacityScheduler cs = (CapacityScheduler) rm.getRMContext().getScheduler();
    SchedulerNode node = cs.getSchedulerNode(nodeId);
    Assert.assertEquals(numContainers, node.getNumContainers());
  }
  
  @Test
  public void testPreferenceOfNeedyAppsTowardsNodePartitions() throws Exception {
    
    mgr.addToCluserNodeLabels(ImmutableSet.of("x", "y"));
    mgr.updateNodeLabels(Arrays.asList(NodeLabel.newInstance("y", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("y")));

    MockRM rm1 = new MockRM(TestUtils.getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB); // label = y
    MockNM nm2 = rm1.registerNode("h2:1234", 100 * GB); // label = <empty>

    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "b1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm2);
    
    RMApp app2 = rm1.submitApp(1 * GB, "app", "user", null, "b1");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    am1.allocate("*", 1 * GB, 8, new ArrayList<ContainerId>());
    am2.allocate("*", 1 * GB, 8, new ArrayList<ContainerId>(), "y");
    
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());
    
    for (int i = 0; i < 50; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
    }
    
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(am2.getApplicationAttemptId());
    checkNumOfContainersInAnAppOnGivenNode(0, nm1.getNodeId(), schedulerApp1);
    checkNumOfContainersInAnAppOnGivenNode(9, nm2.getNodeId(), schedulerApp1);
    checkNumOfContainersInAnAppOnGivenNode(8, nm1.getNodeId(), schedulerApp2);
    checkNumOfContainersInAnAppOnGivenNode(1, nm2.getNodeId(), schedulerApp2);
    
    rm1.close();
  }
  
  private void checkNumOfContainersInAnAppOnGivenNode(int expectedNum,
      NodeId nodeId, FiCaSchedulerApp app) {
    int num = 0;
    for (RMContainer container : app.getLiveContainers()) {
      if (container.getAllocatedNode().equals(nodeId)) {
        num++;
      }
    }
    Assert.assertEquals(expectedNum, num);
  }
  
  @Test
  public void
      testPreferenceOfNeedyPrioritiesUnderSameAppTowardsNodePartitions()
          throws Exception {
    
    mgr.addToCluserNodeLabels(ImmutableSet.of("x", "y"));
    mgr.updateNodeLabels(Arrays.asList(NodeLabel.newInstance("y", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("y")));

    MockRM rm1 = new MockRM(TestUtils.getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB); // label = y
    MockNM nm2 = rm1.registerNode("h2:1234", 100 * GB); // label = <empty>
    
    ContainerId nextContainerId;

    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "b1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm2);
    
    nextContainerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    am1.allocate("*", 1 * GB, 1, 1, new ArrayList<ContainerId>(), "");
    am1.allocate("*", 1 * GB, 1, 2, new ArrayList<ContainerId>(), "y");
    Assert.assertTrue(rm1.waitForState(nm1, nextContainerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    
    checkPendingResource(rm1, 1, am1.getApplicationAttemptId(), 1 * GB);
    checkPendingResource(rm1, 2, am1.getApplicationAttemptId(), 0 * GB);
    
    rm1.close();
  }
  
  @Test
  public void testNonLabeledResourceRequestGetPreferrenceToNonLabeledNode()
      throws Exception {
    
    mgr.addToCluserNodeLabels(ImmutableSet.of("x", "y"));
    mgr.updateNodeLabels(Arrays.asList(NodeLabel.newInstance("x", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    MockRM rm1 = new MockRM(TestUtils.getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB); // label = y
    MockNM nm2 = rm1.registerNode("h2:1234", 100 * GB); // label = <empty>
    
    ContainerId nextContainerId;

    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "b1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm2);
    
    am1.allocate("*", 1 * GB, 6, 1, new ArrayList<ContainerId>(), "");
    for (int i = 2; i < 2 + 6; i++) {
      nextContainerId =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), i);
      Assert.assertTrue(rm1.waitForState(Arrays.asList(nm1, nm2),
          nextContainerId, RMContainerState.ALLOCATED, 10 * 1000));
    }
    checkLaunchedContainerNumOnNode(rm1, nm1.getNodeId(), 0);
    checkLaunchedContainerNumOnNode(rm1, nm2.getNodeId(), 7);   
    
    rm1.close();
  }

  @Test
  public void testPreferenceOfQueuesTowardsNodePartitions()
      throws Exception {
    
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(this.conf);
    
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b", "c"});
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    csConf.setCapacity(A, 33);
    csConf.setAccessibleNodeLabels(A, toSet("x"));
    csConf.setCapacityByLabel(A, "x", 33);
    csConf.setQueues(A, new String[] {"a1", "a2"});
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    csConf.setCapacity(B, 33);
    csConf.setAccessibleNodeLabels(B, toSet("x"));
    csConf.setCapacityByLabel(B, "x", 33);
    csConf.setQueues(B, new String[] {"b1", "b2"});
    
    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    csConf.setCapacity(C, 34);
    csConf.setAccessibleNodeLabels(C, toSet("x"));
    csConf.setCapacityByLabel(C, "x", 34);
    csConf.setQueues(C, new String[] {"c1", "c2"});
    
    final String A1 = A + ".a1";
    csConf.setCapacity(A1, 50);
    csConf.setCapacityByLabel(A1, "x", 100);
    csConf.setDefaultNodeLabelExpression(A1, "x");
    
    final String A2 = A + ".a2";
    csConf.setCapacity(A2, 50);
    csConf.setCapacityByLabel(A2, "x", 0);
    
    final String B1 = B + ".b1";
    csConf.setCapacity(B1, 50);
    csConf.setCapacityByLabel(B1, "x", 100);
    csConf.setDefaultNodeLabelExpression(B1, "x");
    
    final String B2 = B + ".b2";
    csConf.setCapacity(B2, 50);
    csConf.setCapacityByLabel(B2, "x", 0);
    
    final String C1 = C + ".c1";
    csConf.setCapacity(C1, 50);
    csConf.setCapacityByLabel(C1, "x", 100);
    csConf.setDefaultNodeLabelExpression(C1, "x");
    
    final String C2 = C + ".c2";
    csConf.setCapacity(C2, 50);
    csConf.setCapacityByLabel(C2, "x", 0);
    
    mgr.addToCluserNodeLabels(ImmutableSet.of("x", "y"));
    mgr.updateNodeLabels(Arrays.asList(NodeLabel.newInstance("x", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 20 * GB); // label = x
    MockNM nm2 = rm1.registerNode("h2:1234", 100 * GB); // label = <empty>

    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    
    RMApp app2 = rm1.submitApp(1 * GB, "app", "user", null, "a2");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);
    
    RMApp app3 = rm1.submitApp(1 * GB, "app", "user", null, "b1");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm1);
    
    RMApp app4 = rm1.submitApp(1 * GB, "app", "user", null, "b2");
    MockAM am4 = MockRM.launchAndRegisterAM(app4, rm1, nm2);
    
    RMApp app5 = rm1.submitApp(1 * GB, "app", "user", null, "c1");
    MockAM am5 = MockRM.launchAndRegisterAM(app5, rm1, nm1);
    
    RMApp app6 = rm1.submitApp(1 * GB, "app", "user", null, "c2");
    MockAM am6 = MockRM.launchAndRegisterAM(app6, rm1, nm2);
    
    am1.allocate("*", 1 * GB, 5, new ArrayList<ContainerId>());
    am2.allocate("*", 1 * GB, 5, new ArrayList<ContainerId>());
    am3.allocate("*", 1 * GB, 5, new ArrayList<ContainerId>());
    am4.allocate("*", 1 * GB, 5, new ArrayList<ContainerId>());
    am5.allocate("*", 1 * GB, 5, new ArrayList<ContainerId>());
    am6.allocate("*", 1 * GB, 5, new ArrayList<ContainerId>());
    
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    for (int i = 0; i < 15; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }
    
    checkLaunchedContainerNumOnNode(rm1, nm1.getNodeId(), 18);

    checkPendingResource(rm1, 1, am1.getApplicationAttemptId(), 0 * GB);
    checkPendingResource(rm1, 1, am2.getApplicationAttemptId(), 5 * GB);
    checkPendingResource(rm1, 1, am3.getApplicationAttemptId(), 0 * GB);
    checkPendingResource(rm1, 1, am4.getApplicationAttemptId(), 5 * GB);
    checkPendingResource(rm1, 1, am5.getApplicationAttemptId(), 0 * GB);
    checkPendingResource(rm1, 1, am6.getApplicationAttemptId(), 5 * GB);

    rm1.close();
  }
  
  @Test
  public void testQueuesWithoutAccessUsingPartitionedNodes() throws Exception {
    
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(this.conf);
    
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    csConf.setCapacity(A, 50);
    csConf.setAccessibleNodeLabels(A, toSet("x"));
    csConf.setCapacityByLabel(A, "x", 100);
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    csConf.setCapacity(B, 50);
    csConf.setAccessibleNodeLabels(B, new HashSet<String>());
    csConf.setUserLimitFactor(B, 5);
    
    mgr.addToCluserNodeLabels(ImmutableSet.of("x"));
    mgr.updateNodeLabels(Arrays.asList(NodeLabel.newInstance("x", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 10 * GB); // label = x
    MockNM nm2 = rm1.registerNode("h2:1234", 10 * GB); // label = <empty>

    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "b");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm2);
    
    am1.allocate("*", 1 * GB, 50, new ArrayList<ContainerId>());
    
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());
    
    SchedulerNode schedulerNode1 = cs.getSchedulerNode(nm1.getNodeId());
    
    int cycleWaited = 0;
    for (int i = 0; i < 50; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
      if (schedulerNode1.getNumContainers() == 0) {
        cycleWaited++;
      }
    }
    Assert.assertEquals(10, cycleWaited);
    
    checkLaunchedContainerNumOnNode(rm1, nm1.getNodeId(), 10);
    checkLaunchedContainerNumOnNode(rm1, nm2.getNodeId(), 10);

    rm1.close();
  }
  
  @Test
  public void testAMContainerAllocationWillAlwaysBeExclusive()
      throws Exception {
    
    mgr.addToCluserNodeLabels(ImmutableSet.of("x", "y"));
    mgr.updateNodeLabels(Arrays.asList(NodeLabel.newInstance("x", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    MockRM rm1 = new MockRM(TestUtils.getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB); // label = x

    rm1.submitApp(1 * GB, "app", "user", null, "b1");
   
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    
    for (int i = 0; i < 50; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }
    
    Assert.assertEquals(0, cs.getSchedulerNode(nm1.getNodeId())
        .getNumContainers());
    
    rm1.close();
  }
  
  @Test
  public void
      testQueueMaxCapacitiesWillNotBeHonoredWhenNotRespectingExclusivity()
          throws Exception {

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(this.conf);

    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] { "a",
        "b" });
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    csConf.setCapacity(A, 50);
    csConf.setAccessibleNodeLabels(A, toSet("x"));
    csConf.setCapacityByLabel(A, "x", 50);
    csConf.setMaximumCapacityByLabel(A, "x", 50);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    csConf.setCapacity(B, 50);
    csConf.setAccessibleNodeLabels(B, toSet("x"));
    csConf.setCapacityByLabel(B, "x", 50);
    csConf.setMaximumCapacityByLabel(B, "x", 50);

    mgr.addToCluserNodeLabels(ImmutableSet.of("x"));
    mgr.updateNodeLabels(Arrays.asList(NodeLabel.newInstance("x", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 10 * GB); // label = x
    MockNM nm2 = rm1.registerNode("h2:1234", 10 * GB); // label = <empty>

    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "a");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm2);

    am1.allocate("*", 1 * GB, 10, new ArrayList<ContainerId>());

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());

    SchedulerNode schedulerNode1 = cs.getSchedulerNode(nm1.getNodeId());

    for (int i = 0; i < 50; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }
    
    Assert.assertEquals(10, schedulerNode1.getNumContainers());

    rm1.close();
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestParentQueue.java
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
        final Resource allocatedResource = Resources.createResource(allocation);
        if (queue instanceof ParentQueue) {
          ((ParentQueue)queue).allocateResource(clusterResource, 
              allocatedResource, RMNodeLabelsManager.NO_LABEL);
        } else {
          FiCaSchedulerApp app1 = getMockApplication(0, "");
          ((LeafQueue)queue).allocateResource(clusterResource, app1, 
        if (allocation > 0) {
          doReturn(new CSAssignment(Resources.none(), type)).when(queue)
              .assignContainers(eq(clusterResource), eq(node),
                  any(ResourceLimits.class), any(SchedulingMode.class));

          Resource available = node.getAvailableResource();
        return new CSAssignment(allocatedResource, type);
      }
    }).when(queue).assignContainers(eq(clusterResource), eq(node),
        any(ResourceLimits.class), any(SchedulingMode.class));
  }
  
  private float computeQueueAbsoluteUsedCapacity(CSQueue queue, 
    LeafQueue a = (LeafQueue)queues.get(A);
    LeafQueue b = (LeafQueue)queues.get(B);
    
    a.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    b.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    queues.get(CapacitySchedulerConfiguration.ROOT).getQueueResourceUsage()
    .incPending(Resources.createResource(1 * GB));
    
    stubQueueAllocation(a, clusterResource, node_0, 0*GB);
    stubQueueAllocation(b, clusterResource, node_0, 1*GB);
    root.assignContainers(clusterResource, node_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyQueueMetrics(a, 0*GB, clusterResource);
    verifyQueueMetrics(b, 1*GB, clusterResource);
    
    stubQueueAllocation(a, clusterResource, node_1, 2*GB);
    stubQueueAllocation(b, clusterResource, node_1, 1*GB);
    root.assignContainers(clusterResource, node_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    InOrder allocationOrder = inOrder(a, b);
    allocationOrder.verify(a).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    allocationOrder.verify(b).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    verifyQueueMetrics(a, 2*GB, clusterResource);
    verifyQueueMetrics(b, 2*GB, clusterResource);

    stubQueueAllocation(a, clusterResource, node_0, 1*GB);
    stubQueueAllocation(b, clusterResource, node_0, 2*GB);
    root.assignContainers(clusterResource, node_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder = inOrder(b, a);
    allocationOrder.verify(b).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    allocationOrder.verify(a).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    verifyQueueMetrics(a, 3*GB, clusterResource);
    verifyQueueMetrics(b, 4*GB, clusterResource);

    stubQueueAllocation(a, clusterResource, node_0, 0*GB);
    stubQueueAllocation(b, clusterResource, node_0, 4*GB);
    root.assignContainers(clusterResource, node_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder = inOrder(b, a);
    allocationOrder.verify(b).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    allocationOrder.verify(a).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    verifyQueueMetrics(a, 3*GB, clusterResource);
    verifyQueueMetrics(b, 8*GB, clusterResource);

    stubQueueAllocation(a, clusterResource, node_1, 1*GB);
    stubQueueAllocation(b, clusterResource, node_1, 1*GB);
    root.assignContainers(clusterResource, node_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder = inOrder(a, b);
    allocationOrder.verify(b).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    allocationOrder.verify(a).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    verifyQueueMetrics(a, 4*GB, clusterResource);
    verifyQueueMetrics(b, 9*GB, clusterResource);
  }

    CSQueue a = queues.get(A);
    a.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    CSQueue b = queues.get(B);
    b.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    CSQueue c = queues.get(C);
    c.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    CSQueue d = queues.get(D);
    d.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));

    CSQueue a1 = queues.get(A1);
    a1.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    CSQueue a2 = queues.get(A2);
    a2.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));

    CSQueue b1 = queues.get(B1);
    b1.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    CSQueue b2 = queues.get(B2);
    b2.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    CSQueue b3 = queues.get(B3);
    b3.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    queues.get(CapacitySchedulerConfiguration.ROOT).getQueueResourceUsage()
    .incPending(Resources.createResource(1 * GB));

    stubQueueAllocation(a, clusterResource, node_0, 0*GB);
    stubQueueAllocation(c, clusterResource, node_0, 1*GB);
    stubQueueAllocation(d, clusterResource, node_0, 0*GB);
    root.assignContainers(clusterResource, node_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyQueueMetrics(a, 0*GB, clusterResource);
    verifyQueueMetrics(b, 0*GB, clusterResource);
    verifyQueueMetrics(c, 1*GB, clusterResource);
    stubQueueAllocation(b2, clusterResource, node_1, 4*GB);
    stubQueueAllocation(c, clusterResource, node_1, 0*GB);
    root.assignContainers(clusterResource, node_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyQueueMetrics(a, 0*GB, clusterResource);
    verifyQueueMetrics(b, 4*GB, clusterResource);
    verifyQueueMetrics(c, 1*GB, clusterResource);
    stubQueueAllocation(b3, clusterResource, node_0, 2*GB);
    stubQueueAllocation(c, clusterResource, node_0, 2*GB);
    root.assignContainers(clusterResource, node_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    InOrder allocationOrder = inOrder(a, c, b);
    allocationOrder.verify(a).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    allocationOrder.verify(c).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    allocationOrder.verify(b).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    verifyQueueMetrics(a, 1*GB, clusterResource);
    verifyQueueMetrics(b, 6*GB, clusterResource);
    verifyQueueMetrics(c, 3*GB, clusterResource);
    stubQueueAllocation(b1, clusterResource, node_2, 1*GB);
    stubQueueAllocation(c, clusterResource, node_2, 1*GB);
    root.assignContainers(clusterResource, node_2, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder = inOrder(a, a2, a1, b, c);
    allocationOrder.verify(a).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    allocationOrder.verify(a2).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    allocationOrder.verify(b).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    allocationOrder.verify(c).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    verifyQueueMetrics(a, 3*GB, clusterResource);
    verifyQueueMetrics(b, 8*GB, clusterResource);
    verifyQueueMetrics(c, 4*GB, clusterResource);
    LeafQueue a = (LeafQueue)queues.get(A);
    LeafQueue b = (LeafQueue)queues.get(B);
    a.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    b.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    queues.get(CapacitySchedulerConfiguration.ROOT).getQueueResourceUsage()
    .incPending(Resources.createResource(1 * GB));
    
    stubQueueAllocation(a, clusterResource, node_0, 0*GB, NodeType.OFF_SWITCH);
    stubQueueAllocation(b, clusterResource, node_0, 1*GB, NodeType.OFF_SWITCH);
    root.assignContainers(clusterResource, node_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyQueueMetrics(a, 0*GB, clusterResource);
    verifyQueueMetrics(b, 1*GB, clusterResource);
    
    stubQueueAllocation(a, clusterResource, node_1, 2*GB, NodeType.RACK_LOCAL);
    stubQueueAllocation(b, clusterResource, node_1, 1*GB, NodeType.OFF_SWITCH);
    root.assignContainers(clusterResource, node_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    InOrder allocationOrder = inOrder(a, b);
    allocationOrder.verify(a).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    allocationOrder.verify(b).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    verifyQueueMetrics(a, 2*GB, clusterResource);
    verifyQueueMetrics(b, 2*GB, clusterResource);
    
    stubQueueAllocation(a, clusterResource, node_0, 1*GB, NodeType.NODE_LOCAL);
    stubQueueAllocation(b, clusterResource, node_0, 2*GB, NodeType.OFF_SWITCH);
    root.assignContainers(clusterResource, node_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder = inOrder(b, a);
    allocationOrder.verify(b).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    allocationOrder.verify(a).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    verifyQueueMetrics(a, 2*GB, clusterResource);
    verifyQueueMetrics(b, 4*GB, clusterResource);

    LeafQueue b3 = (LeafQueue)queues.get(B3);
    LeafQueue b2 = (LeafQueue)queues.get(B2);
    b2.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    b3.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    queues.get(CapacitySchedulerConfiguration.ROOT).getQueueResourceUsage()
    .incPending(Resources.createResource(1 * GB));
    
    CSQueue b = queues.get(B);
    b.getQueueResourceUsage().incPending(Resources.createResource(1 * GB));
    
    stubQueueAllocation(b2, clusterResource, node_0, 0*GB, NodeType.OFF_SWITCH);
    stubQueueAllocation(b3, clusterResource, node_0, 1*GB, NodeType.OFF_SWITCH);
    root.assignContainers(clusterResource, node_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    verifyQueueMetrics(b2, 0*GB, clusterResource);
    verifyQueueMetrics(b3, 1*GB, clusterResource);
    
    stubQueueAllocation(b2, clusterResource, node_1, 1*GB, NodeType.RACK_LOCAL);
    stubQueueAllocation(b3, clusterResource, node_1, 1*GB, NodeType.OFF_SWITCH);
    root.assignContainers(clusterResource, node_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    InOrder allocationOrder = inOrder(b2, b3);
    allocationOrder.verify(b2).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    allocationOrder.verify(b3).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    verifyQueueMetrics(b2, 1*GB, clusterResource);
    verifyQueueMetrics(b3, 2*GB, clusterResource);
    
    stubQueueAllocation(b2, clusterResource, node_0, 1*GB, NodeType.NODE_LOCAL);
    stubQueueAllocation(b3, clusterResource, node_0, 1*GB, NodeType.OFF_SWITCH);
    root.assignContainers(clusterResource, node_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    allocationOrder = inOrder(b3, b2);
    allocationOrder.verify(b3).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    allocationOrder.verify(b2).assignContainers(eq(clusterResource), 
        any(FiCaSchedulerNode.class), anyResourceLimits(), any(SchedulingMode.class));
    verifyQueueMetrics(b2, 1*GB, clusterResource);
    verifyQueueMetrics(b3, 3*GB, clusterResource);


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestReservations.java
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(2 * GB, a.getUsedResources().getMemory());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(5 * GB, a.getUsedResources().getMemory());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(8 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(13 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(18 * GB, a.getUsedResources().getMemory());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(18 * GB, a.getUsedResources().getMemory());
    assertEquals(18 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(2 * GB, a.getUsedResources().getMemory());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(5 * GB, a.getUsedResources().getMemory());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(8 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(13 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(18 * GB, a.getUsedResources().getMemory());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(18 * GB, a.getUsedResources().getMemory());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(2 * GB, a.getUsedResources().getMemory());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(5 * GB, a.getUsedResources().getMemory());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(8 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(13 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(13 * GB, a.getUsedResources().getMemory());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(2 * GB, a.getUsedResources().getMemory());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(5 * GB, a.getUsedResources().getMemory());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(8 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    Resource capability = Resources.createResource(32 * GB, 0);
    boolean res =
        a.canAssignToThisQueue(clusterResource,
            RMNodeLabelsManager.NO_LABEL, new ResourceLimits(
                clusterResource), capability, Resources.none(),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertFalse(res);

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(13 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    capability = Resources.createResource(5 * GB, 0);
    res =
        a.canAssignToThisQueue(clusterResource,
            RMNodeLabelsManager.NO_LABEL, new ResourceLimits(
                clusterResource), capability, Resources.createResource(5 * GB),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertTrue(res);

    res =
        a.canAssignToThisQueue(clusterResource,
            RMNodeLabelsManager.NO_LABEL, new ResourceLimits(
                clusterResource), capability, Resources.none(),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertFalse(res);

    refreshQueuesTurnOffReservationsContLook(a, csConf);
    res =
        a.canAssignToThisQueue(clusterResource,
            RMNodeLabelsManager.NO_LABEL, new ResourceLimits(
                clusterResource), capability, Resources.none(),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertFalse(res);

    res =
        a.canAssignToThisQueue(clusterResource,
            RMNodeLabelsManager.NO_LABEL, new ResourceLimits(
                clusterResource), capability, Resources.createResource(5 * GB),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertFalse(res);
  }

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(2 * GB, a.getUsedResources().getMemory());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(5 * GB, a.getUsedResources().getMemory());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(8 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(13 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(5 * GB, app_0.getCurrentReservation().getMemory());
    Resource limit = Resources.createResource(12 * GB, 0);
    boolean res = a.canAssignToUser(clusterResource, user_0, limit, app_0,
        true, "");
    assertTrue(res);

    res = a.canAssignToUser(clusterResource, user_0, limit, app_0, false, "");
    assertFalse(res);

    refreshQueuesTurnOffReservationsContLook(a, csConf);

    res = a.canAssignToUser(clusterResource, user_0, limit, app_0, true, "");
    assertFalse(res);
  }

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(2 * GB, a.getUsedResources().getMemory());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(5 * GB, a.getUsedResources().getMemory());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(8 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(Resources.createResource(10 * GB)), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(8 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    a.assignContainers(clusterResource, node_2,
        new ResourceLimits(Resources.createResource(10 * GB)), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(8 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(13 * GB, a.getUsedResources().getMemory());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());

    a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(21 * GB, a.getUsedResources().getMemory());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(8 * GB, a.getMetrics().getReservedMB());
    a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertEquals(21 * GB, a.getUsedResources().getMemory());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(8 * GB, a.getMetrics().getReservedMB());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestUtils.java
    request.setCapability(capability);
    request.setRelaxLocality(relaxLocality);
    request.setPriority(priority);
    request.setNodeLabelExpression(RMNodeLabelsManager.NO_LABEL);
    return request;
  }
  
    conf.setCapacity(B1, 100);
    conf.setMaximumCapacity(B1, 100);
    conf.setCapacityByLabel(B1, "y", 100);
    conf.setMaximumApplicationMasterResourcePerQueuePercent(B1, 1f);

    final String C1 = C + ".c1";
    conf.setQueues(C, new String[] {"c1"});

