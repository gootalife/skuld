hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/capacity/ProportionalCapacityPreemptionPolicy.java
package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

  private float percentageClusterPreemptionAllowed;
  private double naturalTerminationFactor;
  private boolean observeOnly;
  private Map<String, Map<String, TempQueuePerPartition>> queueToPartitions =
      new HashMap<>();
  private RMNodeLabelsManager nlm;

  public ProportionalCapacityPreemptionPolicy() {
    clock = new SystemClock();
      config.getFloat(TOTAL_PREEMPTION_PER_ROUND, (float) 0.1);
    observeOnly = config.getBoolean(OBSERVE_ONLY, false);
    rc = scheduler.getResourceCalculator();
    nlm = scheduler.getRMContext().getNodeLabelManager();
  }
  
  @VisibleForTesting
  public void editSchedule() {
    CSQueue root = scheduler.getRootQueue();
    Resource clusterResources = Resources.clone(scheduler.getClusterResource());
    containerBasedPreemptOrKill(root, clusterResources);
  }
  
  private void containerBasedPreemptOrKill(CSQueue root,
      Resource clusterResources) {
    Set<String> allPartitions = new HashSet<>();
    allPartitions.addAll(scheduler.getRMContext()
        .getNodeLabelManager().getClusterNodeLabelNames());
    allPartitions.add(RMNodeLabelsManager.NO_LABEL);

    synchronized (scheduler) {
      queueToPartitions.clear();

      for (String partitionToLookAt : allPartitions) {
        cloneQueues(root,
            nlm.getResourceByLabel(partitionToLookAt, clusterResources),
            partitionToLookAt);
      }
    }

    Resource totalPreemptionAllowed = Resources.multiply(clusterResources,
        percentageClusterPreemptionAllowed);

    Set<String> leafQueueNames = null;
    for (String partition : allPartitions) {
      TempQueuePerPartition tRoot =
          getQueueByPartition(CapacitySchedulerConfiguration.ROOT, partition);
      tRoot.idealAssigned = tRoot.guaranteed;

      leafQueueNames =
          recursivelyComputeIdealAssignment(tRoot, totalPreemptionAllowed);
    }

    Map<ApplicationAttemptId,Set<RMContainer>> toPreempt =
        getContainersToPreempt(leafQueueNames, clusterResources);

    if (LOG.isDebugEnabled()) {
      logToCSV(new ArrayList<String>(leafQueueNames));
    }

    for (Map.Entry<ApplicationAttemptId,Set<RMContainer>> e
         : toPreempt.entrySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Send to scheduler: in app=" + e.getKey()
            + " #containers-to-be-preempted=" + e.getValue().size());
      }
      for (RMContainer container : e.getValue()) {
        if (preempted.get(container) != null &&
  private Set<String> recursivelyComputeIdealAssignment(
      TempQueuePerPartition root, Resource totalPreemptionAllowed) {
    Set<String> leafQueueNames = new HashSet<>();
    if (root.getChildren() != null &&
        root.getChildren().size() > 0) {
      computeIdealResourceDistribution(rc, root.getChildren(),
          totalPreemptionAllowed, root.idealAssigned);
      for(TempQueuePerPartition t : root.getChildren()) {
        leafQueueNames.addAll(recursivelyComputeIdealAssignment(t,
            totalPreemptionAllowed));
      }
    } else {
      return ImmutableSet.of(root.queueName);
    }
    return leafQueueNames;
  }

  private void computeIdealResourceDistribution(ResourceCalculator rc,
      List<TempQueuePerPartition> queues, Resource totalPreemptionAllowed,
      Resource tot_guarant) {

    List<TempQueuePerPartition> qAlloc = new ArrayList<TempQueuePerPartition>(queues);
    Resource unassigned = Resources.clone(tot_guarant);

    Set<TempQueuePerPartition> nonZeroGuarQueues = new HashSet<TempQueuePerPartition>();
    Set<TempQueuePerPartition> zeroGuarQueues = new HashSet<TempQueuePerPartition>();

    for (TempQueuePerPartition q : qAlloc) {
      if (Resources
          .greaterThan(rc, tot_guarant, q.guaranteed, Resources.none())) {
        nonZeroGuarQueues.add(q);
    Resource totPreemptionNeeded = Resource.newInstance(0, 0);
    for (TempQueuePerPartition t:queues) {
      if (Resources.greaterThan(rc, tot_guarant, t.current, t.idealAssigned)) {
        Resources.addTo(totPreemptionNeeded,
            Resources.subtract(t.current, t.idealAssigned));

    for (TempQueuePerPartition t : queues) {
      t.assignPreemption(scalingFactor, rc, tot_guarant);
    }
    if (LOG.isDebugEnabled()) {
      long time = clock.getTime();
      for (TempQueuePerPartition t : queues) {
        LOG.debug(time + ": " + t);
      }
    }
  private void computeFixpointAllocation(ResourceCalculator rc,
      Resource tot_guarant, Collection<TempQueuePerPartition> qAlloc,
      Resource unassigned, boolean ignoreGuarantee) {
    TQComparator tqComparator = new TQComparator(rc, tot_guarant);
    PriorityQueue<TempQueuePerPartition> orderedByNeed =
        new PriorityQueue<TempQueuePerPartition>(10, tqComparator);
    for (Iterator<TempQueuePerPartition> i = qAlloc.iterator(); i.hasNext();) {
      TempQueuePerPartition q = i.next();
      if (Resources.greaterThan(rc, tot_guarant, q.current, q.guaranteed)) {
        q.idealAssigned = Resources.add(q.guaranteed, q.untouchableExtra);
      } else {
      Collection<TempQueuePerPartition> underserved =
          getMostUnderservedQueues(orderedByNeed, tqComparator);
      for (Iterator<TempQueuePerPartition> i = underserved.iterator(); i
          .hasNext();) {
        TempQueuePerPartition sub = i.next();
        Resource wQavail = Resources.multiplyAndNormalizeUp(rc,
            unassigned, sub.normalizedGuarantee, Resource.newInstance(1, 1));
        Resource wQidle = sub.offer(wQavail, rc, tot_guarant);
  protected Collection<TempQueuePerPartition> getMostUnderservedQueues(
      PriorityQueue<TempQueuePerPartition> orderedByNeed, TQComparator tqComparator) {
    ArrayList<TempQueuePerPartition> underserved = new ArrayList<TempQueuePerPartition>();
    while (!orderedByNeed.isEmpty()) {
      TempQueuePerPartition q1 = orderedByNeed.remove();
      underserved.add(q1);
      TempQueuePerPartition q2 = orderedByNeed.peek();
  private void resetCapacity(ResourceCalculator rc, Resource clusterResource,
      Collection<TempQueuePerPartition> queues, boolean ignoreGuar) {
    Resource activeCap = Resource.newInstance(0, 0);
    
    if (ignoreGuar) {
      for (TempQueuePerPartition q : queues) {
        q.normalizedGuarantee = (float)  1.0f / ((float) queues.size());
      }
    } else {
      for (TempQueuePerPartition q : queues) {
        Resources.addTo(activeCap, q.guaranteed);
      }
      for (TempQueuePerPartition q : queues) {
        q.normalizedGuarantee = Resources.divide(rc, clusterResource,
            q.guaranteed, activeCap);
      }
    }
  }

  private String getPartitionByNodeId(NodeId nodeId) {
    return scheduler.getSchedulerNode(nodeId).getPartition();
  }

  private boolean tryPreemptContainerAndDeductResToObtain(
      Map<String, Resource> resourceToObtainByPartitions,
      RMContainer rmContainer, Resource clusterResource,
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap) {
    ApplicationAttemptId attemptId = rmContainer.getApplicationAttemptId();

    if (preemptMapContains(preemptMap, attemptId, rmContainer)) {
      return false;
    }

    String nodePartition = getPartitionByNodeId(rmContainer.getAllocatedNode());
    Resource toObtainByPartition =
        resourceToObtainByPartitions.get(nodePartition);

    if (null != toObtainByPartition
        && Resources.greaterThan(rc, clusterResource, toObtainByPartition,
            Resources.none())) {
      Resources.subtractFrom(toObtainByPartition,
          rmContainer.getAllocatedResource());
      if (Resources.lessThanOrEqual(rc, clusterResource, toObtainByPartition,
          Resources.none())) {
        resourceToObtainByPartitions.remove(nodePartition);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Marked container=" + rmContainer.getContainerId()
            + " in partition=" + nodePartition + " will be preempted");
      }
      addToPreemptMap(preemptMap, attemptId, rmContainer);
      return true;
    }

    return false;
  }

  private boolean preemptMapContains(
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      ApplicationAttemptId attemptId, RMContainer rmContainer) {
    Set<RMContainer> rmContainers;
    if (null == (rmContainers = preemptMap.get(attemptId))) {
      return false;
    }
    return rmContainers.contains(rmContainer);
  }

  private void addToPreemptMap(
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      ApplicationAttemptId appAttemptId, RMContainer containerToPreempt) {
    Set<RMContainer> set;
    if (null == (set = preemptMap.get(appAttemptId))) {
      set = new HashSet<RMContainer>();
      preemptMap.put(appAttemptId, set);
    }
    set.add(containerToPreempt);
  }

  private Map<ApplicationAttemptId,Set<RMContainer>> getContainersToPreempt(
      Set<String> leafQueueNames, Resource clusterResource) {

    Map<ApplicationAttemptId, Set<RMContainer>> preemptMap =
        new HashMap<ApplicationAttemptId, Set<RMContainer>>();
    List<RMContainer> skippedAMContainerlist = new ArrayList<RMContainer>();

    for (String queueName : leafQueueNames) {
      if (getQueueByPartition(queueName,
          RMNodeLabelsManager.NO_LABEL).preemptionDisabled) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("skipping from queue=" + queueName
              + " because it's a non-preemptable queue");
        }
        continue;
      }

      LeafQueue leafQueue = null;

      Map<String, Resource> resToObtainByPartition =
          new HashMap<String, Resource>();
      for (TempQueuePerPartition qT : getQueuePartitions(queueName)) {
        leafQueue = qT.leafQueue;
        if (Resources.greaterThan(rc, clusterResource, qT.current,
          Resource resToObtain =
              Resources.multiply(qT.toBePreempted, naturalTerminationFactor);
          if (Resources.greaterThan(rc, clusterResource, resToObtain,
              Resources.none())) {
            resToObtainByPartition.put(qT.partition, resToObtain);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Queue=" + queueName + " partition=" + qT.partition
                  + " resource-to-obtain=" + resToObtain);
            }
          }
          qT.actuallyPreempted = Resources.clone(resToObtain);
        } else {
          qT.actuallyPreempted = Resources.none();
        }
      }

      synchronized (leafQueue) {
        Map<String, TreeSet<RMContainer>> ignorePartitionExclusivityContainers =
            leafQueue.getIgnoreExclusivityRMContainers();
        for (String partition : resToObtainByPartition.keySet()) {
          if (ignorePartitionExclusivityContainers.containsKey(partition)) {
            TreeSet<RMContainer> rmContainers =
                ignorePartitionExclusivityContainers.get(partition);
            for (RMContainer c : rmContainers.descendingSet()) {
              boolean preempted =
                  tryPreemptContainerAndDeductResToObtain(
                      resToObtainByPartition, c, clusterResource, preemptMap);
              if (!preempted) {
                break;
              }
            }
          }
        }

        Resource skippedAMSize = Resource.newInstance(0, 0);
        Iterator<FiCaSchedulerApp> desc =
            leafQueue.getOrderingPolicy().getPreemptionIterator();
        while (desc.hasNext()) {
          FiCaSchedulerApp fc = desc.next();
          if (resToObtainByPartition.isEmpty()) {
            break;
          }

          preemptFrom(fc, clusterResource, resToObtainByPartition,
              skippedAMContainerlist, skippedAMSize, preemptMap);
        }

        Resource maxAMCapacityForThisQueue = Resources.multiply(
            Resources.multiply(clusterResource,
                leafQueue.getAbsoluteCapacity()),
            leafQueue.getMaxAMResourcePerQueuePercent());

        preemptAMContainers(clusterResource, preemptMap, skippedAMContainerlist,
            resToObtainByPartition, skippedAMSize, maxAMCapacityForThisQueue);
      }
    }

    return preemptMap;
  }

  private void preemptAMContainers(Resource clusterResource,
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      List<RMContainer> skippedAMContainerlist,
      Map<String, Resource> resToObtainByPartition, Resource skippedAMSize,
      Resource maxAMCapacityForThisQueue) {
    for (RMContainer c : skippedAMContainerlist) {
      if (resToObtainByPartition.isEmpty()) {
        break;
      }
          maxAMCapacityForThisQueue)) {
        break;
      }

      boolean preempted =
          tryPreemptContainerAndDeductResToObtain(resToObtainByPartition, c,
              clusterResource, preemptMap);
      if (preempted) {
        Resources.subtractFrom(skippedAMSize, c.getAllocatedResource());
      }
    }
    skippedAMContainerlist.clear();
  }
  private void preemptFrom(FiCaSchedulerApp app,
      Resource clusterResource, Map<String, Resource> resToObtainByPartition,
      List<RMContainer> skippedAMContainerlist, Resource skippedAMSize,
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap) {
    ApplicationAttemptId appId = app.getApplicationAttemptId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Looking at application=" + app.getApplicationAttemptId()
          + " resourceToObtain=" + resToObtainByPartition);
    }

    List<RMContainer> reservedContainers =
        new ArrayList<RMContainer>(app.getReservedContainers());
    for (RMContainer c : reservedContainers) {
      if (resToObtainByPartition.isEmpty()) {
        return;
      }

      tryPreemptContainerAndDeductResToObtain(resToObtainByPartition, c,
          clusterResource, preemptMap);

      if (!observeOnly) {
        dispatcher.handle(new ContainerPreemptEvent(appId, c,
            ContainerPreemptEventType.DROP_RESERVATION));
      }
    }

    List<RMContainer> liveContainers =
      new ArrayList<RMContainer>(app.getLiveContainers());

    sortContainers(liveContainers);

    for (RMContainer c : liveContainers) {
      if (resToObtainByPartition.isEmpty()) {
        return;
      }

      if (c.isAMContainer()) {
        skippedAMContainerlist.add(c);
        Resources.addTo(skippedAMSize, c.getAllocatedResource());
        continue;
      }

      tryPreemptContainerAndDeductResToObtain(resToObtainByPartition, c,
          clusterResource, preemptMap);
    }
  }

  private TempQueuePerPartition cloneQueues(CSQueue curQueue,
      Resource partitionResource, String partitionToLookAt) {
    TempQueuePerPartition ret;
    synchronized (curQueue) {
      String queueName = curQueue.getQueueName();
      QueueCapacities qc = curQueue.getQueueCapacities();
      float absUsed = qc.getAbsoluteUsedCapacity(partitionToLookAt);
      float absCap = qc.getAbsoluteCapacity(partitionToLookAt);
      float absMaxCap = qc.getAbsoluteMaximumCapacity(partitionToLookAt);
      boolean preemptionDisabled = curQueue.getPreemptionDisabled();

      Resource current = Resources.multiply(partitionResource, absUsed);
      Resource guaranteed = Resources.multiply(partitionResource, absCap);
      Resource maxCapacity = Resources.multiply(partitionResource, absMaxCap);

      try {
        if (!scheduler.getRMContext().getNodeLabelManager()
            .isExclusiveNodeLabel(partitionToLookAt)) {
          maxCapacity =
              Resources.max(rc, partitionResource, maxCapacity, current);
        }
      } catch (IOException e) {
      }

      Resource extra = Resource.newInstance(0, 0);
      if (Resources.greaterThan(rc, partitionResource, current, guaranteed)) {
        extra = Resources.subtract(current, guaranteed);
      }
      if (curQueue instanceof LeafQueue) {
        LeafQueue l = (LeafQueue) curQueue;
        Resource pending =
            l.getQueueResourceUsage().getPending(partitionToLookAt);
        ret = new TempQueuePerPartition(queueName, current, pending, guaranteed,
            maxCapacity, preemptionDisabled, partitionToLookAt);
        if (preemptionDisabled) {
          ret.untouchableExtra = extra;
        } else {
        ret.setLeafQueue(l);
      } else {
        Resource pending = Resource.newInstance(0, 0);
        ret =
            new TempQueuePerPartition(curQueue.getQueueName(), current, pending,
                guaranteed, maxCapacity, false, partitionToLookAt);
        Resource childrensPreemptable = Resource.newInstance(0, 0);
        for (CSQueue c : curQueue.getChildQueues()) {
          TempQueuePerPartition subq =
              cloneQueues(c, partitionResource, partitionToLookAt);
          Resources.addTo(childrensPreemptable, subq.preemptableExtra);
          ret.addChild(subq);
        }
        if (Resources.greaterThanOrEqual(
              rc, partitionResource, childrensPreemptable, extra)) {
          ret.untouchableExtra = Resource.newInstance(0, 0);
        } else {
          ret.untouchableExtra =
        }
      }
    }
    addTempQueuePartition(ret);
    return ret;
  }

  private void logToCSV(List<String> leafQueueNames){
    Collections.sort(leafQueueNames);
    String queueState = " QUEUESTATE: " + clock.getTime();
    StringBuilder sb = new StringBuilder();
    sb.append(queueState);

    for (String queueName : leafQueueNames) {
      TempQueuePerPartition tq =
          getQueueByPartition(queueName, RMNodeLabelsManager.NO_LABEL);
      sb.append(", ");
      tq.appendLogString(sb);
    }
    LOG.debug(sb.toString());
  }

  private void addTempQueuePartition(TempQueuePerPartition queuePartition) {
    String queueName = queuePartition.queueName;

    Map<String, TempQueuePerPartition> queuePartitions;
    if (null == (queuePartitions = queueToPartitions.get(queueName))) {
      queuePartitions = new HashMap<String, TempQueuePerPartition>();
      queueToPartitions.put(queueName, queuePartitions);
    }
    queuePartitions.put(queuePartition.partition, queuePartition);
  }

  private TempQueuePerPartition getQueueByPartition(String queueName,
      String partition) {
    Map<String, TempQueuePerPartition> partitionToQueues = null;
    if (null == (partitionToQueues = queueToPartitions.get(queueName))) {
      return null;
    }
    return partitionToQueues.get(partition);
  }

  private Collection<TempQueuePerPartition> getQueuePartitions(String queueName) {
    if (!queueToPartitions.containsKey(queueName)) {
      return null;
    }
    return queueToPartitions.get(queueName).values();
  }

  static class TempQueuePerPartition {
    final String queueName;
    final Resource current;
    final Resource pending;
    final Resource guaranteed;
    final Resource maxCapacity;
    final String partition;
    Resource idealAssigned;
    Resource toBePreempted;
    Resource actuallyPreempted;
    Resource untouchableExtra;
    Resource preemptableExtra;

    double normalizedGuarantee;

    final ArrayList<TempQueuePerPartition> children;
    LeafQueue leafQueue;
    boolean preemptionDisabled;

    TempQueuePerPartition(String queueName, Resource current, Resource pending,
        Resource guaranteed, Resource maxCapacity, boolean preemptionDisabled,
        String partition) {
      this.queueName = queueName;
      this.current = current;
      this.pending = pending;
      this.actuallyPreempted = Resource.newInstance(0, 0);
      this.toBePreempted = Resource.newInstance(0, 0);
      this.normalizedGuarantee = Float.NaN;
      this.children = new ArrayList<TempQueuePerPartition>();
      this.untouchableExtra = Resource.newInstance(0, 0);
      this.preemptableExtra = Resource.newInstance(0, 0);
      this.preemptionDisabled = preemptionDisabled;
      this.partition = partition;
    }

    public void setLeafQueue(LeafQueue l){
    public void addChild(TempQueuePerPartition q) {
      assert leafQueue == null;
      children.add(q);
      Resources.addTo(pending, q.pending);
    }

    public void addChildren(ArrayList<TempQueuePerPartition> queues) {
      assert leafQueue == null;
      children.addAll(queues);
    }


    public ArrayList<TempQueuePerPartition> getChildren(){
      return children;
    }


    public void printAll() {
      LOG.info(this.toString());
      for (TempQueuePerPartition sub : this.getChildren()) {
        sub.printAll();
      }
    }

  }

  static class TQComparator implements Comparator<TempQueuePerPartition> {
    private ResourceCalculator rc;
    private Resource clusterRes;

    }

    @Override
    public int compare(TempQueuePerPartition tq1, TempQueuePerPartition tq2) {
      if (getIdealPctOfGuaranteed(tq1) < getIdealPctOfGuaranteed(tq2)) {
        return -1;
      }
    private double getIdealPctOfGuaranteed(TempQueuePerPartition q) {
      double pctOver = Integer.MAX_VALUE;
      if (q != null && Resources.greaterThan(
          rc, clusterRes, q.guaranteed, Resources.none())) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmcontainer/RMContainerImpl.java
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

@SuppressWarnings({"unchecked", "rawtypes"})
public class RMContainerImpl implements RMContainer, Comparable<RMContainer> {

  private static final Log LOG = LogFactory.getLog(RMContainerImpl.class);

    }
    return nodeLabelExpression;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RMContainer) {
      if (null != getContainerId()) {
        return getContainerId().equals(((RMContainer) obj).getContainerId());
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    if (null != getContainerId()) {
      return getContainerId().hashCode();
    }
    return super.hashCode();
  }

  @Override
  public int compareTo(RMContainer o) {
    if (containerId != null && o.getContainerId() != null) {
      return containerId.compareTo(o.getContainerId());
    }
    return -1;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacityScheduler.java
  static final PartitionedQueueComparator partitionedQueueComparator =
      new PartitionedQueueComparator();

  public static final Comparator<FiCaSchedulerApp> applicationComparator =
    new Comparator<FiCaSchedulerApp>() {
    @Override
    public int compare(FiCaSchedulerApp a1, FiCaSchedulerApp a2) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/LeafQueue.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.server.utils.Lock.NoLock;
  private OrderingPolicy<FiCaSchedulerApp> 
    orderingPolicy = new FifoOrderingPolicy<FiCaSchedulerApp>();

  private Map<String, TreeSet<RMContainer>> ignorePartitionExclusivityRMContainers =
      new HashMap<>();
  
  public LeafQueue(CapacitySchedulerContext cs, 
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    super(cs, queueName, parent, old);
          Resource assigned = assignment.getResource();
          if (Resources.greaterThan(
              resourceCalculator, clusterResource, assigned, Resources.none())) {
            RMContainer reservedOrAllocatedRMContainer =
                application.getRMContainer(assignment
                    .getAssignmentInformation()
                    .getFirstAllocatedOrReservedContainerId());

            allocateResource(clusterResource, application, assigned,
                node.getPartition(), reservedOrAllocatedRMContainer);
            
          orderingPolicy.containerReleased(application, rmContainer);
          
          releaseResource(clusterResource, application,
              container.getResource(), node.getPartition(), rmContainer);
          LOG.info("completedContainer" +
              " container=" + container +
              " queue=" + this +

  synchronized void allocateResource(Resource clusterResource,
      SchedulerApplicationAttempt application, Resource resource,
      String nodePartition, RMContainer rmContainer) {
    super.allocateResource(clusterResource, resource, nodePartition);
    
    if (null != rmContainer && rmContainer.getNodeLabelExpression().equals(
        RMNodeLabelsManager.NO_LABEL)
        && !nodePartition.equals(RMNodeLabelsManager.NO_LABEL)) {
      TreeSet<RMContainer> rmContainers = null;
      if (null == (rmContainers =
          ignorePartitionExclusivityRMContainers.get(nodePartition))) {
        rmContainers = new TreeSet<>();
        ignorePartitionExclusivityRMContainers.put(nodePartition, rmContainers);
      }
      rmContainers.add(rmContainer);
    }

    String userName = application.getUser();
    User user = getUser(userName);
  }

  synchronized void releaseResource(Resource clusterResource,
      FiCaSchedulerApp application, Resource resource, String nodePartition,
      RMContainer rmContainer) {
    super.releaseResource(clusterResource, resource, nodePartition);
    
    if (null != rmContainer && rmContainer.getNodeLabelExpression().equals(
        RMNodeLabelsManager.NO_LABEL)
        && !nodePartition.equals(RMNodeLabelsManager.NO_LABEL)) {
      if (ignorePartitionExclusivityRMContainers.containsKey(nodePartition)) {
        Set<RMContainer> rmContainers =
            ignorePartitionExclusivityRMContainers.get(nodePartition);
        rmContainers.remove(rmContainer);
        if (rmContainers.isEmpty()) {
          ignorePartitionExclusivityRMContainers.remove(nodePartition);
        }
      }
    }

    String userName = application.getUser();
    User user = getUser(userName);
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      allocateResource(clusterResource, attempt, rmContainer.getContainer()
          .getResource(), node.getPartition(), rmContainer);
    }
    getParent().recoverContainer(clusterResource, attempt, rmContainer);
  }
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      allocateResource(clusterResource, application, rmContainer.getContainer()
          .getResource(), node.getPartition(), rmContainer);
      LOG.info("movedContainer" + " container=" + rmContainer.getContainer()
          + " resource=" + rmContainer.getContainer().getResource()
          + " queueMoveIn=" + this + " usedCapacity=" + getUsedCapacity()
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      releaseResource(clusterResource, application, rmContainer.getContainer()
          .getResource(), node.getPartition(), rmContainer);
      LOG.info("movedContainer" + " container=" + rmContainer.getContainer()
          + " resource=" + rmContainer.getContainer().getResource()
          + " queueMoveOut=" + this + " usedCapacity=" + getUsedCapacity()
    }
  }
  
  public synchronized Map<String, TreeSet<RMContainer>>
      getIgnoreExclusivityRMContainers() {
    return ignorePartitionExclusivityRMContainers;
  }

  public void setCapacity(float capacity) {
    queueCapacities.setCapacity(capacity);
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/common/AssignmentInformation.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AssignmentInformation {
  public List<AssignmentDetails> getReservationDetails() {
    return operationDetails.get(Operation.RESERVATION);
  }

  private ContainerId getFirstContainerIdFromOperation(Operation op) {
    if (null != operationDetails.get(Operation.ALLOCATION)) {
      List<AssignmentDetails> assignDetails =
          operationDetails.get(Operation.ALLOCATION);
      if (!assignDetails.isEmpty()) {
        return assignDetails.get(0).containerId;
      }
    }
    return null;
  }

  public ContainerId getFirstAllocatedOrReservedContainerId() {
    ContainerId containerId = null;
    containerId = getFirstContainerIdFromOperation(Operation.ALLOCATION);
    if (null != containerId) {
      return containerId;
    }
    return getFirstContainerIdFromOperation(Operation.RESERVATION);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/capacity/TestProportionalCapacityPreemptionPolicy.java
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.WAIT_TIME_BEFORE_KILL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEventType.KILL_CONTAINER;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEventType.PREEMPT_CONTAINER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestProportionalCapacityPreemptionPolicy {

    setAMContainer = false;
  }

  @Test
  public void testPreemptSkippedAMContainers() {
    int[][] qData = new int[][] {
    clusterResources =
      Resource.newInstance(leafAbsCapacities(qData[0], qData[7]), 0);
    when(mCS.getClusterResource()).thenReturn(clusterResources);
    when(lm.getResourceByLabel(anyString(), any(Resource.class))).thenReturn(
        clusterResources);

    SchedulerNode mNode = mock(SchedulerNode.class);
    when(mNode.getPartition()).thenReturn(RMNodeLabelsManager.NO_LABEL);
    when(mCS.getSchedulerNode(any(NodeId.class))).thenReturn(mNode);
    return policy;
  }

    float tot = leafAbsCapacities(abs, queues);
    Deque<ParentQueue> pqs = new LinkedList<ParentQueue>();
    ParentQueue root = mockParentQueue(null, queues[0], pqs);
    when(root.getQueueName()).thenReturn(CapacitySchedulerConfiguration.ROOT);
    when(root.getAbsoluteUsedCapacity()).thenReturn(used[0] / tot);
    when(root.getAbsoluteCapacity()).thenReturn(abs[0] / tot);
    when(root.getAbsoluteMaximumCapacity()).thenReturn(maxCap[0] / tot);
    QueueCapacities rootQc = new QueueCapacities(true);
    rootQc.setAbsoluteUsedCapacity(used[0] / tot);
    rootQc.setAbsoluteCapacity(abs[0] / tot);
    rootQc.setAbsoluteMaximumCapacity(maxCap[0] / tot);
    when(root.getQueueCapacities()).thenReturn(rootQc);
    when(root.getQueuePath()).thenReturn(CapacitySchedulerConfiguration.ROOT);
    boolean preemptionDisabled = mockPreemptionStatus("root");
    when(root.getPreemptionDisabled()).thenReturn(preemptionDisabled);

      when(q.getAbsoluteUsedCapacity()).thenReturn(used[i] / tot);
      when(q.getAbsoluteCapacity()).thenReturn(abs[i] / tot);
      when(q.getAbsoluteMaximumCapacity()).thenReturn(maxCap[i] / tot);

      QueueCapacities qc = new QueueCapacities(false);
      qc.setAbsoluteUsedCapacity(used[i] / tot);
      qc.setAbsoluteCapacity(abs[i] / tot);
      qc.setAbsoluteMaximumCapacity(maxCap[i] / tot);
      when(q.getQueueCapacities()).thenReturn(qc);

      String parentPathName = p.getQueuePath();
      parentPathName = (parentPathName == null) ? "root" : parentPathName;
      String queuePathName = (parentPathName+"."+queueName).replace("/","root");
    return pq;
  }

  @SuppressWarnings("rawtypes")
  LeafQueue mockLeafQueue(ParentQueue p, float tot, int i, int[] abs, 
      int[] used, int[] pending, int[] reserved, int[] apps, int[] gran) {
    LeafQueue lq = mock(LeafQueue.class);
        new ArrayList<ApplicationAttemptId>();
    when(lq.getTotalResourcePending()).thenReturn(
        Resource.newInstance(pending[i], 0));
    ResourceUsage ru = new ResourceUsage();
    ru.setPending(Resource.newInstance(pending[i], 0));
    when(lq.getQueueResourceUsage()).thenReturn(ru);
    final NavigableSet<FiCaSchedulerApp> qApps = new TreeSet<FiCaSchedulerApp>(
      new Comparator<FiCaSchedulerApp>() {
    when(mC.getContainerId()).thenReturn(cId);
    when(mC.getContainer()).thenReturn(c);
    when(mC.getApplicationAttemptId()).thenReturn(appAttId);
    when(mC.getAllocatedResource()).thenReturn(r);
    if (priority.AMCONTAINER.getValue() == cpriority) {
      when(mC.isAMContainer()).thenReturn(true);
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/capacity/TestProportionalCapacityPreemptionPolicyForNodePartitions.java
hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestChildQueueOrder.java
        } else {
          FiCaSchedulerApp app1 = getMockApplication(0, "");
          ((LeafQueue)queue).allocateResource(clusterResource, app1, 
              allocatedResource, null, null);
        }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestLeafQueue.java
    qb.finishApplication(app_0.getApplicationId(), user_0);
    qb.finishApplication(app_2.getApplicationId(), user_1);
    qb.releaseResource(clusterResource, app_0, app_0.getResource(u0Priority),
        null, null);
    qb.releaseResource(clusterResource, app_2, app_2.getResource(u1Priority),
        null, null);

    qb.setUserLimit(50);
    qb.setUserLimitFactor(1);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestNodeLabelContainerAllocation.java
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.junit.Assert;
import org.junit.Before;
    Assert.assertEquals(10, schedulerNode1.getNumContainers());

    LeafQueue leafQueue = (LeafQueue) cs.getQueue("a");
    Assert.assertFalse(leafQueue.getIgnoreExclusivityRMContainers().containsKey(
        "y"));
    Assert.assertEquals(10,
        leafQueue.getIgnoreExclusivityRMContainers().get("x").size());

    cs.handle(new AppAttemptRemovedSchedulerEvent(
        am1.getApplicationAttemptId(), RMAppAttemptState.FINISHED, false));
    Assert.assertFalse(leafQueue.getIgnoreExclusivityRMContainers().containsKey(
        "x"));

    rm1.close();
  }
  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestParentQueue.java
        } else {
          FiCaSchedulerApp app1 = getMockApplication(0, "");
          ((LeafQueue)queue).allocateResource(clusterResource, app1, 
              allocatedResource, null, null);
        }
        

