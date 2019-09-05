hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/nodelabels/RMNodeLabelsManager.java
    }
  }

  public void updateNodeResource(NodeId node, Resource newResource) {
    deactivateNode(node);
    activateNode(node, newResource);
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/AbstractYarnScheduler.java
    Resource newResource = resourceOption.getResource();
    Resource oldResource = node.getTotalResource();
    if(!oldResource.equals(newResource)) {
      rmContext.getNodeLabelManager().updateNodeResource(nm.getNodeID(),
          newResource);
      
      LOG.info("Update resource on node: " + node.getNodeName()
          + " from: " + oldResource + ", to: "

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/ResourceUsage.java
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
      readLock.unlock();
    }
  }
  
  public Set<String> getNodePartitionsSet() {
    try {
      readLock.lock();
      return usages.keySet();
    } finally {
      readLock.unlock();
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/AbstractCSQueue.java
    this.acls = csContext.getConfiguration().getAcls(getQueuePath());

    CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
        minimumAllocation, this, labelManager, null);
    
    queueUsage.incUsed(nodePartition, resource);

    ++numContainers;
    CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
        minimumAllocation, this, labelManager, nodePartition);
  }
  
  protected synchronized void releaseResource(Resource clusterResource,
      Resource resource, String nodePartition) {
    queueUsage.decUsed(nodePartition, resource);

    CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
        minimumAllocation, this, labelManager, nodePartition);
    --numContainers;
  }
  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CSQueueUtils.java
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.Sets;

class CSQueueUtils {

  final static float EPSILON = 0.0001f;
  
    }
  }
  
  private static void updateUsedCapacity(final ResourceCalculator rc,
      final Resource totalPartitionResource, final Resource minimumAllocation,
      ResourceUsage queueResourceUsage, QueueCapacities queueCapacities,
      String nodePartition) {
    float absoluteUsedCapacity = 0.0f;
    float usedCapacity = 0.0f;

    if (Resources.greaterThan(rc, totalPartitionResource,
        totalPartitionResource, Resources.none())) {
      Resource queueGuranteedResource =
          Resources.multiply(totalPartitionResource,
              queueCapacities.getAbsoluteCapacity(nodePartition));

      queueGuranteedResource =
          Resources.max(rc, totalPartitionResource, queueGuranteedResource,
              minimumAllocation);

      Resource usedResource = queueResourceUsage.getUsed(nodePartition);
      absoluteUsedCapacity =
          Resources.divide(rc, totalPartitionResource, usedResource,
              totalPartitionResource);
      usedCapacity =
          Resources.divide(rc, totalPartitionResource, usedResource,
              queueGuranteedResource);
    }

    queueCapacities
        .setAbsoluteUsedCapacity(nodePartition, absoluteUsedCapacity);
    queueCapacities.setUsedCapacity(nodePartition, usedCapacity);
  }
  
  private static Resource getNonPartitionedMaxAvailableResourceToQueue(
      final ResourceCalculator rc, Resource totalNonPartitionedResource,
      CSQueue queue) {
    Resource queueLimit = Resources.none();
    Resource usedResources = queue.getUsedResources();

    if (Resources.greaterThan(rc, totalNonPartitionedResource,
        totalNonPartitionedResource, Resources.none())) {
      queueLimit =
          Resources.multiply(totalNonPartitionedResource,
              queue.getAbsoluteCapacity());
    }

    Resource available = Resources.subtract(queueLimit, usedResources);
    return Resources.max(rc, totalNonPartitionedResource, available,
        Resources.none());
  }
  
  @Lock(CSQueue.class)
  public static void updateQueueStatistics(
      final ResourceCalculator rc, final Resource cluster, final Resource minimumAllocation,
      final CSQueue childQueue, final RMNodeLabelsManager nlm, 
      final String nodePartition) {
    QueueCapacities queueCapacities = childQueue.getQueueCapacities();
    ResourceUsage queueResourceUsage = childQueue.getQueueResourceUsage();
    
    if (nodePartition == null) {
      for (String partition : Sets.union(
          queueCapacities.getNodePartitionsSet(),
          queueResourceUsage.getNodePartitionsSet())) {
        updateUsedCapacity(rc, nlm.getResourceByLabel(partition, cluster),
            minimumAllocation, queueResourceUsage, queueCapacities, partition);
      }
    } else {
      updateUsedCapacity(rc, nlm.getResourceByLabel(nodePartition, cluster),
          minimumAllocation, queueResourceUsage, queueCapacities, nodePartition);
    }
    
    if (nodePartition == null
        || nodePartition.equals(RMNodeLabelsManager.NO_LABEL)) {
      childQueue.getMetrics().setAvailableResourcesToQueue(
          getNonPartitionedMaxAvailableResourceToQueue(rc,
              nlm.getResourceByLabel(RMNodeLabelsManager.NO_LABEL, cluster),
              childQueue));
    }
   }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacityScheduler.java
  protected final long THREAD_JOIN_TIMEOUT_MS = 1000;

  static final Comparator<CSQueue> nonPartitionedQueueComparator =
      new Comparator<CSQueue>() {
    @Override
    public int compare(CSQueue q1, CSQueue q2) {
      if (q1.getUsedCapacity() < q2.getUsedCapacity()) {
    }
  };
  
  static final PartitionedQueueComparator partitionedQueueComparator =
      new PartitionedQueueComparator();

  static final Comparator<FiCaSchedulerApp> applicationComparator = 
    new Comparator<FiCaSchedulerApp>() {
    @Override
  }

  @Override
  public Comparator<CSQueue> getNonPartitionedQueueComparator() {
    return nonPartitionedQueueComparator;
  }
  
  @Override
  public PartitionedQueueComparator getPartitionedQueueComparator() {
    return partitionedQueueComparator;
  }

  @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacitySchedulerContext.java

  ResourceCalculator getResourceCalculator();

  Comparator<CSQueue> getNonPartitionedQueueComparator();
  
  PartitionedQueueComparator getPartitionedQueueComparator();
  
  FiCaSchedulerNode getNode(NodeId nodeId);
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/LeafQueue.java
    setQueueResourceLimitsInfo(clusterResource);
    
    CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
        minimumAllocation, this, labelManager, null);


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/ParentQueue.java
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

  protected final Set<CSQueue> childQueues;  
  private final boolean rootQueue;
  final Comparator<CSQueue> nonPartitionedQueueComparator;
  final PartitionedQueueComparator partitionQueueComparator;
  volatile int numApplications;
  private final CapacitySchedulerContext scheduler;

      String queueName, CSQueue parent, CSQueue old) throws IOException {
    super(cs, queueName, parent, old);
    this.scheduler = cs;
    this.nonPartitionedQueueComparator = cs.getNonPartitionedQueueComparator();
    this.partitionQueueComparator = cs.getPartitionedQueueComparator();

    this.rootQueue = (parent == null);

          ". Must be " + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE);
    }
    
    this.childQueues = new TreeSet<CSQueue>(nonPartitionedQueueComparator);
    
    setupQueueConfigs(cs.getClusterResource());

    return new ResourceLimits(childLimit);
  }
  
  private Iterator<CSQueue> sortAndGetChildrenAllocationIterator(FiCaSchedulerNode node) {
    if (node.getPartition().equals(RMNodeLabelsManager.NO_LABEL)) {
      return childQueues.iterator();
    }

    partitionQueueComparator.setPartitionToLookAt(node.getPartition());
    List<CSQueue> childrenList = new ArrayList<>(childQueues);
    Collections.sort(childrenList, partitionQueueComparator);
    return childrenList.iterator();
  }
  
  private synchronized CSAssignment assignContainersToChildQueues(
      Resource cluster, FiCaSchedulerNode node, ResourceLimits limits,
      SchedulingMode schedulingMode) {
    printChildQueues();

    for (Iterator<CSQueue> iter = sortAndGetChildrenAllocationIterator(node); iter
        .hasNext();) {
      CSQueue childQueue = iter.next();
      if(LOG.isDebugEnabled()) {
        LOG.debug("Trying to assign to queue: " + childQueue.getQueuePath()
      if (Resources.greaterThan(
              resourceCalculator, cluster, 
              assignment.getResource(), Resources.none())) {
        if (RMNodeLabelsManager.NO_LABEL.equals(node.getPartition())) {
          iter.remove();
          LOG.info("Re-sorting assigned queue: " + childQueue.getQueuePath()
              + " stats: " + childQueue);
          childQueues.add(childQueue);
          if (LOG.isDebugEnabled()) {
            printChildQueues();
          }
        }
        break;
      }
    }
      childQueue.updateClusterResource(clusterResource, childLimits);
    }
    
    CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
        minimumAllocation, this, labelManager, null);
  }
  
  @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/PartitionedQueueComparator.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/PartitionedQueueComparator.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Comparator;

public class PartitionedQueueComparator implements Comparator<CSQueue> {
  private String partitionToLookAt = null;
  
  public void setPartitionToLookAt(String partitionToLookAt) {
    this.partitionToLookAt = partitionToLookAt;
  }
  

  @Override
  public int compare(CSQueue q1, CSQueue q2) {
    boolean q1Accessible =
        q1.getAccessibleNodeLabels().contains(partitionToLookAt);
    boolean q2Accessible =
        q2.getAccessibleNodeLabels().contains(partitionToLookAt);
    if (q1Accessible && !q2Accessible) {
      return -1;
    } else if (!q1Accessible && q2Accessible) {
      return 1;
    }
    
    float used1 = q1.getQueueCapacities().getUsedCapacity(partitionToLookAt);
    float used2 = q2.getQueueCapacities().getUsedCapacity(partitionToLookAt);
    if (Math.abs(used1 - used2) < 1e-6) {
      float cap1 = q1.getQueueCapacities().getCapacity(partitionToLookAt);
      float cap2 = q2.getQueueCapacities().getCapacity(partitionToLookAt);
      
      if (Math.abs(cap1 - cap2) < 1e-6) {
        return q1.getQueueName().compareTo(q2.getQueueName());
      }
      return Float.compare(cap2, cap1);
    }
    
    return Float.compare(used1, used2);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/QueueCapacities.java
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;

public class QueueCapacities {
  private static final String NL = CommonNodeLabelsManager.NO_LABEL;
  private static final float LABEL_DOESNT_EXIST_CAP = 0f;
      readLock.unlock();
    }
  }
  
  public Set<String> getNodePartitionsSet() {
    try {
      readLock.lock();
      return capacitiesMap.keySet();
    } finally {
      readLock.unlock();
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/ReservationQueue.java
          + " from " + newlyParsedQueue.getQueuePath());
    }
    super.reinitialize(newlyParsedQueue, clusterResource);
    CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
        minimumAllocation, this, labelManager, null);

    updateQuotas(parent.getUserLimitForReservation(),
        parent.getUserLimitFactor(),
        parent.getMaxApplicationsForReservations(),

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestApplicationLimits.java
        thenReturn(Resources.createResource(10 * 16 * GB, 10 * 32));
    when(csContext.getApplicationComparator()).
        thenReturn(CapacityScheduler.applicationComparator);
    when(csContext.getNonPartitionedQueueComparator()).
        thenReturn(CapacityScheduler.nonPartitionedQueueComparator);
    when(csContext.getResourceCalculator()).
        thenReturn(resourceCalculator);
    when(csContext.getRMContext()).thenReturn(rmContext);
        thenReturn(Resources.createResource(16*GB, 16));
    when(csContext.getApplicationComparator()).
        thenReturn(CapacityScheduler.applicationComparator);
    when(csContext.getNonPartitionedQueueComparator()).
        thenReturn(CapacityScheduler.nonPartitionedQueueComparator);
    when(csContext.getResourceCalculator()).thenReturn(resourceCalculator);
    when(csContext.getRMContext()).thenReturn(rmContext);
    
        thenReturn(Resources.createResource(16*GB));
    when(csContext.getApplicationComparator()).
        thenReturn(CapacityScheduler.applicationComparator);
    when(csContext.getNonPartitionedQueueComparator()).
        thenReturn(CapacityScheduler.nonPartitionedQueueComparator);
    when(csContext.getResourceCalculator()).thenReturn(resourceCalculator);
    when(csContext.getRMContext()).thenReturn(rmContext);
    

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestChildQueueOrder.java
    thenReturn(Resources.createResource(100 * 16 * GB, 100 * 32));
    when(csContext.getApplicationComparator()).
    thenReturn(CapacityScheduler.applicationComparator);
    when(csContext.getNonPartitionedQueueComparator()).
    thenReturn(CapacityScheduler.nonPartitionedQueueComparator);
    when(csContext.getResourceCalculator()).
    thenReturn(resourceComparator);
    when(csContext.getRMContext()).thenReturn(rmContext);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestLeafQueue.java
        thenReturn(Resources.createResource(100 * 16 * GB, 100 * 32));
    when(csContext.getApplicationComparator()).
    thenReturn(CapacityScheduler.applicationComparator);
    when(csContext.getNonPartitionedQueueComparator()).
        thenReturn(CapacityScheduler.nonPartitionedQueueComparator);
    when(csContext.getResourceCalculator()).
        thenReturn(resourceCalculator);
    when(csContext.getRMContext()).thenReturn(rmContext);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestNodeLabelContainerAllocation.java

    rm1.close();
  }
  
  private void checkQueueUsedCapacity(String queueName, CapacityScheduler cs,
      String nodePartition, float usedCapacity, float absoluteUsedCapacity) {
    float epsilon = 1e-6f;
    CSQueue queue = cs.getQueue(queueName);
    Assert.assertNotNull("Failed to get queue=" + queueName, queue);

    Assert.assertEquals(usedCapacity, queue.getQueueCapacities()
        .getUsedCapacity(nodePartition), epsilon);
    Assert.assertEquals(absoluteUsedCapacity, queue.getQueueCapacities()
        .getAbsoluteUsedCapacity(nodePartition), epsilon);
  }
  
  private void doNMHeartbeat(MockRM rm, NodeId nodeId, int nHeartbeat) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    RMNode rmNode1 = rm.getRMContext().getRMNodes().get(nodeId);
    for (int i = 0; i < nHeartbeat; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }
  }
  
  private void waitSchedulerNodeJoined(MockRM rm, int expectedNodeNum)
      throws InterruptedException {
    int totalWaitTick = 100; // wait 10 sec at most.
    while (expectedNodeNum > rm.getResourceScheduler().getNumClusterNodes()
        && totalWaitTick > 0) {
      Thread.sleep(100);
      totalWaitTick--;
    }
  }
  
  @Test
  public void testQueueUsedCapacitiesUpdate()
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
    
    csConf.setQueues(A, new String[] { "a1", "a2" });
    
    final String A1 = A + ".a1";
    csConf.setCapacity(A1, 50);
    csConf.setAccessibleNodeLabels(A1, toSet("x"));
    csConf.setCapacityByLabel(A1, "x", 50);
    
    final String A2 = A + ".a2";
    csConf.setCapacity(A2, 50);
    csConf.setAccessibleNodeLabels(A2, toSet("x"));
    csConf.setCapacityByLabel(A2, "x", 50);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    csConf.setCapacity(B, 50);
    csConf.setAccessibleNodeLabels(B, toSet("x"));
    csConf.setCapacityByLabel(B, "x", 50);

    mgr.addToCluserNodeLabels(ImmutableSet.of("x"));
    mgr.updateNodeLabels(Arrays.asList(NodeLabel.newInstance("x", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    MockRM rm = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();
    
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    checkQueueUsedCapacity("a", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a", cs, "", 0f, 0f);
    checkQueueUsedCapacity("a1", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a1", cs, "", 0f, 0f);
    checkQueueUsedCapacity("a2", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a2", cs, "", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "", 0f, 0f);
    
    MockNM nm1 = rm.registerNode("h1:1234", 10 * GB); // label = x
    MockNM nm2 = rm.registerNode("h2:1234", 10 * GB); // label = <empty>
    
    checkQueueUsedCapacity("a", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a", cs, "", 0f, 0f);
    checkQueueUsedCapacity("a1", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a1", cs, "", 0f, 0f);
    checkQueueUsedCapacity("a2", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a2", cs, "", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "", 0f, 0f);

    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);

    am1.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>());
    
    doNMHeartbeat(rm, nm2.getNodeId(), 10);
    
    checkQueueUsedCapacity("a", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a", cs, "", 0.4f, 0.2f);
    checkQueueUsedCapacity("a1", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a1", cs, "", 0.8f, 0.2f);
    checkQueueUsedCapacity("a2", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a2", cs, "", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "", 0.2f, 0.2f);
    
    am1.allocate("*", 1 * GB, 2, new ArrayList<ContainerId>(), "x");
    doNMHeartbeat(rm, nm1.getNodeId(), 10);
    
    checkQueueUsedCapacity("a", cs, "x", 0.4f, 0.2f);
    checkQueueUsedCapacity("a", cs, "", 0.4f, 0.2f);
    checkQueueUsedCapacity("a1", cs, "x", 0.8f, 0.2f);
    checkQueueUsedCapacity("a1", cs, "", 0.8f, 0.2f);
    checkQueueUsedCapacity("a2", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a2", cs, "", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "x", 0.2f, 0.2f);
    checkQueueUsedCapacity("root", cs, "", 0.2f, 0.2f);
    
    RMApp app2 = rm.submitApp(1 * GB, "app", "user", null, "a2");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);

    am2.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>(), "x");
    doNMHeartbeat(rm, nm1.getNodeId(), 10);
    
    checkQueueUsedCapacity("a", cs, "x", 0.6f, 0.3f);
    checkQueueUsedCapacity("a", cs, "", 0.6f, 0.3f);
    checkQueueUsedCapacity("a1", cs, "x", 0.8f, 0.2f);
    checkQueueUsedCapacity("a1", cs, "", 0.8f, 0.2f);
    checkQueueUsedCapacity("a2", cs, "x", 0.4f, 0.1f);
    checkQueueUsedCapacity("a2", cs, "", 0.4f, 0.1f);
    checkQueueUsedCapacity("b", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "x", 0.3f, 0.3f);
    checkQueueUsedCapacity("root", cs, "", 0.3f, 0.3f);
    
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h3", 0), toSet("x")));
    rm.registerNode("h3:1234", 10 * GB); // label = x
    rm.registerNode("h4:1234", 10 * GB); // label = <empty>
    
    waitSchedulerNodeJoined(rm, 4);
    
    checkQueueUsedCapacity("a", cs, "x", 0.3f, 0.15f);
    checkQueueUsedCapacity("a", cs, "", 0.3f, 0.15f);
    checkQueueUsedCapacity("a1", cs, "x", 0.4f, 0.1f);
    checkQueueUsedCapacity("a1", cs, "", 0.4f, 0.1f);
    checkQueueUsedCapacity("a2", cs, "x", 0.2f, 0.05f);
    checkQueueUsedCapacity("a2", cs, "", 0.2f, 0.05f);
    checkQueueUsedCapacity("b", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "x", 0.15f, 0.15f);
    checkQueueUsedCapacity("root", cs, "", 0.15f, 0.15f);
    
    csConf.setCapacity(A, 100); // was 50
    csConf.setCapacityByLabel(A, "x", 100); // was 50
    csConf.setCapacity(B, 0); // was 50
    csConf.setCapacityByLabel(B, "x", 0); // was 50
    cs.reinitialize(csConf, rm.getRMContext());
    
    checkQueueUsedCapacity("a", cs, "x", 0.15f, 0.15f);
    checkQueueUsedCapacity("a", cs, "", 0.15f, 0.15f);
    checkQueueUsedCapacity("a1", cs, "x", 0.2f, 0.1f);
    checkQueueUsedCapacity("a1", cs, "", 0.2f, 0.1f);
    checkQueueUsedCapacity("a2", cs, "x", 0.1f, 0.05f);
    checkQueueUsedCapacity("a2", cs, "", 0.1f, 0.05f);
    checkQueueUsedCapacity("b", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "x", 0.15f, 0.15f);
    checkQueueUsedCapacity("root", cs, "", 0.15f, 0.15f);
    
    am1.allocate(null, Arrays.asList(
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2),
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 3),
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 4)));
    checkQueueUsedCapacity("a", cs, "x", 0.05f, 0.05f);
    checkQueueUsedCapacity("a", cs, "", 0.10f, 0.10f);
    checkQueueUsedCapacity("a1", cs, "x", 0.0f, 0.0f);
    checkQueueUsedCapacity("a1", cs, "", 0.1f, 0.05f);
    checkQueueUsedCapacity("a2", cs, "x", 0.1f, 0.05f);
    checkQueueUsedCapacity("a2", cs, "", 0.1f, 0.05f);
    checkQueueUsedCapacity("b", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "x", 0.05f, 0.05f);
    checkQueueUsedCapacity("root", cs, "", 0.10f, 0.10f);

    rm.close();
  }
  
  @Test
  public void testOrderOfAllocationOnPartitions()
          throws Exception {

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(this.conf);

    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] { "a",
        "b", "c", "d" });
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    csConf.setCapacity(A, 25);
    csConf.setAccessibleNodeLabels(A, toSet("x"));
    csConf.setCapacityByLabel(A, "x", 30);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    csConf.setCapacity(B, 25);
    csConf.setAccessibleNodeLabels(B, toSet("x"));
    csConf.setCapacityByLabel(B, "x", 70);
    
    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    csConf.setCapacity(C, 25);
    
    final String D = CapacitySchedulerConfiguration.ROOT + ".d";
    csConf.setCapacity(D, 25);

    mgr.addToCluserNodeLabels(ImmutableSet.of("x"));
    mgr.updateNodeLabels(Arrays.asList(NodeLabel.newInstance("x", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    MockRM rm = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();
    
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    MockNM nm1 = rm.registerNode("h1:1234", 10 * GB); // label = x
    MockNM nm2 = rm.registerNode("h2:1234", 10 * GB); // label = <empty>
    
    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "a");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);
    
    RMApp app2 = rm.submitApp(1 * GB, "app", "user", null, "b");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);
    
    RMApp app3 = rm.submitApp(1 * GB, "app", "user", null, "c");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm, nm2);
    
    RMApp app4 = rm.submitApp(1 * GB, "app", "user", null, "d");
    MockAM am4 = MockRM.launchAndRegisterAM(app4, rm, nm2);

    am1.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>(), "x");
    am2.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>(), "x");
    doNMHeartbeat(rm, nm1.getNodeId(), 1);
    checkNumOfContainersInAnAppOnGivenNode(1, nm1.getNodeId(),
        cs.getApplicationAttempt(am2.getApplicationAttemptId()));
    
    am2.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>(), "x");
    doNMHeartbeat(rm, nm1.getNodeId(), 1);
    checkNumOfContainersInAnAppOnGivenNode(1, nm1.getNodeId(),
        cs.getApplicationAttempt(am1.getApplicationAttemptId()));
    
    am1.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>(), "");
    am2.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>(), "");
    doNMHeartbeat(rm, nm1.getNodeId(), 2);
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am2.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(1, nm1.getNodeId(),
        cs.getApplicationAttempt(am1.getApplicationAttemptId()));
    
    doNMHeartbeat(rm, nm1.getNodeId(), 2);
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am2.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am1.getApplicationAttemptId()));
    
    am2.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>(), "");
    am3.allocate("*", 1 * GB, 2, new ArrayList<ContainerId>(), "");
    am4.allocate("*", 1 * GB, 2, new ArrayList<ContainerId>(), "");
    doNMHeartbeat(rm, nm1.getNodeId(), 2);
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am1.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(3, nm1.getNodeId(),
        cs.getApplicationAttempt(am2.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(0, nm1.getNodeId(),
        cs.getApplicationAttempt(am3.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(0, nm1.getNodeId(),
        cs.getApplicationAttempt(am4.getApplicationAttemptId()));
    
    doNMHeartbeat(rm, nm1.getNodeId(), 1);
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am1.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(3, nm1.getNodeId(),
        cs.getApplicationAttempt(am2.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(1, nm1.getNodeId(),
        cs.getApplicationAttempt(am3.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(0, nm1.getNodeId(),
        cs.getApplicationAttempt(am4.getApplicationAttemptId()));
    
    doNMHeartbeat(rm, nm1.getNodeId(), 2);
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am1.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(3, nm1.getNodeId(),
        cs.getApplicationAttempt(am2.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(1, nm1.getNodeId(),
        cs.getApplicationAttempt(am3.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(1, nm1.getNodeId(),
        cs.getApplicationAttempt(am4.getApplicationAttemptId()));
    
    doNMHeartbeat(rm, nm1.getNodeId(), 1);
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am1.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(3, nm1.getNodeId(),
        cs.getApplicationAttempt(am2.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am3.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(1, nm1.getNodeId(),
        cs.getApplicationAttempt(am4.getApplicationAttemptId()));

  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestParentQueue.java
        thenReturn(Resources.createResource(100 * 16 * GB, 100 * 32));
    when(csContext.getApplicationComparator()).
    thenReturn(CapacityScheduler.applicationComparator);
    when(csContext.getNonPartitionedQueueComparator()).
    thenReturn(CapacityScheduler.nonPartitionedQueueComparator);
    when(csContext.getResourceCalculator()).
    thenReturn(resourceComparator);
    when(csContext.getRMContext()).thenReturn(rmContext);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestReservations.java
        Resources.createResource(100 * 16 * GB, 100 * 12));
    when(csContext.getApplicationComparator()).thenReturn(
        CapacityScheduler.applicationComparator);
    when(csContext.getNonPartitionedQueueComparator()).thenReturn(
        CapacityScheduler.nonPartitionedQueueComparator);
    when(csContext.getResourceCalculator()).thenReturn(resourceCalculator);
    when(csContext.getRMContext()).thenReturn(rmContext);
    RMContainerTokenSecretManager containerTokenSecretManager = new RMContainerTokenSecretManager(

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fifo/TestFifoScheduler.java
import org.apache.hadoop.yarn.server.resourcemanager.Task;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
        scheduler);
    ((RMContextImpl) rmContext).setSystemMetricsPublisher(
        mock(SystemMetricsPublisher.class));
    NullRMNodeLabelsManager nlm = new NullRMNodeLabelsManager();
    nlm.init(new Configuration());
    rmContext.setNodeLabelManager(nlm);

    scheduler.setRMContext(rmContext);
    scheduler.init(conf);

