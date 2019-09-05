hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/security/ContainerTokenIdentifier.java
import org.apache.hadoop.yarn.api.records.impl.pb.LogAggregationContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.ContainerTokenIdentifierProto;

import com.google.protobuf.TextFormat;
      String hostName, String appSubmitter, Resource r, long expiryTimeStamp,
      int masterKeyId, long rmIdentifier, Priority priority, long creationTime) {
    this(containerID, hostName, appSubmitter, r, expiryTimeStamp, masterKeyId,
        rmIdentifier, priority, creationTime, null,
        CommonNodeLabelsManager.NO_LABEL);
  }

  public ContainerTokenIdentifier(ContainerId containerID, String hostName,
      String appSubmitter, Resource r, long expiryTimeStamp, int masterKeyId,
      long rmIdentifier, Priority priority, long creationTime,
      LogAggregationContext logAggregationContext, String nodeLabelExpression) {
    ContainerTokenIdentifierProto.Builder builder = 
        ContainerTokenIdentifierProto.newBuilder();
    if (containerID != null) {
      builder.setLogAggregationContext(
          ((LogAggregationContextPBImpl)logAggregationContext).getProto());
    }
    
    if (nodeLabelExpression != null) {
      builder.setNodeLabelExpression(nodeLabelExpression);
    }
    
    proto = builder.build();
  }

        containerId);
  }
  
  public String getNodeLabelExpression() {
    if (proto.hasNodeLabelExpression()) {
      return proto.getNodeLabelExpression();
    }
    return CommonNodeLabelsManager.NO_LABEL;
  }

  @InterfaceAudience.Private
  public static class Renewer extends Token.TrivialRenewer {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/NMContainerStatus.java
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.util.Records;

public abstract class NMContainerStatus {
  
  public static NMContainerStatus newInstance(ContainerId containerId,
      ContainerState containerState, Resource allocatedResource,
      String diagnostics, int containerExitStatus, Priority priority,
      long creationTime) {
    return newInstance(containerId, containerState, allocatedResource,
        diagnostics, containerExitStatus, priority, creationTime,
        CommonNodeLabelsManager.NO_LABEL);
  }

  public static NMContainerStatus newInstance(ContainerId containerId,
      ContainerState containerState, Resource allocatedResource,
      String diagnostics, int containerExitStatus, Priority priority,
      long creationTime, String nodeLabelExpression) {
    NMContainerStatus status =
        Records.newRecord(NMContainerStatus.class);
    status.setContainerId(containerId);
    status.setContainerExitStatus(containerExitStatus);
    status.setPriority(priority);
    status.setCreationTime(creationTime);
    status.setNodeLabelExpression(nodeLabelExpression);
    return status;
  }

  public abstract long getCreationTime();

  public abstract void setCreationTime(long creationTime);
  
  public abstract String getNodeLabelExpression();

  public abstract void setNodeLabelExpression(
      String nodeLabelExpression);
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/NMContainerStatusPBImpl.java
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
    builder.setCreationTime(creationTime);
  }
  
  @Override
  public String getNodeLabelExpression() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (p.hasNodeLabelExpression()) {
      return p.getNodeLabelExpression();
    }
    return CommonNodeLabelsManager.NO_LABEL;
  }

  @Override
  public void setNodeLabelExpression(String nodeLabelExpression) {
    maybeInitBuilder();
    if (nodeLabelExpression == null) {
      builder.clearNodeLabelExpression();
      return;
    }
    builder.setNodeLabelExpression(nodeLabelExpression);
  }

  private void mergeLocalToBuilder() {
    if (this.containerId != null
        && !((ContainerIdPBImpl) containerId).getProto().equals(

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/container/ContainerImpl.java
      return NMContainerStatus.newInstance(this.containerId, getCurrentState(),
          getResource(), diagnostics.toString(), exitCode,
          containerTokenIdentifier.getPriority(),
          containerTokenIdentifier.getCreationTime(),
          containerTokenIdentifier.getNodeLabelExpression());
    } finally {
      this.readLock.unlock();
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/TestContainerManager.java
    ContainerTokenIdentifier containerTokenIdentifier =
        new ContainerTokenIdentifier(cId, nodeId.toString(), user, r,
          System.currentTimeMillis() + 100000L, 123, rmIdentifier,
          Priority.newInstance(0), 0, logAggregationContext, null);
    Token containerToken =
        BuilderUtils
          .newContainerToken(nodeId, containerTokenSecretManager

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmcontainer/RMContainer.java
  List<ResourceRequest> getResourceRequests();

  String getNodeHttpAddress();
  
  String getNodeLabelExpression();
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmcontainer/RMContainerImpl.java
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRunningOnNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerAllocatedEvent;
  private final EventHandler eventHandler;
  private final ContainerAllocationExpirer containerAllocationExpirer;
  private final String user;
  private final String nodeLabelExpression;

  private Resource reservedResource;
  private NodeId reservedNode;
      ApplicationAttemptId appAttemptId, NodeId nodeId, String user,
      RMContext rmContext) {
    this(container, appAttemptId, nodeId, user, rmContext, System
        .currentTimeMillis(), "");
  }

  public RMContainerImpl(Container container,
      ApplicationAttemptId appAttemptId, NodeId nodeId, String user,
      RMContext rmContext, String nodeLabelExpression) {
    this(container, appAttemptId, nodeId, user, rmContext, System
      .currentTimeMillis(), nodeLabelExpression);
  }

  public RMContainerImpl(Container container,
      ApplicationAttemptId appAttemptId, NodeId nodeId, String user,
      RMContext rmContext, long creationTime, String nodeLabelExpression) {
    this.stateMachine = stateMachineFactory.make(this);
    this.containerId = container.getId();
    this.nodeId = nodeId;
    this.containerAllocationExpirer = rmContext.getContainerAllocationExpirer();
    this.isAMContainer = false;
    this.resourceRequests = null;
    this.nodeLabelExpression = nodeLabelExpression;

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
      readLock.unlock();
    }
  }

  @Override
  public String getNodeLabelExpression() {
    if (nodeLabelExpression == null) {
      return RMNodeLabelsManager.NO_LABEL;
    }
    return nodeLabelExpression;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/AbstractYarnScheduler.java
    RMContainer rmContainer =
        new RMContainerImpl(container, attemptId, node.getNodeID(),
          applications.get(attemptId.getApplicationId()).getUser(), rmContext,
          status.getCreationTime(), status.getNodeLabelExpression());
    return rmContainer;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/SchedulerApplicationAttempt.java
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
        container.setContainerToken(rmContext.getContainerTokenSecretManager()
            .createContainerToken(container.getId(), container.getNodeId(),
                getUser(), container.getResource(), container.getPriority(),
                rmContainer.getCreationTime(), this.logAggregationContext,
                rmContainer.getNodeLabelExpression()));
        NMToken nmToken =
            rmContext.getNMTokenSecretManager().createAndGetNMToken(getUser(),
              getApplicationAttemptId(), container);
        this.attemptResourceUsage, nodePartition, cluster,
        schedulingMode);
  }
  
  @VisibleForTesting
  public ResourceUsage getAppAttemptResourceUsage() {
    return this.attemptResourceUsage;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/common/fica/FiCaSchedulerApp.java
    }
    
    RMContainer rmContainer =
        new RMContainerImpl(container, this.getApplicationAttemptId(),
            node.getNodeID(), appSchedulingInfo.getUser(), this.rmContext,
            request.getNodeLabelExpression());

    newlyAllocatedContainers.add(rmContainer);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/security/RMContainerTokenSecretManager.java
      String appSubmitter, Resource capability, Priority priority,
      long createTime) {
    return createContainerToken(containerId, nodeId, appSubmitter, capability,
      priority, createTime, null, null);
  }

  public Token createContainerToken(ContainerId containerId, NodeId nodeId,
      String appSubmitter, Resource capability, Priority priority,
      long createTime, LogAggregationContext logAggregationContext,
      String nodeLabelExpression) {
    byte[] password;
    ContainerTokenIdentifier tokenIdentifier;
    long expiryTimeStamp =
            appSubmitter, capability, expiryTimeStamp, this.currentMasterKey
              .getMasterKey().getKeyId(),
            ResourceManager.getClusterTimeStamp(), priority, createTime,
            logAggregationContext, nodeLabelExpression);
      password = this.createPassword(tokenIdentifier);

    } finally {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMRestart.java
  
  public static NMContainerStatus createNMContainerStatus(
      ApplicationAttemptId appAttemptId, int id, ContainerState containerState) {
    return createNMContainerStatus(appAttemptId, id, containerState,
        RMNodeLabelsManager.NO_LABEL);
  }

  public static NMContainerStatus createNMContainerStatus(
      ApplicationAttemptId appAttemptId, int id, ContainerState containerState,
      String nodeLabelExpression) {
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, id);
    NMContainerStatus containerReport =
        NMContainerStatus.newInstance(containerId, containerState,
            Resource.newInstance(1024, 1), "recover container", 0,
            Priority.newInstance(0), 0, nodeLabelExpression);
    return containerReport;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestContainerAllocation.java
        public Token createContainerToken(ContainerId containerId,
            NodeId nodeId, String appSubmitter, Resource capability,
            Priority priority, long createTime,
            LogAggregationContext logAggregationContext, String nodeLabelExp) {
          numRetries++;
          return super.createContainerToken(containerId, nodeId, appSubmitter,
              capability, priority, createTime, logAggregationContext,
              nodeLabelExp);
        }
      };
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestWorkPreservingRMRestartForNodeLabel.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestWorkPreservingRMRestartForNodeLabel.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.TestRMRestart;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class TestWorkPreservingRMRestartForNodeLabel {
  private Configuration conf;
  private static final int GB = 1024; // 1024 MB
  
  RMNodeLabelsManager mgr;

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
      ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }
  
  @SuppressWarnings("unchecked")
  private <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
    return set;
  }
  
  private void checkRMContainerLabelExpression(ContainerId containerId,
      MockRM rm, String labelExpression) {
    RMContainer container =
        rm.getRMContext().getScheduler().getRMContainer(containerId);
    Assert.assertNotNull("Cannot find RMContainer=" + containerId, container);
    Assert.assertEquals(labelExpression,
        container.getNodeLabelExpression());
  }
  
  @SuppressWarnings("rawtypes")
  public static void waitForNumContainersToRecover(int num, MockRM rm,
      ApplicationAttemptId attemptId) throws Exception {
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();
    SchedulerApplicationAttempt attempt =
        scheduler.getApplicationAttempt(attemptId);
    while (attempt == null) {
      System.out.println("Wait for scheduler attempt " + attemptId
          + " to be created");
      Thread.sleep(200);
      attempt = scheduler.getApplicationAttempt(attemptId);
    }
    while (attempt.getLiveContainers().size() < num) {
      System.out.println("Wait for " + num
          + " containers to recover. currently: "
          + attempt.getLiveContainers().size());
      Thread.sleep(200);
    }
  }
  
  private void checkAppResourceUsage(String partition, ApplicationId appId,
      MockRM rm, int expectedMemUsage) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    FiCaSchedulerApp app =
        cs.getSchedulerApplications().get(appId).getCurrentAppAttempt();
    Assert.assertEquals(expectedMemUsage, app.getAppAttemptResourceUsage()
        .getUsed(partition).getMemory());
  }
  
  private void checkQueueResourceUsage(String partition, String queueName, MockRM rm, int expectedMemUsage) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = cs.getQueue(queueName);
    Assert.assertEquals(expectedMemUsage, queue.getQueueResourceUsage()
        .getUsed(partition).getMemory());
  }

  @Test
  public void testWorkPreservingRestartForNodeLabel() throws Exception {

    mgr.addToCluserNodeLabels(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x"),
        NodeId.newInstance("h2", 0), toSet("y")));

    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    
    conf = TestUtils.getConfigurationWithDefaultQueueLabels(conf);
    
    MockRM rm1 =
        new MockRM(conf,
            memStore) {
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
    Assert.assertTrue(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am1.getApplicationAttemptId(), 1), rm1, "x");
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am1.getApplicationAttemptId(), 2), rm1, "x");

    RMApp app2 = rm1.submitApp(200, "app", "user", null, "b1");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    am2.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newContainerId(am2.getApplicationAttemptId(), 2);
    Assert.assertTrue(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am2.getApplicationAttemptId(), 1), rm1, "y");
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am2.getApplicationAttemptId(), 2), rm1, "y");
    
    RMApp app3 = rm1.submitApp(200, "app", "user", null, "c1");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm3);

    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newContainerId(am3.getApplicationAttemptId(), 2);
    Assert.assertTrue(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED, 10 * 1000));
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am3.getApplicationAttemptId(), 1), rm1, "");
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am3.getApplicationAttemptId(), 2), rm1, "");
    
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
    mgr.addToCluserNodeLabels(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x"),
        NodeId.newInstance("h2", 0), toSet("y")));
    MockRM rm2 =
        new MockRM(conf,
            memStore) {
          @Override
          public RMNodeLabelsManager createNodeLabelManager() {
            return mgr;
          }
        };
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    nm2.setResourceTrackerService(rm2.getResourceTrackerService());
    nm3.setResourceTrackerService(rm2.getResourceTrackerService());
    
    NMContainerStatus app1c1 =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 1,
          ContainerState.RUNNING, "x");
    NMContainerStatus app1c2 =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 2,
          ContainerState.RUNNING, "x");
    nm1.registerNode(Arrays.asList(app1c1, app1c2), null);
    waitForNumContainersToRecover(2, rm2, am1.getApplicationAttemptId());
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am1.getApplicationAttemptId(), 1), rm1, "x");
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am1.getApplicationAttemptId(), 2), rm1, "x");
    
    NMContainerStatus app2c1 =
        TestRMRestart.createNMContainerStatus(am2.getApplicationAttemptId(), 1,
          ContainerState.RUNNING, "y");
    NMContainerStatus app2c2 =
        TestRMRestart.createNMContainerStatus(am2.getApplicationAttemptId(), 2,
          ContainerState.RUNNING, "y");
    nm2.registerNode(Arrays.asList(app2c1, app2c2), null);
    waitForNumContainersToRecover(2, rm2, am2.getApplicationAttemptId());
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am2.getApplicationAttemptId(), 1), rm1, "y");
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am2.getApplicationAttemptId(), 2), rm1, "y");
    
    NMContainerStatus app3c1 =
        TestRMRestart.createNMContainerStatus(am3.getApplicationAttemptId(), 1,
          ContainerState.RUNNING, "");
    NMContainerStatus app3c2 =
        TestRMRestart.createNMContainerStatus(am3.getApplicationAttemptId(), 2,
          ContainerState.RUNNING, "");
    nm3.registerNode(Arrays.asList(app3c1, app3c2), null);
    waitForNumContainersToRecover(2, rm2, am3.getApplicationAttemptId());
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am3.getApplicationAttemptId(), 1), rm1, "");
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am3.getApplicationAttemptId(), 2), rm1, "");
    
    checkAppResourceUsage("x", app1.getApplicationId(), rm1, 2 * GB);
    checkAppResourceUsage("y", app2.getApplicationId(), rm1, 2 * GB);
    checkAppResourceUsage("", app3.getApplicationId(), rm1, 2 * GB);
    checkQueueResourceUsage("x", "a1", rm1, 2 * GB);
    checkQueueResourceUsage("y", "b1", rm1, 2 * GB);
    checkQueueResourceUsage("", "c1", rm1, 2 * GB);
    checkQueueResourceUsage("x", "a", rm1, 2 * GB);
    checkQueueResourceUsage("y", "b", rm1, 2 * GB);
    checkQueueResourceUsage("", "c", rm1, 2 * GB);
    checkQueueResourceUsage("x", "root", rm1, 2 * GB);
    checkQueueResourceUsage("y", "root", rm1, 2 * GB);
    checkQueueResourceUsage("", "root", rm1, 2 * GB);


    rm1.close();
    rm2.close();
  }
}
\No newline at end of file

