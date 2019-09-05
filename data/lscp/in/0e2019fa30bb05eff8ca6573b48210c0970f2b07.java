hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmcontainer/RMContainerImpl.java

    .addTransition(RMContainerState.ACQUIRED, RMContainerState.RUNNING,
        RMContainerEventType.LAUNCHED)
    .addTransition(RMContainerState.ACQUIRED, RMContainerState.COMPLETED,
        RMContainerEventType.FINISHED, new FinishedTransition())
    .addTransition(RMContainerState.ACQUIRED, RMContainerState.RELEASED,
        RMContainerEventType.RELEASED, new KillTransition())
    .addTransition(RMContainerState.ACQUIRED, RMContainerState.EXPIRED,
    }
  }

  private static final class ContainerRescheduledTransition extends
      FinishedTransition {

    }
  }

  private static final class KillTransition extends FinishedTransition {

    @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmnode/RMNodeImpl.java
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRunningOnNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
  private long lastHealthReportTime;
  private String nodeManagerVersion;

  private final ContainerAllocationExpirer containerAllocationExpirer;
  private final Set<ContainerId> launchedContainers =
    new HashSet<ContainerId>();
    this.stateMachine = stateMachineFactory.make(this);
    
    this.nodeUpdateQueue = new ConcurrentLinkedQueue<UpdatedContainerInfo>();

    this.containerAllocationExpirer = context.getContainerAllocationExpirer();
  }

  @Override
          launchedContainers.add(containerId);
          newlyLaunchedContainers.add(remoteContainer);
          containerAllocationExpirer.unregister(containerId);
        }
      } else {
        launchedContainers.remove(containerId);
        completedContainers.add(remoteContainer);
        containerAllocationExpirer.unregister(containerId);
      }
    }
    if (newlyLaunchedContainers.size() != 0 || completedContainers.size() != 0) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMNodeTransitions.java
import java.util.List;

import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
    InlineDispatcher rmDispatcher = new InlineDispatcher();
    
    rmContext =
        new RMContextImpl(rmDispatcher, mock(ContainerAllocationExpirer.class),
          null, null, mock(DelegationTokenRenewer.class), null, null, null,
          null, null);
    NodesListManager nodesListManager = mock(NodesListManager.class);
    HostsFileReader reader = mock(HostsFileReader.class);
    when(nodesListManager.getHostsReader()).thenReturn(reader);
  public void tearDown() throws Exception {
  }
  
  private RMNodeStatusEvent getMockRMNodeStatusEvent(
      List<ContainerStatus> containerStatus) {
    NodeHeartbeatResponse response = mock(NodeHeartbeatResponse.class);

    NodeHealthStatus healthStatus = mock(NodeHealthStatus.class);
    doReturn(healthStatus).when(event).getNodeHealthStatus();
    doReturn(response).when(event).getLatestResponse();
    doReturn(RMNodeEventType.STATUS_UPDATE).when(event).getType();
    if (containerStatus != null) {
      doReturn(containerStatus).when(event).getContainers();
    }
    return event;
  }
  
    
    RMNodeStatusEvent statusEvent = getMockRMNodeStatusEvent(null);
    ContainerStatus containerStatus = mock(ContainerStatus.class);
    doReturn(completedContainerId).when(containerStatus).getContainerId();
    doReturn(Collections.singletonList(containerStatus)).
        BuilderUtils.newApplicationAttemptId(
            BuilderUtils.newApplicationId(1, 1), 1), 2);

    RMNodeStatusEvent statusEventFromNode1 = getMockRMNodeStatusEvent(null);
    RMNodeStatusEvent statusEventFromNode2_1 = getMockRMNodeStatusEvent(null);
    RMNodeStatusEvent statusEventFromNode2_2 = getMockRMNodeStatusEvent(null);

    ContainerStatus containerStatusFromNode1 = mock(ContainerStatus.class);
    ContainerStatus containerStatusFromNode2_1 = mock(ContainerStatus.class);
        BuilderUtils.newApplicationAttemptId(
            BuilderUtils.newApplicationId(1, 1), 1), 1);
        
    RMNodeStatusEvent statusEvent1 = getMockRMNodeStatusEvent(null);
    RMNodeStatusEvent statusEvent2 = getMockRMNodeStatusEvent(null);

    ContainerStatus containerStatus1 = mock(ContainerStatus.class);
    ContainerStatus containerStatus2 = mock(ContainerStatus.class);

    RMNodeStatusEvent statusEvent = getMockRMNodeStatusEvent(null);
    node.handle(statusEvent);
    Assert.assertEquals(1, node.getContainersToCleanUp().size());
    Assert.assertEquals(1, node.getAppsToCleanup().size());
        null, null));
    Assert.assertEquals(nmVersion2, node.getNodeManagerVersion());
  }

  @Test
  public void testContainerExpire() throws Exception {
    ContainerAllocationExpirer mockExpirer =
        mock(ContainerAllocationExpirer.class);
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId1 = ContainerId.newContainerId(appAttemptId, 1L);
    ContainerId containerId2 = ContainerId.newContainerId(appAttemptId, 2L);
    mockExpirer.register(containerId1);
    mockExpirer.register(containerId2);
    verify(mockExpirer).register(containerId1);
    verify(mockExpirer).register(containerId2);
    ((RMContextImpl) rmContext).setContainerAllocationExpirer(mockExpirer);
    RMNodeImpl rmNode = getRunningNode();
    ContainerStatus status1 =
        ContainerStatus
          .newInstance(containerId1, ContainerState.RUNNING, "", 0);
    ContainerStatus status2 =
        ContainerStatus.newInstance(containerId2, ContainerState.COMPLETE, "",
          0);
    List<ContainerStatus> statusList = new ArrayList<ContainerStatus>();
    statusList.add(status1);
    statusList.add(status2);
    RMNodeStatusEvent statusEvent = getMockRMNodeStatusEvent(statusList);
    rmNode.handle(statusEvent);
    verify(mockExpirer).unregister(containerId1);
    verify(mockExpirer).unregister(containerId2);
  }
}

