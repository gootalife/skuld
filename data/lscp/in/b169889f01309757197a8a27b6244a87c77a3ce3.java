hadoop-tools/hadoop-sls/src/main/java/org/apache/hadoop/yarn/sls/nodemanager/NodeInfo.java
    private NodeState state;
    private List<ContainerId> toCleanUpContainers;
    private List<ApplicationId> toCleanUpApplications;
    private List<ApplicationId> runningApplications;

    public FakeRMNodeImpl(NodeId nodeId, String nodeAddr, String httpAddress,
        Resource perNode, String rackName, String healthReport,
      this.state = state;
      toCleanUpApplications = new ArrayList<ApplicationId>();
      toCleanUpContainers = new ArrayList<ContainerId>();
      runningApplications = new ArrayList<ApplicationId>();
    }

    public NodeId getNodeID() {
      return toCleanUpApplications;
    }

    public List<ApplicationId> getRunningApps() {
      return runningApplications;
    }

    public void updateNodeHeartbeatResponseForCleanup(
            NodeHeartbeatResponse response) {
    }

hadoop-tools/hadoop-sls/src/main/java/org/apache/hadoop/yarn/sls/scheduler/RMNodeWrapper.java
    return node.getAppsToCleanup();
  }

  @Override
  public List<ApplicationId> getRunningApps() {
    return node.getRunningApps();
  }

  @Override
  public void updateNodeHeartbeatResponseForCleanup(
          NodeHeartbeatResponse nodeHeartbeatResponse) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmnode/RMNode.java

  public List<ApplicationId> getAppsToCleanup();

  List<ApplicationId> getRunningApps();


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmnode/RMNodeImpl.java
      new HashSet<ContainerId>();

  private final List<ApplicationId> finishedApplications =
      new ArrayList<ApplicationId>();

  private final List<ApplicationId> runningApplications =
      new ArrayList<ApplicationId>();

  private NodeHeartbeatResponse latestNodeHeartBeatResponse = recordFactory
      .newRecordInstance(NodeHeartbeatResponse.class);

  }
  
  @Override
  public List<ApplicationId> getRunningApps() {
    this.readLock.lock();
    try {
      return new ArrayList<ApplicationId>(this.runningApplications);
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ContainerId> getContainersToCleanUp() {

      LOG.warn("Cannot get RMApp by appId=" + appId
          + ", just added it to finishedApplications list for cleanup");
      rmNode.finishedApplications.add(appId);
      rmNode.runningApplications.remove(appId);
      return;
    }

    rmNode.runningApplications.add(appId);
    context.getDispatcher().getEventHandler()
        .handle(new RMAppRunningOnNodeEvent(appId, nodeId));
  }

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      ApplicationId appId = ((RMNodeCleanAppEvent) event).getAppId();
      rmNode.finishedApplications.add(appId);
      rmNode.runningApplications.remove(appId);
    }
  }

            + "cleanup, no further processing");
        continue;
      }

      ApplicationId containerAppId =
          containerId.getApplicationAttemptId().getApplicationId();

      if (finishedApplications.contains(containerAppId)) {
        LOG.info("Container " + containerId
            + " belongs to an application that is already killed,"
            + " no further processing");
        continue;
      } else if (!runningApplications.contains(containerAppId)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Container " + containerId
              + " is the first container get launched for application "
              + containerAppId);
        }
        runningApplications.add(containerAppId);
      }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/MockNodes.java
      return null;
    }

    @Override
    public List<ApplicationId> getRunningApps() {
      return null;
    }

    @Override
    public void updateNodeHeartbeatResponseForCleanup(NodeHeartbeatResponse response) {
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMNodeTransitions.java
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
    Assert.assertEquals(finishedAppId, hbrsp.getApplicationsToCleanup().get(0));
  }

  @Test(timeout=20000)
  public void testUpdateHeartbeatResponseForAppLifeCycle() {
    RMNodeImpl node = getRunningNode();
    NodeId nodeId = node.getNodeID();

    ApplicationId runningAppId = BuilderUtils.newApplicationId(0, 1);
    ContainerId runningContainerId = BuilderUtils.newContainerId(
        BuilderUtils.newApplicationAttemptId(
        runningAppId, 0), 0);

    ContainerStatus status = ContainerStatus.newInstance(runningContainerId,
        ContainerState.RUNNING, "", 0);
    List<ContainerStatus> statusList = new ArrayList<ContainerStatus>();
    statusList.add(status);
    NodeHealthStatus nodeHealth = NodeHealthStatus.newInstance(true,
        "", System.currentTimeMillis());
    node.handle(new RMNodeStatusEvent(nodeId, nodeHealth,
        statusList, null, null));

    Assert.assertEquals(1, node.getRunningApps().size());

    ApplicationId finishedAppId = runningAppId;
    node.handle(new RMNodeCleanAppEvent(nodeId, finishedAppId));
    Assert.assertEquals(1, node.getAppsToCleanup().size());
    Assert.assertEquals(0, node.getRunningApps().size());
  }

  private RMNodeImpl getRunningNode() {
    return getRunningNode(null, 0);
  }

