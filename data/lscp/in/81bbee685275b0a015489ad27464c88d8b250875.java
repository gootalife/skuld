hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/RMActiveServiceContext.java
  private final ConcurrentMap<NodeId, RMNode> nodes =
      new ConcurrentHashMap<NodeId, RMNode>();

  private final ConcurrentMap<NodeId, RMNode> inactiveNodes =
      new ConcurrentHashMap<NodeId, RMNode>();

  private final ConcurrentMap<ApplicationId, ByteBuffer> systemCredentials =
      new ConcurrentHashMap<ApplicationId, ByteBuffer>();

  @Private
  @Unstable
  public ConcurrentMap<NodeId, RMNode> getInactiveRMNodes() {
    return this.inactiveNodes;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/RMContext.java
  
  ConcurrentMap<ApplicationId, ByteBuffer> getSystemCredentialsForApps();

  ConcurrentMap<NodeId, RMNode> getInactiveRMNodes();

  ConcurrentMap<NodeId, RMNode> getRMNodes();


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/RMContextImpl.java
  }

  @Override
  public ConcurrentMap<NodeId, RMNode> getInactiveRMNodes() {
    return activeServiceContext.getInactiveRMNodes();
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmnode/RMNodeImpl.java
      RMNodeStartedEvent startEvent = (RMNodeStartedEvent) event;
      List<NMContainerStatus> containers = null;

      NodeId nodeId = rmNode.nodeId;
      if (rmNode.context.getInactiveRMNodes().containsKey(nodeId)) {
        RMNode previouRMNode = rmNode.context.getInactiveRMNodes().get(nodeId);
        rmNode.context.getInactiveRMNodes().remove(nodeId);
        rmNode.updateMetricsForRejoinedNode(previouRMNode.getState());
      } else {
      rmNode.context.getRMNodes().remove(rmNode.nodeId);
      LOG.info("Deactivating Node " + rmNode.nodeId + " as it is now "
          + finalState);
      rmNode.context.getInactiveRMNodes().put(rmNode.nodeId, rmNode);

      rmNode.updateMetricsForDeactivatedNode(initialState, finalState);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMWebServices.java
    RMNode ni = this.rm.getRMContext().getRMNodes().get(nid);
    boolean isInactive = false;
    if (ni == null) {
      ni = this.rm.getRMContext().getInactiveRMNodes().get(nid);
      if (ni == null) {
        throw new NotFoundException("nodeId, " + nodeId + ", is not found");
      }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMNodeTransitions.java
    Assert.assertEquals(NodeState.LOST, node.getState());
  }

  @Test
  public void testRunningExpireMultiple() {
    RMNodeImpl node1 = getRunningNode(null, 10001);
    RMNodeImpl node2 = getRunningNode(null, 10002);
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node1.handle(new RMNodeEvent(node1.getNodeID(), RMNodeEventType.EXPIRE));
    Assert.assertEquals("Active Nodes", initialActive - 1, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost + 1, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes", initialUnhealthy,
        cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes", initialDecommissioned,
        cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes", initialRebooted,
        cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.LOST, node1.getState());
    Assert.assertTrue("Node " + node1.toString() + " should be inactive",
        rmContext.getInactiveRMNodes().containsKey(node1.getNodeID()));
    Assert.assertFalse("Node " + node2.toString() + " should not be inactive",
        rmContext.getInactiveRMNodes().containsKey(node2.getNodeID()));

    node2.handle(new RMNodeEvent(node1.getNodeID(), RMNodeEventType.EXPIRE));
    Assert.assertEquals("Active Nodes", initialActive - 2, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost + 2, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes", initialUnhealthy,
        cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes", initialDecommissioned,
        cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes", initialRebooted,
        cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.LOST, node2.getState());
    Assert.assertTrue("Node " + node1.toString() + " should be inactive",
        rmContext.getInactiveRMNodes().containsKey(node1.getNodeID()));
    Assert.assertTrue("Node " + node2.toString() + " should be inactive",
        rmContext.getInactiveRMNodes().containsKey(node2.getNodeID()));
  }

  @Test
  public void testUnhealthyExpire() {
    RMNodeImpl node = getUnhealthyNode();
  }

  private RMNodeImpl getRunningNode() {
    return getRunningNode(null, 0);
  }

  private RMNodeImpl getRunningNode(String nmVersion) {
    return getRunningNode(nmVersion, 0);
  }

  private RMNodeImpl getRunningNode(String nmVersion, int port) {
    NodeId nodeId = BuilderUtils.newNodeId("localhost", port);
    Resource capability = Resource.newInstance(4096, 4);
    RMNodeImpl node = new RMNodeImpl(nodeId, rmContext, null, 0, 0, null,
        capability, nmVersion);
    node.handle(new RMNodeStartedEvent(node.getNodeID(), null, null));
    Assert.assertEquals(NodeState.RUNNING, node.getState());
    return node;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebApp.java
    
    final List<RMNode> deactivatedNodes =
        MockNodes.deactivatedNodes(racks, numNodes, newResource(mbsPerNode));
    final ConcurrentMap<NodeId, RMNode> deactivatedNodesMap =
        Maps.newConcurrentMap();
    for (RMNode node : deactivatedNodes) {
      deactivatedNodesMap.put(node.getNodeID(), node);
    }

    RMContextImpl rmContext = new RMContextImpl(null, null, null, null,
         return applicationsMaps;
       }
       @Override
       public ConcurrentMap<NodeId, RMNode> getInactiveRMNodes() {
         return deactivatedNodesMap;
       }
       @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServicesNodes.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
    assertEquals("incorrect number of elements", 2, nodeArray.length());
    for (int i = 0; i < nodeArray.length(); ++i) {
      JSONObject info = nodeArray.getJSONObject(i);
      String[] node = info.get("id").toString().split(":");
      NodeId nodeId = NodeId.newInstance(node[0], Integer.parseInt(node[1]));
      RMNode rmNode = rm.getRMContext().getInactiveRMNodes().get(nodeId);
      WebServicesTestUtils.checkStringMatch("nodeHTTPAddress", "",
          info.getString("nodeHTTPAddress"));
      WebServicesTestUtils.checkStringMatch("state", rmNode.getState()

    assertEquals("Incorrect Node Information.", "h2:1234", id);

    NodeId nodeId = NodeId.newInstance("h2", 1234);
    RMNode rmNode = rm.getRMContext().getInactiveRMNodes().get(nodeId);
    WebServicesTestUtils.checkStringMatch("nodeHTTPAddress", "",
        info.getString("nodeHTTPAddress"));
    WebServicesTestUtils.checkStringMatch("state",

