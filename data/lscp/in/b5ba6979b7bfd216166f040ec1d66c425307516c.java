hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/main/java/org/apache/hadoop/mapred/ResourceMgrDelegate.java
  }

  @Override
  public Map<NodeId, Set<NodeLabel>> getNodeToLabels() throws YarnException,
      IOException {
    return client.getNodeToLabels();
  }

  @Override
  public Map<NodeLabel, Set<NodeId>> getLabelsToNodes() throws YarnException,
      IOException {
    return client.getLabelsToNodes();
  }

  @Override
  public Map<NodeLabel, Set<NodeId>> getLabelsToNodes(Set<String> labels)
      throws YarnException, IOException {
    return client.getLabelsToNodes(labels);
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/protocolrecords/GetLabelsToNodesResponse.java
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.util.Records;

public abstract class GetLabelsToNodesResponse {
  public static GetLabelsToNodesResponse newInstance(
      Map<NodeLabel, Set<NodeId>> map) {
	GetLabelsToNodesResponse response =
        Records.newRecord(GetLabelsToNodesResponse.class);
    response.setLabelsToNodes(map);

  @Public
  @Evolving
  public abstract void setLabelsToNodes(Map<NodeLabel, Set<NodeId>> map);

  @Public
  @Evolving
  public abstract Map<NodeLabel, Set<NodeId>> getLabelsToNodes();
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/protocolrecords/GetNodesToLabelsResponse.java
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.util.Records;

public abstract class GetNodesToLabelsResponse {
  public static GetNodesToLabelsResponse newInstance(
      Map<NodeId, Set<NodeLabel>> map) {
    GetNodesToLabelsResponse response =
        Records.newRecord(GetNodesToLabelsResponse.class);
    response.setNodeToLabels(map);

  @Public
  @Evolving
  public abstract void setNodeToLabels(Map<NodeId, Set<NodeLabel>> map);

  @Public
  @Evolving
  public abstract Map<NodeId, Set<NodeLabel>> getNodeToLabels();
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/api/YarnClient.java
  @Public
  @Unstable
  public abstract Map<NodeId, Set<NodeLabel>> getNodeToLabels()
      throws YarnException, IOException;

  @Public
  @Unstable
  public abstract Map<NodeLabel, Set<NodeId>> getLabelsToNodes()
      throws YarnException, IOException;

  @Public
  @Unstable
  public abstract Map<NodeLabel, Set<NodeId>> getLabelsToNodes(
      Set<String> labels) throws YarnException, IOException;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/api/impl/YarnClientImpl.java
  }
  
  @Override
  public Map<NodeId, Set<NodeLabel>> getNodeToLabels() throws YarnException,
      IOException {
    return rmClient.getNodeToLabels(GetNodesToLabelsRequest.newInstance())
        .getNodeToLabels();
  }

  @Override
  public Map<NodeLabel, Set<NodeId>> getLabelsToNodes() throws YarnException,
      IOException {
    return rmClient.getLabelsToNodes(GetLabelsToNodesRequest.newInstance())
        .getLabelsToNodes();
  }

  @Override
  public Map<NodeLabel, Set<NodeId>> getLabelsToNodes(Set<String> labels)
      throws YarnException, IOException {
    return rmClient.getLabelsToNodes(
        GetLabelsToNodesRequest.newInstance(labels)).getLabelsToNodes();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/api/impl/TestYarnClient.java
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
    client.start();

    Map<NodeLabel, Set<NodeId>> expectedLabelsToNodes =
        ((MockYarnClient)client).getLabelsToNodesMap();
    Map<NodeLabel, Set<NodeId>> labelsToNodes = client.getLabelsToNodes();
    Assert.assertEquals(labelsToNodes, expectedLabelsToNodes);
    Assert.assertEquals(labelsToNodes.size(), 3);

    client.close();
  }

  @Test (timeout = 10000)
  public void testGetNodesToLabels() throws YarnException, IOException {
    Configuration conf = new Configuration();
    final YarnClient client = new MockYarnClient();
    client.init(conf);
    client.start();

    Map<NodeId, Set<NodeLabel>> expectedNodesToLabels = ((MockYarnClient) client)
        .getNodeToLabelsMap();
    Map<NodeId, Set<NodeLabel>> nodesToLabels = client.getNodeToLabels();
    Assert.assertEquals(nodesToLabels, expectedNodesToLabels);
    Assert.assertEquals(nodesToLabels.size(), 1);

    Set<NodeLabel> labels = nodesToLabels.get(NodeId.newInstance("host", 0));
    for (NodeLabel label : labels) {
      Assert.assertFalse(label.isExclusive());
    }

    client.stop();
    client.close();
  }

  private static class MockYarnClient extends YarnClientImpl {

    private ApplicationReport mockReport;
    private List<ApplicationReport> reports;
    private HashMap<ApplicationId, List<ApplicationAttemptReport>> attempts = 
      mock(GetContainerReportResponse.class);
    GetLabelsToNodesResponse mockLabelsToNodesResponse =
      mock(GetLabelsToNodesResponse.class);
    GetNodesToLabelsResponse mockNodeToLabelsResponse =
        mock(GetNodesToLabelsResponse.class);

    public MockYarnClient() {
      super();
        when(rmClient.getLabelsToNodes(any(GetLabelsToNodesRequest.class)))
            .thenReturn(mockLabelsToNodesResponse);
        
        when(rmClient.getNodeToLabels(any(GetNodesToLabelsRequest.class)))
            .thenReturn(mockNodeToLabelsResponse);

        historyClient = mock(AHSClient.class);
        
      } catch (YarnException e) {
    }

    @Override
    public Map<NodeLabel, Set<NodeId>> getLabelsToNodes()
        throws YarnException, IOException {
      when(mockLabelsToNodesResponse.getLabelsToNodes()).thenReturn(
          getLabelsToNodesMap());
    }

    @Override
    public Map<NodeLabel, Set<NodeId>> getLabelsToNodes(Set<String> labels)
        throws YarnException, IOException {
      when(mockLabelsToNodesResponse.getLabelsToNodes()).thenReturn(
          getLabelsToNodesMap(labels));
      return super.getLabelsToNodes(labels);
    }

    public Map<NodeLabel, Set<NodeId>> getLabelsToNodesMap() {
      Map<NodeLabel, Set<NodeId>> map = new HashMap<NodeLabel, Set<NodeId>>();
      Set<NodeId> setNodeIds =
          new HashSet<NodeId>(Arrays.asList(
          NodeId.newInstance("host1", 0), NodeId.newInstance("host2", 0)));
      map.put(NodeLabel.newInstance("x"), setNodeIds);
      map.put(NodeLabel.newInstance("y"), setNodeIds);
      map.put(NodeLabel.newInstance("z"), setNodeIds);
      return map;
    }

    public Map<NodeLabel, Set<NodeId>> getLabelsToNodesMap(Set<String> labels) {
      Map<NodeLabel, Set<NodeId>> map = new HashMap<NodeLabel, Set<NodeId>>();
      Set<NodeId> setNodeIds =
          new HashSet<NodeId>(Arrays.asList(
          NodeId.newInstance("host1", 0), NodeId.newInstance("host2", 0)));
      for(String label : labels) {
        map.put(NodeLabel.newInstance(label), setNodeIds);
      }
      return map;
    }

    @Override
    public Map<NodeId, Set<NodeLabel>> getNodeToLabels() throws YarnException,
        IOException {
      when(mockNodeToLabelsResponse.getNodeToLabels()).thenReturn(
          getNodeToLabelsMap());
      return super.getNodeToLabels();
    }

    public Map<NodeId, Set<NodeLabel>> getNodeToLabelsMap() {
      Map<NodeId, Set<NodeLabel>> map = new HashMap<NodeId, Set<NodeLabel>>();
      Set<NodeLabel> setNodeLabels = new HashSet<NodeLabel>(Arrays.asList(
          NodeLabel.newInstance("x", false),
          NodeLabel.newInstance("y", false)));
      map.put(NodeId.newInstance("host", 0), setNodeLabels);
      return map;
    }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/protocolrecords/impl/pb/GetLabelsToNodesResponsePBImpl.java
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeLabelPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.proto.YarnProtos.LabelsToNodeIdsProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeLabelProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetLabelsToNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetLabelsToNodesResponseProtoOrBuilder;

  GetLabelsToNodesResponseProto.Builder builder = null;
  boolean viaProto = false;

  private Map<NodeLabel, Set<NodeId>> labelsToNodes;

  public GetLabelsToNodesResponsePBImpl() {
    this.builder = GetLabelsToNodesResponseProto.newBuilder();
    }
    GetLabelsToNodesResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<LabelsToNodeIdsProto> list = p.getLabelsToNodesList();
    this.labelsToNodes = new HashMap<NodeLabel, Set<NodeId>>();

    for (LabelsToNodeIdsProto c : list) {
      Set<NodeId> setNodes = new HashSet<NodeId>();
        setNodes.add(node);
      }
      if (!setNodes.isEmpty()) {
        this.labelsToNodes
            .put(new NodeLabelPBImpl(c.getNodeLabels()), setNodes);
      }
    }
  }
          public Iterator<LabelsToNodeIdsProto> iterator() {
            return new Iterator<LabelsToNodeIdsProto>() {

              Iterator<Entry<NodeLabel, Set<NodeId>>> iter =
                  labelsToNodes.entrySet().iterator();

              @Override

              @Override
              public LabelsToNodeIdsProto next() {
                Entry<NodeLabel, Set<NodeId>> now = iter.next();
                Set<NodeIdProto> nodeProtoSet = new HashSet<NodeIdProto>();
                for(NodeId n : now.getValue()) {
                  nodeProtoSet.add(convertToProtoFormat(n));
                }
                return LabelsToNodeIdsProto.newBuilder()
                    .setNodeLabels(convertToProtoFormat(now.getKey()))
                    .addAllNodeId(nodeProtoSet)
                    .build();
              }

    return ((NodeIdPBImpl)t).getProto();
  }

  private NodeLabelProto convertToProtoFormat(NodeLabel l) {
    return ((NodeLabelPBImpl)l).getProto();
  }

  @Override
  public int hashCode() {
    assert false : "hashCode not designed";
  @Override
  @Public
  @Evolving
  public void setLabelsToNodes(Map<NodeLabel, Set<NodeId>> map) {
    initLabelsToNodes();
    labelsToNodes.clear();
    labelsToNodes.putAll(map);
  @Override
  @Public
  @Evolving
  public Map<NodeLabel, Set<NodeId>> getLabelsToNodes() {
    initLabelsToNodes();
    return this.labelsToNodes;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/protocolrecords/impl/pb/GetNodesToLabelsResponsePBImpl.java
package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeLabelPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdToLabelsInfoProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeLabelProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNodesToLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNodesToLabelsResponseProtoOrBuilder;

  GetNodesToLabelsResponseProto.Builder builder = null;
  boolean viaProto = false;

  private Map<NodeId, Set<NodeLabel>> nodeToLabels;

  public GetNodesToLabelsResponsePBImpl() {
    this.builder = GetNodesToLabelsResponseProto.newBuilder();
      return;
    }
    GetNodesToLabelsResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<NodeIdToLabelsInfoProto> list = p.getNodeToLabelsList();
    this.nodeToLabels = new HashMap<NodeId, Set<NodeLabel>>();

    for (NodeIdToLabelsInfoProto c : list) {
      Set<NodeLabel> labels = new HashSet<NodeLabel>();
      for (NodeLabelProto l : c.getNodeLabelsList()) {
        labels.add(new NodeLabelPBImpl(l));
      }
      this.nodeToLabels.put(new NodeIdPBImpl(c.getNodeId()), labels);
    }
  }

    if (nodeToLabels == null) {
      return;
    }
    Iterable<NodeIdToLabelsInfoProto> iterable =
        new Iterable<NodeIdToLabelsInfoProto>() {
          @Override
          public Iterator<NodeIdToLabelsInfoProto> iterator() {
            return new Iterator<NodeIdToLabelsInfoProto>() {

              Iterator<Entry<NodeId, Set<NodeLabel>>> iter = nodeToLabels
                  .entrySet().iterator();

              @Override
              }

              @Override
              public NodeIdToLabelsInfoProto next() {
                Entry<NodeId, Set<NodeLabel>> now = iter.next();
                Set<NodeLabelProto> labelProtoList =
                    new HashSet<NodeLabelProto>();
                for (NodeLabel l : now.getValue()) {
                  labelProtoList.add(convertToProtoFormat(l));
                }
                return NodeIdToLabelsInfoProto.newBuilder()
                    .setNodeId(convertToProtoFormat(now.getKey()))
                    .addAllNodeLabels(labelProtoList).build();
              }

              @Override
  }

  @Override
  public Map<NodeId, Set<NodeLabel>> getNodeToLabels() {
    initNodeToLabels();
    return this.nodeToLabels;
  }

  @Override
  public void setNodeToLabels(Map<NodeId, Set<NodeLabel>> map) {
    initNodeToLabels();
    nodeToLabels.clear();
    nodeToLabels.putAll(map);
    return ((NodeIdPBImpl)t).getProto();
  }
  
  private NodeLabelProto convertToProtoFormat(NodeLabel t) {
    return ((NodeLabelPBImpl)t).getProto();
  }

  @Override
  public int hashCode() {
    assert false : "hashCode not designed";

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/ReplaceLabelsOnNodeRequestPBImpl.java
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.NodeIdToLabelsNameProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
      return;
    }
    ReplaceLabelsOnNodeRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<NodeIdToLabelsNameProto> list = p.getNodeToLabelsList();
    this.nodeIdToLabels = new HashMap<NodeId, Set<String>>();

    for (NodeIdToLabelsNameProto c : list) {
      this.nodeIdToLabels.put(new NodeIdPBImpl(c.getNodeId()),
          Sets.newHashSet(c.getNodeLabelsList()));
    }
    if (nodeIdToLabels == null) {
      return;
    }
    Iterable<NodeIdToLabelsNameProto> iterable =
        new Iterable<NodeIdToLabelsNameProto>() {
          @Override
          public Iterator<NodeIdToLabelsNameProto> iterator() {
            return new Iterator<NodeIdToLabelsNameProto>() {

              Iterator<Entry<NodeId, Set<String>>> iter = nodeIdToLabels
                  .entrySet().iterator();
              }

              @Override
              public NodeIdToLabelsNameProto next() {
                Entry<NodeId, Set<String>> now = iter.next();
                return NodeIdToLabelsNameProto.newBuilder()
                    .setNodeId(convertToProtoFormat(now.getKey())).clearNodeLabels()
                    .addAllNodeLabels(now.getValue()).build();
              }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ClientRMService.java
      GetNodesToLabelsRequest request) throws YarnException, IOException {
    RMNodeLabelsManager labelsMgr = rmContext.getNodeLabelManager();
    GetNodesToLabelsResponse response =
        GetNodesToLabelsResponse.newInstance(labelsMgr.getNodeLabelsInfo());
    return response;
  }

    RMNodeLabelsManager labelsMgr = rmContext.getNodeLabelManager();
    if (request.getNodeLabels() == null || request.getNodeLabels().isEmpty()) {
      return GetLabelsToNodesResponse.newInstance(
          labelsMgr.getLabelsInfoToNodes());
    } else {
      return GetLabelsToNodesResponse.newInstance(
          labelsMgr.getLabelsInfoToNodes(request.getNodeLabels()));
    }
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestClientRMService.java
      };
    };
    rm.start();
    NodeLabel labelX = NodeLabel.newInstance("x", false);
    NodeLabel labelY = NodeLabel.newInstance("y");
    RMNodeLabelsManager labelsMgr = rm.getRMContext().getNodeLabelManager();
    labelsMgr.addToCluserNodeLabels(ImmutableSet.of(labelX, labelY));

    NodeId node1 = NodeId.newInstance("host1", 1234);
    NodeId node2 = NodeId.newInstance("host2", 1234);
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    ApplicationClientProtocol client = (ApplicationClientProtocol) rpc
        .getProxy(ApplicationClientProtocol.class, rmAddress, conf);

    GetClusterNodeLabelsResponse response = client
        .getClusterNodeLabels(GetClusterNodeLabelsRequest.newInstance());
    Assert.assertTrue(response.getNodeLabels().containsAll(
        Arrays.asList(labelX, labelY)));

    GetNodesToLabelsResponse response1 = client
        .getNodeToLabels(GetNodesToLabelsRequest.newInstance());
    Map<NodeId, Set<NodeLabel>> nodeToLabels = response1.getNodeToLabels();
    Assert.assertTrue(nodeToLabels.keySet().containsAll(
        Arrays.asList(node1, node2)));
    Assert.assertTrue(nodeToLabels.get(node1)
        .containsAll(Arrays.asList(labelX)));
    Assert.assertTrue(nodeToLabels.get(node2)
        .containsAll(Arrays.asList(labelY)));
    for (NodeLabel x : nodeToLabels.get(node1)) {
      Assert.assertFalse(x.isExclusive());
    }
    for (NodeLabel y : nodeToLabels.get(node2)) {
      Assert.assertTrue(y.isExclusive());
    }
    Assert.assertFalse(nodeToLabels.get(node1).containsAll(
        Arrays.asList(NodeLabel.newInstance("x"))));

    rpc.stopProxy(client, conf);
    rm.close();
      };
    };
    rm.start();

    NodeLabel labelX = NodeLabel.newInstance("x", false);
    NodeLabel labelY = NodeLabel.newInstance("y", false);
    NodeLabel labelZ = NodeLabel.newInstance("z", false);
    RMNodeLabelsManager labelsMgr = rm.getRMContext().getNodeLabelManager();
    labelsMgr.addToCluserNodeLabels(ImmutableSet.of(labelX, labelY, labelZ));

    NodeId node1A = NodeId.newInstance("host1", 1234);
    NodeId node1B = NodeId.newInstance("host1", 5678);
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    ApplicationClientProtocol client = (ApplicationClientProtocol) rpc
        .getProxy(ApplicationClientProtocol.class, rmAddress, conf);

    GetClusterNodeLabelsResponse response = client
        .getClusterNodeLabels(GetClusterNodeLabelsRequest.newInstance());
    Assert.assertTrue(response.getNodeLabels().containsAll(
        Arrays.asList(labelX, labelY, labelZ)));

    GetLabelsToNodesResponse response1 = client
        .getLabelsToNodes(GetLabelsToNodesRequest.newInstance());
    Map<NodeLabel, Set<NodeId>> labelsToNodes = response1.getLabelsToNodes();
    for (Map.Entry<NodeLabel, Set<NodeId>> nltn : labelsToNodes.entrySet()) {
      Assert.assertFalse(nltn.getKey().isExclusive());
    }
    Assert.assertTrue(labelsToNodes.keySet().containsAll(
        Arrays.asList(labelX, labelY, labelZ)));
    Assert.assertTrue(labelsToNodes.get(labelX).containsAll(
        Arrays.asList(node1A)));
    Assert.assertTrue(labelsToNodes.get(labelY).containsAll(
        Arrays.asList(node2A, node3A)));
    Assert.assertTrue(labelsToNodes.get(labelZ).containsAll(
        Arrays.asList(node1B, node3B)));

    Set<String> setlabels = new HashSet<String>(Arrays.asList(new String[]{"x",
        "z"}));
    GetLabelsToNodesResponse response2 = client
        .getLabelsToNodes(GetLabelsToNodesRequest.newInstance(setlabels));
    labelsToNodes = response2.getLabelsToNodes();
    for (Map.Entry<NodeLabel, Set<NodeId>> nltn : labelsToNodes.entrySet()) {
      Assert.assertFalse(nltn.getKey().isExclusive());
    }
    Assert.assertTrue(labelsToNodes.keySet().containsAll(
        Arrays.asList(labelX, labelZ)));
    Assert.assertTrue(labelsToNodes.get(labelX).containsAll(
        Arrays.asList(node1A)));
    Assert.assertTrue(labelsToNodes.get(labelZ).containsAll(
        Arrays.asList(node1B, node3B)));
    Assert.assertEquals(labelsToNodes.get(labelY), null);

    rpc.stopProxy(client, conf);
    rm.close();

