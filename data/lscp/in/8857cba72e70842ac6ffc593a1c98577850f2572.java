hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/nodelabels/CommonNodeLabelsManager.java
    }
  }
  
  public Set<NodeLabel> getLabelsInfoByNode(NodeId nodeId) {
    try {
      readLock.lock();
      Set<String> labels = getLabelsByNode(nodeId, nodeCollections);
      if (labels.isEmpty()) {
        return EMPTY_NODELABEL_SET;
      }
      Set<NodeLabel> nodeLabels = createNodeLabelFromLabelNames(labels);
      return nodeLabels;
    } finally {
      readLock.unlock();
    }
  }

  private Set<NodeLabel> createNodeLabelFromLabelNames(Set<String> labels) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/nodelabels/RMNodeLabelsManager.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMWebServices.java

    NodeToLabelsInfo ntl = new NodeToLabelsInfo();
    HashMap<String, NodeLabelsInfo> ntlMap = ntl.getNodeToLabels();
    Map<NodeId, Set<NodeLabel>> nodeIdToLabels = rm.getRMContext()
        .getNodeLabelManager().getNodeLabelsInfo();

    for (Map.Entry<NodeId, Set<NodeLabel>> nitle : nodeIdToLabels.entrySet()) {
      List<NodeLabel> labels = new ArrayList<NodeLabel>(nitle.getValue());
      ntlMap.put(nitle.getKey().toString(), new NodeLabelsInfo(labels));
    }

    return ntl;

    LabelsToNodesInfo lts = new LabelsToNodesInfo();
    Map<NodeLabelInfo, NodeIDsInfo> ltsMap = lts.getLabelsToNodes();
    Map<NodeLabel, Set<NodeId>> labelsToNodeId = null;
    if (labels == null || labels.size() == 0) {
      labelsToNodeId =
          rm.getRMContext().getNodeLabelManager().getLabelsInfoToNodes();
    } else {
      labelsToNodeId =
          rm.getRMContext().getNodeLabelManager().getLabelsInfoToNodes(labels);
    }

    for (Entry<NodeLabel, Set<NodeId>> entry : labelsToNodeId.entrySet()) {
      List<String> nodeIdStrList = new ArrayList<String>();
      for (NodeId nodeId : entry.getValue()) {
        nodeIdStrList.add(nodeId.toString());
  @Path("/nodes/{nodeId}/get-labels")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public NodeLabelsInfo getLabelsOnNode(@Context HttpServletRequest hsr,
      @PathParam("nodeId") String nodeId) throws IOException {
    init();

    NodeId nid = ConverterUtils.toNodeIdWithDefaultPort(nodeId);
    List<NodeLabel> labels = new ArrayList<NodeLabel>(rm.getRMContext()
        .getNodeLabelManager().getLabelsInfoByNode(nid));
    return new NodeLabelsInfo(labels);
  }

  protected Response killApp(RMApp app, UserGroupInformation callerUGI,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServicesNodeLabels.java
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(1, nlsifo.getNodeLabels().size());
    for (NodeLabelInfo nl : nlsifo.getNodeLabelsInfo()) {
      assertEquals("a", nl.getName());
      assertTrue(nl.getExclusivity());
    }
    
    nlsifo = new NodeLabelsInfo();
    nlsifo.getNodeLabelsInfo().add(new NodeLabelInfo("b", false));
    response =
        r.path("ws").path("v1").path("cluster")
            .path("add-node-labels").queryParam("user.name", userName)
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(2, nlsifo.getNodeLabels().size());
    for (NodeLabelInfo nl : nlsifo.getNodeLabelsInfo()) {
      if (nl.getName().equals("b")) {
        assertFalse(nl.getExclusivity());
      }
    }
    
    MultivaluedMapImpl params = new MultivaluedMapImpl();
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    LabelsToNodesInfo ltni = response.getEntity(LabelsToNodesInfo.class);
    assertEquals(2, ltni.getLabelsToNodes().size());
    NodeIDsInfo nodes = ltni.getLabelsToNodes().get(
        new NodeLabelInfo("b", false));
    assertTrue(nodes.getNodeIDs().contains("nid2:0"));
    assertTrue(nodes.getNodeIDs().contains("nid1:0"));
    nodes = ltni.getLabelsToNodes().get(new NodeLabelInfo("a"));
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsInfo().contains(new NodeLabelInfo("a")));

    
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsInfo().contains(
        new NodeLabelInfo("b", false)));
            
    NodeToLabelsEntryList ntli = new NodeToLabelsEntryList();
    NodeToLabelsInfo ntlinfo = response.getEntity(NodeToLabelsInfo.class);
    NodeLabelsInfo nlinfo = ntlinfo.getNodeToLabels().get("nid:0");
    assertEquals(1, nlinfo.getNodeLabels().size());
    assertTrue(nlinfo.getNodeLabelsInfo().contains(new NodeLabelInfo("a")));
    
    params = new MultivaluedMapImpl();
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsInfo().isEmpty());
    
    params = new MultivaluedMapImpl();
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsInfo().contains(new NodeLabelInfo("a")));
    
    params = new MultivaluedMapImpl();
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsInfo().contains(new NodeLabelInfo("a")));
    
    response =
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(1, nlsifo.getNodeLabels().size());
    for (NodeLabelInfo nl : nlsifo.getNodeLabelsInfo()) {
      assertEquals("a", nl.getName());
      assertTrue(nl.getExclusivity());
    }
    
    params = new MultivaluedMapImpl();
    nlsifo = new NodeLabelsInfo();
    nlsifo.getNodeLabelsInfo().add(new NodeLabelInfo("x", false));
    nlsifo.getNodeLabelsInfo().add(new NodeLabelInfo("y", false));
    response =
        r.path("ws")
            .path("v1")
            .entity(toJson(nlsifo, NodeLabelsInfo.class),
                MediaType.APPLICATION_JSON).post(ClientResponse.class);
    params = new MultivaluedMapImpl();
    params.add("labels", "y");
    response =
    ntlinfo = response.getEntity(NodeToLabelsInfo.class);
    nlinfo = ntlinfo.getNodeToLabels().get("nid:0");
    assertEquals(1, nlinfo.getNodeLabels().size());
    assertFalse(nlinfo.getNodeLabelsInfo().contains(
        new NodeLabelInfo("x", false)));

    response =
    ntlinfo = response.getEntity(NodeToLabelsInfo.class);
    nlinfo = ntlinfo.getNodeToLabels().get("nid:0");
    assertEquals(1, nlinfo.getNodeLabels().size());
    assertFalse(nlinfo.getNodeLabelsInfo().contains(
        new NodeLabelInfo("x", false)));

    params = new MultivaluedMapImpl();
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(new NodeLabelInfo("y", false),
        nlsifo.getNodeLabelsInfo().get(0));
    assertEquals("y", nlsifo.getNodeLabelsInfo().get(0).getName());
    assertFalse(nlsifo.getNodeLabelsInfo().get(0).getExclusivity());

    params = new MultivaluedMapImpl();
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals("z", nlsifo.getNodeLabelsInfo().get(0).getName());
    assertFalse(nlsifo.getNodeLabelsInfo().get(0).getExclusivity());
    assertEquals(1, nlsifo.getNodeLabels().size());
  }


