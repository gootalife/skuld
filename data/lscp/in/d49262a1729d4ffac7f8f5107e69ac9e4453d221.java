hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/NodeIDsInfo.java

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "labelsToNodesInfo")
  @XmlElement(name="nodes")
  protected ArrayList<String> nodeIDsList = new ArrayList<String>();

  public NodeIDsInfo() {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMWebServices.java
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LocalResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewApplication;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntry;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntryList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;

    NodeToLabelsInfo ntl = new NodeToLabelsInfo();
    HashMap<String, NodeLabelsInfo> ntlMap = ntl.getNodeToLabels();
    Map<NodeId, Set<String>> nodeIdToLabels = rm.getRMContext()
        .getNodeLabelManager().getNodeLabels();

    for (Map.Entry<NodeId, Set<String>> nitle : nodeIdToLabels.entrySet()) {
      ntlMap.put(nitle.getKey().toString(),
    init();

    LabelsToNodesInfo lts = new LabelsToNodesInfo();
    Map<NodeLabelInfo, NodeIDsInfo> ltsMap = lts.getLabelsToNodes();
    Map<String, Set<NodeId>> labelsToNodeId = null;
    if (labels == null || labels.size() == 0) {
      labelsToNodeId =
      for (NodeId nodeId : entry.getValue()) {
        nodeIdStrList.add(nodeId.toString());
      }
      ltsMap.put(new NodeLabelInfo(entry.getKey()), new NodeIDsInfo(
          nodeIdStrList));
    }
    return lts;
  }
  @POST
  @Path("/replace-node-to-labels")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response replaceLabelsOnNodes(final NodeToLabelsEntryList newNodeToLabels,
      @Context HttpServletRequest hsr) throws IOException {
    Map<NodeId, Set<String>> nodeIdToLabels =
        new HashMap<NodeId, Set<String>>();

    for (NodeToLabelsEntry nitle : newNodeToLabels.getNodeToLabels()) {
      nodeIdToLabels.put(
          ConverterUtils.toNodeIdWithDefaultPort(nitle.getNodeId()),
          new HashSet<String>(nitle.getNodeLabels()));
    }

    return replaceLabelsOnNode(nodeIdToLabels, hsr, "/replace-node-to-labels");
  @POST
  @Path("/nodes/{nodeId}/replace-labels")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response replaceLabelsOnNode(
      @QueryParam("labels") Set<String> newNodeLabelsName,
      @Context HttpServletRequest hsr, @PathParam("nodeId") String nodeId)
      throws Exception {
    NodeId nid = ConverterUtils.toNodeIdWithDefaultPort(nodeId);
    Map<NodeId, Set<String>> newLabelsForNode =
        new HashMap<NodeId, Set<String>>();
    newLabelsForNode.put(nid,
        new HashSet<String>(newNodeLabelsName));

    return replaceLabelsOnNode(newLabelsForNode, hsr,
        "/nodes/nodeid/replace-labels");
  }

  private Response replaceLabelsOnNode(
    throws IOException {
    init();

    List<NodeLabel> nodeLabels = rm.getRMContext().getNodeLabelManager()
        .getClusterNodeLabels();
    NodeLabelsInfo ret = new NodeLabelsInfo(nodeLabels);

    return ret;
  }
    }
    
    rm.getRMContext().getNodeLabelManager()
        .addToCluserNodeLabels(newNodeLabels.getNodeLabels());
            
    return Response.status(Status.OK).build();

  @POST
  @Path("/remove-node-labels")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response removeFromCluserNodeLabels(
      @QueryParam("labels") Set<String> oldNodeLabels,
      @Context HttpServletRequest hsr) throws Exception {
    init();

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
      throw new AuthorizationException(msg);
    }

    rm.getRMContext()
        .getNodeLabelManager()
        .removeFromClusterNodeLabels(
            new HashSet<String>(oldNodeLabels));

    return Response.status(Status.OK).build();
  }
  
  @GET

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/LabelsToNodesInfo.java
@XmlAccessorType(XmlAccessType.FIELD)
public class LabelsToNodesInfo {

  protected Map<NodeLabelInfo, NodeIDsInfo> labelsToNodes =
    new HashMap<NodeLabelInfo, NodeIDsInfo>();

  public LabelsToNodesInfo() {
  } // JAXB needs this

  public Map<NodeLabelInfo, NodeIDsInfo> getLabelsToNodes() {
   return labelsToNodes;
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/NodeLabelInfo.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/NodeLabelInfo.java

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.NodeLabel;

@XmlRootElement(name = "nodeLabelInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeLabelInfo {

  private String name;
  private boolean exclusivity;

  public NodeLabelInfo() {
  }

  public NodeLabelInfo(String name) {
    this.name = name;
    this.exclusivity = true;
  }

  public NodeLabelInfo(String name, boolean exclusivity) {
    this.name = name;
    this.exclusivity = exclusivity;
  }

  public NodeLabelInfo(NodeLabel label) {
    this.name = label.getName();
    this.exclusivity = label.isExclusive();
  }

  public String getName() {
    return name;
  }

  public boolean getExclusivity() {
    return exclusivity;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    NodeLabelInfo other = (NodeLabelInfo) obj;
    if (!getName().equals(other.getName())) {
      return false;
    }
    if (getExclusivity() != other.getExclusivity()) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return (getName().hashCode() << 16) + (getExclusivity() ? 1 : 0);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/NodeLabelsInfo.java

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.NodeLabel;

@XmlRootElement(name = "nodeLabelsInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeLabelsInfo {

  @XmlElement(name = "nodeLabelInfo")
  private ArrayList<NodeLabelInfo> nodeLabelsInfo =
    new ArrayList<NodeLabelInfo>();

  public NodeLabelsInfo() {
  }

  public NodeLabelsInfo(ArrayList<NodeLabelInfo> nodeLabels) {
    this.nodeLabelsInfo = nodeLabels;
  }

  public NodeLabelsInfo(List<NodeLabel> nodeLabels) {
    this.nodeLabelsInfo = new ArrayList<NodeLabelInfo>();
    for (NodeLabel label : nodeLabels) {
      this.nodeLabelsInfo.add(new NodeLabelInfo(label));
    }
  }
  
  public NodeLabelsInfo(Set<String> nodeLabelsName) {
    this.nodeLabelsInfo = new ArrayList<NodeLabelInfo>();
    for (String labelName : nodeLabelsName) {
      this.nodeLabelsInfo.add(new NodeLabelInfo(labelName));
    }
  }

  public ArrayList<NodeLabelInfo> getNodeLabelsInfo() {
    return nodeLabelsInfo;
  }

  public Set<NodeLabel> getNodeLabels() {
    Set<NodeLabel> nodeLabels = new HashSet<NodeLabel>();
    for (NodeLabelInfo label : nodeLabelsInfo) {
      nodeLabels.add(NodeLabel.newInstance(label.getName(),
          label.getExclusivity()));
    }
    return nodeLabels;
  }
  
  public List<String> getNodeLabelsName() {
    ArrayList<String> nodeLabelsName = new ArrayList<String>();
    for (NodeLabelInfo label : nodeLabelsInfo) {
      nodeLabelsName.add(label.getName());
    }
    return nodeLabelsName;
  }

  public void setNodeLabelsInfo(ArrayList<NodeLabelInfo> nodeLabelInfo) {
    this.nodeLabelsInfo = nodeLabelInfo;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/NodeToLabelsEntry.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/NodeToLabelsEntry.java

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.*;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlElement;

@XmlRootElement(name = "nodeToLabelsEntry")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeToLabelsEntry {

  @XmlElement(name = "nodeId")
  private String nodeId;

  @XmlElement(name = "labels")
  private ArrayList<String> labels = new ArrayList<String>();

  public NodeToLabelsEntry() {
  }

  public NodeToLabelsEntry(String nodeId, ArrayList<String> labels) {
    this.nodeId = nodeId;
    this.labels = labels;
  }

  public String getNodeId() {
    return nodeId;
  }

  public ArrayList<String> getNodeLabels() {
    return labels;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/NodeToLabelsEntryList.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/NodeToLabelsEntryList.java

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.*;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "nodeToLabelsName")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeToLabelsEntryList {

  protected ArrayList<NodeToLabelsEntry> nodeToLabels =
      new ArrayList<NodeToLabelsEntry>();

  public NodeToLabelsEntryList() {
  }

  public ArrayList<NodeToLabelsEntry> getNodeToLabels() {
    return nodeToLabels;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/NodeToLabelsInfo.java
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeToLabelsInfo {

  private HashMap<String, NodeLabelsInfo> nodeToLabels =
      new HashMap<String, NodeLabelsInfo>();

  public NodeToLabelsInfo() {
  }

  public HashMap<String, NodeLabelsInfo> getNodeToLabels() {
    return nodeToLabels;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServicesNodeLabels.java

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntry;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntryList;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.codehaus.jettison.json.JSONException;
import org.junit.Test;

import com.google.inject.Guice;
    WebResource r = resource();

    ClientResponse response;

    NodeLabelsInfo nlsifo = new NodeLabelsInfo();
    nlsifo.getNodeLabelsInfo().add(new NodeLabelInfo("a"));
    response =
        r.path("ws").path("v1").path("cluster")
            .path("add-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(nlsifo, NodeLabelsInfo.class), MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals("a", nlsifo.getNodeLabelsInfo().get(0).getName());
    assertEquals(1, nlsifo.getNodeLabels().size());
    
    nlsifo = new NodeLabelsInfo();
    nlsifo.getNodeLabelsInfo().add(new NodeLabelInfo("b"));
    response =
        r.path("ws").path("v1").path("cluster")
            .path("add-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(nlsifo, NodeLabelsInfo.class), MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(2, nlsifo.getNodeLabels().size());
    
    MultivaluedMapImpl params = new MultivaluedMapImpl();
    params.add("labels", "a");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    params = new MultivaluedMapImpl();
    params.add("labels", "b");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid1:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    params = new MultivaluedMapImpl();
    params.add("labels", "b");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid2:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    LabelsToNodesInfo ltni = response.getEntity(LabelsToNodesInfo.class);
    assertEquals(2, ltni.getLabelsToNodes().size());
    NodeIDsInfo nodes = ltni.getLabelsToNodes().get(new NodeLabelInfo("b"));
    assertTrue(nodes.getNodeIDs().contains("nid2:0"));
    assertTrue(nodes.getNodeIDs().contains("nid1:0"));
    nodes = ltni.getLabelsToNodes().get(new NodeLabelInfo("a"));
    assertTrue(nodes.getNodeIDs().contains("nid:0"));

    params = new MultivaluedMapImpl();
    params.add("labels", "a");
    response =
        r.path("ws").path("v1").path("cluster")
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ltni = response.getEntity(LabelsToNodesInfo.class);
    assertEquals(1, ltni.getLabelsToNodes().size());
    nodes = ltni.getLabelsToNodes().get(new NodeLabelInfo("a"));
    assertTrue(nodes.getNodeIDs().contains("nid:0"));

            .path("get-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsName().contains("a"));

    
    params = new MultivaluedMapImpl();
    params.add("labels", "b");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

            .path("get-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsName().contains("b"));
            
    NodeToLabelsEntryList ntli = new NodeToLabelsEntryList();
    ArrayList<String> labels = new ArrayList<String>();
    labels.add("a");
    NodeToLabelsEntry nli = new NodeToLabelsEntry("nid:0", labels);
    ntli.getNodeToLabels().add(nli);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("replace-node-to-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(ntli, NodeToLabelsEntryList.class),
              MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
        
            .path("get-node-to-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    NodeToLabelsInfo ntlinfo = response.getEntity(NodeToLabelsInfo.class);
    NodeLabelsInfo nlinfo = ntlinfo.getNodeToLabels().get("nid:0");
    assertEquals(1, nlinfo.getNodeLabels().size());
    assertTrue(nlinfo.getNodeLabelsName().contains("a"));
    
    params = new MultivaluedMapImpl();
    params.add("labels", "");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");
            .path("get-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsName().contains(""));
    
    params = new MultivaluedMapImpl();
    params.add("labels", "a");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

            .path("get-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsName().contains("a"));
    
    params = new MultivaluedMapImpl();
    params.add("labels", "b");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("replace-labels")
            .queryParam("user.name", notUserName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    response =
            .path("get-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsName().contains("a"));
    
    response =
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(2, nlsifo.getNodeLabels().size());
    
    params = new MultivaluedMapImpl();
    params.add("labels", "b");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("remove-node-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    response =
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals("a", nlsifo.getNodeLabelsInfo().get(0).getName());
    assertEquals(1, nlsifo.getNodeLabels().size());
    
    params = new MultivaluedMapImpl();
    params.add("labels", "a");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("remove-node-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    response =
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(0, nlsifo.getNodeLabels().size());

    nlsifo = new NodeLabelsInfo();
    nlsifo.getNodeLabelsInfo().add(new NodeLabelInfo("x"));
    nlsifo.getNodeLabelsInfo().add(new NodeLabelInfo("y"));
    response =
        r.path("ws")
            .path("v1")
            .path("add-node-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(nlsifo, NodeLabelsInfo.class),
                MediaType.APPLICATION_JSON).post(ClientResponse.class);
    nlsifo = new NodeLabelsInfo();
    nlsifo.getNodeLabelsInfo().add(new NodeLabelInfo("y"));
    params = new MultivaluedMapImpl();
    params.add("labels", "y");
    response =
        r.path("ws").path("v1").path("cluster").path("nodes").path("nid:0")
            .path("replace-labels").queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    rmWebService.isDistributedNodeLabelConfiguration = true;

    ntli = new NodeToLabelsEntryList();
    labels = new ArrayList<String>();
    labels.add("x");
    nli = new NodeToLabelsEntry("nid:0", labels);
    ntli.getNodeToLabels().add(nli);
    response =
        r.path("ws")
            .path("v1")
            .path("replace-node-to-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(ntli, NodeToLabelsEntryList.class),
                MediaType.APPLICATION_JSON).post(ClientResponse.class);

            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ntlinfo = response.getEntity(NodeToLabelsInfo.class);
    nlinfo = ntlinfo.getNodeToLabels().get("nid:0");
    assertEquals(1, nlinfo.getNodeLabels().size());
    assertFalse(nlinfo.getNodeLabels().contains("x"));

    response =
        r.path("ws").path("v1").path("cluster").path("nodes").path("nid:0")
            .path("replace-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabelName\": [\"x\"]}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ntlinfo = response.getEntity(NodeToLabelsInfo.class);
    nlinfo = ntlinfo.getNodeToLabels().get("nid:0");
    assertEquals(1, nlinfo.getNodeLabels().size());
    assertFalse(nlinfo.getNodeLabels().contains("x"));

    params = new MultivaluedMapImpl();
    params.add("labels", "x");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("remove-node-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    response =
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals("y", nlsifo.getNodeLabelsInfo().get(0).getName());

    params = new MultivaluedMapImpl();
    params.add("labels", "y");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("remove-node-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsInfo().isEmpty());

    nlsifo = new NodeLabelsInfo();
    nlsifo.getNodeLabelsInfo().add(new NodeLabelInfo("z", false));
    response =
        r.path("ws")
            .path("v1")
            .path("cluster")
            .path("add-node-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(nlsifo, NodeLabelsInfo.class),
                MediaType.APPLICATION_JSON).post(ClientResponse.class);

    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals("z", nlsifo.getNodeLabelsInfo().get(0).getName());
    assertEquals(false, nlsifo.getNodeLabelsInfo().get(0).getExclusivity());
    assertEquals(1, nlsifo.getNodeLabels().size());
  }

  @SuppressWarnings("rawtypes")

