hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/NodeIDsInfo.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/NodeIDsInfo.java

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "labelsToNodesInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeIDsInfo {

  protected ArrayList<String> nodeIDsList = new ArrayList<String>();

  public NodeIDsInfo() {
  } // JAXB needs this

  public NodeIDsInfo(List<String> nodeIdsList) {
    this.nodeIDsList.addAll(nodeIdsList);
  }

  public ArrayList<String> getNodeIDs() {
    return nodeIDsList;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMWebServices.java

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.ByteBuffer;
import java.security.AccessControlException;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppQueue;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.DelegationToken;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FifoSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LocalResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewApplication;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.StatisticsItemInfo;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.AdHocLogDumper;
import org.apache.hadoop.yarn.util.ConverterUtils;
    return ntl;
  }

  @GET
  @Path("/label-mappings")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public LabelsToNodesInfo getLabelsToNodes(
      @QueryParam("labels") Set<String> labels) throws IOException {
    init();

    LabelsToNodesInfo lts = new LabelsToNodesInfo();
    Map<String, NodeIDsInfo> ltsMap = lts.getLabelsToNodes();
    Map<String, Set<NodeId>> labelsToNodeId = null;
    if (labels == null || labels.size() == 0) {
      labelsToNodeId =
          rm.getRMContext().getNodeLabelManager().getLabelsToNodes();
    } else {
      labelsToNodeId =
          rm.getRMContext().getNodeLabelManager().getLabelsToNodes(labels);
    }

    for (Entry<String, Set<NodeId>> entry : labelsToNodeId.entrySet()) {
      List<String> nodeIdStrList = new ArrayList<String>();
      for (NodeId nodeId : entry.getValue()) {
        nodeIdStrList.add(nodeId.toString());
      }
      ltsMap.put(entry.getKey(), new NodeIDsInfo(nodeIdStrList));
    }
    return lts;
  }

  @POST
  @Path("/replace-node-to-labels")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/LabelsToNodesInfo.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/LabelsToNodesInfo.java

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.NodeIDsInfo;

@XmlRootElement(name = "labelsToNodesInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class LabelsToNodesInfo {

  protected Map<String, NodeIDsInfo> labelsToNodes =
    new HashMap<String, NodeIDsInfo>();

  public LabelsToNodesInfo() {
  } // JAXB needs this

  public Map<String, NodeIDsInfo> getLabelsToNodes() {
   return labelsToNodes;
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServicesNodeLabels.java
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONMarshaller;
import com.sun.jersey.api.json.JSONUnmarshaller;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid1:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\": [\"b\"]}",
              MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid2:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\": [\"b\"]}",
              MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    response =
        r.path("ws").path("v1").path("cluster")
            .path("label-mappings").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    LabelsToNodesInfo ltni = response.getEntity(LabelsToNodesInfo.class);
    assertEquals(2, ltni.getLabelsToNodes().size());
    NodeIDsInfo nodes = ltni.getLabelsToNodes().get("b");
    assertTrue(nodes.getNodeIDs().contains("nid2:0"));
    assertTrue(nodes.getNodeIDs().contains("nid1:0"));
    nodes = ltni.getLabelsToNodes().get("a");
    assertTrue(nodes.getNodeIDs().contains("nid:0"));

    MultivaluedMapImpl params = new MultivaluedMapImpl();
    params.add("labels", "a");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("label-mappings").queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ltni = response.getEntity(LabelsToNodesInfo.class);
    assertEquals(1, ltni.getLabelsToNodes().size());
    nodes = ltni.getLabelsToNodes().get("a");
    assertTrue(nodes.getNodeIDs().contains("nid:0"));

    response =
        r.path("ws").path("v1").path("cluster")

