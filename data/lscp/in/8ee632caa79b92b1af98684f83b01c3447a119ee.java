hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
  public static final String DEFAULT_NODELABEL_CONFIGURATION_TYPE =
      CENTALIZED_NODELABEL_CONFIGURATION_TYPE;

  @Private
  public static boolean isDistributedNodeLabelConfiguration(Configuration conf) {
    return DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE.equals(conf.get(
        NODELABEL_CONFIGURATION_TYPE, DEFAULT_NODELABEL_CONFIGURATION_TYPE));
  }

  public YarnConfiguration() {
    super();
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/nodelabels/CommonNodeLabelsManager.java
  protected NodeLabelsStore store;
  private boolean nodeLabelsEnabled = false;

  private boolean isDistributedNodeLabelConfiguration = false;

    nodeLabelsEnabled =
        conf.getBoolean(YarnConfiguration.NODE_LABELS_ENABLED,
            YarnConfiguration.DEFAULT_NODE_LABELS_ENABLED);

    isDistributedNodeLabelConfiguration  =
        YarnConfiguration.isDistributedNodeLabelConfiguration(conf);

    if (nodeLabelsEnabled) {
      initNodeLabelStore(conf);
    }
  protected void initNodeLabelStore(Configuration conf) throws Exception {
    this.store = new FileSystemNodeLabelsStore(this);
    this.store.init(conf);
    this.store.recover(isDistributedNodeLabelConfiguration);
  }

      }
    }
    
    if (null != dispatcher && !isDistributedNodeLabelConfiguration) {
      dispatcher.getEventHandler().handle(
          new UpdateNodeToLabelsMappingsEvent(newNMToLabels));
    }
      readLock.lock();
      List<NodeLabel> nodeLabels = new ArrayList<>();
      for (RMNodeLabel label : labelCollections.values()) {
        if (!label.getLabelName().equals(NO_LABEL)) {
          nodeLabels.add(NodeLabel.newInstance(label.getLabelName(),
              label.getIsExclusive()));
        }
      }
      return nodeLabels;
    } finally {
      readLock.unlock();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/nodelabels/FileSystemNodeLabelsStore.java
    ensureCloseEditlogFile();
  }

  @Override
  public void recover(boolean ignoreNodeToLabelsMappings) throws YarnException,
      IOException {
                new ReplaceLabelsOnNodeRequestPBImpl(
                    ReplaceLabelsOnNodeRequestProto.parseDelimitedFrom(is))
                    .getNodeToLabels();
            if (!ignoreNodeToLabelsMappings) {
              mgr.replaceLabelsOnNode(map);
            }
            break;
          }
          }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/nodelabels/NodeLabelsStore.java
      throws IOException;

  public abstract void recover(boolean ignoreNodeToLabelsMappings)
      throws IOException, YarnException;
  
  public void init(Configuration conf) throws Exception {}
  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/nodelabels/DummyCommonNodeLabelsManager.java
    this.store = new NodeLabelsStore(this) {

      @Override
      public void recover(boolean ignoreNodeToLabelsMappings)
          throws IOException {
      }

      @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/nodelabels/TestCommonNodeLabelsManager.java
      Assert.assertTrue(expectedAddedLabelNames.contains(label.getName()));
    }
  }

  @Test(timeout = 5000)
  public void testReplaceLabelsOnNodeInDistributedMode() throws Exception {
    mgr.stop();
    mgr = new DummyCommonNodeLabelsManager();
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);

    mgr.init(conf);
    mgr.start();

    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    Set<String> labelsByNode = mgr.getLabelsByNode(toNodeId("n1"));

    Assert.assertNull(
        "Labels are not expected to be written to the NodeLabelStore",
        mgr.lastNodeToLabels);
    Assert.assertNotNull("Updated labels should be available from the Mgr",
        labelsByNode);
    Assert.assertTrue(labelsByNode.contains("p1"));
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/nodelabels/TestFileSystemNodeLabelsStore.java
    mgr.stop();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 10000)
  public void testRecoverWithDistributedNodeLabels() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p4"));
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p5", "p6"));
    mgr.replaceLabelsOnNode((Map) ImmutableMap.of(toNodeId("n1"), toSet("p1"),
        toNodeId("n2"), toSet("p2")));
    mgr.replaceLabelsOnNode((Map) ImmutableMap.of(toNodeId("n3"), toSet("p3"),
        toNodeId("n4"), toSet("p4"), toNodeId("n5"), toSet("p5"),
        toNodeId("n6"), toSet("p6"), toNodeId("n7"), toSet("p6")));

    mgr.removeFromClusterNodeLabels(toSet("p1"));
    mgr.removeFromClusterNodeLabels(Arrays.asList("p3", "p5"));
    mgr.stop();

    mgr = new MockNodeLabelManager();
    Configuration cf = new Configuration(conf);
    cf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);
    mgr.init(cf);

    Assert.assertEquals(3, mgr.getClusterNodeLabels().size());
    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Arrays.asList("p2", "p4", "p6")));

    Assert.assertTrue("During recovery in distributed node-labels setup, "
        + "node to labels mapping should not be recovered ", mgr
        .getNodeLabels().size() == 0);

    mgr.stop();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 10000)
  public void testEditlogRecover() throws Exception {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/AdminService.java
  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  @VisibleForTesting
  boolean isDistributedNodeLabelConfiguration = false;

  public AdminService(ResourceManager rm, RMContext rmContext) {
    super(AdminService.class.getName());
    this.rm = rm;
        YarnConfiguration.DEFAULT_YARN_ADMIN_ACL)), UserGroupInformation
        .getCurrentUser());
    rmId = conf.get(YarnConfiguration.RM_HA_ID);

    isDistributedNodeLabelConfiguration =
        YarnConfiguration.isDistributedNodeLabelConfiguration(conf);

    super.serviceInit(conf);
  }

  @Override
  public RemoveFromClusterNodeLabelsResponse removeFromClusterNodeLabels(
      RemoveFromClusterNodeLabelsRequest request) throws YarnException, IOException {
    String operation = "removeFromClusterNodeLabels";
    final String msg = "remove labels.";

    UserGroupInformation user = checkAcls(operation);

    checkRMStatus(user.getShortUserName(), operation, msg);

    RemoveFromClusterNodeLabelsResponse response =
        recordFactory.newRecordInstance(RemoveFromClusterNodeLabelsResponse.class);
    try {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(request.getNodeLabels());
      RMAuditLogger
          .logSuccess(user.getShortUserName(), operation, "AdminService");
      return response;
    } catch (IOException ioe) {
      throw logAndWrapException(ioe, user.getShortUserName(), operation, msg);
    }
  }

  @Override
  public ReplaceLabelsOnNodeResponse replaceLabelsOnNode(
      ReplaceLabelsOnNodeRequest request) throws YarnException, IOException {
    String operation = "replaceLabelsOnNode";
    final String msg = "set node to labels.";

    checkAndThrowIfDistributedNodeLabelConfEnabled(operation);
    UserGroupInformation user = checkAcls(operation);

    checkRMStatus(user.getShortUserName(), operation, msg);

    ReplaceLabelsOnNodeResponse response =
        recordFactory.newRecordInstance(ReplaceLabelsOnNodeResponse.class);
      rmContext.getNodeLabelManager().replaceLabelsOnNode(
          request.getNodeToLabels());
      RMAuditLogger
          .logSuccess(user.getShortUserName(), operation, "AdminService");
      return response;
    } catch (IOException ioe) {
      throw logAndWrapException(ioe, user.getShortUserName(), operation, msg);
    }
  }

  private void checkRMStatus(String user, String operation, String msg)
      throws StandbyException {
    if (!isRMActive()) {
      RMAuditLogger.logFailure(user, operation, "",
          "AdminService", "ResourceManager is not active. Can not " + msg);
      throwStandbyException();
    }
  }

  private YarnException logAndWrapException(Exception exception, String user,
      String operation, String msg) throws YarnException {
    LOG.warn("Exception " + msg, exception);
    RMAuditLogger.logFailure(user, operation, "",
        "AdminService", "Exception " + msg);
    return RPCUtil.getRemoteException(exception);
  }

  private void checkAndThrowIfDistributedNodeLabelConfEnabled(String operation)
      throws YarnException {
    if (isDistributedNodeLabelConfiguration) {
      String msg =
          String.format("Error when invoke method=%s because of "
              + "distributed node label configuration enabled.", operation);
      LOG.error(msg);
      throw RPCUtil.getRemoteException(new IOException(msg));
    }
  }

  @Override
  public CheckForDecommissioningNodesResponse checkForDecommissioningNodes(
      CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest)

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ResourceTrackerService.java
  private int minAllocMb;
  private int minAllocVcores;

  private boolean isDistributedNodeLabelsConf;

  static {
    resync.setNodeAction(NodeAction.RESYNC);
        YarnConfiguration.RM_NODEMANAGER_MINIMUM_VERSION,
        YarnConfiguration.DEFAULT_RM_NODEMANAGER_MINIMUM_VERSION);

    isDistributedNodeLabelsConf =
        YarnConfiguration.isDistributedNodeLabelConfiguration(conf);

    super.serviceInit(conf);
  }

    Set<String> nodeLabels = request.getNodeLabels();
    if (isDistributedNodeLabelsConf && nodeLabels != null) {
      try {
        updateNodeLabelsFromNMReport(nodeLabels, nodeId);
        response.setAreNodeLabelsAcceptedByRM(true);
    this.rmContext.getDispatcher().getEventHandler().handle(nodeStatusEvent);

    if (isDistributedNodeLabelsConf && request.getNodeLabels() != null) {
      try {
        updateNodeLabelsFromNMReport(request.getNodeLabels(), nodeId);
        nodeHeartBeatResponse.setAreNodeLabelsAcceptedByRM(true);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMWebServices.java
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;

  private final Configuration conf;
  private @Context HttpServletResponse response;

  @VisibleForTesting
  boolean isDistributedNodeLabelConfiguration = false;

  public final static String DELEGATION_TOKEN_HEADER =
      "Hadoop-YARN-RM-Delegation-Token";

  public RMWebServices(final ResourceManager rm, Configuration conf) {
    this.rm = rm;
    this.conf = conf;
    isDistributedNodeLabelConfiguration =
        YarnConfiguration.isDistributedNodeLabelConfiguration(conf);
  }

  private void checkAndThrowIfDistributedNodeLabelConfEnabled(String operation)
      throws IOException {
    if (isDistributedNodeLabelConfiguration) {
      String msg =
          String.format("Error when invoke method=%s because of "
              + "distributed node label configuration enabled.", operation);
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  RMWebServices(ResourceManager rm, Configuration conf,
  @POST
  @Path("/replace-node-to-labels")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response replaceLabelsOnNodes(final NodeToLabelsInfo newNodeToLabels,
      @Context HttpServletRequest hsr) throws IOException {
    Map<NodeId, Set<String>> nodeIdToLabels =
        new HashMap<NodeId, Set<String>>();

    for (Map.Entry<String, NodeLabelsInfo> nitle : newNodeToLabels
        .getNodeToLabels().entrySet()) {
      nodeIdToLabels.put(
          ConverterUtils.toNodeIdWithDefaultPort(nitle.getKey()),
          new HashSet<String>(nitle.getValue().getNodeLabels()));
    }

    return replaceLabelsOnNode(nodeIdToLabels, hsr, "/replace-node-to-labels");
  }

  @POST
  @Path("/nodes/{nodeId}/replace-labels")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response replaceLabelsOnNode(NodeLabelsInfo newNodeLabelsInfo,
      @Context HttpServletRequest hsr, @PathParam("nodeId") String nodeId)
      throws Exception {
    NodeId nid = ConverterUtils.toNodeIdWithDefaultPort(nodeId);
    Map<NodeId, Set<String>> newLabelsForNode =
        new HashMap<NodeId, Set<String>>();
    newLabelsForNode.put(nid,
        new HashSet<String>(newNodeLabelsInfo.getNodeLabels()));

    return replaceLabelsOnNode(newLabelsForNode, hsr, "/nodes/nodeid/replace-labels");
  }

  private Response replaceLabelsOnNode(
      Map<NodeId, Set<String>> newLabelsForNode, HttpServletRequest hsr,
      String operation) throws IOException {
    init();

    checkAndThrowIfDistributedNodeLabelConfEnabled("replaceLabelsOnNode");

    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    if (callerUGI == null) {
      String msg =
          "Unable to obtain user name, user not authenticated for"
              + " post to ..." + operation;
      throw new AuthorizationException(msg);
    }

    if (!rm.getRMContext().getNodeLabelManager().checkAccess(callerUGI)) {
      String msg =
          "User " + callerUGI.getShortUserName() + " not authorized"
              + " for post to ..." + operation;
      throw new AuthorizationException(msg);
    }

    rm.getRMContext().getNodeLabelManager()
        .replaceLabelsOnNode(newLabelsForNode);

    return Response.status(Status.OK).build();
  }

  }

  protected Response killApp(RMApp app, UserGroupInformation callerUGI,
      HttpServletRequest hsr) throws IOException, InterruptedException {


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMAdminService.java

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.DataOutputStream;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class TestRMAdminService {

    }
  }

  @Test
  public void testModifyLabelsOnNodesWithDistributedConfigurationDisabled()
      throws IOException, YarnException {
    MockRM rm = new MockRM();
    ((RMContextImpl) rm.getRMContext())
        .setHAServiceState(HAServiceState.ACTIVE);
    RMNodeLabelsManager labelMgr = rm.rmContext.getNodeLabelManager();

    labelMgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    rm.adminService.replaceLabelsOnNode(ReplaceLabelsOnNodeRequest
        .newInstance(ImmutableMap.of(NodeId.newInstance("host", 0),
            (Set<String>) ImmutableSet.of("x"))));
    rm.close();
  }

  @Test(expected = YarnException.class)
  public void testModifyLabelsOnNodesWithDistributedConfigurationEnabled()
      throws IOException, YarnException {
    MockRM rm = new MockRM();
    rm.adminService.isDistributedNodeLabelConfiguration = true;

    ((RMContextImpl) rm.getRMContext())
        .setHAServiceState(HAServiceState.ACTIVE);
    RMNodeLabelsManager labelMgr = rm.rmContext.getNodeLabelManager();

    labelMgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    rm.adminService.replaceLabelsOnNode(ReplaceLabelsOnNodeRequest
        .newInstance(ImmutableMap.of(NodeId.newInstance("host", 0),
            (Set<String>) ImmutableSet.of("x"))));
    rm.close();
  }

  @Test
  public void testRemoveClusterNodeLabelsWithDistributedConfigurationEnabled()
      throws IOException, YarnException {
    MockRM rm = new MockRM();
    ((RMContextImpl) rm.getRMContext())
        .setHAServiceState(HAServiceState.ACTIVE);
    RMNodeLabelsManager labelMgr = rm.rmContext.getNodeLabelManager();
    rm.adminService.isDistributedNodeLabelConfiguration = true;

    labelMgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    rm.adminService
        .removeFromClusterNodeLabels(RemoveFromClusterNodeLabelsRequest
            .newInstance((Set<String>) ImmutableSet.of("x")));

    Set<String> clusterNodeLabels = labelMgr.getClusterNodeLabelNames();
    assertEquals(1,clusterNodeLabels.size());
    rm.close();
  }

  private String writeConfigurationXML(Configuration conf, String confXMLName)
      throws IOException {
    DataOutputStream output = null;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/nodelabels/NullRMNodeLabelsManager.java
    this.store = new NodeLabelsStore(this) {

      @Override
      public void recover(boolean ignoreNodeToLabelsMappings)
          throws IOException {
      }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServices.java
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
        null, null, null, null, null);
    when(mockRM.getRMContext()).thenReturn(rmContext);
    when(mockRM.getClientRMService()).thenReturn(mockClientSvc);
    rmContext.setNodeLabelManager(mock(RMNodeLabelsManager.class));

    RMWebServices webSvc = new RMWebServices(mockRM, new Configuration(),
        mock(HttpServletResponse.class));

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServicesNodeLabels.java
package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringWriter;

import javax.ws.rs.core.MediaType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONMarshaller;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

  private String userName;
  private String notUserName;
  private RMWebServices rmWebService;

  private Injector injector = Guice.createInjector(new ServletModule() {

    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      try {
        userName = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException ioe) {
      conf = new YarnConfiguration();
      conf.set(YarnConfiguration.YARN_ADMIN_ACL, userName);
      rm = new MockRM(conf);
      rmWebService = new RMWebServices(rm,conf);
      bind(RMWebServices.class).toInstance(rmWebService);
      bind(GenericExceptionHandler.class);
      bind(ResourceManager.class).toInstance(rm);
      filter("/*").through(
          TestRMWebServicesAppsModification.TestRMCustomAuthFilter.class);
    ClientResponse response;
    JSONObject json;
    JSONArray jarr;

    response =
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    String res = response.getEntity(String.class);
    assertTrue(res.equals("null"));

    response =
        r.path("ws")
            .path("v1")
            .path("cluster")
            .path("add-node-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\":[\"x\",\"y\"]}",
                MediaType.APPLICATION_JSON).post(ClientResponse.class);
    response =
        r.path("ws").path("v1").path("cluster").path("nodes").path("nid:0")
            .path("replace-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\": [\"y\"]}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    rmWebService.isDistributedNodeLabelConfiguration = true;

    ntli = new NodeToLabelsInfo();
    nli = new NodeLabelsInfo();
    nli.getNodeLabels().add("x");
    ntli.getNodeToLabels().put("nid:0", nli);
    response =
        r.path("ws")
            .path("v1")
            .path("cluster")
            .path("replace-node-to-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(ntli, NodeToLabelsInfo.class),
                MediaType.APPLICATION_JSON).post(ClientResponse.class);

    response =
        r.path("ws").path("v1").path("cluster").path("get-node-to-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ntli = response.getEntity(NodeToLabelsInfo.class);
    nli = ntli.getNodeToLabels().get("nid:0");
    assertEquals(1, nli.getNodeLabels().size());
    assertFalse(nli.getNodeLabels().contains("x"));

    response =
        r.path("ws").path("v1").path("cluster").path("nodes").path("nid:0")
            .path("replace-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\": [\"x\"]}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    response =
        r.path("ws").path("v1").path("cluster").path("get-node-to-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ntli = response.getEntity(NodeToLabelsInfo.class);
    nli = ntli.getNodeToLabels().get("nid:0");
    assertEquals(1, nli.getNodeLabels().size());
    assertFalse(nli.getNodeLabels().contains("x"));

    response =
        r.path("ws").path("v1").path("cluster")
            .path("remove-node-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\":\"x\"}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    assertEquals("y", json.getString("nodeLabels"));
  }

  @SuppressWarnings("rawtypes")
    jm.marshallToJSON(nsli, sw);
    return sw.toString();
  }
}

