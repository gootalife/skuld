hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/nodelabels/CommonNodeLabelsManager.java
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.NodeId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

@Private
public class CommonNodeLabelsManager extends AbstractService {
  protected static final Log LOG = LogFactory.getLog(CommonNodeLabelsManager.class);
  private static final int MAX_LABEL_LENGTH = 255;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/nodelabels/NodeLabelTestBase.java
    return set;
  }
  
  @SuppressWarnings("unchecked")
  public static Set<NodeLabel> toNodeLabelSet(String... nodeLabelsStr) {
    if (null == nodeLabelsStr) {
      return null;
    }
    Set<NodeLabel> labels = new HashSet<NodeLabel>();
    for (String label : nodeLabelsStr) {
      labels.add(NodeLabel.newInstance(label));
    }
    return labels;
  }

  public NodeId toNodeId(String str) {
    if (str.contains(":")) {
      int idx = str.indexOf(':');

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/NodeHeartbeatRequest.java
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.util.Records;
  
  public static NodeHeartbeatRequest newInstance(NodeStatus nodeStatus,
      MasterKey lastKnownContainerTokenMasterKey,
      MasterKey lastKnownNMTokenMasterKey, Set<NodeLabel> nodeLabels) {
    NodeHeartbeatRequest nodeHeartbeatRequest =
        Records.newRecord(NodeHeartbeatRequest.class);
    nodeHeartbeatRequest.setNodeStatus(nodeStatus);
  public abstract MasterKey getLastKnownNMTokenMasterKey();
  public abstract void setLastKnownNMTokenMasterKey(MasterKey secretKey);
  
  public abstract Set<NodeLabel> getNodeLabels();
  public abstract void setNodeLabels(Set<NodeLabel> nodeLabels);

  public abstract List<LogAggregationReport>
      getLogAggregationReportsForApps();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/RegisterNodeManagerRequest.java

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

  public static RegisterNodeManagerRequest newInstance(NodeId nodeId,
      int httpPort, Resource resource, String nodeManagerVersionId,
      List<NMContainerStatus> containerStatuses,
      List<ApplicationId> runningApplications, Set<NodeLabel> nodeLabels) {
    RegisterNodeManagerRequest request =
        Records.newRecord(RegisterNodeManagerRequest.class);
    request.setHttpPort(httpPort);
  public abstract Resource getResource();
  public abstract String getNMVersion();
  public abstract List<NMContainerStatus> getNMContainerStatuses();
  public abstract Set<NodeLabel> getNodeLabels();
  public abstract void setNodeLabels(Set<NodeLabel> nodeLabels);
  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/NodeHeartbeatRequestPBImpl.java
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeLabelPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeLabelProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.LogAggregationReportProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeLabelsProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeLabelsProto.Builder;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
  private NodeStatus nodeStatus = null;
  private MasterKey lastKnownContainerTokenMasterKey = null;
  private MasterKey lastKnownNMTokenMasterKey = null;
  private Set<NodeLabel> labels = null;
  private List<LogAggregationReport> logAggregationReportsForApps = null;

  public NodeHeartbeatRequestPBImpl() {
    }
    if (this.labels != null) {
      builder.clearNodeLabels();
      Builder newBuilder = NodeLabelsProto.newBuilder();
      for (NodeLabel label : labels) {
        newBuilder.addNodeLabels(convertToProtoFormat(label));
      }
      builder.setNodeLabels(newBuilder.build());
    }
    if (this.logAggregationReportsForApps != null) {
      addLogAggregationStatusForAppsToProto();
  }

  @Override
  public Set<NodeLabel> getNodeLabels() {
    initNodeLabels();
    return this.labels;
  }

  @Override
  public void setNodeLabels(Set<NodeLabel> nodeLabels) {
    maybeInitBuilder();
    builder.clearNodeLabels();
    this.labels = nodeLabels;
      labels = null;
      return;
    }
    NodeLabelsProto nodeLabels = p.getNodeLabels();
    labels = new HashSet<NodeLabel>();
    for(NodeLabelProto nlp : nodeLabels.getNodeLabelsList()) {
      labels.add(convertFromProtoFormat(nlp));
    }
  }

  private NodeLabelPBImpl convertFromProtoFormat(NodeLabelProto p) {
    return new NodeLabelPBImpl(p);
  }

  private NodeLabelProto convertToProtoFormat(NodeLabel t) {
    return ((NodeLabelPBImpl)t).getProto();
  }

  @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/RegisterNodeManagerRequestPBImpl.java

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeLabelPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeLabelProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NMContainerStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeLabelsProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeLabelsProto.Builder;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
  private NodeId nodeId = null;
  private List<NMContainerStatus> containerStatuses = null;
  private List<ApplicationId> runningApplications = null;
  private Set<NodeLabel> labels = null;

  public RegisterNodeManagerRequestPBImpl() {
    builder = RegisterNodeManagerRequestProto.newBuilder();
    }
    if (this.labels != null) {
      builder.clearNodeLabels();
      Builder newBuilder = NodeLabelsProto.newBuilder();
      for (NodeLabel label : labels) {
        newBuilder.addNodeLabels(convertToProtoFormat(label));
      }
      builder.setNodeLabels(newBuilder.build());
    }
  }

  }
  
  @Override
  public Set<NodeLabel> getNodeLabels() {
    initNodeLabels();
    return this.labels;
  }

  @Override
  public void setNodeLabels(Set<NodeLabel> nodeLabels) {
    maybeInitBuilder();
    builder.clearNodeLabels();
    this.labels = nodeLabels;
      labels=null;
      return;
    }
    NodeLabelsProto nodeLabels = p.getNodeLabels();
    labels = new HashSet<NodeLabel>();
    for(NodeLabelProto nlp : nodeLabels.getNodeLabelsList()) {
      labels.add(convertFromProtoFormat(nlp));
    }
  }

  private NodeLabelPBImpl convertFromProtoFormat(NodeLabelProto p) {
    return new NodeLabelPBImpl(p);
  }

  private NodeLabelProto convertToProtoFormat(NodeLabel t) {
    return ((NodeLabelPBImpl)t).getProto();
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/test/java/org/apache/hadoop/yarn/TestYarnServerApiClasses.java
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
    Assert.assertTrue(original.getNodeLabels()
        .containsAll(copy.getNodeLabels()));
    original.setNodeLabels(new HashSet<NodeLabel> ());
    copy = new NodeHeartbeatRequestPBImpl(
        original.getProto());
    Assert.assertNotNull(copy.getNodeLabels());

  @Test
  public void testRegisterNodeManagerRequestWithValidLabels() {
    HashSet<NodeLabel> nodeLabels = getValidNodeLabels();
    RegisterNodeManagerRequest request =
        RegisterNodeManagerRequest.newInstance(
            NodeId.newInstance("host", 1234), 1234, Resource.newInstance(0, 0),
    Assert.assertEquals(true, nodeLabels.containsAll(copy.getNodeLabels()));

    request.setNodeLabels(new HashSet<NodeLabel> ());
    copy = new RegisterNodeManagerRequestPBImpl(
        ((RegisterNodeManagerRequestPBImpl) request).getProto());
    Assert.assertNotNull(copy.getNodeLabels());
    Assert.assertEquals(0, copy.getNodeLabels().size());
  }

  private HashSet<NodeLabel> getValidNodeLabels() {
    HashSet<NodeLabel> nodeLabels = new HashSet<NodeLabel>();
    nodeLabels.add(NodeLabel.newInstance("java"));
    nodeLabels.add(NodeLabel.newInstance("windows"));
    nodeLabels.add(NodeLabel.newInstance("gpu"));
    nodeLabels.add(NodeLabel.newInstance("x86"));
    return nodeLabels;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeStatusUpdaterImpl.java
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
  protected void registerWithRM()
      throws YarnException, IOException {
    List<NMContainerStatus> containerReports = getNMContainerStatuses();
    Set<NodeLabel> nodeLabels = null;
    if (hasNodeLabelsProvider) {
      nodeLabels = nodeLabelsProvider.getNodeLabels();
      nodeLabels =
          (null == nodeLabels) ? CommonNodeLabelsManager.EMPTY_NODELABEL_SET
              : nodeLabels;
    }
    RegisterNodeManagerRequest request =
      @SuppressWarnings("unchecked")
      public void run() {
        int lastHeartbeatID = 0;
        Set<NodeLabel> lastUpdatedNodeLabelsToRM = null;
        if (hasNodeLabelsProvider) {
          lastUpdatedNodeLabelsToRM = nodeLabelsProvider.getNodeLabels();
          lastUpdatedNodeLabelsToRM =
              (null == lastUpdatedNodeLabelsToRM) ? CommonNodeLabelsManager.EMPTY_NODELABEL_SET
                  : lastUpdatedNodeLabelsToRM;
        }
        while (!isStopped) {
          try {
            NodeHeartbeatResponse response = null;
            Set<NodeLabel> nodeLabelsForHeartbeat = null;
            NodeStatus nodeStatus = getNodeStatus(lastHeartbeatID);

            if (hasNodeLabelsProvider) {
              nodeLabelsForHeartbeat = nodeLabelsProvider.getNodeLabels();
              nodeLabelsForHeartbeat =
                  (nodeLabelsForHeartbeat == null) ? CommonNodeLabelsManager.EMPTY_NODELABEL_SET
                      : nodeLabelsForHeartbeat;
              if (!areNodeLabelsUpdated(nodeLabelsForHeartbeat,
                  lastUpdatedNodeLabelsToRM)) {
      private boolean areNodeLabelsUpdated(Set<NodeLabel> nodeLabelsNew,
          Set<NodeLabel> nodeLabelsOld) {
        if (nodeLabelsNew.size() != nodeLabelsOld.size()
            || !nodeLabelsOld.containsAll(nodeLabelsNew)) {
          return true;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/nodelabels/NodeLabelsProvider.java
import java.util.Set;

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.NodeLabel;

  public abstract Set<NodeLabel> getNodeLabels();
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestNodeStatusUpdaterForLabels.java

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.NodeLabelTestBase;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;

  private class ResourceTrackerForLabels implements ResourceTracker {
    int heartbeatID = 0;
    Set<NodeLabel> labels;

    private boolean receivedNMHeartbeat = false;
    private boolean receivedNMRegister = false;
  public static class DummyNodeLabelsProvider extends NodeLabelsProvider {

    @SuppressWarnings("unchecked")
    private Set<NodeLabel> nodeLabels = CommonNodeLabelsManager.EMPTY_NODELABEL_SET;

    public DummyNodeLabelsProvider() {
      super(DummyNodeLabelsProvider.class.getName());
    }

    @Override
    public synchronized Set<NodeLabel> getNodeLabels() {
      return nodeLabels;
    }

    synchronized void setNodeLabels(Set<NodeLabel> nodeLabels) {
      this.nodeLabels = nodeLabels;
    }
  }
    resourceTracker.resetNMHeartbeatReceiveFlag();
    nm.start();
    resourceTracker.waitTillRegister();
    assertNLCollectionEquals(resourceTracker.labels,
        dummyLabelsProviderRef
            .getNodeLabels());

    resourceTracker.waitTillHeartbeat();// wait till the first heartbeat
    resourceTracker.resetNMHeartbeatReceiveFlag();

    dummyLabelsProviderRef.setNodeLabels(toNodeLabelSet("P"));

    nm.getNodeStatusUpdater().sendOutofBandHeartBeat();
    resourceTracker.waitTillHeartbeat();
    assertNLCollectionEquals(resourceTracker.labels,
        dummyLabelsProviderRef
            .getNodeLabels());
    resourceTracker.resetNMHeartbeatReceiveFlag();


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ResourceTrackerService.java
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
    }
  }

  static Set<String> convertToStringSet(Set<NodeLabel> nodeLabels) {
    if (null == nodeLabels) {
      return null;
    }
    Set<String> labels = new HashSet<String>();
    for (NodeLabel label : nodeLabels) {
      labels.add(label.getName());
    }
    return labels;
  }

  @SuppressWarnings("unchecked")
  @Override
  public RegisterNodeManagerResponse registerNodeManager(
    }

    Set<String> nodeLabels = convertToStringSet(request.getNodeLabels());
    if (isDistributedNodeLabelsConf && nodeLabels != null) {
      try {
        updateNodeLabelsFromNMReport(nodeLabels, nodeId);
    if (isDistributedNodeLabelsConf && request.getNodeLabels() != null) {
      try {
        updateNodeLabelsFromNMReport(
            convertToStringSet(request.getNodeLabels()), nodeId);
        nodeHeartBeatResponse.setAreNodeLabelsAcceptedByRM(true);
      } catch (IOException ex) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestResourceTrackerService.java
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
    registerReq.setNodeId(nodeId);
    registerReq.setHttpPort(1234);
    registerReq.setNMVersion(YarnVersionInfo.getVersion());
    registerReq.setNodeLabels(toSet(NodeLabel.newInstance("A")));
    RegisterNodeManagerResponse response =
        resourceTrackerService.registerNodeManager(registerReq);

    Assert.assertEquals("Action should be normal on valid Node Labels",
        NodeAction.NORMAL, response.getNodeAction());
    assertCollectionEquals(nodeLabelsMgr.getNodeLabels().get(nodeId),
        ResourceTrackerService.convertToStringSet(registerReq.getNodeLabels()));
    Assert.assertTrue("Valid Node Labels were not accepted by RM",
        response.getAreNodeLabelsAcceptedByRM());
    rm.stop();
    registerReq.setNodeId(nodeId);
    registerReq.setHttpPort(1234);
    registerReq.setNMVersion(YarnVersionInfo.getVersion());
    registerReq.setNodeLabels(toNodeLabelSet("A", "B", "C"));
    RegisterNodeManagerResponse response =
        resourceTrackerService.registerNodeManager(registerReq);

    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    req.setNMVersion(YarnVersionInfo.getVersion());
    req.setNodeLabels(toNodeLabelSet("#Y"));
    RegisterNodeManagerResponse response =
        resourceTrackerService.registerNodeManager(req);

    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    req.setNMVersion(YarnVersionInfo.getVersion());
    req.setNodeLabels(toNodeLabelSet("A"));
    RegisterNodeManagerResponse response =
        resourceTrackerService.registerNodeManager(req);
    registerReq.setNodeId(nodeId);
    registerReq.setHttpPort(1234);
    registerReq.setNMVersion(YarnVersionInfo.getVersion());
    registerReq.setNodeLabels(toNodeLabelSet("A")); // Node register label
    RegisterNodeManagerResponse registerResponse =
        resourceTrackerService.registerNodeManager(registerReq);

    NodeHeartbeatRequest heartbeatReq =
        Records.newRecord(NodeHeartbeatRequest.class);
    heartbeatReq.setNodeLabels(toNodeLabelSet("B")); // Node heartbeat label update
    NodeStatus nodeStatusObject = getNodeStatusObject(nodeId);
    heartbeatReq.setNodeStatus(nodeStatusObject);
    heartbeatReq.setLastKnownNMTokenMasterKey(registerResponse
    Assert.assertEquals("InValid Node Labels were not accepted by RM",
        NodeAction.NORMAL, nodeHeartbeatResponse.getNodeAction());
    assertCollectionEquals(nodeLabelsMgr.getNodeLabels().get(nodeId),
        ResourceTrackerService.convertToStringSet(heartbeatReq.getNodeLabels()));
    Assert.assertTrue("Valid Node Labels were not accepted by RM",
        nodeHeartbeatResponse.getAreNodeLabelsAcceptedByRM());
    
    registerReq.setNodeId(nodeId);
    registerReq.setHttpPort(1234);
    registerReq.setNMVersion(YarnVersionInfo.getVersion());
    registerReq.setNodeLabels(toNodeLabelSet("A"));
    RegisterNodeManagerResponse registerResponse =
        resourceTrackerService.registerNodeManager(registerReq);

    NodeHeartbeatRequest heartbeatReq =
        Records.newRecord(NodeHeartbeatRequest.class);
    heartbeatReq.setNodeLabels(toNodeLabelSet("B", "#C")); // Invalid heart beat labels
    heartbeatReq.setNodeStatus(getNodeStatusObject(nodeId));
    heartbeatReq.setLastKnownNMTokenMasterKey(registerResponse
        .getNMTokenMasterKey());
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    req.setNMVersion(YarnVersionInfo.getVersion());
    req.setNodeLabels(toNodeLabelSet("A", "B", "C"));
    RegisterNodeManagerResponse registerResponse =
        resourceTrackerService.registerNodeManager(req);

    NodeHeartbeatRequest heartbeatReq =
        Records.newRecord(NodeHeartbeatRequest.class);
    heartbeatReq.setNodeLabels(toNodeLabelSet("B")); // Valid heart beat labels
    heartbeatReq.setNodeStatus(getNodeStatusObject(nodeId));
    heartbeatReq.setLastKnownNMTokenMasterKey(registerResponse
        .getNMTokenMasterKey());

