hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/NodeState.java
  REBOOTED,

  DECOMMISSIONING,

  SHUTDOWN;

  public boolean isUnusable() {
    return (this == UNHEALTHY || this == DECOMMISSIONED
        || this == LOST || this == SHUTDOWN);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/ResourceTracker.java
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;

public interface ResourceTracker {
  
  @Idempotent
  RegisterNodeManagerResponse registerNodeManager(
      RegisterNodeManagerRequest request) throws YarnException, IOException;

  @AtMostOnce
  NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
      throws YarnException, IOException;

  @Idempotent
  UnRegisterNodeManagerResponse unRegisterNodeManager(
      UnRegisterNodeManagerRequest request) throws YarnException, IOException;
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/impl/pb/client/ResourceTrackerPBClientImpl.java
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.UnRegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ResourceTrackerPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UnRegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UnRegisterNodeManagerResponsePBImpl;

import com.google.protobuf.ServiceException;

    }
  }

  @Override
  public UnRegisterNodeManagerResponse unRegisterNodeManager(
      UnRegisterNodeManagerRequest request) throws YarnException, IOException {
    UnRegisterNodeManagerRequestProto requestProto =
        ((UnRegisterNodeManagerRequestPBImpl) request).getProto();
    try {
      return new UnRegisterNodeManagerResponsePBImpl(
          proxy.unRegisterNodeManager(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/impl/pb/service/ResourceTrackerPBServiceImpl.java
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.UnRegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.UnRegisterNodeManagerResponseProto;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ResourceTrackerPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UnRegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UnRegisterNodeManagerResponsePBImpl;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
    try {
      RegisterNodeManagerResponse response = real.registerNodeManager(request);
      return ((RegisterNodeManagerResponsePBImpl)response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }
    try {
      NodeHeartbeatResponse response = real.nodeHeartbeat(request);
      return ((NodeHeartbeatResponsePBImpl)response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public UnRegisterNodeManagerResponseProto unRegisterNodeManager(
      RpcController controller, UnRegisterNodeManagerRequestProto proto)
      throws ServiceException {
    UnRegisterNodeManagerRequestPBImpl request =
        new UnRegisterNodeManagerRequestPBImpl(proto);
    try {
      UnRegisterNodeManagerResponse response = real
          .unRegisterNodeManager(request);
      return ((UnRegisterNodeManagerResponsePBImpl) response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/UnRegisterNodeManagerRequest.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/UnRegisterNodeManagerRequest.java

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Records;

public abstract class UnRegisterNodeManagerRequest {
  public static UnRegisterNodeManagerRequest newInstance(NodeId nodeId) {
    UnRegisterNodeManagerRequest nodeHeartbeatRequest = Records
        .newRecord(UnRegisterNodeManagerRequest.class);
    nodeHeartbeatRequest.setNodeId(nodeId);
    return nodeHeartbeatRequest;
  }

  public abstract NodeId getNodeId();

  public abstract void setNodeId(NodeId nodeId);
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/UnRegisterNodeManagerResponse.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/UnRegisterNodeManagerResponse.java

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

public abstract class UnRegisterNodeManagerResponse {
  public static UnRegisterNodeManagerResponse newInstance() {
    return Records.newRecord(UnRegisterNodeManagerResponse.class);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/UnRegisterNodeManagerRequestPBImpl.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/UnRegisterNodeManagerRequestPBImpl.java

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.UnRegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.UnRegisterNodeManagerRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;

public class UnRegisterNodeManagerRequestPBImpl extends
    UnRegisterNodeManagerRequest {
  private UnRegisterNodeManagerRequestProto proto =
      UnRegisterNodeManagerRequestProto.getDefaultInstance();
  private UnRegisterNodeManagerRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private NodeId nodeId = null;

  public UnRegisterNodeManagerRequestPBImpl() {
    builder = UnRegisterNodeManagerRequestProto.newBuilder();
  }

  public UnRegisterNodeManagerRequestPBImpl(
      UnRegisterNodeManagerRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public UnRegisterNodeManagerRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.nodeId != null) {
      builder.setNodeId(convertToProtoFormat(this.nodeId));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = UnRegisterNodeManagerRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public NodeId getNodeId() {
    UnRegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nodeId != null) {
      return this.nodeId;
    }
    if (!p.hasNodeId()) {
      return null;
    }
    this.nodeId = convertFromProtoFormat(p.getNodeId());
    return this.nodeId;
  }

  @Override
  public void setNodeId(NodeId updatedNodeId) {
    maybeInitBuilder();
    if (updatedNodeId == null) {
      builder.clearNodeId();
    }
    this.nodeId = updatedNodeId;
  }

  private NodeIdPBImpl convertFromProtoFormat(NodeIdProto p) {
    return new NodeIdPBImpl(p);
  }

  private NodeIdProto convertToProtoFormat(NodeId t) {
    return ((NodeIdPBImpl) t).getProto();
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/UnRegisterNodeManagerResponsePBImpl.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/UnRegisterNodeManagerResponsePBImpl.java

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.UnRegisterNodeManagerResponseProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;

public class UnRegisterNodeManagerResponsePBImpl extends
    UnRegisterNodeManagerResponse {
  private UnRegisterNodeManagerResponseProto proto =
      UnRegisterNodeManagerResponseProto.getDefaultInstance();
  private UnRegisterNodeManagerResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private boolean rebuild = false;

  public UnRegisterNodeManagerResponsePBImpl() {
    builder = UnRegisterNodeManagerResponseProto.newBuilder();
  }

  public UnRegisterNodeManagerResponsePBImpl(
      UnRegisterNodeManagerResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public UnRegisterNodeManagerResponseProto getProto() {
    if (rebuild) {
      mergeLocalToProto();
    }
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    proto = builder.build();
    rebuild = false;
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = UnRegisterNodeManagerResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/test/java/org/apache/hadoop/yarn/TestResourceTrackerPBClientImpl.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl;
import org.apache.hadoop.yarn.factories.impl.pb.RpcServerFactoryPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

  }

  @Test
  public void testUnRegisterNodeManager() throws Exception {
    UnRegisterNodeManagerRequest request = UnRegisterNodeManagerRequest
        .newInstance(NodeId.newInstance("host1", 1234));
    assertNotNull(client.unRegisterNodeManager(request));

    ResourceTrackerTestImpl.exception = true;
    try {
      client.unRegisterNodeManager(request);
      fail("there  should be YarnException");
    } catch (YarnException e) {
      assertTrue(e.getMessage().startsWith("testMessage"));
    } finally {
      ResourceTrackerTestImpl.exception = false;
    }
  }

  public static class ResourceTrackerTestImpl implements ResourceTracker {

      return recordFactory.newRecordInstance(NodeHeartbeatResponse.class);
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      if (exception) {
        throw new YarnException("testMessage");
      }
      return UnRegisterNodeManagerResponse.newInstance();
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/test/java/org/apache/hadoop/yarn/TestYSCRPCFactories.java
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.junit.Test;

public class TestYSCRPCFactories {
      return null;
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return null;
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/test/java/org/apache/hadoop/yarn/TestYarnServerApiClasses.java
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UnRegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
    Assert.assertEquals(0, copy.getNodeLabels().size());
  }

  @Test
  public void testUnRegisterNodeManagerRequestPBImpl() throws Exception {
    UnRegisterNodeManagerRequestPBImpl request = new UnRegisterNodeManagerRequestPBImpl();
    NodeId nodeId = NodeId.newInstance("host", 1234);
    request.setNodeId(nodeId);

    UnRegisterNodeManagerRequestPBImpl copy = new UnRegisterNodeManagerRequestPBImpl(
        request.getProto());
    Assert.assertEquals(nodeId, copy.getNodeId());
  }

  private HashSet<NodeLabel> getValidNodeLabels() {
    HashSet<NodeLabel> nodeLabels = new HashSet<NodeLabel>();
    nodeLabels.add(NodeLabel.newInstance("java"));

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeStatusUpdaterImpl.java
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factories.impl.pb.RecordFactoryPBImpl;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.ResourceManagerConstants;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
  private Runnable statusUpdaterRunnable;
  private Thread  statusUpdater;
  private long rmIdentifier = ResourceManagerConstants.RM_INVALID_IDENTIFIER;
  private boolean registeredWithRM = false;
  Set<ContainerId> pendingContainersToRemove = new HashSet<ContainerId>();

  private final NodeLabelsProvider nodeLabelsProvider;

  @Override
  protected void serviceStop() throws Exception {
    if (this.registeredWithRM && !this.isStopped
        && !isNMUnderSupervisionWithRecoveryEnabled()
        && !context.getDecommissioned()) {
      unRegisterNM();
    }
    this.isStopped = true;
    stopRMProxy();
    super.serviceStop();
  }

  private boolean isNMUnderSupervisionWithRecoveryEnabled() {
    Configuration config = getConfig();
    return config.getBoolean(YarnConfiguration.NM_RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_NM_RECOVERY_ENABLED)
        && config.getBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED,
            YarnConfiguration.DEFAULT_NM_RECOVERY_SUPERVISED);
  }

  private void unRegisterNM() {
    RecordFactory recordFactory = RecordFactoryPBImpl.get();
    UnRegisterNodeManagerRequest request = recordFactory
        .newRecordInstance(UnRegisterNodeManagerRequest.class);
    request.setNodeId(this.nodeId);
    try {
      resourceTracker.unRegisterNodeManager(request);
      LOG.info("Successfully Unregistered the Node " + this.nodeId
          + " with ResourceManager.");
    } catch (Exception e) {
      LOG.warn("Unregistration of the Node " + this.nodeId + " failed.", e);
    }
  }

  protected void rebootNodeStatusUpdaterAndRegisterWithRM() {
    this.isStopped = true;
            + "version error, " + message);
      }
    }
    this.registeredWithRM = true;
    MasterKey masterKey = regNMResponse.getContainerTokenMasterKey();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/LocalRMInterface.java
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;

    NodeHeartbeatResponse response = recordFactory.newRecordInstance(NodeHeartbeatResponse.class);
    return response;
  }

  @Override
  public UnRegisterNodeManagerResponse unRegisterNodeManager(
      UnRegisterNodeManagerRequest request) throws YarnException, IOException {
    UnRegisterNodeManagerResponse response = recordFactory
        .newRecordInstance(UnRegisterNodeManagerResponse.class);
    return response;
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/MockNodeStatusUpdater.java
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
              null, null, null, 1000L);
      return nhResponse;
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return recordFactory
          .newRecordInstance(UnRegisterNodeManagerResponse.class);
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestNodeStatusUpdater.java
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
            1000L);
      return nhResponse;
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return recordFactory
          .newRecordInstance(UnRegisterNodeManagerResponse.class);
    }
  }

  private class MyNodeStatusUpdater extends NodeStatusUpdaterImpl {
      nhResponse.setDiagnosticsMessage(shutDownMessage);
      return nhResponse;
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return recordFactory
          .newRecordInstance(UnRegisterNodeManagerResponse.class);
    }
  }

  private class MyResourceTracker3 implements ResourceTracker {
      }
      return nhResponse;
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return recordFactory
          .newRecordInstance(UnRegisterNodeManagerResponse.class);
    }
  }

      nhResponse.setSystemCredentialsForApps(appCredentials);
      return nhResponse;
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return recordFactory
          .newRecordInstance(UnRegisterNodeManagerResponse.class);
    }
  }

  private class MyResourceTracker5 implements ResourceTracker {
          "NodeHeartbeat exception");
      }
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return recordFactory
          .newRecordInstance(UnRegisterNodeManagerResponse.class);
    }
  }

  private class MyResourceTracker6 implements ResourceTracker {
              null, null, null, 1000L);
      return nhResponse;
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return recordFactory
          .newRecordInstance(UnRegisterNodeManagerResponse.class);
    }
  }

  @Before

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestNodeStatusUpdaterForLabels.java
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
      }
      return nhResponse;
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return null;
    }
  }

  public static class DummyNodeLabelsProvider extends NodeLabelsProvider {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ClusterMetrics.java
  @Metric("# of lost NMs") MutableGaugeInt numLostNMs;
  @Metric("# of unhealthy NMs") MutableGaugeInt numUnhealthyNMs;
  @Metric("# of Rebooted NMs") MutableGaugeInt numRebootedNMs;
  @Metric("# of Shutdown NMs") MutableGaugeInt numShutdownNMs;
  @Metric("AM container launch delay") MutableRate aMLaunchDelay;
  @Metric("AM register delay") MutableRate aMRegisterDelay;

    numRebootedNMs.decr();
  }

  public int getNumShutdownNMs() {
    return numShutdownNMs.value();
  }

  public void incrNumShutdownNMs() {
    numShutdownNMs.incr();
  }

  public void decrNumShutdownNMs() {
    numShutdownNMs.decr();
  }

  public void incrNumActiveNodes() {
    numActiveNMs.incr();
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ResourceTrackerService.java
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
    return nodeHeartBeatResponse;
  }

  @SuppressWarnings("unchecked")
  @Override
  public UnRegisterNodeManagerResponse unRegisterNodeManager(
      UnRegisterNodeManagerRequest request) throws YarnException, IOException {
    UnRegisterNodeManagerResponse response = recordFactory
        .newRecordInstance(UnRegisterNodeManagerResponse.class);
    NodeId nodeId = request.getNodeId();
    RMNode rmNode = this.rmContext.getRMNodes().get(nodeId);
    if (rmNode == null) {
      LOG.info("Node not found, ignoring the unregister from node id : "
          + nodeId);
      return response;
    }
    LOG.info("Node with node id : " + nodeId
        + " has shutdown, hence unregistering the node.");
    this.nmLivelinessMonitor.unregister(nodeId);
    this.rmContext.getDispatcher().getEventHandler()
        .handle(new RMNodeEvent(nodeId, RMNodeEventType.SHUTDOWN));
    return response;
  }

  private void updateNodeLabelsFromNMReport(Set<String> nodeLabels,
      NodeId nodeId) throws IOException {
    try {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmnode/RMNodeEventType.java
  STATUS_UPDATE,
  REBOOTING,
  RECONNECTED,
  SHUTDOWN,

  CLEANUP_APP,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmnode/RMNodeImpl.java
         RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())
     .addTransition(NodeState.RUNNING, NodeState.RUNNING,
         RMNodeEventType.RESOURCE_UPDATE, new UpdateNodeResourceWhenRunningTransition())
     .addTransition(NodeState.RUNNING, NodeState.SHUTDOWN,
         RMNodeEventType.SHUTDOWN,
         new DeactivateNodeTransition(NodeState.SHUTDOWN))

     .addTransition(NodeState.REBOOTED, NodeState.REBOOTED,
     .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
         RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
         new AddContainersToBeRemovedFromNMTransition())
     .addTransition(NodeState.UNHEALTHY, NodeState.SHUTDOWN,
         RMNodeEventType.SHUTDOWN,
         new DeactivateNodeTransition(NodeState.SHUTDOWN))

     .addTransition(NodeState.SHUTDOWN, NodeState.SHUTDOWN,
         RMNodeEventType.RESOURCE_UPDATE,
         new UpdateNodeResourceWhenUnusableTransition())
     .addTransition(NodeState.SHUTDOWN, NodeState.SHUTDOWN,
         RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
         new AddContainersToBeRemovedFromNMTransition())

     .installTopology(); 
    case UNHEALTHY:
      metrics.decrNumUnhealthyNMs();
      break;
    case SHUTDOWN:
      metrics.decrNumShutdownNMs();
      break;
    default:
      LOG.debug("Unexpected previous node state");    
    }
    case UNHEALTHY:
      metrics.incrNumUnhealthyNMs();
      break;
    case SHUTDOWN:
      metrics.incrNumShutdownNMs();
      break;
    default:
      LOG.debug("Unexpected final state");
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/MetricsOverviewTable.java
        th().$class("ui-state-default")._("Lost Nodes")._().
        th().$class("ui-state-default")._("Unhealthy Nodes")._().
        th().$class("ui-state-default")._("Rebooted Nodes")._().
        th().$class("ui-state-default")._("Shutdown Nodes")._().
      _().
    _().
    tbody().$class("ui-widget-content").
        td().a(url("nodes/lost"),String.valueOf(clusterMetrics.getLostNodes()))._().
        td().a(url("nodes/unhealthy"),String.valueOf(clusterMetrics.getUnhealthyNodes()))._().
        td().a(url("nodes/rebooted"),String.valueOf(clusterMetrics.getRebootedNodes()))._().
        td().a(url("nodes/shutdown"),String.valueOf(clusterMetrics.getShutdownNodes()))._().
      _().
    _()._();
    

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/NodesPage.java
        case DECOMMISSIONED:
        case LOST:
        case REBOOTED:
        case SHUTDOWN:
          rmNodes = this.rm.getRMContext().getInactiveRMNodes().values();
          isInactive = true;
          break;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/ClusterMetricsInfo.java
  protected int decommissionedNodes;
  protected int rebootedNodes;
  protected int activeNodes;
  protected int shutdownNodes;

  public ClusterMetricsInfo() {
  } // JAXB needs this
    this.unhealthyNodes = clusterMetrics.getUnhealthyNMs();
    this.decommissionedNodes = clusterMetrics.getNumDecommisionedNMs();
    this.rebootedNodes = clusterMetrics.getNumRebootedNMs();
    this.shutdownNodes = clusterMetrics.getNumShutdownNMs();
    this.totalNodes = activeNodes + lostNodes + decommissionedNodes
        + rebootedNodes + unhealthyNodes + shutdownNodes;
  }

  public int getAppsSubmitted() {
    return this.decommissionedNodes;
  }

  public int getShutdownNodes() {
    return this.shutdownNodes;
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMNodeTransitions.java
    Assert.assertEquals(NodeState.REBOOTED, node.getState());
  }

  @Test
  public void testNMShutdown() {
    RMNodeImpl node = getRunningNode();
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.SHUTDOWN));
    Assert.assertEquals(NodeState.SHUTDOWN, node.getState());
  }

  @Test
  public void testUnhealthyNMShutdown() {
    RMNodeImpl node = getUnhealthyNode();
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.SHUTDOWN));
    Assert.assertEquals(NodeState.SHUTDOWN, node.getState());
  }

  @Test(timeout=20000)
  public void testUpdateHeartbeatResponseForCleanup() {
    RMNodeImpl node = getRunningNode();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestResourceTrackerService.java
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
        ClusterMetrics.getMetrics().getUnhealthyNMs());
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testHandleContainerStatusInvalidCompletions() throws Exception {
    rm = new MockRM(new YarnConfiguration());

  }

  @Test
  public void testNMUnregistration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();

    ResourceTrackerService resourceTrackerService = rm
        .getResourceTrackerService();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);

    int shutdownNMsCount = ClusterMetrics.getMetrics()
        .getNumShutdownNMs();
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));

    UnRegisterNodeManagerRequest request = Records
        .newRecord(UnRegisterNodeManagerRequest.class);
    request.setNodeId(nm1.getNodeId());
    resourceTrackerService.unRegisterNodeManager(request);
    checkShutdownNMCount(rm, ++shutdownNMsCount);

    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.RESYNC.equals(nodeHeartbeat.getNodeAction()));
  }

  @Test
  public void testUnhealthyNMUnregistration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();

    ResourceTrackerService resourceTrackerService = rm
        .getResourceTrackerService();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    Assert.assertEquals(0, ClusterMetrics.getMetrics().getUnhealthyNMs());
    nm1.nodeHeartbeat(true);
    int shutdownNMsCount = ClusterMetrics.getMetrics().getNumShutdownNMs();

    nm1.nodeHeartbeat(false);
    checkUnealthyNMCount(rm, nm1, true, 1);
    UnRegisterNodeManagerRequest request = Records
        .newRecord(UnRegisterNodeManagerRequest.class);
    request.setNodeId(nm1.getNodeId());
    resourceTrackerService.unRegisterNodeManager(request);
    checkShutdownNMCount(rm, ++shutdownNMsCount);
  }

  @Test
  public void testInvalidNMUnregistration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();
    ResourceTrackerService resourceTrackerService = rm
        .getResourceTrackerService();
    int shutdownNMsCount = ClusterMetrics.getMetrics()
        .getNumShutdownNMs();
    int decommisionedNMsCount = ClusterMetrics.getMetrics()
        .getNumDecommisionedNMs();

    UnRegisterNodeManagerRequest request = Records
        .newRecord(UnRegisterNodeManagerRequest.class);
    request.setNodeId(BuilderUtils.newNodeId("host", 1234));
    resourceTrackerService.unRegisterNodeManager(request);
    checkShutdownNMCount(rm, 0);
    checkDecommissionedNMCount(rm, 0);

    MockNM nm1 = new MockNM("host1:1234", 5120, resourceTrackerService);
    RegisterNodeManagerResponse response = nm1.registerNode();
    Assert.assertEquals(NodeAction.NORMAL, response.getNodeAction());
    writeToHostsFile("host2");
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    rm.getNodesListManager().refreshNodes(conf);
    NodeHeartbeatResponse heartbeatResponse = nm1.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.SHUTDOWN, heartbeatResponse.getNodeAction());
    checkShutdownNMCount(rm, shutdownNMsCount);
    checkDecommissionedNMCount(rm, ++decommisionedNMsCount);
    request.setNodeId(nm1.getNodeId());
    resourceTrackerService.unRegisterNodeManager(request);
    checkShutdownNMCount(rm, shutdownNMsCount);
    checkDecommissionedNMCount(rm, decommisionedNMsCount);

    MockNM nm2 = new MockNM("host2:1234", 5120, resourceTrackerService);
    RegisterNodeManagerResponse response2 = nm2.registerNode();
    Assert.assertEquals(NodeAction.NORMAL, response2.getNodeAction());
    writeToHostsFile("host1");
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    rm.getNodesListManager().refreshNodes(conf);
    request.setNodeId(nm2.getNodeId());
    resourceTrackerService.unRegisterNodeManager(request);
    checkShutdownNMCount(rm, shutdownNMsCount);
    checkDecommissionedNMCount(rm, ++decommisionedNMsCount);
  }

  private void writeToHostsFile(String... hosts) throws IOException {
    if (!hostFile.exists()) {
      TEMP_DIR.mkdirs();
        ClusterMetrics.getMetrics().getNumDecommisionedNMs());
  }

  private void checkShutdownNMCount(MockRM rm, int count)
      throws InterruptedException {
    int waitCount = 0;
    while (ClusterMetrics.getMetrics().getNumShutdownNMs() != count
        && waitCount++ < 20) {
      synchronized (this) {
        wait(100);
      }
    }
    Assert.assertEquals("The shutdown metrics are not updated", count,
        ClusterMetrics.getMetrics().getNumShutdownNMs());
  }

  @After
  public void tearDown() {
    if (hostFile != null && hostFile.exists()) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestNodesPage.java
public class TestNodesPage {
  
  final int numberOfRacks = 2;
  final int numberOfNodesPerRack = 8;
  final int numberOfLostNodesPerRack = numberOfNodesPerRack

  final int numberOfThInMetricsTable = 21;
  final int numberOfActualTableHeaders = 13;

  private Injector injector;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServices.java
          WebServicesTestUtils.getXmlInt(element, "unhealthyNodes"),
          WebServicesTestUtils.getXmlInt(element, "decommissionedNodes"),
          WebServicesTestUtils.getXmlInt(element, "rebootedNodes"),
          WebServicesTestUtils.getXmlInt(element, "activeNodes"),
          WebServicesTestUtils.getXmlInt(element, "shutdownNodes"));
    }
  }

      Exception {
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject clusterinfo = json.getJSONObject("clusterMetrics");
    assertEquals("incorrect number of elements", 24, clusterinfo.length());
    verifyClusterMetrics(
        clusterinfo.getInt("appsSubmitted"), clusterinfo.getInt("appsCompleted"),
        clusterinfo.getInt("reservedMB"), clusterinfo.getInt("availableMB"),
        clusterinfo.getInt("totalMB"), clusterinfo.getInt("totalNodes"),
        clusterinfo.getInt("lostNodes"), clusterinfo.getInt("unhealthyNodes"),
        clusterinfo.getInt("decommissionedNodes"),
        clusterinfo.getInt("rebootedNodes"),clusterinfo.getInt("activeNodes"),
        clusterinfo.getInt("shutdownNodes"));
  }

  public void verifyClusterMetrics(int submittedApps, int completedApps,
      int reservedMB, int availableMB, int allocMB, int reservedVirtualCores,
      int availableVirtualCores, int allocVirtualCores, int totalVirtualCores,
      int containersAlloc, int totalMB, int totalNodes, int lostNodes,
      int unhealthyNodes, int decommissionedNodes, int rebootedNodes,
      int activeNodes, int shutdownNodes) throws JSONException, Exception {

    ResourceScheduler rs = rm.getResourceScheduler();
    QueueMetrics metrics = rs.getRootQueueMetrics();
        clusterMetrics.getNumRebootedNMs(), rebootedNodes);
    assertEquals("activeNodes doesn't match", clusterMetrics.getNumActiveNMs(),
        activeNodes);
    assertEquals("shutdownNodes doesn't match",
        clusterMetrics.getNumShutdownNMs(), shutdownNodes);
  }

  @Test

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-tests/src/test/java/org/apache/hadoop/yarn/server/MiniYARNCluster.java
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.MemoryApplicationHistoryStore;
              }
              return response;
            }

            @Override
            public UnRegisterNodeManagerResponse unRegisterNodeManager(
                UnRegisterNodeManagerRequest request) throws YarnException,
                IOException {
              return recordFactory
                  .newRecordInstance(UnRegisterNodeManagerResponse.class);
            }
          };
        }


