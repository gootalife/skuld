hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/utils/YarnServerBuilderUtils.java

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  public static NodeHeartbeatResponse newNodeHeartbeatResponse(
      NodeAction action, String diagnosticsMessage) {
    NodeHeartbeatResponse response = recordFactory
        .newRecordInstance(NodeHeartbeatResponse.class);
    response.setNodeAction(action);
    response.setDiagnosticsMessage(diagnosticsMessage);
    return response;
  }

  public static NodeHeartbeatResponse newNodeHeartbeatResponse(int responseId,
      NodeAction action, List<ContainerId> containersToCleanUp,
      List<ApplicationId> applicationsToCleanUp,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ResourceTrackerService.java
  private InetSocketAddress resourceTrackerAddress;
  private String minimumNodeManagerVersion;

  private int minAllocMb;
  private int minAllocVcores;

  private boolean isDistributedNodeLabelsConf;

  public ResourceTrackerService(RMContext rmContext,
      NodesListManager nodesListManager,
      NMLivelinessMonitor nmLivelinessMonitor,
          "Disallowed NodeManager nodeId: " + nodeId + " hostname: "
              + nodeId.getHost();
      LOG.info(message);
      return YarnServerBuilderUtils.newNodeHeartbeatResponse(
          NodeAction.SHUTDOWN, message);
    }

      String message = "Node not found resyncing " + remoteNodeStatus.getNodeId();
      LOG.info(message);
      return YarnServerBuilderUtils.newNodeHeartbeatResponse(NodeAction.RESYNC,
          message);
    }

              + lastNodeHeartbeatResponse.getResponseId() + " nm response id:"
              + remoteNodeStatus.getResponseId();
      LOG.info(message);
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeEvent(nodeId, RMNodeEventType.REBOOTING));
      return YarnServerBuilderUtils.newNodeHeartbeatResponse(NodeAction.RESYNC,
          message);
    }


