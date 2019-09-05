hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FairScheduler.java
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationConstants;
        + " with event: " + event);
  }

  private synchronized void addNode(List<NMContainerStatus> containerReports,
      RMNode node) {
    FSSchedulerNode schedulerNode = new FSSchedulerNode(node, usePortForNodeName);
    nodes.put(node.getNodeID(), schedulerNode);
    Resources.addTo(clusterResource, node.getTotalCapability());
    updateMaximumAllocation(schedulerNode, true);

    triggerUpdate();
    queueMgr.getRootQueue().recomputeSteadyShares();
    LOG.info("Added node " + node.getNodeAddress() +
        " cluster capacity: " + clusterResource);

    recoverContainersOnNode(containerReports, node);
    updateRootQueueMetrics();
  }

  private synchronized void removeNode(RMNode rmNode) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)event;
      addNode(nodeAddedEvent.getContainerReports(),
          nodeAddedEvent.getAddedRMNode());
      break;
    case NODE_REMOVED:

