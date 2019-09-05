hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DecommissionManager.java
  @VisibleForTesting
  public void startDecommission(DatanodeDescriptor node) {
    if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
      hbManager.startDecommission(node);
      if (node.isDecommissionInProgress()) {
        for (DatanodeStorageInfo storage : node.getStorageInfos()) {
          LOG.info("Starting decommission of {} {} with {} blocks",
              node, storage, storage.numBlocks());
        }
        node.decommissioningStatus.setStartTime(monotonicNow());
        pendingNodes.add(node);
      }
    } else {
      LOG.trace("startDecommission: Node {} in {}, nothing to do." +
          node, node.getAdminState());
    }
  }

  @VisibleForTesting
  public void stopDecommission(DatanodeDescriptor node) {
    if (node.isDecommissionInProgress() || node.isDecommissioned()) {
      hbManager.stopDecommission(node);
      pendingNodes.remove(node);
      decomNodeBlocks.remove(node);
    } else {
      LOG.trace("stopDecommission: Node {} in {}, nothing to do." +
          node, node.getAdminState());
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/HeartbeatManager.java
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HeartbeatManager implements DatanodeStatistics {
  static final Logger LOG = LoggerFactory.getLogger(HeartbeatManager.class);

  }

  synchronized void startDecommission(final DatanodeDescriptor node) {
    if (!node.isAlive) {
      LOG.info("Dead node {} is decommissioned immediately.", node);
      node.setDecommissioned();
    } else {
      stats.subtract(node);
      node.startDecommission();
      stats.add(node);
    }
  }

  synchronized void stopDecommission(final DatanodeDescriptor node) {
    LOG.info("Stopping decommissioning of {} node {}",
        node.isAlive ? "live" : "dead", node);
    if (!node.isAlive) {
      node.stopDecommission();
    } else {
      stats.subtract(node);
      node.stopDecommission();
      stats.add(node);
    }
  }
  

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestNamenodeCapacityReport.java
        dn.shutdown();
        DFSTestUtil.setDatanodeDead(dnd);
        BlockManagerTestUtil.checkHeartbeat(namesystem.getBlockManager());
        dnm.getDecomManager().startDecommission(dnd);
        expectedInServiceNodes--;
        assertEquals(expectedInServiceNodes, namesystem.getNumLiveDataNodes());
        assertEquals(expectedInServiceNodes, getNumDNInService(namesystem));
        dnm.getDecomManager().stopDecommission(dnd);
        assertEquals(expectedInServiceNodes, getNumDNInService(namesystem));
      }


