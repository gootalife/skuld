hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockPlacementPolicyDefault.java
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
    if (preferLocalNode && localMachine instanceof DatanodeDescriptor
        && clusterMap.contains(localMachine)) {
      DatanodeDescriptor localDatanode = (DatanodeDescriptor) localMachine;
      if (excludedNodes.add(localMachine)) { // was not in the excluded list

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestDeadDatanode.java
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.net.Node;
import org.junit.After;
import org.junit.Test;

    assertEquals(cmd[0].getAction(), RegisterCommand.REGISTER
        .getAction());
  }

  @Test
  public void testDeadNodeAsBlockTarget() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();

    String poolId = cluster.getNamesystem().getBlockPoolId();
    DataNode dn = cluster.getDataNodes().get(0);
    DatanodeRegistration reg = DataNodeTestUtils.getDNRegistrationForBP(cluster
        .getDataNodes().get(0), poolId);
    BlockManager bm = cluster.getNamesystem().getBlockManager();
    DatanodeManager dm = bm.getDatanodeManager();
    Node clientNode = dm.getDatanode(reg);

    DFSTestUtil.waitForDatanodeState(cluster, reg.getDatanodeUuid(), true,
        20000);

    dn.shutdown();
    DFSTestUtil.waitForDatanodeState(cluster, reg.getDatanodeUuid(), false,
        20000);
    DatanodeStorageInfo[] results = bm.chooseTarget4NewBlock("/hello", 3,
        clientNode, new HashSet<Node>(), 256 * 1024 * 1024L, null, (byte) 7);
    for (DatanodeStorageInfo datanodeStorageInfo : results) {
      assertFalse("Dead node should not be choosen", datanodeStorageInfo
          .getDatanodeDescriptor().equals(clientNode));
    }
  }
}

