hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
        new HashMap<String, Map<String,Object>>();
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    blockManager.getDatanodeManager().fetchDatanodes(live, null, true);
    for (Iterator<DatanodeDescriptor> it = live.iterator(); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      if (node.isDecommissionInProgress() || node.isDecommissioned()) {
        it.remove();
      }
    }

    if (live.size() > 0) {
      float totalDfsUsed = 0;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDecommission.java
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hdfs.server.blockmanagement.DecommissionManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

    assertEquals("Unexpected number of pending nodes", pending,
        decomManager.getNumPendingNodes());
  }

  @Test
  public void testNodeUsageAfterDecommissioned()
      throws IOException, InterruptedException {
    nodeUsageVerification(2, new long[] { 26384L, 26384L },
        AdminStates.DECOMMISSIONED);
  }

  @Test
  public void testNodeUsageWhileDecommissioining()
      throws IOException, InterruptedException {
    nodeUsageVerification(1, new long[] { 26384L },
        AdminStates.DECOMMISSION_INPROGRESS);
  }

  @SuppressWarnings({ "unchecked" })
  public void nodeUsageVerification(int numDatanodes, long[] nodesCapacity,
      AdminStates decommissionState) throws IOException, InterruptedException {
    Map<String, Map<String, String>> usage = null;
    DatanodeInfo decommissionedNodeInfo = null;
    String zeroNodeUsage = "0.00%";
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1);
    FileSystem fileSys = null;
    Path file1 = new Path("testNodeUsage.dat");
    try {
      SimulatedFSDataset.setFactory(conf);
      cluster =
          new MiniDFSCluster.Builder(conf)
              .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(1))
              .numDataNodes(numDatanodes)
              .simulatedCapacities(nodesCapacity).build();
      cluster.waitActive();
      DFSClient client = getDfsClient(cluster.getNameNode(0), conf);
      validateCluster(client, numDatanodes);

      ArrayList<ArrayList<DatanodeInfo>> namenodeDecomList =
          new ArrayList<ArrayList<DatanodeInfo>>(1);
      namenodeDecomList.add(0, new ArrayList<DatanodeInfo>(numDatanodes));

      if (decommissionState == AdminStates.DECOMMISSIONED) {
        ArrayList<DatanodeInfo> decommissionedNode = namenodeDecomList.get(0);
        decommissionedNodeInfo = decommissionNode(0, null,
            decommissionedNode, decommissionState);
      }
      fileSys = cluster.getFileSystem(0);
      FSNamesystem ns = cluster.getNamesystem(0);
      writeFile(fileSys, file1, 1);
      Thread.sleep(2000);

      usage = (Map<String, Map<String, String>>) JSON.parse(ns.getNodeUsage());
      String minUsageBeforeDecom = usage.get("nodeUsage").get("min");
      assertTrue(!minUsageBeforeDecom.equalsIgnoreCase(zeroNodeUsage));

      if (decommissionState == AdminStates.DECOMMISSION_INPROGRESS) {
        ArrayList<DatanodeInfo> decommissioningNodes = namenodeDecomList.
            get(0);
        decommissionedNodeInfo = decommissionNode(0, null,
            decommissioningNodes, decommissionState);
        usage = (Map<String, Map<String, String>>)
            JSON.parse(ns.getNodeUsage());
        assertTrue(usage.get("nodeUsage").get("min").
            equalsIgnoreCase(zeroNodeUsage));
      }
      recommissionNode(0, decommissionedNodeInfo);

      usage = (Map<String, Map<String, String>>) JSON.parse(ns.getNodeUsage());
      String nodeusageAfterRecommi =
          decommissionState == AdminStates.DECOMMISSION_INPROGRESS
              ? minUsageBeforeDecom
              : zeroNodeUsage;
      assertTrue(usage.get("nodeUsage").get("min").
          equalsIgnoreCase(nodeusageAfterRecommi));
    } finally {
      cleanupFile(fileSys, file1);
      cluster.shutdown();
    }
  }
}

