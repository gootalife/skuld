hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  public static final String  DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY = "dfs.web.authentication.kerberos.keytab";
  public static final String  DFS_NAMENODE_MAX_OP_SIZE_KEY = "dfs.namenode.max.op.size";
  public static final int     DFS_NAMENODE_MAX_OP_SIZE_DEFAULT = 50 * 1024 * 1024;
  public static final String  DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY =
      "dfs.namenode.available-space-block-placement-policy.balanced-space-preference-fraction";
  public static final float   DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT =
      0.6f;

  public static final String DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY = "dfs.block.local-path-access.user";
  public static final String DFS_DOMAIN_SOCKET_PATH_KEY = "dfs.domain.socket.path";

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/AvailableSpaceBlockPlacementPolicy.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/AvailableSpaceBlockPlacementPolicy.java

package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.net.NetworkTopology;

public class AvailableSpaceBlockPlacementPolicy extends
    BlockPlacementPolicyDefault {
  private static final Log LOG = LogFactory
      .getLog(AvailableSpaceBlockPlacementPolicy.class);
  private static final Random RAND = new Random();
  private int balancedPreference =
      (int) (100 * DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT);

  @Override
  public void initialize(Configuration conf, FSClusterStats stats,
      NetworkTopology clusterMap, Host2NodesMap host2datanodeMap) {
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
    float balancedPreferencePercent =
        conf.getFloat(
          DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY,
          DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT);

    LOG.info("Available space block placement policy initialized: "
        + DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
        + " = " + balancedPreferencePercent);

    if (balancedPreferencePercent > 1.0) {
      LOG.warn("The value of "
          + DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
          + " is greater than 1.0 but should be in the range 0.0 - 1.0");
    }
    if (balancedPreferencePercent < 0.5) {
      LOG.warn("The value of "
          + DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
          + " is less than 0.5 so datanodes with more used percent will"
          + " receive  more block allocations.");
    }
    balancedPreference = (int) (100 * balancedPreferencePercent);
  }

  @Override
  protected DatanodeDescriptor chooseDataNode(String scope) {
    DatanodeDescriptor a = (DatanodeDescriptor) clusterMap.chooseRandom(scope);
    DatanodeDescriptor b = (DatanodeDescriptor) clusterMap.chooseRandom(scope);
    int ret = compareDataNode(a, b);
    if (ret == 0) {
      return a;
    } else if (ret < 0) {
      return (RAND.nextInt(100) < balancedPreference) ? a : b;
    } else {
      return (RAND.nextInt(100) < balancedPreference) ? b : a;
    }
  }

  protected int compareDataNode(final DatanodeDescriptor a,
      final DatanodeDescriptor b) {
    if (a.equals(b)
        || Math.abs(a.getDfsUsedPercent() - b.getDfsUsedPercent()) < 5) {
      return 0;
    }
    return a.getDfsUsedPercent() < b.getDfsUsedPercent() ? -1 : 1;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockPlacementPolicyDefault.java
    boolean badTarget = false;
    DatanodeStorageInfo firstChosen = null;
    while(numOfReplicas > 0 && numOfAvailableNodes > 0) {
      DatanodeDescriptor chosenNode = chooseDataNode(scope);
      if (excludedNodes.add(chosenNode)) { //was not in the excluded list
        if (LOG.isDebugEnabled()) {
          builder.append("\nNode ").append(NodeBase.getPath(chosenNode)).append(" [");
    return firstChosen;
  }

  protected DatanodeDescriptor chooseDataNode(final String scope) {
    return (DatanodeDescriptor) clusterMap.chooseRandom(scope);
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestAvailableSpaceBlockPlacementPolicy.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestAvailableSpaceBlockPlacementPolicy.java

package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.File;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.TestBlockStoragePolicy;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.test.PathUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvailableSpaceBlockPlacementPolicy {
  private final static int numRacks = 4;
  private final static int nodesPerRack = 5;
  private final static int blockSize = 1024;
  private final static int chooseTimes = 10000;
  private final static String file = "/tobers/test";
  private final static int replica = 3;

  private static DatanodeStorageInfo[] storages;
  private static DatanodeDescriptor[] dataNodes;
  private static Configuration conf;
  private static NameNode namenode;
  private static BlockPlacementPolicy placementPolicy;
  private static NetworkTopology cluster;

  @BeforeClass
  public static void setupCluster() throws Exception {
    conf = new HdfsConfiguration();
    conf.setFloat(
      DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY,
      0.6f);
    String[] racks = new String[numRacks];
    for (int i = 0; i < numRacks; i++) {
      racks[i] = "/rack" + i;
    }

    String[] owerRackOfNodes = new String[numRacks * nodesPerRack];
    for (int i = 0; i < nodesPerRack; i++) {
      for (int j = 0; j < numRacks; j++) {
        owerRackOfNodes[i * numRacks + j] = racks[j];
      }
    }

    storages = DFSTestUtil.createDatanodeStorageInfos(owerRackOfNodes);
    dataNodes = DFSTestUtil.toDatanodeDescriptor(storages);

    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    File baseDir = PathUtils.getTestDir(AvailableSpaceBlockPlacementPolicy.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, new File(baseDir, "name").getPath());
    conf.set(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
      AvailableSpaceBlockPlacementPolicy.class.getName());

    DFSTestUtil.formatNameNode(conf);
    namenode = new NameNode(conf);

    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    placementPolicy = bm.getBlockPlacementPolicy();
    cluster = bm.getDatanodeManager().getNetworkTopology();
    for (int i = 0; i < nodesPerRack * numRacks; i++) {
      cluster.add(dataNodes[i]);
    }

    setupDataNodeCapacity();
  }

  private static void updateHeartbeatWithUsage(DatanodeDescriptor dn,
      long capacity, long dfsUsed, long remaining, long blockPoolUsed,
      long dnCacheCapacity, long dnCacheUsed, int xceiverCount,
      int volFailures) {
    dn.getStorageInfos()[0].setUtilizationForTesting(
        capacity, dfsUsed, remaining, blockPoolUsed);
    dn.updateHeartbeat(
        BlockManagerTestUtil.getStorageReportsForDatanode(dn),
        dnCacheCapacity, dnCacheUsed, xceiverCount, volFailures, null);
  }

  private static void setupDataNodeCapacity() {
    for (int i = 0; i < nodesPerRack * numRacks; i++) {
      if ((i % 2) == 0) {
        updateHeartbeatWithUsage(dataNodes[i], 2 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
          0L, 2 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize, 0L, 0L, 0L, 0, 0);
      } else {
        updateHeartbeatWithUsage(dataNodes[i], 2 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
          HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize, HdfsServerConstants.MIN_BLOCKS_FOR_WRITE
      }
    }
  }

  @Test
  public void testPolicyReplacement() {
    Assert.assertTrue((placementPolicy instanceof AvailableSpaceBlockPlacementPolicy));
  }

  @Test
  public void testChooseTarget() {
    int total = 0;
    int moreRemainingNode = 0;
    for (int i = 0; i < chooseTimes; i++) {
      DatanodeStorageInfo[] targets =
          namenode
              .getNamesystem()
              .getBlockManager()
              .getBlockPlacementPolicy()
              .chooseTarget(file, replica, null, new ArrayList<DatanodeStorageInfo>(), false, null,
                blockSize, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY);

      Assert.assertTrue(targets.length == replica);
      for (int j = 0; j < replica; j++) {
        total++;
        if (targets[j].getDatanodeDescriptor().getRemainingPercent() > 60) {
          moreRemainingNode++;
        }
      }
    }
    Assert.assertTrue(total == replica * chooseTimes);
    double possibility = 1.0 * moreRemainingNode / total;
    Assert.assertTrue(possibility > 0.52);
    Assert.assertTrue(possibility < 0.55);
  }

  @AfterClass
  public static void teardownCluster() {
    if (namenode != null) {
      namenode.stop();
    }
  }
}

