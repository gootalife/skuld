hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockPlacementPolicyRackFaultTolerant.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockPlacementPolicyRackFaultTolerant.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.util.*;

@InterfaceAudience.Private
public class BlockPlacementPolicyRackFaultTolerant extends BlockPlacementPolicyDefault {

  @Override
  protected int[] getMaxNodesPerRack(int numOfChosen, int numOfReplicas) {
    int clusterSize = clusterMap.getNumOfLeaves();
    int totalNumOfReplicas = numOfChosen + numOfReplicas;
    if (totalNumOfReplicas > clusterSize) {
      numOfReplicas -= (totalNumOfReplicas-clusterSize);
      totalNumOfReplicas = clusterSize;
    }
    int numOfRacks = clusterMap.getNumOfRacks();
    if (numOfRacks == 1 || totalNumOfReplicas <= 1) {
      return new int[] {numOfReplicas, totalNumOfReplicas};
    }
    if(totalNumOfReplicas<numOfRacks){
      return new int[] {numOfReplicas, 1};
    }
    int maxNodesPerRack = (totalNumOfReplicas - 1) / numOfRacks + 1;
    return new int[] {numOfReplicas, maxNodesPerRack};
  }

  @Override
  protected Node chooseTargetInOrder(int numOfReplicas,
                                 Node writer,
                                 final Set<Node> excludedNodes,
                                 final long blocksize,
                                 final int maxNodesPerRack,
                                 final List<DatanodeStorageInfo> results,
                                 final boolean avoidStaleNodes,
                                 final boolean newBlock,
                                 EnumMap<StorageType, Integer> storageTypes)
                                 throws NotEnoughReplicasException {
    int totalReplicaExpected = results.size() + numOfReplicas;
    int numOfRacks = clusterMap.getNumOfRacks();
    if (totalReplicaExpected < numOfRacks ||
        totalReplicaExpected % numOfRacks == 0) {
      writer = chooseOnce(numOfReplicas, writer, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
      return writer;
    }

    assert totalReplicaExpected > (maxNodesPerRack -1) * numOfRacks;

    HashMap<String, Integer> rackCounts = new HashMap<>();
    for (DatanodeStorageInfo dsInfo : results) {
      String rack = dsInfo.getDatanodeDescriptor().getNetworkLocation();
      Integer count = rackCounts.get(rack);
      if (count != null) {
        rackCounts.put(rack, count + 1);
      } else {
        rackCounts.put(rack, 1);
      }
    }
    int excess = 0; // Sum of the above (maxNodesPerRack-1) part of nodes in results
    for (int count : rackCounts.values()) {
      if (count > maxNodesPerRack -1) {
        excess += count - (maxNodesPerRack -1);
      }
    }
    numOfReplicas = Math.min(totalReplicaExpected - results.size(),
        (maxNodesPerRack -1) * numOfRacks - (results.size() - excess));

    writer = chooseOnce(numOfReplicas, writer, new HashSet<>(excludedNodes),
        blocksize, maxNodesPerRack -1, results, avoidStaleNodes, storageTypes);

    for (DatanodeStorageInfo resultStorage : results) {
      addToExcludedNodes(resultStorage.getDatanodeDescriptor(), excludedNodes);
    }

    numOfReplicas = totalReplicaExpected - results.size();
    chooseOnce(numOfReplicas, writer, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes, storageTypes);

    return writer;
  }

  private Node chooseOnce(int numOfReplicas,
                            Node writer,
                            final Set<Node> excludedNodes,
                            final long blocksize,
                            final int maxNodesPerRack,
                            final List<DatanodeStorageInfo> results,
                            final boolean avoidStaleNodes,
                            EnumMap<StorageType, Integer> storageTypes)
                            throws NotEnoughReplicasException {
    if (numOfReplicas == 0) {
      return writer;
    }
    writer = chooseLocalStorage(writer, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes, storageTypes, true)
        .getDatanodeDescriptor();
    if (--numOfReplicas == 0) {
      return writer;
    }
    chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    return writer;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestBlockPlacementPolicyRackFaultTolerant.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestBlockPlacementPolicyRackFaultTolerant.java
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyRackFaultTolerant;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.net.StaticMapping;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBlockPlacementPolicyRackFaultTolerant {

  private static final int DEFAULT_BLOCK_SIZE = 1024;
  private MiniDFSCluster cluster = null;
  private NamenodeProtocols nameNodeRpc = null;
  private FSNamesystem namesystem = null;
  private PermissionStatus perm = null;

  @Before
  public void setup() throws IOException {
    StaticMapping.resetMap();
    Configuration conf = new HdfsConfiguration();
    final ArrayList<String> rackList = new ArrayList<String>();
    final ArrayList<String> hostList = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 2; j++) {
        rackList.add("/rack" + i);
        hostList.add("/host" + i + j);
      }
    }
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        BlockPlacementPolicyRackFaultTolerant.class,
        BlockPlacementPolicy.class);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE / 2);
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(hostList.size())
        .racks(rackList.toArray(new String[rackList.size()]))
        .hosts(hostList.toArray(new String[hostList.size()]))
        .build();
    cluster.waitActive();
    nameNodeRpc = cluster.getNameNodeRpc();
    namesystem = cluster.getNamesystem();
    perm = new PermissionStatus("TestBlockPlacementPolicyEC", null,
        FsPermission.getDefault());
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testChooseTarget() throws Exception {
    doTestChooseTargetNormalCase();
    doTestChooseTargetSpecialCase();
  }

  private void doTestChooseTargetNormalCase() throws Exception {
    String clientMachine = "client.foo.com";
    short[][] testSuite = {
        {3, 2}, {3, 7}, {3, 8}, {3, 10}, {9, 1}, {10, 1}, {10, 6}, {11, 6},
        {11, 9}
    };
    int fileCount = 0;
    for (int i = 0; i < 5; i++) {
      for (short[] testCase : testSuite) {
        short replication = testCase[0];
        short additionalReplication = testCase[1];
        String src = "/testfile" + (fileCount++);
        HdfsFileStatus fileStatus = namesystem.startFile(src, perm,
            clientMachine, clientMachine, EnumSet.of(CreateFlag.CREATE), true,
            replication, DEFAULT_BLOCK_SIZE, null, false);

        LocatedBlock locatedBlock = nameNodeRpc.addBlock(src, clientMachine,
            null, null, fileStatus.getFileId(), null);
        doTestLocatedBlock(replication, locatedBlock);

        LocatedBlock additionalLocatedBlock =
            nameNodeRpc.getAdditionalDatanode(src, fileStatus.getFileId(),
                locatedBlock.getBlock(), locatedBlock.getLocations(),
                locatedBlock.getStorageIDs(), new DatanodeInfo[0],
                additionalReplication, clientMachine);
        doTestLocatedBlock(replication + additionalReplication, additionalLocatedBlock);
      }
    }
  }

  private void doTestChooseTargetSpecialCase() throws Exception {
    String clientMachine = "client.foo.com";
    String src = "/testfile_1_";
    HdfsFileStatus fileStatus = namesystem.startFile(src, perm,
        clientMachine, clientMachine, EnumSet.of(CreateFlag.CREATE), true,
        (short) 20, DEFAULT_BLOCK_SIZE, null, false);

    LocatedBlock locatedBlock = nameNodeRpc.addBlock(src, clientMachine,
        null, null, fileStatus.getFileId(), null);
    doTestLocatedBlock(20, locatedBlock);

    DatanodeInfo[] locs = locatedBlock.getLocations();
    String[] storageIDs = locatedBlock.getStorageIDs();

    for (int time = 0; time < 5; time++) {
      shuffle(locs, storageIDs);
      for (int i = 1; i < locs.length; i++) {
        DatanodeInfo[] partLocs = new DatanodeInfo[i];
        String[] partStorageIDs = new String[i];
        System.arraycopy(locs, 0, partLocs, 0, i);
        System.arraycopy(storageIDs, 0, partStorageIDs, 0, i);
        for (int j = 1; j < 20 - i; j++) {
          LocatedBlock additionalLocatedBlock =
              nameNodeRpc.getAdditionalDatanode(src, fileStatus.getFileId(),
                  locatedBlock.getBlock(), partLocs,
                  partStorageIDs, new DatanodeInfo[0],
                  j, clientMachine);
          doTestLocatedBlock(i + j, additionalLocatedBlock);
        }
      }
    }
  }

  private void shuffle(DatanodeInfo[] locs, String[] storageIDs) {
    int length = locs.length;
    Object[][] pairs = new Object[length][];
    for (int i = 0; i < length; i++) {
      pairs[i] = new Object[]{locs[i], storageIDs[i]};
    }
    DFSUtil.shuffle(pairs);
    for (int i = 0; i < length; i++) {
      locs[i] = (DatanodeInfo) pairs[i][0];
      storageIDs[i] = (String) pairs[i][1];
    }
  }

  private void doTestLocatedBlock(int replication, LocatedBlock locatedBlock) {
    assertEquals(replication, locatedBlock.getLocations().length);

    HashMap<String, Integer> racksCount = new HashMap<String, Integer>();
    for (DatanodeInfo node :
        locatedBlock.getLocations()) {
      addToRacksCount(node.getNetworkLocation(), racksCount);
    }

    int minCount = Integer.MAX_VALUE;
    int maxCount = Integer.MIN_VALUE;
    for (Integer rackCount : racksCount.values()) {
      minCount = Math.min(minCount, rackCount);
      maxCount = Math.max(maxCount, rackCount);
    }
    assertTrue(maxCount - minCount <= 1);
  }

  private void addToRacksCount(String rack, HashMap<String, Integer> racksCount) {
    Integer count = racksCount.get(rack);
    if (count == null) {
      racksCount.put(rack, 1);
    } else {
      racksCount.put(rack, count + 1);
    }
  }
}

