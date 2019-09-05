hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DatanodeStorageInfo.java
    this.blockReportCount = blockReportCount;
  }

  public boolean areBlockContentsStale() {
    return blockContentsStale;
  }

    return getState() == State.FAILED && numBlocks != 0;
  }

  public String getStorageID() {
    return storageID;
  }

  public StorageType getStorageType() {
    return storageType;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NamenodeFsck.java
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;
  private boolean showStoragePolcies = false;
  private boolean showCorruptFileBlocks = false;

  private boolean showReplicaDetails = false;
  private long staleInterval;
        networktopology,
        namenode.getNamesystem().getBlockManager().getDatanodeManager()
        .getHost2DatanodeMap());
    this.staleInterval =
        conf.getLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
          DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
    
    for (Iterator<String> it = pmap.keySet().iterator(); it.hasNext();) {
      String key = it.next();
      else if (key.equals("blocks")) { this.showBlocks = true; }
      else if (key.equals("locations")) { this.showLocations = true; }
      else if (key.equals("racks")) { this.showRacks = true; }
      else if (key.equals("replicadetails")) {
        this.showReplicaDetails = true;
      }
      else if (key.equals("storagepolicies")) { this.showStoragePolcies = true; }
      else if (key.equals("openforwrite")) {this.showOpenFiles = true; }
      else if (key.equals("listcorruptfileblocks")) {
      ExtendedBlock block = lBlk.getBlock();
      boolean isCorrupt = lBlk.isCorrupt();
      String blkName = block.toString();
      BlockManager bm = namenode.getNamesystem().getBlockManager();
      NumberReplicas numberReplicas = bm.countNodes(block.getLocalBlock());
      int liveReplicas = numberReplicas.liveReplicas();
      int decommissionedReplicas = numberReplicas.decommissioned();;
      int decommissioningReplicas = numberReplicas.decommissioning();
      int totalReplicas = liveReplicas + decommissionedReplicas +
          decommissioningReplicas;
      res.totalReplicas += totalReplicas;
      Collection<DatanodeDescriptor> corruptReplicas = null;
      if (showReplicaDetails) {
        corruptReplicas = bm.getCorruptReplicas(block.getLocalBlock());
      }
      short targetFileReplication = file.getReplication();
      res.numExpectedReplicas += targetFileReplication;
      if(totalReplicas < minReplication){
        missize += block.getNumBytes();
      } else {
        report.append(" repl=" + liveReplicas);
        if (showLocations || showRacks || showReplicaDetails) {
          StringBuilder sb = new StringBuilder("[");
          Iterable<DatanodeStorageInfo> storages = bm.getStorages(block.getLocalBlock());
          for (Iterator<DatanodeStorageInfo> iterator = storages.iterator(); iterator.hasNext();) {
            DatanodeStorageInfo storage = iterator.next();
            DatanodeDescriptor dnDesc = storage.getDatanodeDescriptor();
            if (showRacks) {
              sb.append(NodeBase.getPath(dnDesc));
            } else {
              sb.append(new DatanodeInfoWithStorage(dnDesc, storage.getStorageID(), storage
                  .getStorageType()));
            }
            if (showReplicaDetails) {
              LightWeightLinkedSet<Block> blocksExcess =
                  bm.excessReplicateMap.get(dnDesc.getDatanodeUuid());
              sb.append("(");
              if (dnDesc.isDecommissioned()) {
                sb.append("DECOMMISSIONED)");
              } else if (dnDesc.isDecommissionInProgress()) {
                sb.append("DECOMMISSIONING)");
              } else if (corruptReplicas != null && corruptReplicas.contains(dnDesc)) {
                sb.append("CORRUPT)");
              } else if (blocksExcess != null && blocksExcess.contains(block.getLocalBlock())) {
                sb.append("EXCESS)");
              } else if (dnDesc.isStale(this.staleInterval)) {
                sb.append("STALE_NODE)");
              } else if (storage.areBlockContentsStale()) {
                sb.append("STALE_BLOCK_CONTENT)");
              } else {
                sb.append("LIVE)");
              }
            }
            if (iterator.hasNext()) {
              sb.append(", ");
            }
          }
          sb.append(']');
          report.append(" " + sb.toString());

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/DFSck.java
      + "\t-storagepolicies\tprint out storage policy summary for the blocks\n\n"
      + "\t-blockId\tprint out which file this blockId belongs to, locations"
      + " (nodes, racks) of this block, and other diagnostics info"
      + " (under replicated, corrupted or not, etc)\n"
      + "\t-replicaDetails\tprint out each replica details \n\n"
      + "Please Note:\n"
      + "\t1. By default fsck ignores files opened for write, "
      + "use -openforwrite to report such files. They are usually "
      else if (args[idx].equals("-blocks")) { url.append("&blocks=1"); }
      else if (args[idx].equals("-locations")) { url.append("&locations=1"); }
      else if (args[idx].equals("-racks")) { url.append("&racks=1"); }
      else if (args[idx].equals("-replicaDetails")) {
        url.append("&replicadetails=1");
      }
      else if (args[idx].equals("-storagepolicies")) { url.append("&storagepolicies=1"); }
      else if (args[idx].equals("-list-corruptfileblocks")) {
        url.append("&listcorruptfileblocks=1");

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFsck.java
    }
  }

  @Test(timeout = 60000)
  public void testFsckReplicaDetails() throws Exception {

    final short REPL_FACTOR = 1;
    short NUM_DN = 1;
    final long blockSize = 512;
    final long fileSize = 1024;
    boolean checkDecommissionInProgress = false;
    String[] racks = { "/rack1" };
    String[] hosts = { "host1" };

    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);

    MiniDFSCluster cluster;
    DistributedFileSystem dfs;
    cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).hosts(hosts).racks(racks).build();
    cluster.waitClusterUp();
    dfs = cluster.getFileSystem();

    final String testFile = new String("/testfile");
    final Path path = new Path(testFile);
    DFSTestUtil.createFile(dfs, path, fileSize, REPL_FACTOR, 1000L);
    DFSTestUtil.waitReplication(dfs, path, REPL_FACTOR);
    try {
      String fsckOut = runFsck(conf, 0, true, testFile, "-files", "-blocks", "-replicaDetails");
      assertTrue(fsckOut.contains(NamenodeFsck.HEALTHY_STATUS));
      assertTrue(fsckOut.contains("(LIVE)"));

      ExtendedBlock eb = DFSTestUtil.getFirstBlock(dfs, path);
      DatanodeDescriptor dn =
          cluster.getNameNode().getNamesystem().getBlockManager()
              .getBlockCollection(eb.getLocalBlock()).getBlocks()[0].getDatanode(0);
      cluster.getNameNode().getNamesystem().getBlockManager().getDatanodeManager()
          .getDecomManager().startDecommission(dn);
      String dnName = dn.getXferAddr();

      fsckOut = runFsck(conf, 0, true, testFile, "-files", "-blocks", "-replicaDetails");
      assertTrue(fsckOut.contains("(DECOMMISSIONING)"));

      cluster.startDataNodes(conf, 1, true, null, null, null);
      DatanodeInfo datanodeInfo = null;
      do {
        Thread.sleep(2000);
        for (DatanodeInfo info : dfs.getDataNodeStats()) {
          if (dnName.equals(info.getXferAddr())) {
            datanodeInfo = info;
          }
        }
        if (!checkDecommissionInProgress && datanodeInfo != null
            && datanodeInfo.isDecommissionInProgress()) {
          checkDecommissionInProgress = true;
        }
      } while (datanodeInfo != null && !datanodeInfo.isDecommissioned());

      fsckOut = runFsck(conf, 0, true, testFile, "-files", "-blocks", "-replicaDetails");
      assertTrue(fsckOut.contains("(DECOMMISSIONED)"));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }


