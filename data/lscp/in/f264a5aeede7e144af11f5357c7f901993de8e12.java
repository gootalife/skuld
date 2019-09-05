hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
      if (LOG.isDebugEnabled()) {
        LOG.debug("Processing previouly queued message " + rbi);
      }
      if (rbi.getReportedState() == null) {
        DatanodeStorageInfo storageInfo = rbi.getStorageInfo();
        removeStoredBlock(rbi.getBlock(),
            storageInfo.getDatanodeDescriptor());
      } else {
        processAndHandleReportedBlock(rbi.getStorageInfo(),
            rbi.getBlock(), rbi.getReportedState(), null);
      }
    }
  }
  
    }
  }

  private void removeStoredBlock(DatanodeStorageInfo storageInfo, Block block,
      DatanodeDescriptor node) {
    if (shouldPostponeBlocksFromFuture &&
        namesystem.isGenStampInFuture(block)) {
      queueReportedBlock(storageInfo, block, null,
          QUEUE_REASON_FUTURE_GENSTAMP);
      return;
    }
    removeStoredBlock(block, node);
  }

    for (ReceivedDeletedBlockInfo rdbi : srdb.getBlocks()) {
      switch (rdbi.getStatus()) {
      case DELETED_BLOCK:
        removeStoredBlock(storageInfo, rdbi.getBlock(), node);
        deleted++;
        break;
      case RECEIVED_BLOCK:

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestBlockReplacement.java
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Time;
import org.junit.Test;
    }
  }

  @Test
  public void testDeletedBlockWhenAddBlockIsInEdit() throws Exception {
    Configuration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf)
       .nnTopology(MiniDFSNNTopology.simpleHATopology())
       .numDataNodes(1).build();
    DFSClient client = null;
    try {
      cluster.waitActive();
      assertEquals("Number of namenodes is not 2", 2,
          cluster.getNumNameNodes());
      cluster.transitionToActive(0);
      assertTrue("Namenode 0 should be in active state",
          cluster.getNameNode(0).isActiveState());
      assertTrue("Namenode 1 should be in standby state",
          cluster.getNameNode(1).isStandbyState());

      DataNodeTestUtils.triggerHeartbeat(cluster.getDataNodes().get(0));
      FileSystem fs = cluster.getFileSystem(0);

      cluster.getDataNodes().get(0).triggerBlockReport(
          new BlockReportOptions.Factory().setIncremental(false).build());

      Path fileName = new Path("/tmp.txt");
      DFSTestUtil.createFile(fs, fileName, 10L, (short)1, 1234L);
      DFSTestUtil.waitReplication(fs,fileName, (short)1);

      client = new DFSClient(cluster.getFileSystem(0).getUri(), conf);
      List<LocatedBlock> locatedBlocks = client.getNamenode().
          getBlockLocations("/tmp.txt", 0, 10L).getLocatedBlocks();
      assertTrue(locatedBlocks.size() == 1);
      assertTrue(locatedBlocks.get(0).getLocations().length == 1);

      cluster.startDataNodes(conf, 1, true, null, null, null, null);
      assertEquals("Number of datanodes should be 2", 2,
          cluster.getDataNodes().size());

      DataNode dn0 = cluster.getDataNodes().get(0);
      DataNode dn1 = cluster.getDataNodes().get(1);
      String activeNNBPId = cluster.getNamesystem(0).getBlockPoolId();
      DatanodeDescriptor sourceDnDesc = NameNodeAdapter.getDatanode(
          cluster.getNamesystem(0), dn0.getDNRegistrationForBP(activeNNBPId));
      DatanodeDescriptor destDnDesc = NameNodeAdapter.getDatanode(
          cluster.getNamesystem(0), dn1.getDNRegistrationForBP(activeNNBPId));

      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, fileName);

      LOG.info("replaceBlock:  " + replaceBlock(block,
          (DatanodeInfo)sourceDnDesc, (DatanodeInfo)sourceDnDesc,
          (DatanodeInfo)destDnDesc));
      Thread.sleep(3000);
      cluster.getDataNodes().get(0).triggerBlockReport(
         new BlockReportOptions.Factory().setIncremental(true).build());

      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);

      assertTrue("Namenode 1 should be in active state",
         cluster.getNameNode(1).isActiveState());
      assertTrue("Namenode 0 should be in standby state",
         cluster.getNameNode(0).isStandbyState());
      client.close();

      client = new DFSClient(cluster.getFileSystem(1).getUri(), conf);
      List<LocatedBlock> locatedBlocks1 = client.getNamenode()
          .getBlockLocations("/tmp.txt", 0, 10L).getLocatedBlocks();

      assertEquals(1, locatedBlocks1.size());
      assertEquals("The block should be only on 1 datanode ", 1,
          locatedBlocks1.get(0).getLocations().length);
    } finally {
      IOUtils.cleanup(null, client);
      cluster.shutdown();
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestDNFencing.java
    banner("NN2 Metadata immediately after failover");
    doMetasave(nn2);
    
    banner("Triggering heartbeats and block reports so that fencing is completed");
    cluster.triggerHeartbeats();
    cluster.triggerBlockReports();

