hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoContiguous.java
    return true;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
      if (storageInfo.getBlockReportCount() == 0) {
        LOG.info("Processing first storage report for " +
            storageInfo.getStorageID() + " from datanode " +
            nodeID.getDatanodeUuid());
        processFirstBlockReport(storageInfo, newReport);
      } else {
        invalidatedBlocks = processReport(storageInfo, newReport);
    for (BlockReportReplica iblk : report) {
      ReplicaState reportedState = iblk.getState();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Initial report of block " + iblk.getBlockName()
            + " on " + storageInfo.getDatanodeDescriptor() + " size " +
            iblk.getNumBytes() + " replicaState = " + reportedState);
      }
      if (shouldPostponeBlocksFromFuture &&
          namesystem.isGenStampInFuture(iblk)) {
        queueReportedBlock(storageInfo, iblk, reportedState,
        storageInfo, ucBlock.reportedBlock, ucBlock.reportedState);

    if (ucBlock.reportedState == ReplicaState.FINALIZED &&
        (block.findStorageInfo(storageInfo) < 0)) {
      addStoredBlock(block, storageInfo, null, true);
    }
  } 

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestNameNodePrunesMissingStorages.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi.FsVolumeReferences;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
      }
      String volumeDirectoryToRemove = null;
      try (FsVolumeReferences volumes =
          datanodeToRemoveStorageFrom.getFSDataset().getFsVolumeReferences()) {
        assertEquals(NUM_STORAGES_PER_DN, volumes.size());
        for (FsVolumeSpi volume : volumes) {
      }
    }
  }

  private static void rewriteVersionFile(File versionFile,
                            String newStorageId) throws IOException {
    BufferedReader in = new BufferedReader(new FileReader(versionFile));
    File newVersionFile =
        new File(versionFile.getParent(), UUID.randomUUID().toString());
    Writer out = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(newVersionFile), "UTF-8"));
    final String STORAGE_ID = "storageID=";
    boolean success = false;
    try {
      String line;
      while ((line = in.readLine()) != null) {
        if (line.startsWith(STORAGE_ID)) {
          out.write(STORAGE_ID + newStorageId + "\n");
        } else {
          out.write(line + "\n");
        }
      }
      in.close();
      in = null;
      out.close();
      out = null;
      newVersionFile.renameTo(versionFile);
      success = true;
    } finally {
      if (in != null) {
        in.close();
      }
      if (out != null) {
        out.close();
      }
      if (!success) {
        versionFile.delete();
      }
    }
  }

  @Test(timeout=300000)
  public void testRenamingStorageIds() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 0);
    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(conf).numDataNodes(1)
        .storagesPerDatanode(1)
        .build();
    GenericTestUtils.setLogLevel(BlockManager.LOG, Level.ALL);
    try {
      cluster.waitActive();
      final Path TEST_PATH = new Path("/foo1");
      DistributedFileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH, 1, (short)1, 0xdeadbeef);
      DataNode dn = cluster.getDataNodes().get(0);
      FsVolumeReferences volumeRefs =
          dn.getFSDataset().getFsVolumeReferences();
      final String newStorageId = DatanodeStorage.generateUuid();
      try {
        File currentDir = new File(volumeRefs.get(0).getBasePath(), "current");
        File versionFile = new File(currentDir, "VERSION");
        rewriteVersionFile(versionFile, newStorageId);
      } finally {
        volumeRefs.close();
      }
      final ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, TEST_PATH);
      cluster.restartDataNodes();
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          cluster.getNamesystem().writeLock();
          try {
            Iterator<DatanodeStorageInfo> storageInfoIter =
                cluster.getNamesystem().getBlockManager().
                    getStorages(block.getLocalBlock()).iterator();
            if (!storageInfoIter.hasNext()) {
              LOG.info("Expected to find a storage for " +
                      block.getBlockName() + ", but nothing was found.  " +
                      "Continuing to wait.");
              return false;
            }
            DatanodeStorageInfo info = storageInfoIter.next();
            if (!newStorageId.equals(info.getStorageID())) {
              LOG.info("Expected " + block.getBlockName() + " to " +
                  "be in storage id " + newStorageId + ", but it " +
                  "was in " + info.getStorageID() + ".  Continuing " +
                  "to wait.");
              return false;
            }
            LOG.info("Successfully found " + block.getBlockName() + " in " +
                "be in storage id " + newStorageId);
          } finally {
            cluster.getNamesystem().writeUnlock();
          }
          return true;
        }
      }, 20, 100000);
    } finally {
      cluster.shutdown();
    }
  }
}

