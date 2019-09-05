hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImage.java
      purgeOldStorage(nnf);
      archivalManager.purgeCheckpoints(NameNodeFile.IMAGE_NEW);
    } finally {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFSImage.java
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
    }
  }

  @Test
  public void testRemovalStaleFsimageCkpt() throws IOException {
    MiniDFSCluster cluster = null;
    SecondaryNameNode secondary = null;
    Configuration conf = new HdfsConfiguration();
    try {
      cluster = new MiniDFSCluster.Builder(conf).
          numDataNodes(1).format(true).build();
      conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
          "0.0.0.0:0");
      secondary = new SecondaryNameNode(conf);
      secondary.doCheckpoint();
      NNStorage storage = secondary.getFSImage().storage;
      File currentDir = FSImageTestUtil.
          getCurrentDirs(storage, NameNodeDirType.IMAGE).get(0);
      File staleCkptFile = new File(currentDir.getPath() +
          "/fsimage.ckpt_0000000000000000002");
      staleCkptFile.createNewFile();
      assertTrue(staleCkptFile.exists());
      secondary.doCheckpoint();
      assertFalse(staleCkptFile.exists());
    } finally {
      if (secondary != null) {
        secondary.shutdown();
        secondary = null;
      }
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }


