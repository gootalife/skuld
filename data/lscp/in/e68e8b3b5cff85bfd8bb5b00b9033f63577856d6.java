hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLog.java
      .setClientMachine(
          newNode.getFileUnderConstructionFeature().getClientMachine())
      .setOverwrite(overwrite)
      .setStoragePolicyId(newNode.getLocalStoragePolicyID());

    AclFeature f = newNode.getAclFeature();
    if (f != null) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockStoragePolicy.java

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
      cluster.shutdown();
    }
  }

  @Test
  public void testGetFileStoragePolicyAfterRestartNN() throws Exception {
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(REPLICATION)
        .storageTypes(
            new StorageType[] {StorageType.DISK, StorageType.ARCHIVE})
        .build();
    cluster.waitActive();
    final DistributedFileSystem fs = cluster.getFileSystem();
    try {
      final String file = "/testScheduleWithinSameNode/file";
      Path dir = new Path("/testScheduleWithinSameNode");
      fs.mkdirs(dir);
      fs.setStoragePolicy(dir, "COLD");
      final FSDataOutputStream out = fs.create(new Path(file));
      out.writeChars("testScheduleWithinSameNode");
      out.close();
      fs.setStoragePolicy(dir, "HOT");
      HdfsFileStatus status = fs.getClient().getFileInfo(file);
      Assert
          .assertTrue(
              "File storage policy should be HOT",
              status.getStoragePolicy()
              == HdfsServerConstants.HOT_STORAGE_POLICY_ID);
      cluster.restartNameNode(true);
      status = fs.getClient().getFileInfo(file);
      Assert
          .assertTrue(
              "File storage policy should be HOT",
              status.getStoragePolicy()
              == HdfsServerConstants.HOT_STORAGE_POLICY_ID);

    } finally {
      cluster.shutdown();
    }
  }
}

