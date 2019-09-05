hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/MiniDFSCluster.java

  String makeDataNodeDirs(int dnIndex, StorageType[] storageTypes) throws IOException {
    StringBuilder sb = new StringBuilder();
    for (int j = 0; j < storagesPerDatanode; ++j) {
      if ((storageTypes != null) && (j >= storageTypes.length)) {
        break;
      }
      File dir = getInstanceStorageDir(dnIndex, j);
      dir.mkdirs();
      if (!dir.isDirectory()) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestMiniDFSCluster.java
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.test.PathUtils;
import org.junit.Before;
import org.junit.Test;
      MiniDFSCluster.shutdownCluster(cluster5);
    }
  }

  @Test
  public void testClusterSetDatanodeDifferentStorageType() throws IOException {
    final Configuration conf = new HdfsConfiguration();
    StorageType[][] storageType = new StorageType[][] {
        {StorageType.DISK, StorageType.ARCHIVE}, {StorageType.DISK},
        {StorageType.ARCHIVE}};
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).storageTypes(storageType).build();
    try {
      cluster.waitActive();
      ArrayList<DataNode> dataNodes = cluster.getDataNodes();
      for (int i = 0; i < storageType.length; i++) {
        assertEquals(DataNode.getStorageLocations(dataNodes.get(i).getConf())
            .size(), storageType[i].length);
      }
    } finally {
      MiniDFSCluster.shutdownCluster(cluster);
    }
  }

  @Test
  public void testClusterNoStorageTypeSetForDatanodes() throws IOException {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).build();
    try {
      cluster.waitActive();
      ArrayList<DataNode> dataNodes = cluster.getDataNodes();
      for (DataNode datanode : dataNodes) {
        assertEquals(DataNode.getStorageLocations(datanode.getConf()).size(),
            2);
      }
    } finally {
      MiniDFSCluster.shutdownCluster(cluster);
    }
  }
}

