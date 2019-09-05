hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java
  synchronized void checkDatanodeUuid() throws IOException {
    if (storage.getDatanodeUuid() == null) {
      storage.setDatanodeUuid(generateUuid());
      storage.writeAll();
  }

  public String getDatanodeUuid() {
    return storage == null ? null : storage.getDatanodeUuid();
  }

  boolean shouldRun() {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDataNodeUUID.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDataNodeUUID.java

package org.apache.hadoop.hdfs.server.datanode;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestDataNodeUUID {

  @Test
  public void testDatanodeUuid() throws Exception {

    final InetSocketAddress NN_ADDR = new InetSocketAddress(
      "localhost", 5020);
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, "0.0.0.0:0");
    FileSystem.setDefaultUri(conf,
      "hdfs://" + NN_ADDR.getHostName() + ":" + NN_ADDR.getPort());
    ArrayList<StorageLocation> locations = new ArrayList<>();

    DataNode dn = new DataNode(conf, locations, null);

    String nullString = null;
    assertEquals(dn.getDatanodeUuid(), nullString);

    dn.checkDatanodeUuid();

    assertNotEquals(dn.getDatanodeUuid(), nullString);
  }
}

