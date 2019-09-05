hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/web/resources/NamenodeWebHdfsMethods.java
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.web.JsonUtil;
  static DatanodeInfo chooseDatanode(final NameNode namenode,
      final String path, final HttpOpParam.Op op, final long openOffset,
      final long blocksize, final String excludeDatanodes) throws IOException {
    FSNamesystem fsn = namenode.getNamesystem();
    if (fsn == null) {
      throw new IOException("Namesystem has not been intialized yet.");
    }
    final BlockManager bm = fsn.getBlockManager();
    
    HashSet<Node> excludes = new HashSet<Node>();
    if (excludeDatanodes != null) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/web/resources/TestWebHdfsDataLocality.java
package org.apache.hadoop.hdfs.server.namenode.web.resources;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

  private static final String RACK1 = "/rack1";
  private static final String RACK2 = "/rack2";

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testDataLocality() throws Exception {
    final Configuration conf = WebHdfsTestUtil.createConf();
      cluster.shutdown();
    }
  }

  @Test
  public void testChooseDatanodeBeforeNamesystemInit() throws Exception {
    NameNode nn = mock(NameNode.class);
    when(nn.getNamesystem()).thenReturn(null);
    exception.expect(IOException.class);
    exception.expectMessage("Namesystem has not been intialized yet.");
    NamenodeWebHdfsMethods.chooseDatanode(nn, "/path", PutOpParam.Op.CREATE, 0,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT, null);
  }
}

