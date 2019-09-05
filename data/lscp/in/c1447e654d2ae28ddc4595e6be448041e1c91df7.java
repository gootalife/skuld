hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/ClientNamenodeProtocolTranslatorPB.java
      throws IOException {
    GetDelegationTokenRequestProto req = GetDelegationTokenRequestProto
        .newBuilder()
        .setRenewer(renewer == null ? "" : renewer.toString())
        .build();
    try {
      GetDelegationTokenResponseProto resp = rpcProxy.getDelegationToken(null, req);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/tools/TestDelegationTokenFetcher.java
package org.apache.hadoop.tools;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.tools.DelegationTokenFetcher;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
      fileSys.delete(new Path(tokenFile), true);
    }
  }

  @Test
  public void testDelegationTokenWithoutRenewer() throws Exception {
    conf.setBoolean(DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
        .build();
    FileSystem localFs = FileSystem.getLocal(conf);
    try {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      uri = fs.getUri();
      DelegationTokenFetcher.main(new String[] { "-fs", uri.toString(),
          tokenFile });
      Path p = new Path(localFs.getWorkingDirectory(), tokenFile);
      Credentials creds = Credentials.readTokenStorageFile(p, conf);
      Iterator<Token<?>> itr = creds.getAllTokens().iterator();
      assertTrue(itr.hasNext());
      assertNotNull("Token without renewer shouldn't be null", itr.next());
      assertTrue(!itr.hasNext());
      try {
        DelegationTokenFetcher.main(new String[] { "--renew", tokenFile });
        fail("Should have failed to renew");
      } catch (AccessControlException e) {
        GenericTestUtils.assertExceptionContains(
            "tried to renew a token without a renewer", e);
      }
    } finally {
      cluster.shutdown();
      localFs.delete(new Path(tokenFile), true);
    }
  }
}

