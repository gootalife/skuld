hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
    RPC.stopProxy(namenode);
  }

  public void closeAllFilesBeingWritten(final boolean abort) {
    for(;;) {
      final long inodeId;
      final DFSOutputStream out;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/client/impl/LeaseRenewer.java
    return renewal;
  }

  @VisibleForTesting
  public synchronized void setRenewalTime(final long renewal) {
    this.renewal = renewal;
  }

  private synchronized void addClient(final DFSClient dfsc) {
    for(DFSClient c : dfsclients) {
              + (elapsed/1000) + " seconds.  Aborting ...", ie);
          synchronized (this) {
            while (!dfsclients.isEmpty()) {
              DFSClient dfsClient = dfsclients.get(0);
              dfsClient.closeAllFilesBeingWritten(true);
              closeClient(dfsClient);
            }
            emptyTime = 0;
          }
          break;
        } catch (IOException ie) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSClientRetries.java
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.LeaseRenewer;
import org.apache.hadoop.hdfs.client.HdfsUtils;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
    }
  }

  @Test
  public void testLeaseRenewSocketTimeout() throws Exception
  {
    String file1 = "/testFile1";
    String file2 = "/testFile2";
    conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRY_WINDOW_BASE, 10);
    conf.setInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, 2 * 1000);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      NamenodeProtocols spyNN = spy(cluster.getNameNodeRpc());
      Mockito.doThrow(new SocketTimeoutException()).when(spyNN).renewLease(
          Mockito.anyString());
      DFSClient client = new DFSClient(null, spyNN, conf, null);
      LeaseRenewer leaseRenewer = client.getLeaseRenewer();
      leaseRenewer.setRenewalTime(100);
      OutputStream out1 = client.create(file1, false);

      Mockito.verify(spyNN, timeout(10000).times(1)).renewLease(
          Mockito.anyString());
      verifyEmptyLease(leaseRenewer);
      try {
        out1.write(new byte[256]);
        fail("existing output stream should be aborted");
      } catch (IOException e) {
      }

      client.exists(file2);
      Mockito.doNothing().when(spyNN).renewLease(
          Mockito.anyString());
      leaseRenewer = client.getLeaseRenewer();
      leaseRenewer.setRenewalTime(100);
      OutputStream out2 = client.create(file2, false);
      Mockito.verify(spyNN, timeout(10000).times(2)).renewLease(
          Mockito.anyString());
      out2.write(new byte[256]);
      out2.close();
      verifyEmptyLease(leaseRenewer);
    } finally {
      cluster.shutdown();
    }
  }

    return ret;
  }

  private void verifyEmptyLease(LeaseRenewer leaseRenewer) throws Exception {
    int sleepCount = 0;
    while (!leaseRenewer.isEmpty() && sleepCount++ < 20) {
      Thread.sleep(500);
    }
    assertTrue("Lease should be empty.", leaseRenewer.isEmpty());
  }

  class DFSClientReader implements Runnable {
    
    DFSClient client;

