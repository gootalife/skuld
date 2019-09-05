hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
      peer = TcpPeerServer.peerFromSocketAndKey(saslClient, sock, this,
          blockToken, datanodeId);
      peer.setReadTimeout(socketTimeout);
      peer.setWriteTimeout(socketTimeout);
      success = true;
      return peer;
    } finally {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDistributedFileSystem.java
    }
  }

  @Test(timeout=10000)
  public void testDFSClientPeerReadTimeout() throws IOException {
    final int timeout = 1000;
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, timeout);
      long start = Time.now();
      try {
        peer.getInputStream().read();
        Assert.fail("read should timeout");
      } catch (SocketTimeoutException ste) {
        long delta = Time.now() - start;
        Assert.assertTrue("read timedout too soon", delta >= timeout*0.9);
        Assert.assertTrue("read timedout too late", delta <= timeout*1.1);
      } catch (Throwable t) {
        Assert.fail("wrong exception:"+t);
      }
      cluster.shutdown();
    }
  }

  @Test(timeout=10000)
  public void testDFSClientPeerWriteTimeout() throws IOException {
    final int timeout = 1000;
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, timeout);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      ServerSocket socket = new ServerSocket(0);
      Peer peer = dfs.getClient().newConnectedPeer(
        (InetSocketAddress) socket.getLocalSocketAddress(), null, null);
      long start = Time.now();
      try {
        byte[] buf = new byte[1024 * 1024];
        peer.getOutputStream().write(buf);
        Assert.fail("write should timeout");
      } catch (SocketTimeoutException ste) {
        long delta = Time.now() - start;
        Assert.assertTrue("write timedout too soon", delta >= timeout * 0.9);
        Assert.assertTrue("write timedout too late", delta <= timeout * 1.1);
      } catch (Throwable t) {
        Assert.fail("wrong exception:" + t);
      }
    } finally {
      cluster.shutdown();
    }
  }
}

