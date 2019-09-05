hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/net/unix/DomainSocketWatcher.java
        }
      } catch (InterruptedException e) {
        LOG.info(toString() + " terminating on InterruptedException");
      } catch (Throwable e) {
        LOG.error(toString() + " terminating on exception", e);
      } finally {
        lock.lock();
        try {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNodeFaultInjector.java
  public void getHdfsBlocksMetadata() {}

  public void writeBlockAfterFlush() throws IOException {}

  public void sendShortCircuitShmResponse() throws IOException {}
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataXceiver.java

  private void sendShmSuccessResponse(DomainSocket sock, NewShmInfo shmInfo)
      throws IOException {
    DataNodeFaultInjector.get().sendShortCircuitShmResponse();
    ShortCircuitShmResponseProto.newBuilder().setStatus(SUCCESS).
        setId(PBHelper.convert(shmInfo.shmId)).build().
        writeDelimitedTo(socketOut);
        }
      }
      if ((!success) && (peer == null)) {
        try {
          LOG.warn("Failed to send success response back to the client.  " +
              "Shutting down socket for " + shmInfo.shmId + ".");
          sock.shutdown();
        } catch (IOException e) {
          LOG.warn("Failed to shut down socket in error handler", e);
        }
      }
      IOUtils.cleanup(null, shmInfo);
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/shortcircuit/DfsClientShmManager.java

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/shortcircuit/DomainSocketFactory.java
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
  public void disableDomainSocketPath(String path) {
    pathMap.put(path, PathState.UNUSABLE);
  }

  @VisibleForTesting
  public void clearPathMap() {
    pathMap.invalidateAll();
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/shortcircuit/TestShortCircuitCache.java
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.datanode.ShortCircuitRegistry;
import org.apache.hadoop.hdfs.server.datanode.ShortCircuitRegistry.RegisteredShm;
import org.apache.hadoop.hdfs.shortcircuit.DfsClientShmManager.PerDatanodeVisitorInfo;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
    sockDir.close();
  }

  static private void checkNumberOfSegmentsAndSlots(final int expectedSegments,
        final int expectedSlots, ShortCircuitRegistry registry) {
    registry.visit(new ShortCircuitRegistry.Visitor() {
      @Override
      public void accept(HashMap<ShmId, RegisteredShm> segments,
                         HashMultimap<ExtendedBlockId, Slot> slots) {
        Assert.assertEquals(expectedSegments, segments.size());
        Assert.assertEquals(expectedSlots, slots.size());
      }
    });
  }

  public static class TestCleanupFailureInjector
        extends BlockReaderFactory.FailureInjector {
    @Override
      GenericTestUtils.assertExceptionContains("TCP reads were disabled for " +
          "testing, but we failed to do a non-TCP read.", t);
    }
    checkNumberOfSegmentsAndSlots(1, 1,
        cluster.getDataNodes().get(0).getShortCircuitRegistry());
    cluster.shutdown();
    sockDir.close();
  }

  @Test(timeout=60000)
  public void testDataXceiverHandlesRequestShortCircuitShmFailure()
      throws Exception {
    BlockReaderTestUtil.enableShortCircuitShmTracing();
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    Configuration conf = createShortCircuitConf(
        "testDataXceiverHandlesRequestShortCircuitShmFailure", sockDir);
    conf.setLong(HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_EXPIRY_MS_KEY,
        1000000000L);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    final Path TEST_PATH1 = new Path("/test_file1");
    DFSTestUtil.createFile(fs, TEST_PATH1, 4096,
        (short)1, 0xFADE1);
    LOG.info("Setting failure injector and performing a read which " +
        "should fail...");
    DataNodeFaultInjector failureInjector = Mockito.mock(DataNodeFaultInjector.class);
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        throw new IOException("injected error into sendShmResponse");
      }
    }).when(failureInjector).sendShortCircuitShmResponse();
    DataNodeFaultInjector prevInjector = DataNodeFaultInjector.instance;
    DataNodeFaultInjector.instance = failureInjector;

    try {
      DFSTestUtil.readFileBuffer(fs, TEST_PATH1);
      Assert.fail("expected readFileBuffer to fail, but it succeeded.");
    } catch (Throwable t) {
      GenericTestUtils.assertExceptionContains("TCP reads were disabled for " +
          "testing, but we failed to do a non-TCP read.", t);
    }

    checkNumberOfSegmentsAndSlots(0, 0,
        cluster.getDataNodes().get(0).getShortCircuitRegistry());

    LOG.info("Clearing failure injector and performing another read...");
    DataNodeFaultInjector.instance = prevInjector;

    fs.getClient().getClientContext().getDomainSocketFactory().clearPathMap();

    DFSTestUtil.readFileBuffer(fs, TEST_PATH1);

    checkNumberOfSegmentsAndSlots(1, 1,
        cluster.getDataNodes().get(0).getShortCircuitRegistry());

    cluster.shutdown();
    sockDir.close();
  }

