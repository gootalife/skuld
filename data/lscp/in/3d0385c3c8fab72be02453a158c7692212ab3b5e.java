hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/BlockReaderFactory.java
    public void injectRequestFileDescriptorsFailure() throws IOException {
    }
    public boolean getSupportsReceiptVerification() {
      return true;
    }
  }

  @VisibleForTesting
    final DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(peer.getOutputStream()));
    SlotId slotId = slot == null ? null : slot.getSlotId();
    new Sender(out).requestShortCircuitFds(block, token, slotId, 1,
        failureInjector.getSupportsReceiptVerification());
    DataInputStream in = new DataInputStream(peer.getInputStream());
    BlockOpResponseProto resp = BlockOpResponseProto.parseFrom(
        PBHelper.vintPrefixed(in));

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocol/datatransfer/Receiver.java
    try {
      requestShortCircuitFds(PBHelper.convert(proto.getHeader().getBlock()),
          PBHelper.convert(proto.getHeader().getToken()),
          slotId, proto.getMaxVersion(),
          proto.getSupportsReceiptVerification());
    } finally {
      if (traceScope != null) traceScope.close();
    }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/shortcircuit/TestShortCircuitCache.java
    cluster.shutdown();
    sockDir.close();
  }

  public static class TestPreReceiptVerificationFailureInjector
      extends BlockReaderFactory.FailureInjector {
    @Override
    public boolean getSupportsReceiptVerification() {
      return false;
    }
  }

  @Test(timeout=60000)
  public void testPreReceiptVerificationDfsClientCanDoScr() throws Exception {
    BlockReaderTestUtil.enableShortCircuitShmTracing();
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    Configuration conf = createShortCircuitConf(
        "testPreReceiptVerificationDfsClientCanDoScr", sockDir);
    conf.setLong(
        HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_EXPIRY_MS_KEY,
        1000000000L);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.getClient().getConf().getShortCircuitConf().brfFailureInjector =
        new TestPreReceiptVerificationFailureInjector();
    final Path TEST_PATH1 = new Path("/test_file1");
    DFSTestUtil.createFile(fs, TEST_PATH1, 4096, (short)1, 0xFADE2);
    final Path TEST_PATH2 = new Path("/test_file2");
    DFSTestUtil.createFile(fs, TEST_PATH2, 4096, (short)1, 0xFADE2);
    DFSTestUtil.readFileBuffer(fs, TEST_PATH1);
    DFSTestUtil.readFileBuffer(fs, TEST_PATH2);
    ShortCircuitRegistry registry =
        cluster.getDataNodes().get(0).getShortCircuitRegistry();
    registry.visit(new ShortCircuitRegistry.Visitor() {
      @Override
      public void accept(HashMap<ShmId, RegisteredShm> segments,
                         HashMultimap<ExtendedBlockId, Slot> slots) {
        Assert.assertEquals(1, segments.size());
        Assert.assertEquals(2, slots.size());
      }
    });
    cluster.shutdown();
    sockDir.close();
  }
}

