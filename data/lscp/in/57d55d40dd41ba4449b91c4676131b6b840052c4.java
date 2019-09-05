hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/EncryptionZoneManager.java
    final boolean hasMore = (numResponses < tailMap.size());
    return new BatchedListEntries<EncryptionZone>(zones, hasMore);
  }

  public int getNumEncryptionZones() {
    return encryptionZones.size();
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
    return JSON.toString(info);
  }

  @Override // FSNamesystemMBean
  @Metric({ "NumEncryptionZones", "The number of encryption zones" })
  public int getNumEncryptionZones() {
    return dir.ezManager.getNumEncryptionZones();
  }

  int getNumberOfDatanodes(DatanodeReportType type) {
    readLock();
    try {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/metrics/FSNamesystemMBean.java
  public String getTopUserOpCounts();

  int getNumEncryptionZones();
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestEncryptionZones.java
import static org.mockito.Mockito.anyString;
import static org.apache.hadoop.hdfs.DFSTestUtil.verifyFilesEqual;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
  protected DistributedFileSystem fs;
  private File testRootDir;
  protected final String TEST_KEY = "test_key";
  private static final String NS_METRICS = "FSNamesystem";

  protected FileSystemTestWrapper fsWrapper;
  protected FileContextTestWrapper fcWrapper;
    fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    cluster.restartNameNode(true);
    assertNumZones(numZones);
    assertEquals("Unexpected number of encryption zones!", numZones, cluster
        .getNamesystem().getNumEncryptionZones());
    assertGauge("NumEncryptionZones", numZones, getMetrics(NS_METRICS));
    assertZonePresent(null, zone1.toString());


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFSNamesystemMBean.java
        "PendingDeletionBlocks");
      assertNotNull(pendingDeletionBlocks);
      assertTrue(pendingDeletionBlocks instanceof Long);

      Object encryptionZones = mbs.getAttribute(mxbeanName,
          "NumEncryptionZones");
      assertNotNull(encryptionZones);
      assertTrue(encryptionZones instanceof Integer);
    } finally {
      if (cluster != null) {
        cluster.shutdown();

