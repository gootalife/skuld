hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDataNodeHotSwapVolumes.java
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
  public void testDirectlyReloadAfterCheckDiskError()
      throws IOException, TimeoutException, InterruptedException,
      ReconfigurationException {
    assumeTrue(!Path.WINDOWS);

    startDFSCluster(1, 2);
    createFile(new Path("/test"), 32, (short)2);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDataNodeVolumeFailure.java
  @Test(timeout=150000)
    public void testFailedVolumeBeingRemovedFromDataNode()
      throws InterruptedException, IOException, TimeoutException {
    assumeTrue(!Path.WINDOWS);

    Path file1 = new Path("/test1");
    DFSTestUtil.createFile(fs, file1, 1024, (short) 2, 1L);
    DFSTestUtil.waitReplication(fs, file1, (short) 2);
  @Test
  public void testUnderReplicationAfterVolFailure() throws Exception {
    assumeTrue(!Path.WINDOWS);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDataNodeVolumeFailureReporting.java

  @Before
  public void setUp() throws Exception {
    assumeTrue(!Path.WINDOWS);
    initCluster(1, 2, 1);

