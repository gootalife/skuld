hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetImpl.java
  final Daemon lazyWriter;
  final FsDatasetCache cacheManager;
  private final Configuration conf;
  private final int volFailuresTolerated;
  private volatile boolean fsRunning;

  final ReplicaMap volumeMap;
    this.smallBufferSize = DFSUtil.getSmallBufferSize(conf);
    volFailuresTolerated =
      conf.getInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY,
                  DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT);


    int volsConfigured = (dataDirs == null) ? 0 : dataDirs.length;
    int volsFailed = volumeFailureInfos.size();

    if (volFailuresTolerated < 0 || volFailuresTolerated >= volsConfigured) {
      throw new DiskErrorException("Invalid value configured for "
  @Override // FsDatasetSpi
  public boolean hasEnoughResource() {
    return getNumFailedVolumes() <= volFailuresTolerated;
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDataNodeVolumeFailure.java
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
    File dn0Vol1 = new File(dataDir, "data" + (2 * 0 + 1));
    DataNodeTestUtils.injectDataDirFailure(dn0Vol1);
    DataNode dn0 = cluster.getDataNodes().get(0);
    checkDiskErrorSync(dn0);

    assertFalse(dataDirStrs[0].contains(dn0Vol1.getAbsolutePath()));
  }

  private static void checkDiskErrorSync(DataNode dn)
      throws InterruptedException {
    final long lastDiskErrorCheck = dn.getLastDiskErrorCheck();
    dn.checkDiskErrorAsync();
    int count = 100;
    while (count > 0 && dn.getLastDiskErrorCheck() == lastDiskErrorCheck) {
      Thread.sleep(100);
      count--;
    }
    assertTrue("Disk checking thread does not finish in 10 seconds",
        count > 0);
  }

  @Test(timeout=10000)
  public void testDataNodeShutdownAfterNumFailedVolumeExceedsTolerated()
      throws InterruptedException, IOException {
    final File dn0Vol1 = new File(dataDir, "data" + (2 * 0 + 1));
    final File dn0Vol2 = new File(dataDir, "data" + (2 * 0 + 2));
    DataNodeTestUtils.injectDataDirFailure(dn0Vol1, dn0Vol2);
    DataNode dn0 = cluster.getDataNodes().get(0);
    checkDiskErrorSync(dn0);

    assertFalse(dn0.shouldRun());
  }

  @Test
  public void testVolumeFailureRecoveredByHotSwappingVolume()
      throws InterruptedException, ReconfigurationException, IOException {
    final File dn0Vol1 = new File(dataDir, "data" + (2 * 0 + 1));
    final File dn0Vol2 = new File(dataDir, "data" + (2 * 0 + 2));
    final DataNode dn0 = cluster.getDataNodes().get(0);
    final String oldDataDirs = dn0.getConf().get(
        DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);

    DataNodeTestUtils.injectDataDirFailure(dn0Vol1);
    checkDiskErrorSync(dn0);

    String dataDirs = dn0Vol2.getPath();
    dn0.reconfigurePropertyImpl(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
        dataDirs);

    DataNodeTestUtils.restoreDataDirFromFailure(dn0Vol1);
    dn0.reconfigurePropertyImpl(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
        oldDataDirs);

    DataNodeTestUtils.injectDataDirFailure(dn0Vol2);
    checkDiskErrorSync(dn0);
    assertTrue(dn0.shouldRun());
  }

  @Test
  public void testTolerateVolumeFailuresAfterAddingMoreVolumes()
      throws InterruptedException, ReconfigurationException, IOException {
    final File dn0Vol1 = new File(dataDir, "data" + (2 * 0 + 1));
    final File dn0Vol2 = new File(dataDir, "data" + (2 * 0 + 2));
    final File dn0VolNew = new File(dataDir, "data_new");
    final DataNode dn0 = cluster.getDataNodes().get(0);
    final String oldDataDirs = dn0.getConf().get(
        DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);

    dn0.reconfigurePropertyImpl(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
        oldDataDirs + "," + dn0VolNew.getAbsolutePath());

    DataNodeTestUtils.injectDataDirFailure(dn0Vol1);
    checkDiskErrorSync(dn0);
    assertTrue(dn0.shouldRun());

    DataNodeTestUtils.injectDataDirFailure(dn0Vol2);
    checkDiskErrorSync(dn0);
    assertFalse(dn0.shouldRun());
  }


