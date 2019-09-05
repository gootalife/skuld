hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  public static final String  DFS_DATANODE_MAX_RECEIVER_THREADS_KEY = "dfs.datanode.max.transfer.threads";
  public static final int     DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT = 4096;
  public static final String  DFS_DATANODE_SCAN_PERIOD_HOURS_KEY = "dfs.datanode.scan.period.hours";
  public static final int     DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT = 21 * 24;  // 3 weeks.
  public static final String  DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND = "dfs.block.scanner.volume.bytes.per.second";
  public static final long    DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND_DEFAULT = 1048576L;
  public static final String  DFS_DATANODE_TRANSFERTO_ALLOWED_KEY = "dfs.datanode.transferTo.allowed";

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockScanner.java
      }
    }

    private static long getConfiguredScanPeriodMs(Configuration conf) {
      long tempScanPeriodMs = getUnitTestLong(
          conf, INTERNAL_DFS_DATANODE_SCAN_PERIOD_MS,
              TimeUnit.MILLISECONDS.convert(conf.getLong(
                  DFS_DATANODE_SCAN_PERIOD_HOURS_KEY,
                  DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT), TimeUnit.HOURS));

      if (tempScanPeriodMs == 0) {
        tempScanPeriodMs = TimeUnit.MILLISECONDS.convert(
            DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT, TimeUnit.HOURS);
      }

      return tempScanPeriodMs;
    }

    @SuppressWarnings("unchecked")
    Conf(Configuration conf) {
      this.targetBytesPerSec = Math.max(0L, conf.getLong(
      this.maxStalenessMs = Math.max(0L, getUnitTestLong(conf,
          INTERNAL_DFS_BLOCK_SCANNER_MAX_STALENESS_MS,
          INTERNAL_DFS_BLOCK_SCANNER_MAX_STALENESS_MS_DEFAULT));
      this.scanPeriodMs = getConfiguredScanPeriodMs(conf);
      this.cursorSaveMs = Math.max(0L, getUnitTestLong(conf,
          INTERNAL_DFS_BLOCK_SCANNER_CURSOR_SAVE_INTERVAL_MS,
          INTERNAL_DFS_BLOCK_SCANNER_CURSOR_SAVE_INTERVAL_MS_DEFAULT));
  public boolean isEnabled() {
    return (conf.scanPeriodMs > 0) && (conf.targetBytesPerSec > 0);
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestFsDatasetImpl.java
  public void testChangeVolumeWithRunningCheckDirs() throws IOException {
    RoundRobinVolumeChoosingPolicy<FsVolumeImpl> blockChooser =
        new RoundRobinVolumeChoosingPolicy<>();
    conf.setLong(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);
    final BlockScanner blockScanner = new BlockScanner(datanode, conf);
    final FsVolumeList volumeList = new FsVolumeList(
        Collections.<VolumeFailureInfo>emptyList(), blockScanner, blockChooser);

