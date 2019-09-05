hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
          DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC,
          DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC_DEFAULT);

      if (this.lazyPersistFileScrubIntervalSec < 0) {
        throw new IllegalArgumentException(
            DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC
                + " must be zero (for disable) or greater than zero.");
      }

        lazyPersistFileScrubber = new Daemon(new LazyPersistFileScrubber(
            lazyPersistFileScrubIntervalSec));
        lazyPersistFileScrubber.start();
      } else {
        LOG.warn("Lazy persist file scrubber is disabled,"
            + " configured scrub interval is zero.");
      }

      cacheManager.startMonitorThread();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/LazyPersistTestCase.java
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
      long ramDiskStorageLimit,
      long evictionLowWatermarkReplicas,
      boolean useSCR,
      boolean useLegacyBlockReaderLocal,
      boolean disableScrubber) throws IOException {

    Configuration conf = new Configuration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    if (disableScrubber) {
      conf.setInt(DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC, 0);
    } else {
      conf.setInt(DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC,
          LAZY_WRITE_FILE_SCRUBBER_INTERVAL_SEC);
    }
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL_SEC);
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
                HEARTBEAT_RECHECK_INTERVAL_MSEC);
      return this;
    }

    public ClusterWithRamDiskBuilder disableScrubber() {
      this.disableScrubber = true;
      return this;
    }

    public void build() throws IOException {
      LazyPersistTestCase.this.startUpCluster(
          numDatanodes, hasTransientStorage, storageTypes, ramDiskReplicaCapacity,
          ramDiskStorageLimit, evictionLowWatermarkReplicas,
          useScr, useLegacyBlockReaderLocal,disableScrubber);
    }

    private int numDatanodes = REPL_FACTOR;
    private boolean useScr = false;
    private boolean useLegacyBlockReaderLocal = false;
    private long evictionLowWatermarkReplicas = EVICTION_LOW_WATERMARK;
    private boolean disableScrubber=false;
  }

  protected final void triggerBlockReport()

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistFiles.java
               is(0L));
  }

  @Test
  public void testDisableLazyPersistFileScrubber()
      throws IOException, InterruptedException {
    getClusterBuilder().setRamDiskReplicaCapacity(2).disableScrubber().build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");

    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    cluster.shutdownDataNodes();
    Thread.sleep(30000L);

    Thread.sleep(2 * DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT * 1000);

    Thread.sleep(2 * LAZY_WRITE_FILE_SCRUBBER_INTERVAL_SEC * 1000);

    Assert.assertTrue(fs.exists(path1));

  }

