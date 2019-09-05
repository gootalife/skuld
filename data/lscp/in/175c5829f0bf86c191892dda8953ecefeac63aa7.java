hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  public static final int     DFS_DATANODE_LAZY_WRITER_INTERVAL_DEFAULT_SEC = 60;
  public static final String  DFS_DATANODE_RAM_DISK_REPLICA_TRACKER_KEY = "dfs.datanode.ram.disk.replica.tracker";
  public static final Class<RamDiskReplicaLruTracker>  DFS_DATANODE_RAM_DISK_REPLICA_TRACKER_DEFAULT = RamDiskReplicaLruTracker.class;
  public static final String  DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_KEY = "dfs.datanode.network.counts.cache.max.size";
  public static final int     DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_DEFAULT = Integer.MAX_VALUE;


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetCache.java
    return usedBytesCount.rounder.osPageSize;
  }

  long roundUpPageSize(long count) {
    return usedBytesCount.rounder.roundUp(count);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetImpl.java
    if (allowLazyPersist &&
        lazyWriter != null &&
        b.getNumBytes() % cacheManager.getOsPageSize() == 0 &&
        reserveLockedMemory(b.getNumBytes())) {
      try {
        ref = volumes.getNextTransientVolume(b.getNumBytes());
        datanode.getMetrics().incrRamDiskBlocksWrite();
      } catch(DiskOutOfSpaceException de) {
      } finally {
        if (ref == null) {
          cacheManager.release(b.getNumBytes());

    FsVolumeImpl v = (FsVolumeImpl) ref.getVolume();

    if (allowLazyPersist && !v.isTransientStorage()) {
      datanode.getMetrics().incrRamDiskBlocksWriteFallback();
    }

    File f;
    try {
      f = v.createRbwFile(b.getBlockPoolId(), b.getLocalBlock());
  class LazyWriter implements Runnable {
    private volatile boolean shouldRun = true;
    final int checkpointerInterval;

    public LazyWriter(Configuration conf) {
      this.checkpointerInterval = conf.getInt(
          DFSConfigKeys.DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC,
          DFSConfigKeys.DFS_DATANODE_LAZY_WRITER_INTERVAL_DEFAULT_SEC);
    }

      return succeeded;
    }

    public void evictBlocks(long bytesNeeded) throws IOException {
      int iterations = 0;

      final long cacheCapacity = cacheManager.getCacheCapacity();

      while (iterations++ < MAX_BLOCK_EVICTIONS_PER_ITERATION &&
             (cacheCapacity - cacheManager.getCacheUsed()) < bytesNeeded) {
        RamDiskReplica replicaState = ramDiskReplicaTracker.getNextCandidateForEviction();

        if (replicaState == null) {
        final String bpid = replicaState.getBlockPoolId();

        synchronized (FsDatasetImpl.this) {
          replicaInfo = getReplicaInfo(replicaState.getBlockPoolId(),
                                       replicaState.getBlockId());
          Preconditions.checkState(replicaInfo.getVolume().isTransientStorage());
          blockFile = replicaInfo.getBlockFile();
          metaFile = replicaInfo.getMetaFile();
          ramDiskReplicaTracker.discardReplica(replicaState.getBlockPoolId(),
              replicaState.getBlockId(), false);

          BlockPoolSlice bpSlice =
              replicaState.getLazyPersistVolume().getBlockPoolSlice(bpid);
          File newBlockFile = bpSlice.activateSavedReplica(
          if (replicaState.getNumReads() == 0) {
            datanode.getMetrics().incrRamDiskBlocksEvictedWithoutRead();
          }

          removeOldReplica(replicaInfo, newReplicaInfo, blockFile, metaFile,
              blockFileUsed, metaFileUsed, bpid);
        }
      }
    }

    @Override
    public void run() {
      while (fsRunning && shouldRun) {
        try {
          numSuccessiveFailures = saveNextReplica() ? 0 : (numSuccessiveFailures + 1);

      cacheManager.releaseRoundDown(count);
    }
  }

  @VisibleForTesting
  public void evictLazyPersistBlocks(long bytesNeeded) {
    try {
      ((LazyWriter) lazyWriter.getRunnable()).evictBlocks(bytesNeeded);
    } catch(IOException ioe) {
      LOG.info("Ignoring exception ", ioe);
    }
  }

  boolean reserveLockedMemory(long bytesNeeded) {
    if (cacheManager.reserve(bytesNeeded) > 0) {
      return true;
    }

    bytesNeeded = cacheManager.roundUpPageSize(bytesNeeded);
    evictLazyPersistBlocks(bytesNeeded);
    return cacheManager.reserve(bytesNeeded) > 0;
  }
}


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/balancer/TestBalancer.java
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    conf.setInt(DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC, 1);
    LazyPersistTestCase.initCacheManipulator();
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/LazyPersistTestCase.java
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.google.common.base.Supplier;
import org.apache.commons.lang.UnhandledException;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;

  protected static final int BLOCK_SIZE = 5 * 1024 * 1024;
  protected static final int BUFFER_LENGTH = 4096;
  private static final long HEARTBEAT_INTERVAL_SEC = 1;
  private static final int HEARTBEAT_RECHECK_INTERVAL_MSEC = 500;
  private static final String JMX_RAM_DISK_METRICS_PATTERN = "^RamDisk";
      StorageType[] storageTypes,
      int ramDiskReplicaCapacity,
      long ramDiskStorageLimit,
      long maxLockedMemory,
      boolean useSCR,
      boolean useLegacyBlockReaderLocal,
                HEARTBEAT_RECHECK_INTERVAL_MSEC);
    conf.setInt(DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC,
                LAZY_WRITER_INTERVAL_SEC);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY, 1);
    conf.setLong(DFS_DATANODE_MAX_LOCKED_MEMORY_KEY, maxLockedMemory);

      return this;
    }

    public ClusterWithRamDiskBuilder disableScrubber() {
      this.disableScrubber = true;
      return this;
    public void build() throws IOException {
      LazyPersistTestCase.this.startUpCluster(
          numDatanodes, hasTransientStorage, storageTypes, ramDiskReplicaCapacity,
          ramDiskStorageLimit, maxLockedMemory, useScr, useLegacyBlockReaderLocal,
          disableScrubber);
    }

    private int numDatanodes = REPL_FACTOR;
    private boolean hasTransientStorage = true;
    private boolean useScr = false;
    private boolean useLegacyBlockReaderLocal = false;
    private boolean disableScrubber=false;
  }

      e.printStackTrace();
    }
  }

  protected void waitForMetric(final String metricName, final int expectedValue)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          final int currentValue = Integer.parseInt(jmx.getValue(metricName));
          LOG.info("Waiting for " + metricName +
                       " to reach value " + expectedValue +
                       ", current value = " + currentValue);
          return currentValue == expectedValue;
        } catch (Exception e) {
          throw new UnhandledException("Test failed due to unexpected exception", e);
        }
      }
    }, 1000, Integer.MAX_VALUE);
  }

  protected void triggerEviction(DataNode dn) {
    FsDatasetImpl fsDataset = (FsDatasetImpl) dn.getFSDataset();
    fsDataset.evictLazyPersistBlocks(Long.MAX_VALUE); // Run one eviction cycle.
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistLockedMemory.java
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.io.IOException;
  @Test
  public void testReleaseOnEviction() throws Exception {
    getClusterBuilder().setNumDatanodes(1)
                       .setMaxLockedMemory(BLOCK_SIZE)
                       .setRamDiskReplicaCapacity(BLOCK_SIZE * 2 - 1)
                       .build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final FsDatasetImpl fsd =
        (FsDatasetImpl) cluster.getDataNodes().get(0).getFSDataset();

    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    makeTestFile(path1, BLOCK_SIZE, true);
    assertThat(fsd.getCacheUsed(), is((long) BLOCK_SIZE));

    waitForMetric("RamDiskBlocksLazyPersisted", 1);

    fsd.evictLazyPersistBlocks(Long.MAX_VALUE);
    verifyRamDiskJMXMetric("RamDiskBlocksEvicted", 1);
    waitForLockedBytesUsed(fsd, 0);
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistReplicaPlacement.java
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class TestLazyPersistReplicaPlacement extends LazyPersistTestCase {
    ensureFileReplicasOnStorageType(path, DEFAULT);
  }

  @Test
  public void testSynchronousEviction() throws Exception {
    getClusterBuilder().setMaxLockedMemory(BLOCK_SIZE).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();

    final Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    waitForMetric("RamDiskBlocksLazyPersisted", 1);

    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");
    makeTestFile(path2, BLOCK_SIZE, true);
    verifyRamDiskJMXMetric("RamDiskBlocksEvictedWithoutRead", 1);
  }

  @Test
  public void testFallbackToDiskFull() throws Exception {
    getClusterBuilder().setMaxLockedMemory(BLOCK_SIZE / 2).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

  @Test
  public void testFallbackToDiskPartial()
      throws IOException, InterruptedException {
    getClusterBuilder().setMaxLockedMemory(2 * BLOCK_SIZE).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");


    assertThat(numBlocksOnRamDisk, is(2));
    assertThat(numBlocksOnDisk, is(3));
  }

  @Test
  public void testRamDiskNotChosenByDefault() throws IOException {
    getClusterBuilder().setStorageTypes(new StorageType[] {RAM_DISK, RAM_DISK})
                       .build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyWriter.java
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;
public class TestLazyWriter extends LazyPersistTestCase {
  @Test
  public void testLazyPersistBlocksAreSaved()
      throws IOException, InterruptedException, TimeoutException {
    getClusterBuilder().build();
    final int NUM_BLOCKS = 10;
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE * NUM_BLOCKS, true);
    LocatedBlocks locatedBlocks = ensureFileReplicasOnStorageType(path, RAM_DISK);
    waitForMetric("RamDiskBlocksLazyPersisted", NUM_BLOCKS);
    LOG.info("Verifying copy was saved to lazyPersist/");

    ensureLazyPersistBlocksAreSaved(locatedBlocks);
  }

  @Test
  public void testSynchronousEviction() throws Exception {
    getClusterBuilder().setMaxLockedMemory(BLOCK_SIZE).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();

    final Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    waitForMetric("RamDiskBlocksLazyPersisted", 1);

    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");
    makeTestFile(path2, BLOCK_SIZE, true);
    verifyRamDiskJMXMetric("RamDiskBlocksEvicted", 1);
    verifyRamDiskJMXMetric("RamDiskBlocksEvictedWithoutRead", 1);
  }
  @Test
  public void testRamDiskEvictionBeforePersist()
      throws Exception {
    getClusterBuilder().setMaxLockedMemory(BLOCK_SIZE).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");

    verifyRamDiskJMXMetric("RamDiskBlocksEvicted", 0);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);
    ensureFileReplicasOnStorageType(path2, DEFAULT);

  public void testRamDiskEvictionIsLru()
      throws Exception {
    final int NUM_PATHS = 5;
    getClusterBuilder().setMaxLockedMemory(NUM_PATHS * BLOCK_SIZE).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path paths[] = new Path[NUM_PATHS * 2];

      makeTestFile(paths[i], BLOCK_SIZE, true);
    }

    waitForMetric("RamDiskBlocksLazyPersisted", NUM_PATHS);

    for (int i = 0; i < NUM_PATHS; ++i) {
      ensureFileReplicasOnStorageType(paths[i], RAM_DISK);

    makeTestFile(path, BLOCK_SIZE, true);
    LocatedBlocks locatedBlocks = ensureFileReplicasOnStorageType(path, RAM_DISK);
    waitForMetric("RamDiskBlocksLazyPersisted", 1);

    client.delete(path.toString(), false);
  @Test
  public void testDfsUsageCreateDelete()
      throws IOException, InterruptedException, TimeoutException {
    getClusterBuilder().setRamDiskReplicaCapacity(4).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    assertThat(usedAfterCreate, is((long) BLOCK_SIZE));

    waitForMetric("RamDiskBlocksLazyPersisted", 1);

    long usedAfterPersist = fs.getUsed();
    assertThat(usedAfterPersist, is((long) BLOCK_SIZE));

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestScrLazyPersistFiles.java
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;
import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.NativeCodeLoader;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestScrLazyPersistFiles extends LazyPersistTestCase {

  @BeforeClass
    Assume.assumeThat(NativeCodeLoader.isNativeCodeLoaded() && !Path.WINDOWS,
        equalTo(true));
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));

    final long osPageSize = NativeIO.POSIX.getCacheManipulator().getOperatingSystemPageSize();
    Preconditions.checkState(BLOCK_SIZE >= osPageSize);
    Preconditions.checkState(BLOCK_SIZE % osPageSize == 0);
  }

  @Rule
  @Test
  public void testRamDiskShortCircuitRead()
      throws IOException, InterruptedException, TimeoutException {
    getClusterBuilder().setUseScr(true).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final int SEED = 0xFADED;
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeRandomTestFile(path, BLOCK_SIZE, true, SEED);
    ensureFileReplicasOnStorageType(path, RAM_DISK);
    waitForMetric("RamDiskBlocksLazyPersisted", 1);

    HdfsDataInputStream fis = (HdfsDataInputStream) fs.open(path);

    try {
      byte[] buf = new byte[BUFFER_LENGTH];
      fis.read(0, buf, 0, BUFFER_LENGTH);
      Assert.assertEquals(BUFFER_LENGTH,
        fis.getReadStatistics().getTotalBytesRead());
      Assert.assertEquals(BUFFER_LENGTH,
        fis.getReadStatistics().getTotalShortCircuitBytesRead());
    } finally {
      fis.close();
      fis = null;
  @Test
  public void tesScrDuringEviction()
      throws Exception {
    getClusterBuilder().setUseScr(true).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");

    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);
    waitForMetric("RamDiskBlocksLazyPersisted", 1);

    HdfsDataInputStream fis = (HdfsDataInputStream) fs.open(path1);
    try {
      byte[] buf = new byte[BUFFER_LENGTH];
      fis.read(0, buf, 0, BUFFER_LENGTH);
      triggerEviction(cluster.getDataNodes().get(0));

      fis.read(0, buf, 0, BUFFER_LENGTH);
      assertThat(fis.getReadStatistics().getTotalBytesRead(),
          is((long) 2 * BUFFER_LENGTH));
      assertThat(fis.getReadStatistics().getTotalShortCircuitBytesRead(),
          is((long) 2 * BUFFER_LENGTH));
    } finally {
      IOUtils.closeQuietly(fis);
    }
  }

  @Test
  public void testScrAfterEviction()
      throws IOException, InterruptedException, TimeoutException {
    getClusterBuilder().setUseScr(true)
                       .setUseLegacyBlockReaderLocal(false)
                       .build();
    doShortCircuitReadAfterEvictionTest();
  }

  @Test
  public void testLegacyScrAfterEviction()
      throws IOException, InterruptedException, TimeoutException {
    getClusterBuilder().setUseScr(true)
                       .setUseLegacyBlockReaderLocal(true)
                       .build();
    doShortCircuitReadAfterEvictionTest();

    ClientContext clientContext = client.getClientContext();
    Assert.assertFalse(clientContext.getDisableLegacyBlockReaderLocal());
  }

  private void doShortCircuitReadAfterEvictionTest() throws IOException,
      InterruptedException, TimeoutException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");

    final int SEED = 0xFADED;
    makeRandomTestFile(path1, BLOCK_SIZE, true, SEED);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);
    waitForMetric("RamDiskBlocksLazyPersisted", 1);

    File metaFile = cluster.getBlockMetadataFile(0,
        DFSTestUtil.getFirstBlock(fs, path1));
    assertTrue(metaFile.length() <= BlockMetadataHeader.getHeaderSize());
    assertTrue(verifyReadRandomFile(path1, BLOCK_SIZE, SEED));

    triggerEviction(cluster.getDataNodes().get(0));

        DFSTestUtil.getFirstBlock(fs, path1));
    assertTrue(metaFile.length() > BlockMetadataHeader.getHeaderSize());
    assertTrue(verifyReadRandomFile(path1, BLOCK_SIZE, SEED));
  }

  @Test
  public void testScrBlockFileCorruption() throws IOException,
      InterruptedException, TimeoutException {
    getClusterBuilder().setUseScr(true)
                       .setUseLegacyBlockReaderLocal(false)
                       .build();
    doShortCircuitReadBlockFileCorruptionTest();
  }

  @Test
  public void testLegacyScrBlockFileCorruption() throws IOException,
      InterruptedException, TimeoutException {
    getClusterBuilder().setUseScr(true)
                       .setUseLegacyBlockReaderLocal(true)
                       .build();
    doShortCircuitReadBlockFileCorruptionTest();
  }

  public void doShortCircuitReadBlockFileCorruptionTest() throws IOException,
      InterruptedException, TimeoutException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");

    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);
    waitForMetric("RamDiskBlocksLazyPersisted", 1);
    triggerEviction(cluster.getDataNodes().get(0));

  }

  @Test
  public void testScrMetaFileCorruption() throws IOException,
      InterruptedException, TimeoutException {
    getClusterBuilder().setUseScr(true)
                       .setUseLegacyBlockReaderLocal(false)
                       .build();
    doShortCircuitReadMetaFileCorruptionTest();
  }

  @Test
  public void testLegacyScrMetaFileCorruption() throws IOException,
      InterruptedException, TimeoutException {
    getClusterBuilder().setUseScr(true)
                       .setUseLegacyBlockReaderLocal(true)
                       .build();
    doShortCircuitReadMetaFileCorruptionTest();
  }

  public void doShortCircuitReadMetaFileCorruptionTest() throws IOException,
      InterruptedException, TimeoutException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");

    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);
    waitForMetric("RamDiskBlocksLazyPersisted", 1);
    triggerEviction(cluster.getDataNodes().get(0));


