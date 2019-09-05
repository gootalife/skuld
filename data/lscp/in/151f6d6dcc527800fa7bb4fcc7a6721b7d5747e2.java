hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/ReplicaInPipeline.java
  private long bytesReserved;
  private final long originalBytesReserved;

    this.bytesOnDisk = len;
    this.writer = writer;
    this.bytesReserved = bytesToReserve;
    this.originalBytesReserved = bytesToReserve;
  }

    this.bytesOnDisk = from.getBytesOnDisk();
    this.writer = from.writer;
    this.bytesReserved = from.bytesReserved;
    this.originalBytesReserved = from.originalBytesReserved;
  }

  @Override
    return bytesReserved;
  }
  
  @Override
  public long getOriginalBytesReserved() {
    return originalBytesReserved;
  }

  @Override
  public void releaseAllBytesReserved() {  // ReplicaInPipelineInterface
    getVolume().releaseReservedSpace(bytesReserved);
    getVolume().releaseLockedMemory(bytesReserved);
    bytesReserved = 0;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/ReplicaInfo.java
    return 0;
  }

  public long getOriginalBytesReserved() {
    return 0;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/FsVolumeSpi.java
  public boolean isTransientStorage();

  public void releaseLockedMemory(long bytesToRelease);


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/BlockPoolSlice.java
    if (newReplica.getVolume().isTransientStorage()) {
      lazyWriteReplicaMap.addReplica(bpid, blockId,
          (FsVolumeImpl) newReplica.getVolume(), 0);
    } else {
      lazyWriteReplicaMap.discardReplica(bpid, blockId, false);
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetAsyncDiskService.java

import java.io.File;
import java.io.FileDescriptor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

    @Override
    public void run() {
      final long blockLength = blockFile.length();
      final long metaLength = metaFile.length();
      boolean result;

      result = (trashDirectory == null) ? deleteFiles() : moveFiles();
        if(block.getLocalBlock().getNumBytes() != BlockCommand.NO_ACK){
          datanode.notifyNamenodeDeletedBlock(block, volume.getStorageID());
        }
        volume.onBlockFileDeletion(block.getBlockPoolId(), blockLength);
        volume.onMetaFileDeletion(block.getBlockPoolId(), metaLength);
        LOG.info("Deleted " + block.getBlockPoolId() + " "
            + block.getLocalBlock() + " file " + blockFile);
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetCache.java
    public long roundUp(long count) {
      return (count + osPageSize - 1) & (~(osPageSize - 1));
    }

    public long roundDown(long count) {
      return count & (~(osPageSize - 1));
    }
  }

    long reserve(long count) {
      count = rounder.roundUp(count);
      while (true) {
        long cur = usedBytes.get();
        long next = cur + count;
    long release(long count) {
      count = rounder.roundUp(count);
      return usedBytes.addAndGet(-count);
    }

    long releaseRoundDown(long count) {
      count = rounder.roundDown(count);
      return usedBytes.addAndGet(-count);
    }

    }
  }

  long reserve(long count) {
    return usedBytesCount.reserve(count);
  }

  long release(long count) {
    return usedBytesCount.release(count);
  }

  long releaseRoundDown(long count) {
    return usedBytesCount.releaseRoundDown(count);
  }

  long getOsPageSize() {
    return usedBytesCount.rounder.osPageSize;
  }

      MappableBlock mappableBlock = null;
      ExtendedBlock extBlk = new ExtendedBlock(key.getBlockPoolId(),
          key.getBlockId(), length, genstamp);
      long newUsedBytes = reserve(length);
      boolean reservedBytes = false;
      try {
        if (newUsedBytes < 0) {
        IOUtils.closeQuietly(metaIn);
        if (!success) {
          if (reservedBytes) {
            release(length);
          }
          LOG.debug("Caching of {} was aborted.  We are now caching only {} "
                  + "bytes in total.", key, usedBytesCount.get());
      synchronized (FsDatasetCache.this) {
        mappableBlockMap.remove(key);
      }
      long newUsedBytes = release(value.mappableBlock.getLength());
      numBlocksCached.addAndGet(-1);
      dataset.datanode.getMetrics().incrBlocksUncached(1);
      if (revocationTimeMs != 0) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetImpl.java
    cacheManager = new FsDatasetCache(this);

    if (ramDiskReplicaTracker.numReplicasNotPersisted() > 0 ||
        datanode.getDnConf().getMaxLockedMemory() > 0) {
      lazyWriter = new Daemon(new LazyWriter(conf));
      lazyWriter.start();
    } else {
      lazyWriter = null;
    }

    registerMBean(datanode.getDatanodeUuid());

      " and thus cannot be created.");
    }
    FsVolumeReference ref = null;

    if (allowLazyPersist &&
        lazyWriter != null &&
        b.getNumBytes() % cacheManager.getOsPageSize() == 0 &&
        (cacheManager.reserve(b.getNumBytes())) > 0) {
      try {
        ref = volumes.getNextTransientVolume(b.getNumBytes());
        datanode.getMetrics().incrRamDiskBlocksWrite();
      } catch(DiskOutOfSpaceException de) {
        datanode.getMetrics().incrRamDiskBlocksWriteFallback();
      } finally {
        if (ref == null) {
          cacheManager.release(b.getNumBytes());
        }
      }
    }

    if (ref == null) {
      ref = volumes.getNextVolume(storageType, b.getNumBytes());
    }

    FsVolumeImpl v = (FsVolumeImpl) ref.getVolume();
    File f;
      newReplicaInfo = new FinalizedReplica(replicaInfo, v, dest.getParentFile());

      if (v.isTransientStorage()) {
        releaseLockedMemory(
            replicaInfo.getOriginalBytesReserved() - replicaInfo.getNumBytes(),
            false);
        ramDiskReplicaTracker.addReplica(
            bpid, replicaInfo.getBlockId(), v, replicaInfo.getNumBytes());
        datanode.getMetrics().addRamDiskBytesWrite(replicaInfo.getNumBytes());
      }
    }
  }

  @Override // FsDatasetSpi
  public void invalidate(String bpid, Block invalidBlks[]) throws IOException {
  public void shutdown() {
    fsRunning = false;

    if (lazyWriter != null) {
      ((LazyWriter) lazyWriter.getRunnable()).stop();
      lazyWriter.interrupt();
    }

    if (mbeanName != null) {
      MBeans.unregister(mbeanName);
      volumes.shutdown();
    }

    if (lazyWriter != null) {
      try {
        lazyWriter.join();
      } catch (InterruptedException ie) {
                     "from LazyWriter.join");
      }
    }
  }

  @Override // FSDatasetMBean
  public String getStorageInfo() {
            diskFile.length(), diskGS, vol, diskFile.getParentFile());
        volumeMap.add(bpid, diskBlockInfo);
        if (vol.isTransientStorage()) {
          long lockedBytesReserved =
              cacheManager.reserve(diskBlockInfo.getNumBytes()) > 0 ?
                  diskBlockInfo.getNumBytes() : 0;
          ramDiskReplicaTracker.addReplica(
              bpid, blockId, (FsVolumeImpl) vol, lockedBytesReserved);
        }
        LOG.warn("Added missing block to memory " + diskBlockInfo);
        return;
    boolean ramDiskConfigured = ramDiskConfigured();
    if (ramDiskConfigured &&
        asyncLazyPersistService != null &&
        !asyncLazyPersistService.queryVolume(v.getCurrentDir())) {
      asyncLazyPersistService.addVolume(v.getCurrentDir());
    }

    if (!ramDiskConfigured &&
        asyncLazyPersistService != null &&
        asyncLazyPersistService.queryVolume(v.getCurrentDir())) {
      asyncLazyPersistService.removeVolume(v.getCurrentDir());
    }

    if (blockFile.delete() || !blockFile.exists()) {
      FsVolumeImpl volume = (FsVolumeImpl) replicaInfo.getVolume();
      volume.onBlockFileDeletion(bpid, blockFileUsed);
      if (metaFile.delete() || !metaFile.exists()) {
        volume.onMetaFileDeletion(bpid, metaFileUsed);
      }
    }

    }

    private void evictBlocks() throws IOException {
      int iterations = 0;
      s.add(blockId);
    }
  }

  void releaseLockedMemory(long count, boolean roundup) {
    if (roundup) {
      cacheManager.release(count);
    } else {
      cacheManager.releaseRoundDown(count);
    }
  }
}


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsVolumeImpl.java
    return getBlockPoolSlice(bpid).getTmpDir();
  }

  void onBlockFileDeletion(String bpid, long value) {
    decDfsUsed(bpid, value);
    if (isTransientStorage()) {
      dataset.releaseLockedMemory(value, true);
    }
  }

  void onMetaFileDeletion(String bpid, long value) {
    decDfsUsed(bpid, value);
  }

  private void decDfsUsed(String bpid, long value) {
    synchronized(dataset) {
      BlockPoolSlice bp = bpSlices.get(bpid);
      if (bp != null) {
    }
  }

  @Override
  public void releaseLockedMemory(long bytesToRelease) {
    if (isTransientStorage()) {
      dataset.releaseLockedMemory(bytesToRelease, false);
    }
  }

  private enum SubdirFilter implements FilenameFilter {
    INSTANCE;


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/RamDiskReplicaLruTracker.java
  private class RamDiskReplicaLru extends RamDiskReplica {
    long lastUsedTime;

    private RamDiskReplicaLru(String bpid, long blockId,
                              FsVolumeImpl ramDiskVolume,
                              long lockedBytesReserved) {
      super(bpid, blockId, ramDiskVolume, lockedBytesReserved);
    }

    @Override
  TreeMultimap<Long, RamDiskReplicaLru> replicasPersisted;

  RamDiskReplicaLruTracker() {
    replicaMaps = new HashMap<>();
    replicasNotPersisted = new LinkedList<>();
    replicasPersisted = TreeMultimap.create();
  }

  @Override
  synchronized void addReplica(final String bpid, final long blockId,
                               final FsVolumeImpl transientVolume,
                               long lockedBytesReserved) {
    Map<Long, RamDiskReplicaLru> map = replicaMaps.get(bpid);
    if (map == null) {
      map = new HashMap<>();
      replicaMaps.put(bpid, map);
    }
    RamDiskReplicaLru ramDiskReplicaLru =
        new RamDiskReplicaLru(bpid, blockId, transientVolume,
                              lockedBytesReserved);
    map.put(blockId, ramDiskReplicaLru);
    replicasNotPersisted.add(ramDiskReplicaLru);
  }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/RamDiskReplicaTracker.java
    private final long blockId;
    private File savedBlockFile;
    private File savedMetaFile;
    private long lockedBytesReserved;

    private long creationTime;
    protected AtomicLong numReads = new AtomicLong(0);
    FsVolumeImpl lazyPersistVolume;

    RamDiskReplica(final String bpid, final long blockId,
                   final FsVolumeImpl ramDiskVolume,
                   long lockedBytesReserved) {
      this.bpid = bpid;
      this.blockId = blockId;
      this.ramDiskVolume = ramDiskVolume;
      this.lockedBytesReserved = lockedBytesReserved;
      lazyPersistVolume = null;
      savedMetaFile = null;
      savedBlockFile = null;
    public String toString() {
      return "[BlockPoolID=" + bpid + "; BlockId=" + blockId + "]";
    }

    public long getLockedBytesReserved() {
      return lockedBytesReserved;
    }
  }

  abstract void addReplica(final String bpid, final long blockId,
                           final FsVolumeImpl transientVolume,
                           long lockedBytesReserved);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/MiniDFSCluster.java
hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/balancer/TestBalancer.java
import org.apache.hadoop.hdfs.server.balancer.Balancer.Result;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.LazyPersistTestCase;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
  }

  static void initConfWithRamDisk(Configuration conf,
                                  long ramDiskCapacity) {
    conf.setLong(DFS_BLOCK_SIZE_KEY, DEFAULT_RAM_DISK_BLOCK_SIZE);
    conf.setLong(DFS_DATANODE_MAX_LOCKED_MEMORY_KEY, ramDiskCapacity);
    conf.setInt(DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC, 3);
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    conf.setInt(DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC, 1);
    conf.setInt(DFS_DATANODE_RAM_DISK_LOW_WATERMARK_BYTES, DEFAULT_RAM_DISK_BLOCK_SIZE);
    LazyPersistTestCase.initCacheManipulator();
  }

    final int SEED = 0xFADED;
    final short REPL_FACT = 1;
    Configuration conf = new Configuration();

    final int defaultRamDiskCapacity = 10;
    final long ramDiskStorageLimit =
      ((long) defaultRamDiskCapacity * DEFAULT_RAM_DISK_BLOCK_SIZE) +
      (DEFAULT_RAM_DISK_BLOCK_SIZE - 1);

    initConfWithRamDisk(conf, ramDiskStorageLimit);

    cluster = new MiniDFSCluster
      .Builder(conf)
      .numDataNodes(1)

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/SimulatedFSDataset.java
    public void reserveSpaceForRbw(long bytesToReserve) {
    }

    @Override
    public void releaseLockedMemory(long bytesToRelease) {
    }

    @Override
    public void releaseReservedSpace(long bytesToRelease) {
    }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDirectoryScanner.java
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetTestUtil;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.LazyPersistTestCase;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_LENGTH);
    CONF.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 1);
    CONF.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    CONF.setLong(DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
                 Long.MAX_VALUE);
  }


  @Test (timeout=300000)
  public void testRetainBlockOnPersistentStorage() throws Exception {
    LazyPersistTestCase.initCacheManipulator();
    cluster = new MiniDFSCluster
        .Builder(CONF)
        .storageTypes(new StorageType[] { StorageType.RAM_DISK, StorageType.DEFAULT })

  @Test (timeout=300000)
  public void testDeleteBlockOnTransientStorage() throws Exception {
    LazyPersistTestCase.initCacheManipulator();
    cluster = new MiniDFSCluster
        .Builder(CONF)
        .storageTypes(new StorageType[] { StorageType.RAM_DISK, StorageType.DEFAULT })
      return false;
    }

    @Override
    public void releaseLockedMemory(long bytesToRelease) {
    }

    @Override
    public BlockIterator newBlockIterator(String bpid, String name) {
      throw new UnsupportedOperationException();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestFsDatasetCache.java
    for (int i=0; i<numFiles-1; i++) {
      setHeartbeatResponse(cacheBlocks(fileLocs[i]));
      total = DFSTestUtil.verifyExpectedCacheUsage(
          rounder.roundUp(total + fileSizes[i]), 4 * (i + 1), fsd);
    }

    int curCachedBlocks = 16;
    for (int i=0; i<numFiles-1; i++) {
      setHeartbeatResponse(uncacheBlocks(fileLocs[i]));
      long uncachedBytes = rounder.roundUp(fileSizes[i]);
      total -= uncachedBytes;
      curCachedBlocks -= uncachedBytes / BLOCK_SIZE;
      DFSTestUtil.verifyExpectedCacheUsage(total, curCachedBlocks, fsd);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/extdataset/ExternalVolumeImpl.java
  public void releaseReservedSpace(long bytesToRelease) {
  }

  @Override
  public void releaseLockedMemory(long bytesToRelease) {
  }

  @Override
  public BlockIterator newBlockIterator(String bpid, String name) {
    return null;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/LazyPersistTestCase.java
import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;
import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.tools.JMXGet;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
  static final byte LAZY_PERSIST_POLICY_ID = (byte) 15;

  static {
    DFSTestUtil.setNameNodeLogLevel(Level.DEBUG);
    GenericTestUtils.setLogLevel(FsDatasetImpl.LOG, Level.DEBUG);
  }

  protected static final int BLOCK_SIZE = 5 * 1024 * 1024;
  protected static final int LAZY_WRITER_INTERVAL_SEC = 1;
  protected static final Log LOG = LogFactory.getLog(LazyPersistTestCase.class);
  protected static final short REPL_FACTOR = 1;
  protected final long osPageSize =
      NativeIO.POSIX.getCacheManipulator().getOperatingSystemPageSize();

  protected MiniDFSCluster cluster;
  protected DistributedFileSystem fs;
      int ramDiskReplicaCapacity,
      long ramDiskStorageLimit,
      long evictionLowWatermarkReplicas,
      long maxLockedMemory,
      boolean useSCR,
      boolean useLegacyBlockReaderLocal,
      boolean disableScrubber) throws IOException {

    initCacheManipulator();
    Configuration conf = new Configuration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    if (disableScrubber) {
    conf.setLong(DFS_DATANODE_RAM_DISK_LOW_WATERMARK_BYTES,
                evictionLowWatermarkReplicas * BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY, 1);
    conf.setLong(DFS_DATANODE_MAX_LOCKED_MEMORY_KEY, maxLockedMemory);

    if (useSCR) {
      conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    LOG.info("Cluster startup complete");
  }

  public static void initCacheManipulator() {
    NativeIO.POSIX.setCacheManipulator(new NativeIO.POSIX.CacheManipulator() {
      @Override
      public void mlock(String identifier,
                        ByteBuffer mmap, long length) throws IOException {
        LOG.info("LazyPersistTestCase: faking mlock of " + identifier + " bytes.");
      }

      @Override
      public long getMemlockLimit() {
        LOG.info("LazyPersistTestCase: fake return " + Long.MAX_VALUE);
        return Long.MAX_VALUE;
      }

      @Override
      public boolean verifyCanMlock() {
        LOG.info("LazyPersistTestCase: fake return " + true);
        return true;
      }
    });
  }

  ClusterWithRamDiskBuilder getClusterBuilder() {
    return new ClusterWithRamDiskBuilder();
  }
      return this;
    }

    public ClusterWithRamDiskBuilder setMaxLockedMemory(long maxLockedMemory) {
      this.maxLockedMemory = maxLockedMemory;
      return this;
    }

    public ClusterWithRamDiskBuilder setUseScr(boolean useScr) {
      this.useScr = useScr;
      return this;
      LazyPersistTestCase.this.startUpCluster(
          numDatanodes, hasTransientStorage, storageTypes, ramDiskReplicaCapacity,
          ramDiskStorageLimit, evictionLowWatermarkReplicas,
          maxLockedMemory, useScr, useLegacyBlockReaderLocal, disableScrubber);
    }

    private int numDatanodes = REPL_FACTOR;
    private StorageType[] storageTypes = null;
    private int ramDiskReplicaCapacity = -1;
    private long ramDiskStorageLimit = -1;
    private long maxLockedMemory = Long.MAX_VALUE;
    private boolean hasTransientStorage = true;
    private boolean useScr = false;
    private boolean useLegacyBlockReaderLocal = false;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistLockedMemory.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistLockedMemory.java

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;


import com.google.common.base.Supplier;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.MetricsAsserts;
import org.junit.Test;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;
import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class TestLazyPersistLockedMemory extends LazyPersistTestCase {

  @Test
  public void testWithNoLockedMemory() throws IOException {
    getClusterBuilder().setNumDatanodes(1)
                       .setMaxLockedMemory(0).build();

    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");
    makeTestFile(path, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path, DEFAULT);
  }

  @Test
  public void testReservation()
      throws IOException, TimeoutException, InterruptedException {
    getClusterBuilder().setNumDatanodes(1)
                       .setMaxLockedMemory(BLOCK_SIZE).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final FsDatasetSpi<?> fsd = cluster.getDataNodes().get(0).getFSDataset();

    Path path = new Path("/" + METHOD_NAME + ".dat");
    makeTestFile(path, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path, RAM_DISK);
    assertThat(fsd.getCacheUsed(), is((long) BLOCK_SIZE));
  }

  @Test
  public void testReleaseOnFileDeletion()
      throws IOException, TimeoutException, InterruptedException {
    getClusterBuilder().setNumDatanodes(1)
                       .setMaxLockedMemory(BLOCK_SIZE).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final FsDatasetSpi<?> fsd = cluster.getDataNodes().get(0).getFSDataset();

    Path path = new Path("/" + METHOD_NAME + ".dat");
    makeTestFile(path, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path, RAM_DISK);
    assertThat(fsd.getCacheUsed(), is((long) BLOCK_SIZE));

    fs.delete(path, false);
    DataNodeTestUtils.triggerBlockReport(cluster.getDataNodes().get(0));
    waitForLockedBytesUsed(fsd, 0);
  }

  @Test
  public void testReleaseOnEviction()
      throws IOException, TimeoutException, InterruptedException {
    getClusterBuilder().setNumDatanodes(1)
                       .setMaxLockedMemory(BLOCK_SIZE)
                       .setRamDiskReplicaCapacity(BLOCK_SIZE * 2 - 1)
                       .build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final FsDatasetSpi<?> fsd = cluster.getDataNodes().get(0).getFSDataset();

    Path path = new Path("/" + METHOD_NAME + ".dat");
    makeTestFile(path, BLOCK_SIZE, true);

    waitForLockedBytesUsed(fsd, 0);

    MetricsRecordBuilder rb =
        MetricsAsserts.getMetrics(cluster.getDataNodes().get(0).getMetrics().name());
    MetricsAsserts.assertCounter("RamDiskBlocksEvicted", 1L, rb);
  }

  @Test
  public void testShortBlockFinalized()
      throws IOException, TimeoutException, InterruptedException {
    getClusterBuilder().setNumDatanodes(1).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final FsDatasetSpi<?> fsd = cluster.getDataNodes().get(0).getFSDataset();

    Path path = new Path("/" + METHOD_NAME + ".dat");
    makeTestFile(path, 1, true);
    assertThat(fsd.getCacheUsed(), is(osPageSize));

    fs.delete(path, false);
    waitForLockedBytesUsed(fsd, 0);
  }

  @Test
  public void testWritePipelineFailure()
    throws IOException, TimeoutException, InterruptedException {
    getClusterBuilder().setNumDatanodes(1).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final FsDatasetSpi<?> fsd = cluster.getDataNodes().get(0).getFSDataset();

    Path path = new Path("/" + METHOD_NAME + ".dat");

    EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE, LAZY_PERSIST);
    final FSDataOutputStream fos =
        fs.create(path,
                  FsPermission.getFileDefault(),
                  createFlags,
                  BUFFER_LENGTH,
                  REPL_FACTOR,
                  BLOCK_SIZE,
                  null);

    fos.write(new byte[1]);
    fos.hsync();
    DFSTestUtil.abortStream((DFSOutputStream) fos.getWrappedStream());
    waitForLockedBytesUsed(fsd, osPageSize);

    fs.delete(path, false);
    DataNodeTestUtils.triggerBlockReport(cluster.getDataNodes().get(0));
    waitForLockedBytesUsed(fsd, 0);
  }

  private void waitForLockedBytesUsed(final FsDatasetSpi<?> fsd,
                                      final long expectedLockedBytes)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        long cacheUsed = fsd.getCacheUsed();
        LOG.info("cacheUsed=" + cacheUsed + ", waiting for it to be " + expectedLockedBytes);
        if (cacheUsed < 0) {
          throw new IllegalStateException("cacheUsed unpexpectedly negative");
        }
        return (cacheUsed == expectedLockedBytes);
      }
    }, 1000, 300000);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestWriteToReplica.java
    long available = v.getCapacity()-v.getDfsUsed();
    long expectedLen = blocks[FINALIZED].getNumBytes();
    try {
      v.onBlockFileDeletion(bpid, -available);
      blocks[FINALIZED].setNumBytes(expectedLen+100);
      dataSet.append(blocks[FINALIZED], newGS, expectedLen);
      Assert.fail("Should not have space to append to an RWR replica" + blocks[RWR]);
      Assert.assertTrue(e.getMessage().startsWith(
          "Insufficient space for appending to "));
    }
    v.onBlockFileDeletion(bpid, available);
    blocks[FINALIZED].setNumBytes(expectedLen);

    newGS = blocks[RBW].getGenerationStamp()+1;

