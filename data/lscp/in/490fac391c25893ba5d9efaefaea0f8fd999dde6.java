hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/LazyPersistTestCase.java
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;
import static org.apache.hadoop.fs.StorageType.DEFAULT;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
  protected final void startUpCluster(
      int numDatanodes,
      boolean hasTransientStorage,
      StorageType[] storageTypes,
      int ramDiskReplicaCapacity,
      long ramDiskStorageLimit,
      long evictionLowWatermarkReplicas,
      boolean useSCR,
      boolean useLegacyBlockReaderLocal) throws IOException {

    Configuration conf = new Configuration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
                HEARTBEAT_RECHECK_INTERVAL_MSEC);
    conf.setInt(DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC,
                LAZY_WRITER_INTERVAL_SEC);
    conf.setLong(DFS_DATANODE_RAM_DISK_LOW_WATERMARK_BYTES,
                evictionLowWatermarkReplicas * BLOCK_SIZE);

    if (useSCR) {
      conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
      conf.set(DFS_CLIENT_CONTEXT, UUID.randomUUID().toString());
      conf.set(DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY,
          UserGroupInformation.getCurrentUser().getShortUserName());
      if (useLegacyBlockReaderLocal) {
        conf.setBoolean(DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL, true);
      } else {
        sockDir = new TemporarySocketDirectory();
        conf.set(DFS_DOMAIN_SOCKET_PATH_KEY, new File(sockDir.getDir(),
      }
    }

    Preconditions.checkState(
        ramDiskReplicaCapacity < 0 || ramDiskStorageLimit < 0,
        "Cannot specify non-default values for both ramDiskReplicaCapacity "
            + "and ramDiskStorageLimit");

    long[] capacities;
    if (hasTransientStorage && ramDiskReplicaCapacity >= 0) {
      ramDiskStorageLimit = ((long) ramDiskReplicaCapacity * BLOCK_SIZE) +
          (BLOCK_SIZE - 1);
    }
    capacities = new long[] { ramDiskStorageLimit, -1 };

    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(numDatanodes)
        .storageCapacities(capacities)
        .storageTypes(storageTypes != null ? storageTypes :
                          (hasTransientStorage ? new StorageType[]{RAM_DISK, DEFAULT} : null))
        .build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    client = fs.getClient();
    try {
    LOG.info("Cluster startup complete");
  }

  ClusterWithRamDiskBuilder getClusterBuilder() {
    return new ClusterWithRamDiskBuilder();
  }

  class ClusterWithRamDiskBuilder {
    public ClusterWithRamDiskBuilder setNumDatanodes(
        int numDatanodes) {
      this.numDatanodes = numDatanodes;
      return this;
    }

    public ClusterWithRamDiskBuilder setStorageTypes(
        StorageType[] storageTypes) {
      this.storageTypes = storageTypes;
      return this;
    }

    public ClusterWithRamDiskBuilder setRamDiskReplicaCapacity(
        int ramDiskReplicaCapacity) {
      this.ramDiskReplicaCapacity = ramDiskReplicaCapacity;
      return this;
    }

    public ClusterWithRamDiskBuilder setRamDiskStorageLimit(
        long ramDiskStorageLimit) {
      this.ramDiskStorageLimit = ramDiskStorageLimit;
      return this;
    }

    public ClusterWithRamDiskBuilder setUseScr(boolean useScr) {
      this.useScr = useScr;
      return this;
    }

    public ClusterWithRamDiskBuilder setHasTransientStorage(
        boolean hasTransientStorage) {
      this.hasTransientStorage = hasTransientStorage;
      return this;
    }

    public ClusterWithRamDiskBuilder setUseLegacyBlockReaderLocal(
        boolean useLegacyBlockReaderLocal) {
      this.useLegacyBlockReaderLocal = useLegacyBlockReaderLocal;
      return this;
    }

    public ClusterWithRamDiskBuilder setEvictionLowWatermarkReplicas(
        long evictionLowWatermarkReplicas) {
      this.evictionLowWatermarkReplicas = evictionLowWatermarkReplicas;
      return this;
    }

    public void build() throws IOException {
      LazyPersistTestCase.this.startUpCluster(
          numDatanodes, hasTransientStorage, storageTypes, ramDiskReplicaCapacity,
          ramDiskStorageLimit, evictionLowWatermarkReplicas,
          useScr, useLegacyBlockReaderLocal);
    }

    private int numDatanodes = REPL_FACTOR;
    private StorageType[] storageTypes = null;
    private int ramDiskReplicaCapacity = -1;
    private long ramDiskStorageLimit = -1;
    private boolean hasTransientStorage = true;
    private boolean useScr = false;
    private boolean useLegacyBlockReaderLocal = false;
    private long evictionLowWatermarkReplicas = EVICTION_LOW_WATERMARK;
  }

  protected final void triggerBlockReport()

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistFiles.java
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.StorageType.RAM_DISK;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TestLazyPersistFiles extends LazyPersistTestCase {
  @Test
  public void testAppendIsDenied() throws IOException {
    getClusterBuilder().build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

  @Test
  public void testTruncateIsDenied() throws IOException {
    getClusterBuilder().build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

  @Test
  public void testCorruptFilesAreDiscarded()
      throws IOException, InterruptedException {
    getClusterBuilder().setRamDiskReplicaCapacity(2).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");

  @Test
  public void testConcurrentRead()
    throws Exception {
    getClusterBuilder().setRamDiskReplicaCapacity(2).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final Path path1 = new Path("/" + METHOD_NAME + ".dat");

  @Test
  public void testConcurrentWrites()
    throws IOException, InterruptedException {
    getClusterBuilder().setRamDiskReplicaCapacity(9).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final int SEED = 0xFADED;
    final int NUM_WRITERS = 4;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistPolicy.java
public class TestLazyPersistPolicy extends LazyPersistTestCase {
  @Test
  public void testPolicyNotSetByDefault() throws IOException {
    getClusterBuilder().build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");


  @Test
  public void testPolicyPropagation() throws IOException {
    getClusterBuilder().build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");


  @Test
  public void testPolicyPersistenceInEditLog() throws IOException {
    getClusterBuilder().build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");


  @Test
  public void testPolicyPersistenceInFsImage() throws IOException {
    getClusterBuilder().build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistReplicaPlacement.java
public class TestLazyPersistReplicaPlacement extends LazyPersistTestCase {
  @Test
  public void testPlacementOnRamDisk() throws IOException {
    getClusterBuilder().build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");


  @Test
  public void testPlacementOnSizeLimitedRamDisk() throws IOException {
    getClusterBuilder().setRamDiskReplicaCapacity(3).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");
  @Test
  public void testFallbackToDisk() throws IOException {
    getClusterBuilder().setHasTransientStorage(false).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

  @Test
  public void testFallbackToDiskFull() throws Exception {
    getClusterBuilder().setRamDiskReplicaCapacity(0).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

  @Test
  public void testFallbackToDiskPartial()
      throws IOException, InterruptedException {
    getClusterBuilder().setRamDiskReplicaCapacity(2).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

  @Test
  public void testRamDiskNotChosenByDefault() throws IOException {
    getClusterBuilder().build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistReplicaRecovery.java
  public void testDnRestartWithSavedReplicas()
      throws IOException, InterruptedException {

    getClusterBuilder().build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");

  public void testDnRestartWithUnsavedReplicas()
      throws IOException, InterruptedException {

    getClusterBuilder().build();
    FsDatasetTestUtil.stopLazyWriter(cluster.getDataNodes().get(0));

    final String METHOD_NAME = GenericTestUtils.getMethodName();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyWriter.java
  @Test
  public void testLazyPersistBlocksAreSaved()
      throws IOException, InterruptedException {
    getClusterBuilder().build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

  @Test
  public void testRamDiskEviction() throws Exception {
    getClusterBuilder().setRamDiskReplicaCapacity(1 + EVICTION_LOW_WATERMARK).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");
  @Test
  public void testRamDiskEvictionBeforePersist()
      throws IOException, InterruptedException {
    getClusterBuilder().setRamDiskReplicaCapacity(1).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");
  public void testRamDiskEvictionIsLru()
      throws Exception {
    final int NUM_PATHS = 5;
    getClusterBuilder().setRamDiskReplicaCapacity(NUM_PATHS + EVICTION_LOW_WATERMARK).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path paths[] = new Path[NUM_PATHS * 2];

  @Test
  public void testDeleteBeforePersist()
      throws Exception {
    getClusterBuilder().build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    FsDatasetTestUtil.stopLazyWriter(cluster.getDataNodes().get(0));

  @Test
  public void testDeleteAfterPersist()
      throws Exception {
    getClusterBuilder().build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

  @Test
  public void testDfsUsageCreateDelete()
      throws IOException, InterruptedException {
    getClusterBuilder().setRamDiskReplicaCapacity(4).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestScrLazyPersistFiles.java
  @Test
  public void testRamDiskShortCircuitRead()
    throws IOException, InterruptedException {
    getClusterBuilder().setNumDatanodes(REPL_FACTOR)
                       .setStorageTypes(new StorageType[]{RAM_DISK, DEFAULT})
                       .setRamDiskStorageLimit(2 * BLOCK_SIZE - 1)
                       .setUseScr(true)
                       .build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final int SEED = 0xFADED;
    Path path = new Path("/" + METHOD_NAME + ".dat");
  @Test
  public void testRamDiskEvictionWithShortCircuitReadHandle()
    throws IOException, InterruptedException {
    getClusterBuilder().setNumDatanodes(REPL_FACTOR)
                       .setStorageTypes(new StorageType[]{RAM_DISK, DEFAULT})
                       .setRamDiskStorageLimit(6 * BLOCK_SIZE - 1)
                       .setEvictionLowWatermarkReplicas(3)
                       .setUseScr(true)
                       .build();

    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");
  public void testShortCircuitReadAfterEviction()
      throws IOException, InterruptedException {
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
    getClusterBuilder().setRamDiskReplicaCapacity(1 + EVICTION_LOW_WATERMARK)
                       .setUseScr(true)
                       .setUseLegacyBlockReaderLocal(false)
                       .build();
    doShortCircuitReadAfterEvictionTest();
  }

  @Test
  public void testLegacyShortCircuitReadAfterEviction()
      throws IOException, InterruptedException {
    getClusterBuilder().setRamDiskReplicaCapacity(1 + EVICTION_LOW_WATERMARK)
                       .setUseScr(true)
                       .setUseLegacyBlockReaderLocal(true)
                       .build();
    doShortCircuitReadAfterEvictionTest();
  }

  public void testShortCircuitReadBlockFileCorruption() throws IOException,
      InterruptedException {
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
    getClusterBuilder().setRamDiskReplicaCapacity(1 + EVICTION_LOW_WATERMARK)
                       .setUseScr(true)
                       .setUseLegacyBlockReaderLocal(false)
                       .build();
    doShortCircuitReadBlockFileCorruptionTest();
  }

  @Test
  public void testLegacyShortCircuitReadBlockFileCorruption() throws IOException,
      InterruptedException {
    getClusterBuilder().setRamDiskReplicaCapacity(1 + EVICTION_LOW_WATERMARK)
                       .setUseScr(true)
                       .setUseLegacyBlockReaderLocal(true)
                       .build();
    doShortCircuitReadBlockFileCorruptionTest();
  }

  public void testShortCircuitReadMetaFileCorruption() throws IOException,
      InterruptedException {
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
    getClusterBuilder().setRamDiskReplicaCapacity(1 + EVICTION_LOW_WATERMARK)
                       .setUseScr(true)
                       .setUseLegacyBlockReaderLocal(false)
                       .build();
    doShortCircuitReadMetaFileCorruptionTest();
  }

  @Test
  public void testLegacyShortCircuitReadMetaFileCorruption() throws IOException,
      InterruptedException {
    getClusterBuilder().setRamDiskReplicaCapacity(1 + EVICTION_LOW_WATERMARK)
                       .setUseScr(true)
                       .setUseLegacyBlockReaderLocal(true)
                       .build();
    doShortCircuitReadMetaFileCorruptionTest();
  }


