hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/LazyPersistTestCase.java
import static org.junit.Assert.fail;

public abstract class LazyPersistTestCase {
  static final byte LAZY_PERSIST_POLICY_ID = (byte) 15;

  static {
    DFSTestUtil.setNameNodeLogLevel(Level.ALL);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistFiles.java
import static org.junit.Assert.fail;

public class TestLazyPersistFiles extends LazyPersistTestCase {
  private static final int THREADPOOL_SIZE = 10;

  @Test
  public void testCorruptFilesAreDiscarded()
      throws IOException, InterruptedException {
    startUpCluster(true, 2);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
               is(0L));
  }

    assertThat(testFailed.get(), is(false));
  }

  class WriterRunnable implements Runnable {
    private final int id;
    private final Path paths[];

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistPolicy.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistPolicy.java

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;


public class TestLazyPersistPolicy extends LazyPersistTestCase {
  @Test
  public void testPolicyNotSetByDefault() throws IOException {
    startUpCluster(false, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, 0, false);
    HdfsFileStatus status = client.getFileInfo(path.toString());
    assertThat(status.getStoragePolicy(), not(LAZY_PERSIST_POLICY_ID));
  }

  @Test
  public void testPolicyPropagation() throws IOException {
    startUpCluster(false, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, 0, true);
    HdfsFileStatus status = client.getFileInfo(path.toString());
    assertThat(status.getStoragePolicy(), is(LAZY_PERSIST_POLICY_ID));
  }

  @Test
  public void testPolicyPersistenceInEditLog() throws IOException {
    startUpCluster(false, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, 0, true);
    cluster.restartNameNode(true);

    HdfsFileStatus status = client.getFileInfo(path.toString());
    assertThat(status.getStoragePolicy(), is(LAZY_PERSIST_POLICY_ID));
  }

  @Test
  public void testPolicyPersistenceInFsImage() throws IOException {
    startUpCluster(false, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, 0, true);
    fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
    fs.saveNamespace();
    fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
    cluster.restartNameNode(true);

    HdfsFileStatus status = client.getFileInfo(path.toString());
    assertThat(status.getStoragePolicy(), is(LAZY_PERSIST_POLICY_ID));
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistReplicaPlacement.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistReplicaPlacement.java

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;
import static org.junit.Assert.fail;

public class TestLazyPersistReplicaPlacement extends LazyPersistTestCase {
  @Test
  public void testPlacementOnRamDisk() throws IOException {
    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path, RAM_DISK);
  }

  @Test
  public void testPlacementOnSizeLimitedRamDisk() throws IOException {
    startUpCluster(true, 3);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");

    makeTestFile(path1, BLOCK_SIZE, true);
    makeTestFile(path2, BLOCK_SIZE, true);

    ensureFileReplicasOnStorageType(path1, RAM_DISK);
    ensureFileReplicasOnStorageType(path2, RAM_DISK);
  }

  @Test
  public void testFallbackToDisk() throws IOException {
    startUpCluster(false, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path, DEFAULT);
  }

  @Test
  public void testFallbackToDiskFull() throws Exception {
    startUpCluster(false, 0);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path, DEFAULT);

    verifyRamDiskJMXMetric("RamDiskBlocksWriteFallback", 1);
  }

  @Test
  public void testFallbackToDiskPartial()
      throws IOException, InterruptedException {
    startUpCluster(true, 2);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE * 5, true);

    Thread.sleep(6 * LAZY_WRITER_INTERVAL_SEC * 1000);

    triggerBlockReport();

    int numBlocksOnRamDisk = 0;
    int numBlocksOnDisk = 0;

    long fileLength = client.getFileInfo(path.toString()).getLen();
    LocatedBlocks locatedBlocks =
        client.getLocatedBlocks(path.toString(), 0, fileLength);
    for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      if (locatedBlock.getStorageTypes()[0] == RAM_DISK) {
        numBlocksOnRamDisk++;
      } else if (locatedBlock.getStorageTypes()[0] == DEFAULT) {
        numBlocksOnDisk++;
      }
    }

    assert(numBlocksOnRamDisk <= 2);
    assert(numBlocksOnDisk >= 3);
  }

  @Test
  public void testRamDiskNotChosenByDefault() throws IOException {
    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    try {
      makeTestFile(path, BLOCK_SIZE, false);
      fail("Block placement to RAM_DISK should have failed without lazyPersist flag");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistReplicaRecovery.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistReplicaRecovery.java

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;

public class TestLazyPersistReplicaRecovery extends LazyPersistTestCase {
  @Test
  public void testDnRestartWithSavedReplicas()
      throws IOException, InterruptedException {

    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");

    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    LOG.info("Restarting the DataNode");
    cluster.restartDataNode(0, true);
    cluster.waitActive();
    triggerBlockReport();

    ensureFileReplicasOnStorageType(path1, DEFAULT);
  }

  @Test
  public void testDnRestartWithUnsavedReplicas()
      throws IOException, InterruptedException {

    startUpCluster(true, 1);
    FsDatasetTestUtil.stopLazyWriter(cluster.getDataNodes().get(0));

    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    LOG.info("Restarting the DataNode");
    cluster.restartDataNode(0, true);
    cluster.waitActive();

    ensureFileReplicasOnStorageType(path1, RAM_DISK);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyWriter.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyWriter.java

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestLazyWriter extends LazyPersistTestCase {
  @Test
  public void testLazyPersistBlocksAreSaved()
      throws IOException, InterruptedException {
    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE * 10, true);
    LocatedBlocks locatedBlocks = ensureFileReplicasOnStorageType(path, RAM_DISK);

    Thread.sleep(6 * LAZY_WRITER_INTERVAL_SEC * 1000);

    LOG.info("Verifying copy was saved to lazyPersist/");

    ensureLazyPersistBlocksAreSaved(locatedBlocks);
  }

  @Test
  public void testRamDiskEviction() throws Exception {
    startUpCluster(true, 1 + EVICTION_LOW_WATERMARK);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");

    final int SEED = 0xFADED;
    makeRandomTestFile(path1, BLOCK_SIZE, true, SEED);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    makeTestFile(path2, BLOCK_SIZE, true);
    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);
    triggerBlockReport();

    ensureFileReplicasOnStorageType(path2, RAM_DISK);
    ensureFileReplicasOnStorageType(path1, DEFAULT);

    verifyRamDiskJMXMetric("RamDiskBlocksEvicted", 1);
    verifyRamDiskJMXMetric("RamDiskBlocksEvictedWithoutRead", 1);
  }

  @Test
  public void testRamDiskEvictionBeforePersist()
      throws IOException, InterruptedException {
    startUpCluster(true, 1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");
    final int SEED = 0XFADED;

    FsDatasetTestUtil.stopLazyWriter(cluster.getDataNodes().get(0));

    makeRandomTestFile(path1, BLOCK_SIZE, true, SEED);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    makeTestFile(path2, BLOCK_SIZE, true);

    ensureFileReplicasOnStorageType(path1, RAM_DISK);
    ensureFileReplicasOnStorageType(path2, DEFAULT);

    assert(fs.exists(path1));
    assert(fs.exists(path2));
    assertTrue(verifyReadRandomFile(path1, BLOCK_SIZE, SEED));
  }

  @Test
  public void testRamDiskEvictionIsLru()
      throws Exception {
    final int NUM_PATHS = 5;
    startUpCluster(true, NUM_PATHS + EVICTION_LOW_WATERMARK);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path paths[] = new Path[NUM_PATHS * 2];

    for (int i = 0; i < paths.length; i++) {
      paths[i] = new Path("/" + METHOD_NAME + "." + i +".dat");
    }

    for (int i = 0; i < NUM_PATHS; i++) {
      makeTestFile(paths[i], BLOCK_SIZE, true);
    }

    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);

    for (int i = 0; i < NUM_PATHS; ++i) {
      ensureFileReplicasOnStorageType(paths[i], RAM_DISK);
    }

    ArrayList<Integer> indexes = new ArrayList<Integer>(NUM_PATHS);
    for (int i = 0; i < NUM_PATHS; ++i) {
      indexes.add(i);
    }
    Collections.shuffle(indexes);

    for (int i = 0; i < NUM_PATHS; ++i) {
      LOG.info("Touching file " + paths[indexes.get(i)]);
      DFSTestUtil.readFile(fs, paths[indexes.get(i)]);
    }

    for (int i = 0; i < NUM_PATHS; ++i) {
      makeTestFile(paths[i + NUM_PATHS], BLOCK_SIZE, true);
      triggerBlockReport();
      Thread.sleep(3000);
      ensureFileReplicasOnStorageType(paths[i + NUM_PATHS], RAM_DISK);
      ensureFileReplicasOnStorageType(paths[indexes.get(i)], DEFAULT);
      for (int j = i + 1; j < NUM_PATHS; ++j) {
        ensureFileReplicasOnStorageType(paths[indexes.get(j)], RAM_DISK);
      }
    }

    verifyRamDiskJMXMetric("RamDiskBlocksWrite", NUM_PATHS * 2);
    verifyRamDiskJMXMetric("RamDiskBlocksWriteFallback", 0);
    verifyRamDiskJMXMetric("RamDiskBytesWrite", BLOCK_SIZE * NUM_PATHS * 2);
    verifyRamDiskJMXMetric("RamDiskBlocksReadHits", NUM_PATHS);
    verifyRamDiskJMXMetric("RamDiskBlocksEvicted", NUM_PATHS);
    verifyRamDiskJMXMetric("RamDiskBlocksEvictedWithoutRead", 0);
    verifyRamDiskJMXMetric("RamDiskBlocksDeletedBeforeLazyPersisted", 0);
  }

  @Test
  public void testDeleteBeforePersist()
      throws Exception {
    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    FsDatasetTestUtil.stopLazyWriter(cluster.getDataNodes().get(0));

    Path path = new Path("/" + METHOD_NAME + ".dat");
    makeTestFile(path, BLOCK_SIZE, true);
    LocatedBlocks locatedBlocks =
        ensureFileReplicasOnStorageType(path, RAM_DISK);

    client.delete(path.toString(), false);
    Assert.assertFalse(fs.exists(path));

    assertThat(verifyDeletedBlocks(locatedBlocks), is(true));

    verifyRamDiskJMXMetric("RamDiskBlocksDeletedBeforeLazyPersisted", 1);
  }

  @Test
  public void testDeleteAfterPersist()
      throws Exception {
    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);
    LocatedBlocks locatedBlocks = ensureFileReplicasOnStorageType(path, RAM_DISK);

    Thread.sleep(6 * LAZY_WRITER_INTERVAL_SEC * 1000);

    client.delete(path.toString(), false);
    Assert.assertFalse(fs.exists(path));

    assertThat(verifyDeletedBlocks(locatedBlocks), is(true));

    verifyRamDiskJMXMetric("RamDiskBlocksLazyPersisted", 1);
    verifyRamDiskJMXMetric("RamDiskBytesLazyPersisted", BLOCK_SIZE);
  }

  @Test
  public void testDfsUsageCreateDelete()
      throws IOException, InterruptedException {
    startUpCluster(true, 4);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    long usedBeforeCreate = fs.getUsed();

    makeTestFile(path, BLOCK_SIZE, true);
    long usedAfterCreate = fs.getUsed();

    assertThat(usedAfterCreate, is((long) BLOCK_SIZE));

    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);

    long usedAfterPersist = fs.getUsed();
    assertThat(usedAfterPersist, is((long) BLOCK_SIZE));

    client.delete(path.toString(), false);
    long usedAfterDelete = fs.getUsed();

    assertThat(usedBeforeCreate, is(usedAfterDelete));
  }
}

