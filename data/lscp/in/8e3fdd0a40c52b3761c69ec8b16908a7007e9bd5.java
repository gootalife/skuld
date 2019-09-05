hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
    public void run() {
      while (fsRunning && shouldRun) {
        try {
          if (!isInSafeMode()) {
            clearCorruptLazyPersistFiles();
          } else {
            if (FSNamesystem.LOG.isDebugEnabled()) {
              FSNamesystem.LOG
                  .debug("Namenode is in safemode, skipping scrubbing of corrupted lazy-persist files.");
            }
          }
          Thread.sleep(scrubIntervalSec * 1000);
        } catch (InterruptedException e) {
          FSNamesystem.LOG.info(

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/LazyPersistTestCase.java
                LAZY_WRITER_INTERVAL_SEC);
    conf.setLong(DFS_DATANODE_RAM_DISK_LOW_WATERMARK_BYTES,
                evictionLowWatermarkReplicas * BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY, 1);

    if (useSCR) {
      conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestLazyPersistFiles.java
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Path;
    Assert.assertTrue(fs.exists(path1));

  }

  @Test
  public void testFileShouldNotDiscardedIfNNRestarted() throws IOException,
      InterruptedException {
    getClusterBuilder().setRamDiskReplicaCapacity(2).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    cluster.shutdownDataNodes();

    cluster.restartNameNodes();

    Thread.sleep(2 * DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT * 1000);

    Long corruptBlkCount = (long) Iterators.size(cluster.getNameNode()
        .getNamesystem().getBlockManager().getCorruptReplicaBlockIterator());

    assertThat(corruptBlkCount, is(1L));

    Assert.assertTrue(fs.exists(path1));
  }


