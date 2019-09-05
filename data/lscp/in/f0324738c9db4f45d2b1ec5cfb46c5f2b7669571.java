hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockReceiver.java
      }

    } catch (IOException ioe) {
      replicaInfo.releaseAllBytesReserved();
      if (datanode.isRestarting()) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/ReplicaInPipeline.java
    return bytesReserved;
  }
  
  @Override
  public void releaseAllBytesReserved() {  // ReplicaInPipelineInterface
    getVolume().releaseReservedSpace(bytesReserved);
    bytesReserved = 0;
  }

  @Override // ReplicaInPipelineInterface
  public synchronized void setLastChecksumAndDataLen(long dataLength, byte[] lastChecksum) {
    this.bytesOnDisk = dataLength;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/ReplicaInPipelineInterface.java
  void setBytesAcked(long bytesAcked);
  
  public void releaseAllBytesReserved();


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/SimulatedFSDataset.java
      }
    }

    @Override
    public void releaseAllBytesReserved() {
    }

    @Override
    synchronized public long getBytesOnDisk() {
      if (finalized) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/extdataset/ExternalReplicaInPipeline.java
  public void setBytesAcked(long bytesAcked) {
  }

  @Override
  public void releaseAllBytesReserved() {
  }

  @Override
  public void setLastChecksumAndDataLen(long dataLength, byte[] lastChecksum) {
  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestRbwSpaceReservation.java

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.google.common.base.Supplier;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.OutputStream;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

public class TestRbwSpaceReservation {
  static final Log LOG = LogFactory.getLog(TestRbwSpaceReservation.class);

  private static final int DU_REFRESH_INTERVAL_MSEC = 500;
  private static final int STORAGES_PER_DATANODE = 1;
  private static final int BLOCK_SIZE = 1024 * 1024;
    ((Log4JLogger) DataNode.LOG).getLogger().setLevel(Level.ALL);
  }

  private void startCluster(int blockSize, int numDatanodes, long perVolumeCapacity) throws IOException {
    initConfig(blockSize);

    cluster = new MiniDFSCluster
        .Builder(conf)
        .storagesPerDatanode(STORAGES_PER_DATANODE)
        .numDataNodes(numDatanodes)
        .build();
    fs = cluster.getFileSystem();
    client = fs.getClient();
    cluster.waitActive();

    if (perVolumeCapacity >= 0) {
      for (DataNode dn : cluster.getDataNodes()) {
        for (FsVolumeSpi volume : dn.getFSDataset().getVolumes()) {
          ((FsVolumeImpl) volume).setCapacityForTesting(perVolumeCapacity);
        }
      }
    }

    if (numDatanodes == 1) {
      List<? extends FsVolumeSpi> volumes =
          cluster.getDataNodes().get(0).getFSDataset().getVolumes();
      assertThat(volumes.size(), is(1));
      singletonVolume = ((FsVolumeImpl) volumes.get(0));
    }
  }

      throws IOException, InterruptedException {
    final long configuredCapacity = fileBlockSize * 2 - 1;
    startCluster(BLOCK_SIZE, 1, configuredCapacity);
    FSDataOutputStream out = null;
    Path path = new Path("/" + fileNamePrefix + ".dat");

  @Test (timeout=300000)
  public void testWithLimitedSpace() throws IOException {
    startCluster(BLOCK_SIZE, 1, 2 * BLOCK_SIZE - 1);
    final String methodName = GenericTestUtils.getMethodName();
    Path file1 = new Path("/" + methodName + ".01.dat");
    Path file2 = new Path("/" + methodName + ".02.dat");
      os2 = fs.create(file2);

      byte[] data = new byte[1];
      os1.write(data);
      os1.hsync();
    }
  }

  @Test(timeout=300000)
  public void testSpaceReleasedOnUnexpectedEof()
      throws IOException, InterruptedException, TimeoutException {
    final short replication = 3;
    startCluster(BLOCK_SIZE, replication, -1);

    final String methodName = GenericTestUtils.getMethodName();
    final Path file = new Path("/" + methodName + ".01.dat");

    FSDataOutputStream os = fs.create(file, replication);
    os.write(new byte[1]);
    os.hsync();
    DFSTestUtil.abortStream((DFSOutputStream) os.getWrappedStream());

    for (DataNode dn : cluster.getDataNodes()) {
      final FsVolumeImpl volume = (FsVolumeImpl) dn.getFSDataset().getVolumes().get(0);
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return (volume.getReservedForRbw() == 0);
        }
      }, 500, Integer.MAX_VALUE); // Wait until the test times out.
    }
  }

  @Test (timeout=600000)
  public void stressTest() throws IOException, InterruptedException {
    final int numWriters = 5;
    startCluster(SMALL_BLOCK_SIZE, 1, SMALL_BLOCK_SIZE * numWriters * 10);
    Writer[] writers = new Writer[numWriters];


