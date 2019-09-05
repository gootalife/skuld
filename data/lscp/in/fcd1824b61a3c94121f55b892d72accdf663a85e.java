hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsVolumeImpl.java
  File createRbwFile(String bpid, Block b) throws IOException {
    checkReference();
    reserveSpaceForRbw(b.getNumBytes());
    try {
      return getBlockPoolSlice(bpid).createRbwFile(b);
    } catch (IOException exception) {
      releaseReservedSpace(b.getNumBytes());
      throw exception;
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestRbwSpaceReservation.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeoutException;

    }
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 30000)
  public void testRBWFileCreationError() throws Exception {

    final short replication = 1;
    startCluster(BLOCK_SIZE, replication, -1);

    final FsVolumeImpl fsVolumeImpl = (FsVolumeImpl) cluster.getDataNodes()
        .get(0).getFSDataset().getFsVolumeReferences().get(0);
    final String methodName = GenericTestUtils.getMethodName();
    final Path file = new Path("/" + methodName + ".01.dat");

    BlockPoolSlice blockPoolSlice = Mockito.mock(BlockPoolSlice.class);
    Mockito.when(blockPoolSlice.createRbwFile((Block) Mockito.any()))
        .thenThrow(new IOException("Synthetic IO Exception Throgh MOCK"));

    Field field = FsVolumeImpl.class.getDeclaredField("bpSlices");
    field.setAccessible(true);
    Map<String, BlockPoolSlice> bpSlices = (Map<String, BlockPoolSlice>) field
        .get(fsVolumeImpl);
    bpSlices.put(fsVolumeImpl.getBlockPoolList()[0], blockPoolSlice);

    try {
      FSDataOutputStream os = fs.create(file, replication);
      os.write(new byte[1]);
      os.hsync();
      os.close();
      fail("Expecting IOException file creation failure");
    } catch (IOException e) {
    }

    assertTrue("Expected ZERO but got " + fsVolumeImpl.getReservedForRbw(),
        fsVolumeImpl.getReservedForRbw() == 0);
  }


