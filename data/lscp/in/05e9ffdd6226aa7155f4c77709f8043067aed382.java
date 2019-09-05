hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetImpl.java
    final String directory;
    final long usedSpace; // size of space used by HDFS
    final long freeSpace; // size of free space excluding reserved space
    final long reservedSpace; // size of space reserved for non-HDFS
    final long reservedSpaceForRBW; // size of space reserved RBW

    VolumeInfo(FsVolumeImpl v, long usedSpace, long freeSpace) {
      this.directory = v.toString();
      this.usedSpace = usedSpace;
      this.freeSpace = freeSpace;
      this.reservedSpace = v.getReserved();
      this.reservedSpaceForRBW = v.getReservedForRbw();
    }
  }  

      innerInfo.put("usedSpace", v.usedSpace);
      innerInfo.put("freeSpace", v.freeSpace);
      innerInfo.put("reservedSpace", v.reservedSpace);
      innerInfo.put("reservedSpaceForRBW", v.reservedSpaceForRBW);
      info.put(v.directory, innerInfo);
    }
    return info;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestRbwSpaceReservation.java

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import javax.management.MBeanServer;
import javax.management.ObjectName;

        fsVolumeImpl.getReservedForRbw() == 0);
  }

  @Test(timeout = 30000)
  public void testRBWInJMXBean() throws Exception {

    final short replication = 1;
    startCluster(BLOCK_SIZE, replication, -1);

    final String methodName = GenericTestUtils.getMethodName();
    final Path file = new Path("/" + methodName + ".01.dat");

    try (FSDataOutputStream os = fs.create(file, replication)) {
      os.write(new byte[1]);
      os.hsync();

      final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      final ObjectName mxbeanName = new ObjectName(
          "Hadoop:service=DataNode,name=DataNodeInfo");
      final String volumeInfo = (String) mbs.getAttribute(mxbeanName,
          "VolumeInfo");

      assertTrue(volumeInfo.contains("reservedSpaceForRBW"));
    }
  }


