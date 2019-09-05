hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
    return blockManager.getTotalBlocks();
  }

  @Metric({ "NumFilesUnderConstruction",
      "Number of files under construction" })
  public long getNumFilesUnderConstruction() {
    return leaseManager.countPath();
  }

  @Metric({ "NumActiveClients", "Number of active clients holding lease" })
  public long getNumActiveClients() {
    return leaseManager.countLease();
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/LeaseManager.java
  @VisibleForTesting
  public synchronized int countLease() {return sortedLeases.size();}

  synchronized int countPath() {
    int count = 0;
    for (Lease lease : sortedLeases) {
      count += lease.getFiles().size();
    }
    return count;
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/metrics/TestNameNodeMetrics.java
package org.apache.hadoop.hdfs.server.namenode.metrics;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.assertQuantileGauges;
import java.io.IOException;
import java.util.Random;
import com.google.common.collect.ImmutableList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.test.MetricsAsserts;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
    assertTrue(MetricsAsserts.getLongCounter("TransactionsNumOps", rbNew) >
        startWriteCounter);
  }

  @Test(timeout = 60000)
  public void testNumActiveClientsAndFilesUnderConstructionMetrics()
      throws Exception {
    final Path file1 = getTestPath("testFileAdd1");
    createFile(file1, 100, (short) 3);
    assertGauge("NumActiveClients", 0L, getMetrics(NS_METRICS));
    assertGauge("NumFilesUnderConstruction", 0L, getMetrics(NS_METRICS));

    Path file2 = new Path("/testFileAdd2");
    FSDataOutputStream output2 = fs.create(file2);
    output2.writeBytes("Some test data");
    assertGauge("NumActiveClients", 1L, getMetrics(NS_METRICS));
    assertGauge("NumFilesUnderConstruction", 1L, getMetrics(NS_METRICS));

    Path file3 = new Path("/testFileAdd3");
    FSDataOutputStream output3 = fs.create(file3);
    output3.writeBytes("Some test data");
    assertGauge("NumActiveClients", 1L, getMetrics(NS_METRICS));
    assertGauge("NumFilesUnderConstruction", 2L, getMetrics(NS_METRICS));

    DistributedFileSystem fs1 = (DistributedFileSystem) cluster
        .getNewFileSystemInstance(0);
    try {
      Path file4 = new Path("/testFileAdd4");
      FSDataOutputStream output4 = fs1.create(file4);
      output4.writeBytes("Some test data");
      assertGauge("NumActiveClients", 2L, getMetrics(NS_METRICS));
      assertGauge("NumFilesUnderConstruction", 3L, getMetrics(NS_METRICS));

      Path file5 = new Path("/testFileAdd35");
      FSDataOutputStream output5 = fs1.create(file5);
      output5.writeBytes("Some test data");
      assertGauge("NumActiveClients", 2L, getMetrics(NS_METRICS));
      assertGauge("NumFilesUnderConstruction", 4L, getMetrics(NS_METRICS));

      output2.close();
      output3.close();
      assertGauge("NumActiveClients", 1L, getMetrics(NS_METRICS));
      assertGauge("NumFilesUnderConstruction", 2L, getMetrics(NS_METRICS));

      output4.close();
      output5.close();
      assertGauge("NumActiveClients", 0L, getMetrics(NS_METRICS));
      assertGauge("NumFilesUnderConstruction", 0L, getMetrics(NS_METRICS));
    } finally {
      fs1.close();
    }
  }
}

