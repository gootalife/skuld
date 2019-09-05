hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BPOfferService.java
      dn.transferBlocks(bcmd.getBlockPoolId(), bcmd.getBlocks(),
          bcmd.getTargets(), bcmd.getTargetStorageTypes());
      break;
    case DatanodeProtocol.DNA_INVALIDATE:

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java
                  + Arrays.asList(targets));
            }
          }
        } else {
          metrics.incrBlocksReplicated();
        }
      } catch (IOException ie) {
        LOG.warn(bpReg + ":Failed to transfer " + b + " to " +

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/metrics/DataNodeMetrics.java
    cacheReports.add(latency);
  }

  public void incrBlocksReplicated() {
    blocksReplicated.incr();
  }

  public void incrBlocksWritten() {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDataNodeMetrics.java
import static org.apache.hadoop.test.MetricsAsserts.assertQuantileGauges;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.*;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.util.Time;
    }
  }

  @Test
  public void testDatanodeBlocksReplicatedMetric() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      FileSystem fs = cluster.getFileSystem();
      List<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(datanodes.size(), 1);
      DataNode datanode = datanodes.get(0);

      MetricsRecordBuilder rb = getMetrics(datanode.getMetrics().name());
      long blocksReplicated = getLongCounter("BlocksReplicated", rb);
      assertEquals("No blocks replicated yet", 0, blocksReplicated);

      Path path = new Path("/counter.txt");
      DFSTestUtil.createFile(fs, path, 1024, (short) 2, Time.monotonicNow());
      cluster.startDataNodes(conf, 1, true, StartupOption.REGULAR, null);
      ExtendedBlock firstBlock = DFSTestUtil.getFirstBlock(fs, path);
      DFSTestUtil.waitForReplication(cluster, firstBlock, 1, 2, 0);

      MetricsRecordBuilder rbNew = getMetrics(datanode.getMetrics().name());
      blocksReplicated = getLongCounter("BlocksReplicated", rbNew);
      assertEquals("blocks replicated counter incremented", 1, blocksReplicated);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}

