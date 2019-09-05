hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetImpl.java
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.BlockScanner;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetricHelper;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
    lazyWriter = new Daemon(new LazyWriter(conf));
    lazyWriter.start();
    registerMBean(datanode.getDatanodeUuid());

    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.register("FSDatasetState", "FSDatasetState", this);

    localFS = FileSystem.getLocal(conf);
    blockPinningEnabled = conf.getBoolean(
      DFSConfigKeys.DFS_DATANODE_BLOCK_PINNING_ENABLED,
    return cacheManager.getNumBlocksFailedToUncache();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    try {
      DataNodeMetricHelper.getMetrics(collector, this, "FSDatasetState");
    } catch (Exception e) {
        LOG.warn("Exception thrown while metric collection. Exception : "
          + e.getMessage());
    }
  }

  @Override // FSDatasetMBean
  public long getNumBlocksCached() {
    return cacheManager.getNumBlocksCached();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/metrics/DataNodeMetricHelper.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/metrics/DataNodeMetricHelper.java
package org.apache.hadoop.hdfs.server.datanode.metrics;

import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.Interns;

import java.io.IOException;

public class DataNodeMetricHelper {

  public static void getMetrics(MetricsCollector collector,
                                FSDatasetMBean beanClass, String context)
    throws IOException {

    if (beanClass == null) {
      throw new IOException("beanClass cannot be null");
    }

    String className = beanClass.getClass().getName();

    collector.addRecord(className)
      .setContext(context)
      .addGauge(Interns.info("Capacity", "Total storage capacity"),
        beanClass.getCapacity())
      .addGauge(Interns.info("DfsUsed", "Total bytes used by dfs datanode"),
        beanClass.getDfsUsed())
      .addGauge(Interns.info("Remaining", "Total bytes of free storage"),
        beanClass.getRemaining())
      .add(new MetricsTag(Interns.info("StorageInfo", "Storage ID"),
        beanClass.getStorageInfo()))
      .addGauge(Interns.info("NumFailedVolumes", "Number of failed Volumes" +
        " in the data Node"), beanClass.getNumFailedVolumes())
      .addGauge(Interns.info("LastVolumeFailureDate", "Last Volume failure in" +
        " milliseconds from epoch"), beanClass.getLastVolumeFailureDate())
      .addGauge(Interns.info("EstimatedCapacityLostTotal", "Total capacity lost"
        + " due to volume failure"), beanClass.getEstimatedCapacityLostTotal())
      .addGauge(Interns.info("CacheUsed", "Datanode cache used in bytes"),
        beanClass.getCacheUsed())
      .addGauge(Interns.info("CacheCapacity", "Datanode cache capacity"),
        beanClass.getCacheCapacity())
      .addGauge(Interns.info("NumBlocksCached", "Datanode number" +
        " of blocks cached"), beanClass.getNumBlocksCached())
      .addGauge(Interns.info("NumBlocksFailedToCache", "Datanode number of " +
        "blocks failed to cache"), beanClass.getNumBlocksFailedToCache())
      .addGauge(Interns.info("NumBlocksFailedToUnCache", "Datanode number of" +
          " blocks failed in cache eviction"),
        beanClass.getNumBlocksFailedToUncache());

  }

}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/metrics/FSDatasetMBean.java
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSource;

@InterfaceAudience.Private
public interface FSDatasetMBean extends MetricsSource {
  

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/SimulatedFSDataset.java
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetricHelper;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.DataChecksum;

    return 0l;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    try {
      DataNodeMetricHelper.getMetrics(collector, this, "SimulatedFSDataset");
    } catch (Exception e){
    }
  }

  @Override // FsDatasetSpi
  public synchronized long getLength(ExtendedBlock b) throws IOException {
    final Map<Block, BInfo> map = getMap(b.getBlockPoolId());

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDataNodeFSDataSetSink.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDataNodeFSDataSetSink.java
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.junit.Test;

import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class TestDataNodeFSDataSetSink {
  private static final MetricsSystemImpl ms = new
    MetricsSystemImpl("TestFSDataSet");

  class FSDataSetSinkTest implements MetricsSink {
    private Set<String> nameMap;
    private int count;

    @Override
    public void putMetrics(MetricsRecord record) {
      if (count == 0) {
        for (AbstractMetric m : record.metrics()) {
          if (nameMap.contains(m.name())) {
            count++;
          }
        }

        for (MetricsTag t : record.tags()) {
          if (nameMap.contains(t.name())) {
            count++;
          }
        }
      }
    }

    @Override
    public void flush() {

    }

    @Override
    public void init(SubsetConfiguration conf) {
      nameMap = new TreeSet<>();
      nameMap.add("DfsUsed");
      nameMap.add("Capacity");
      nameMap.add("Remaining");
      nameMap.add("StorageInfo");
      nameMap.add("NumFailedVolumes");
      nameMap.add("LastVolumeFailureDate");
      nameMap.add("EstimatedCapacityLostTotal");
      nameMap.add("CacheUsed");
      nameMap.add("CacheCapacity");
      nameMap.add("NumBlocksCached");
      nameMap.add("NumBlocksFailedToCache");
      nameMap.add("NumBlocksFailedToUnCache");
      nameMap.add("Context");
      nameMap.add("Hostname");
    }

    public int getMapCount() {
      return nameMap.size();
    }

    public int getFoundKeyCount() {
      return count;
    }
  }

  @Test
  public void testFSDataSetMetrics() throws InterruptedException {
    Configuration conf = new HdfsConfiguration();
    String bpid = "FSDatSetSink-Test";
    SimulatedFSDataset fsdataset = new SimulatedFSDataset(null, conf);
    fsdataset.addBlockPool(bpid, conf);
    FSDataSetSinkTest sink = new FSDataSetSinkTest();
    sink.init(null);
    ms.init("Test");
    ms.start();
    ms.register("FSDataSetSource", "FSDataSetSource", fsdataset);
    ms.register("FSDataSetSink", "FSDataSetSink", sink);
    ms.startMetricsMBeans();
    ms.publishMetricsNow();

    Thread.sleep(4000);

    ms.stopMetricsMBeans();
    ms.shutdown();

    assertEquals(sink.getMapCount(), sink.getFoundKeyCount());

  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/extdataset/ExternalDatasetImpl.java
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetricHelper;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.metrics2.MetricsCollector;

public class ExternalDatasetImpl implements FsDatasetSpi<ExternalVolumeImpl> {

    return 0;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    try {
      DataNodeMetricHelper.getMetrics(collector, this, "ExternalDataset");
    } catch (Exception e){
    }
  }

  @Override
  public void setPinning(ExtendedBlock block) throws IOException {    
  }

