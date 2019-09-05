hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockReceiver.java
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
          if (datanode.isRestarting() && isClient && !isTransfer) {
            try (Writer out = new OutputStreamWriter(
                replicaInfo.createRestartMetaStream(), "UTF-8")) {
              out.write(Long.toString(Time.now() + restartBudget));
              out.flush();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/ReplicaInPipeline.java
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import org.apache.hadoop.hdfs.protocol.Block;
    }
  }

  @Override
  public OutputStream createRestartMetaStream() throws IOException {
    File blockFile = getBlockFile();
    File restartMeta = new File(blockFile.getParent()  +
        File.pathSeparator + "." + blockFile.getName() + ".restart");
    if (restartMeta.exists() && !restartMeta.delete()) {
      DataNode.LOG.warn("Failed to delete restart meta file: " +
          restartMeta.getPath());
    }
    return new FileOutputStream(restartMeta);
  }

  @Override
  public String toString() {
    return super.toString()

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/ReplicaInPipelineInterface.java
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.util.DataChecksum;
  public ReplicaOutputStreams createStreams(boolean isCreate,
      DataChecksum requestedChecksum) throws IOException;

  public OutputStream createRestartMetaStream() throws IOException;
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/SimulatedFSDataset.java
      }
    }

    @Override
    public OutputStream createRestartMetaStream() throws IOException {
      return new SimulatedOutputStream();
    }

    @Override
    synchronized public long getBlockId() {
      return theBlock.getBlockId();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/extdataset/ExternalReplicaInPipeline.java
package org.apache.hadoop.hdfs.server.datanode.extdataset;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.ChunkChecksum;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipelineInterface;
    return new ReplicaOutputStreams(null, null, requestedChecksum, false);
  }

  @Override
  public OutputStream createRestartMetaStream() throws IOException {
    return null;
  }

  @Override
  public long getBlockId() {
    return 0;

