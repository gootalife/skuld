hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSOutputStream.java
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
@InterfaceAudience.Private
public class DFSOutputStream extends FSOutputSummer
    implements Syncable, CanSetDropBehind {
  static final Logger LOG = LoggerFactory.getLogger(DFSOutputStream.class);
    if (currentPacket.getNumChunks() == currentPacket.getMaxChunks() ||
        getStreamer().getBytesCurBlock() == blockSize) {
      enqueueCurrentPacketFull();
    }
  }

  void enqueueCurrentPacket() throws IOException {
    getStreamer().waitAndQueuePacket(currentPacket);
    currentPacket = null;
  }

  void enqueueCurrentPacketFull() throws IOException {
    LOG.debug("enqueue full {}, src={}, bytesCurBlock={}, blockSize={},"
        + " appendChunk={}, {}", currentPacket, src, getStreamer()
        .getBytesCurBlock(), blockSize, getStreamer().getAppendChunk(),
        getStreamer());
    enqueueCurrentPacket();
    adjustChunkBoundary();
    endBlock();
  }

  void setCurrentPacketToEmpty() throws InterruptedIOException {
    currentPacket = createPacket(0, 0, getStreamer().getBytesCurBlock(),
        getStreamer().getAndIncCurrentSeqno(), true);
    currentPacket.setSyncBlock(shouldSyncBlock);
  }

  protected void endBlock() throws IOException {
    if (getStreamer().getBytesCurBlock() == blockSize) {
      setCurrentPacketToEmpty();
      enqueueCurrentPacket();
      getStreamer().setBytesCurBlock(0);
      lastFlushOffset = 0;
    }
        }
        if (currentPacket != null) {
          currentPacket.setSyncBlock(isSync);
          enqueueCurrentPacket();
        }
        if (endBlock && getStreamer().getBytesCurBlock() > 0) {
          currentPacket = createPacket(0, 0, getStreamer().getBytesCurBlock(),
              getStreamer().getAndIncCurrentSeqno(), true);
          currentPacket.setSyncBlock(shouldSyncBlock || isSync);
          enqueueCurrentPacket();
          getStreamer().setBytesCurBlock(0);
          lastFlushOffset = 0;
        } else {
      flushBuffer();       // flush from all upper layers

      if (currentPacket != null) {
        enqueueCurrentPacket();
      }

      if (getStreamer().getBytesCurBlock() != 0) {
        setCurrentPacketToEmpty();
      }

      flushInternal();             // flush all data to Datanodes

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DataStreamer.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.BlockWrite;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
      s.close();
    }
  }

  @Override
  public String toString() {
    return  (block == null? null: block.getLocalBlock())
        + "@" + Arrays.toString(getNodes());
  }
}

