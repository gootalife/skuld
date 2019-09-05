hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSOutputStream.java

  protected synchronized void closeImpl() throws IOException {
    if (isClosed()) {
      streamer.getLastException().check(true);
      return;
    }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DataStreamer.java
    packets.clear();
  }
  
  static class LastExceptionInStreamer {
    private IOException thrown;

    synchronized void set(Throwable t) {
      assert t != null;
      this.thrown = t instanceof IOException ?
          (IOException) t : new IOException(t);
    }

    synchronized void clear() {
    }

    synchronized void check(boolean resetToNull) throws IOException {
      if (thrown != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Got Exception while checking", new Throwable(thrown));
        }
        final IOException e = thrown;
        if (resetToNull) {
          thrown = null;
        }
        throw e;
      }
    }

    synchronized void throwException4Close() throws IOException {
      check(false);
      throw new ClosedChannelException();
    }
  }

  private long lastQueuedSeqno = -1;
  private long lastAckedSeqno = -1;
  private long bytesCurBlock = 0; // bytes written in current block
  private final LastExceptionInStreamer lastException = new LastExceptionInStreamer();
  private Socket s;

  private final DFSClient dfsClient;
  LastExceptionInStreamer getLastException(){
    return lastException;
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSOutputStream.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DataStreamer.LastExceptionInStreamer;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
    DataStreamer streamer = (DataStreamer) Whitebox
        .getInternalState(dos, "streamer");
    @SuppressWarnings("unchecked")
    LastExceptionInStreamer ex = (LastExceptionInStreamer) Whitebox
        .getInternalState(streamer, "lastException");
    Throwable thrown = (Throwable) Whitebox.getInternalState(ex, "thrown");
    Assert.assertNull(thrown);

    dos.close();

    } catch (IOException e) {
      Assert.assertEquals(e, dummy);
    }
    thrown = (Throwable) Whitebox.getInternalState(ex, "thrown");
    Assert.assertNull(thrown);
    dos.close();
  }


