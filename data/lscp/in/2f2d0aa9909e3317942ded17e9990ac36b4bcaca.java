hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FSInputChecker.java
    if (count < 0) count = 0;
  }

  final protected synchronized int readAndDiscard(int len) throws IOException {
    int total = 0;
    while (total < len) {
      if (pos >= count) {
        count = readChecksumChunk(buf, 0, maxChunkSize);
        if (count <= 0) {
          break;
        }
      }
      int rd = Math.min(count - pos, len - total);
      pos += rd;
      total += rd;
    }
    return total;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/RemoteBlockReader.java
  private boolean eos = false;
  private boolean sentStatusCode = false;
  
  ByteBuffer checksumBytes = null;
  int dataLeft = 0;
    if (lastChunkLen < 0 && startOffset > firstChunkOffset && len > 0) {
      int toSkip = (int)(startOffset - firstChunkOffset);
      if ( super.readAndDiscard(toSkip) != toSkip ) {
        throw new IOException("Could not skip required number of bytes");
      }
  public synchronized long skip(long n) throws IOException {
    long nSkipped = 0;
    while (nSkipped < n) {
      int toSkip = (int)Math.min(n-nSkipped, Integer.MAX_VALUE);
      int ret = readAndDiscard(toSkip);
      if (ret <= 0) {
        return nSkipped;
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/RemoteBlockReader2.java

  private boolean sentStatusCode = false;

  @VisibleForTesting
  public Peer getPeer() {
    return peer;


  @Override
  public synchronized int read(ByteBuffer buf) throws IOException {
    if (curDataSlice == null || curDataSlice.remaining() == 0 && bytesNeededToFinish > 0) {
      TraceScope scope = Trace.startSpan(
          "RemoteBlockReader2#readNextPacket(" + blockId + ")", Sampler.NEVER);
  public synchronized long skip(long n) throws IOException {
    long skipped = 0;
    while (skipped < n) {
      long needToSkip = n - skipped;
      if (curDataSlice == null || curDataSlice.remaining() == 0 && bytesNeededToFinish > 0) {
        readNextPacket();
      }
      if (curDataSlice.remaining() == 0) {
        break;
      }

      int skip = (int)Math.min(curDataSlice.remaining(), needToSkip);
      curDataSlice.position(curDataSlice.position() + skip);
      skipped += skip;
    }
    return skipped;
  }

  private void readTrailingEmptyPacket() throws IOException {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockReaderBase.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockReaderBase.java
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

abstract public class TestBlockReaderBase {
  private BlockReaderTestUtil util;
  private byte[] blockData;
  private BlockReader reader;

  byte [] getBlockData() {
    int length = 1 << 22;
    byte[] data = new byte[length];
    for (int i = 0; i < length; i++) {
      data[i] = (byte) (i % 133);
    }
    return data;
  }

  private BlockReader getBlockReader(LocatedBlock block) throws Exception {
    return util.getBlockReader(block, 0, blockData.length);
  }

  abstract HdfsConfiguration createConf();

  @Before
  public void setup() throws Exception {
    util = new BlockReaderTestUtil(1, createConf());
    blockData = getBlockData();
    DistributedFileSystem fs = util.getCluster().getFileSystem();
    Path testfile = new Path("/testfile");
    FSDataOutputStream fout = fs.create(testfile);
    fout.write(blockData);
    fout.close();
    LocatedBlock blk = util.getFileBlocks(testfile, blockData.length).get(0);
    reader = getBlockReader(blk);
  }

  @After
  public void shutdown() throws Exception {
    util.shutdown();
  }

  @Test(timeout=60000)
  public void testSkip() throws IOException {
    Random random = new Random();
    byte [] buf = new byte[1];
    for (int pos = 0; pos < blockData.length;) {
      long skip = random.nextInt(100) + 1;
      long skipped = reader.skip(skip);
      if (pos + skip >= blockData.length) {
        assertEquals(blockData.length, pos + skipped);
        break;
      } else {
        assertEquals(skip, skipped);
        pos += skipped;
        assertEquals(1, reader.read(buf, 0, 1));

        assertEquals(blockData[pos], buf[0]);
        pos += 1;
      }
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSInputStream.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSInputStream.java
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.junit.Assume;
import org.junit.Test;

public class TestDFSInputStream {
  private void testSkipInner(MiniDFSCluster cluster) throws IOException {
    DistributedFileSystem fs = cluster.getFileSystem();
    DFSClient client = fs.dfs;
    Path file = new Path("/testfile");
    int fileLength = 1 << 22;
    byte[] fileContent = new byte[fileLength];
    for (int i = 0; i < fileLength; i++) {
      fileContent[i] = (byte) (i % 133);
    }
    FSDataOutputStream fout = fs.create(file);
    fout.write(fileContent);
    fout.close();
    Random random = new Random();
    for (int i = 3; i < 18; i++) {
      DFSInputStream fin = client.open("/testfile");
      for (long pos = 0; pos < fileLength;) {
        long skip = random.nextInt(1 << i) + 1;
        long skipped = fin.skip(skip);
        if (pos + skip >= fileLength) {
          assertEquals(fileLength, pos + skipped);
          break;
        } else {
          assertEquals(skip, skipped);
          pos += skipped;
          int data = fin.read();
          assertEquals(pos % 133, data);
          pos += 1;
        }
      }
      fin.close();
    }
  }

  @Test(timeout=60000)
  public void testSkipWithRemoteBlockReader() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADER, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      testSkipInner(cluster);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=60000)
  public void testSkipWithRemoteBlockReader2() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      testSkipInner(cluster);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=60000)
  public void testSkipWithLocalBlockReader() throws IOException {
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    DomainSocket.disableBindPathValidation();
    Configuration conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        new File(sockDir.getDir(),
          "TestShortCircuitLocalRead._PORT.sock").getAbsolutePath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      DFSInputStream.tcpReadsDisabledForTesting = true;
      testSkipInner(cluster);
    } finally {
      DFSInputStream.tcpReadsDisabledForTesting = false;
      cluster.shutdown();
      sockDir.close();
    }
  }

}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestRemoteBlockReader.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestRemoteBlockReader.java
package org.apache.hadoop.hdfs;

public class TestRemoteBlockReader extends TestBlockReaderBase {

  HdfsConfiguration createConf() {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADER, true);
    return conf;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestRemoteBlockReader2.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestRemoteBlockReader2.java
package org.apache.hadoop.hdfs;

public class TestRemoteBlockReader2 extends TestBlockReaderBase {
  HdfsConfiguration createConf() {
    HdfsConfiguration conf = new HdfsConfiguration();
    return conf;
  }
}

