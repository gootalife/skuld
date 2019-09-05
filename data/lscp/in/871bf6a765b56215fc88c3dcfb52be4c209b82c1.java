hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/DFSTestUtil.java
import org.apache.hadoop.hdfs.MiniDFSCluster.NameNodeInfo;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.datanode.TestTransferRbw;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
    dn.setLastUpdate(Time.now() + offset);
    dn.setLastUpdateMonotonic(Time.monotonicNow() + offset);
  }

  public static void fillExpectedBuf(LocatedBlocks lbs, byte[] expected) {
    Block[] blks = new Block[lbs.getLocatedBlocks().size()];
    for (int i = 0; i < lbs.getLocatedBlocks().size(); i++) {
      blks[i] = lbs.getLocatedBlocks().get(i).getBlock().getLocalBlock();
    }
    int bufPos = 0;
    for (Block b : blks) {
      for (long blkPos = 0; blkPos < b.getNumBytes(); blkPos++) {
        assert bufPos < expected.length;
        expected[bufPos++] = SimulatedFSDataset.simulatedByte(b, blkPos);
      }
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestFileAppend.java
  private void checkFile(DistributedFileSystem fileSys, Path name, int repl)
    throws IOException {
    boolean done = false;

    byte[] expected = 
        new byte[AppendTestUtil.NUM_BLOCKS * AppendTestUtil.BLOCK_SIZE];
    if (simulatedStorage) {
      LocatedBlocks lbs = fileSys.getClient().getLocatedBlocks(name.toString(),
          0, AppendTestUtil.FILE_SIZE);
      DFSTestUtil.fillExpectedBuf(lbs, expected);
    } else {
      System.arraycopy(fileContents, 0, expected, 0, expected.length);
    }
    }
    fileContents = AppendTestUtil.initBuffer(AppendTestUtil.FILE_SIZE);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    try {

    }
    fileContents = AppendTestUtil.initBuffer(AppendTestUtil.FILE_SIZE);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    try {


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestPread.java
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.io.IOUtils;
public class TestPread {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 4096;
  static final int numBlocksPerFile = 12;
  static final int fileSize = numBlocksPerFile * blockSize;
  boolean simulatedStorage;
  boolean isHedgedRead;

    stm.close();
    FSDataInputStream in = fileSys.open(name);
    byte[] buffer = new byte[fileSize];
    in.readFully(0, buffer, 0, 0);
    IOException res = null;
    try { // read beyond the end of the file
      assertTrue("Cannot delete file", false);
    
    DFSTestUtil.createFile(fileSys, name, fileSize, fileSize,
        blockSize, (short) replication, seed);
  }
  
  
  private void pReadFile(FileSystem fileSys, Path name) throws IOException {
    FSDataInputStream stm = fileSys.open(name);
    byte[] expected = new byte[fileSize];
    if (simulatedStorage) {
      assert fileSys instanceof DistributedFileSystem;
      DistributedFileSystem dfs = (DistributedFileSystem) fileSys;
      LocatedBlocks lbs = dfs.getClient().getLocatedBlocks(name.toString(),
          0, fileSize);
      DFSTestUtil.fillExpectedBuf(lbs, expected);
    } else {
      Random rand = new Random(seed);
      rand.nextBytes(expected);
    FileSystem fileSys = cluster.getFileSystem();
    fileSys.setVerifyChecksum(verifyChecksum);
    try {
      Path file1 = new Path("/preadtest.dat");
      writeFile(fileSys, file1);
      pReadFile(fileSys, file1);
      datanodeRestartTest(cluster, fileSys, file1);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestSmallBlock.java
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.junit.Test;

    }
  }
  
  private void checkFile(DistributedFileSystem fileSys, Path name)
      throws IOException {
    BlockLocation[] locations = fileSys.getFileBlockLocations(
        fileSys.getFileStatus(name), 0, fileSize);
    assertEquals("Number of blocks", fileSize, locations.length);
    FSDataInputStream stm = fileSys.open(name);
    byte[] expected = new byte[fileSize];
    if (simulatedStorage) {
      LocatedBlocks lbs = fileSys.getClient().getLocatedBlocks(name.toString(),
          0, fileSize);
      DFSTestUtil.fillExpectedBuf(lbs, expected);
    } else {
      Random rand = new Random(seed);
      rand.nextBytes(expected);
    }
    conf.set(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, "1");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    DistributedFileSystem fileSys = cluster.getFileSystem();
    try {
      Path file1 = new Path("/smallblocktest.dat");
      writeFile(fileSys, file1);
      checkFile(fileSys, file1);
      cleanupFile(fileSys, file1);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/SimulatedFSDataset.java
        Factory.class.getName());
  }

  public static byte simulatedByte(Block b, long offsetInBlk) {
    byte firstByte = (byte) (b.getBlockId() % Byte.MAX_VALUE);
    return (byte) ((firstByte + offsetInBlk) % Byte.MAX_VALUE);
  }
  
  public static final String CONFIG_PROPERTY_CAPACITY =
      "dfs.datanode.simulateddatastorage.capacity";
  
  public static final long DEFAULT_CAPACITY = 2L<<40; // 1 terabyte
  
  public static final String CONFIG_PROPERTY_STATE =
      "dfs.datanode.simulateddatastorage.state";
    synchronized SimulatedInputStream getIStream() {
      if (!finalized) {
         return new SimulatedInputStream(oStream.getLength(), theBlock);
      } else {
        return new SimulatedInputStream(theBlock.getNumBytes(), theBlock);
      }
    }
    
  static private class SimulatedInputStream extends java.io.InputStream {
    final long length; // bytes
    int currentPos = 0;
    byte[] data = null;
    Block theBlock = null;
    
    SimulatedInputStream(long l, Block b) {
      length = l;
      theBlock = b;
    }
    
      if (data !=null) {
        return data[currentPos++];
      } else {
        return simulatedByte(theBlock, currentPos++);
      }
    }
    
      if (data != null) {
        System.arraycopy(data, currentPos, b, 0, bytesRead);
      } else { // all data is zero
        for (int i = 0; i < bytesRead; i++) {
          b[i] = simulatedByte(theBlock, currentPos + i);
        }
      }
      currentPos += bytesRead;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestSimulatedFSDataset.java
    long lengthRead = 0;
    int data;
    while ((data = input.read()) != -1) {
      assertEquals(SimulatedFSDataset.simulatedByte(b.getLocalBlock(),
          lengthRead), data);
      lengthRead++;
    }
    assertEquals(expectedLen, lengthRead);

