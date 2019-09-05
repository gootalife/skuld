hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/SimulatedFSDataset.java
public class SimulatedFSDataset implements FsDatasetSpi<FsVolumeSpi> {
  public final static int BYTE_MASK = 0xff;
  static class Factory extends FsDatasetSpi.Factory<SimulatedFSDataset> {
    @Override
    public SimulatedFSDataset newInstance(DataNode datanode,
  }

  public static byte simulatedByte(Block b, long offsetInBlk) {
    byte firstByte = (byte) (b.getBlockId() & BYTE_MASK);
    return (byte) ((firstByte + offsetInBlk) & BYTE_MASK);
  }
  
  public static final String CONFIG_PROPERTY_CAPACITY =

    @Override
    public int read() throws IOException {
      if (currentPos >= length) {
        return -1;
      }
      if (data !=null) {
        return data[currentPos++];
      } else {
        return simulatedByte(theBlock, currentPos++) & BYTE_MASK;
      }
    }
    

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestSimulatedFSDataset.java
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.SequentialBlockIdGenerator;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetFactory;
  static final String bpid = "BP-TEST";
  static final int NUMBLOCKS = 20;
  static final int BLOCK_LENGTH_MULTIPLIER = 79;
  static final long FIRST_BLK_ID = 1;

  @Before
  public void setUp() throws Exception {
    SimulatedFSDataset.setFactory(conf);
  }
  
  static long blockIdToLen(long blkid) {
    return blkid * BLOCK_LENGTH_MULTIPLIER;
  }

  static int addSomeBlocks(SimulatedFSDataset fsdataset) throws IOException {
    return addSomeBlocks(fsdataset, false);
  }

  static int addSomeBlocks(SimulatedFSDataset fsdataset,
      boolean negativeBlkID) throws IOException {
    return addSomeBlocks(fsdataset, FIRST_BLK_ID, negativeBlkID);
  }

  static int addSomeBlocks(SimulatedFSDataset fsdataset, long startingBlockId,
      boolean negativeBlkID) throws IOException {
    int bytesAdded = 0;
    for (long i = startingBlockId; i < startingBlockId+NUMBLOCKS; ++i) {
      long blkID = negativeBlkID ? i * -1 : i;
      ExtendedBlock b = new ExtendedBlock(bpid, blkID, 0, 0);
      ReplicaInPipelineInterface bInfo = fsdataset.createRbw(
    }
    return bytesAdded;
  }

  static void readSomeBlocks(SimulatedFSDataset fsdataset,
      boolean negativeBlkID) throws IOException {
    for (long i = FIRST_BLK_ID; i <= NUMBLOCKS; ++i) {
      long blkID = negativeBlkID ? i * -1 : i;
      ExtendedBlock b = new ExtendedBlock(bpid, blkID, 0, 0);
      assertTrue(fsdataset.isValidBlock(b));
      assertEquals(blockIdToLen(i), fsdataset.getLength(b));
      checkBlockDataAndSize(fsdataset, b, blockIdToLen(i));
    }
  }
  
  @Test
  @Test
  public void testGetMetaData() throws IOException {
    final SimulatedFSDataset fsdataset = getSimulatedFSDataset();
    ExtendedBlock b = new ExtendedBlock(bpid, FIRST_BLK_ID, 5, 0);
    try {
      assertTrue(fsdataset.getMetaDataInputStream(b) == null);
      assertTrue("Expected an IO exception", false);
    }
    addSomeBlocks(fsdataset); // Only need to add one but ....
    b = new ExtendedBlock(bpid, FIRST_BLK_ID, 0, 0);
    InputStream metaInput = fsdataset.getMetaDataInputStream(b);
    DataInputStream metaDataInput = new DataInputStream(metaInput);
    short version = metaDataInput.readShort();



  static void checkBlockDataAndSize(SimulatedFSDataset fsdataset,
      ExtendedBlock b, long expectedLen) throws IOException {
    InputStream input = fsdataset.getBlockInputStream(b);
    long lengthRead = 0;
    int data;
    while ((data = input.read()) != -1) {
      assertEquals(SimulatedFSDataset.simulatedByte(b.getLocalBlock(),
          lengthRead), (byte) (data & SimulatedFSDataset.BYTE_MASK));
      lengthRead++;
    }
    assertEquals(expectedLen, lengthRead);

  @Test
  public void testWriteRead() throws IOException {
    testWriteRead(false);
    testWriteRead(true);
  }

  private void testWriteRead(boolean negativeBlkID) throws IOException {
    final SimulatedFSDataset fsdataset = getSimulatedFSDataset();
    addSomeBlocks(fsdataset, negativeBlkID);
    readSomeBlocks(fsdataset, negativeBlkID);
  }

  @Test
    SimulatedFSDataset sfsdataset = getSimulatedFSDataset();
    bytesAdded += addSomeBlocks(sfsdataset, NUMBLOCKS+1, false);
    sfsdataset.getBlockReport(bpid);
    assertEquals(NUMBLOCKS, blockReport.getNumberOfBlocks());
    sfsdataset.getBlockReport(bpid);
  @Test
  public void testInValidBlocks() throws IOException {
    final SimulatedFSDataset fsdataset = getSimulatedFSDataset();
    ExtendedBlock b = new ExtendedBlock(bpid, FIRST_BLK_ID, 5, 0);
    checkInvalidBlock(b);
    

