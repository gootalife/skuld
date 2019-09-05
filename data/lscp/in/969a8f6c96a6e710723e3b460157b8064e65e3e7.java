hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFileTruncate.java
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

  static MiniDFSCluster cluster;
  static DistributedFileSystem fs;

 private Path parent;

  @BeforeClass
  public static void startUp() throws IOException {
    conf = new HdfsConfiguration();
    if(cluster != null) cluster.shutdown();
  }

  @Before
  public void setup() throws IOException {
    parent = new Path("/test");
    fs.delete(parent, true);
  }

  public void testBasicTruncate() throws IOException {
    int startingFileSize = 3 * BLOCK_SIZE;

    fs.mkdirs(parent);
    fs.setQuota(parent, 100, 1000);
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
  void testSnapshotWithAppendTruncate(int ... deleteOrder) throws IOException {
    FSDirectory fsDir = cluster.getNamesystem().getFSDirectory();
    fs.mkdirs(parent);
    fs.setQuota(parent, 100, 1000);
    fs.allowSnapshot(parent);
  }

  void testSnapshotWithTruncates(int ... deleteOrder) throws IOException {
    fs.mkdirs(parent);
    fs.setQuota(parent, 100, 1000);
    fs.allowSnapshot(parent);
  public void testTruncateWithDataNodesRestart() throws Exception {
    int startingFileSize = 3 * BLOCK_SIZE;
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    final Path p = new Path(parent, "testTruncateWithDataNodesRestart");

    writeContents(contents, startingFileSize, p);
  public void testCopyOnTruncateWithDataNodesRestart() throws Exception {
    int startingFileSize = 3 * BLOCK_SIZE;
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    final Path p = new Path(parent, "testCopyOnTruncateWithDataNodesRestart");

    writeContents(contents, startingFileSize, p);
  public void testTruncateWithDataNodesRestartImmediately() throws Exception {
    int startingFileSize = 3 * BLOCK_SIZE;
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    final Path p = new Path(parent, "testTruncateWithDataNodesRestartImmediately");

    writeContents(contents, startingFileSize, p);
  public void testTruncateWithDataNodesShutdownImmediately() throws Exception {
    int startingFileSize = 3 * BLOCK_SIZE;
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    final Path p = new Path(parent, "testTruncateWithDataNodesShutdownImmediately");

    writeContents(contents, startingFileSize, p);
  @Test
  public void testUpgradeAndRestart() throws IOException {
    fs.mkdirs(parent);
    fs.setQuota(parent, 100, 1000);
    fs.allowSnapshot(parent);
    FSNamesystem fsn = cluster.getNamesystem();
    String client = "client";
    String clientMachine = "clientMachine";
    String src = "/test/testTruncateRecovery";
    Path srcPath = new Path(src);


  @Test
  public void testTruncateShellCommand() throws Exception {
    final Path src = new Path("/test/testTruncateShellCommand");
    final int oldLength = 2*BLOCK_SIZE + 1;
    final int newLength = BLOCK_SIZE + 1;

  @Test
  public void testTruncateShellCommandOnBlockBoundary() throws Exception {
    final Path src = new Path("/test/testTruncateShellCommandOnBoundary");
    final int oldLength = 2 * BLOCK_SIZE;
    final int newLength = BLOCK_SIZE;

  @Test
  public void testTruncateShellCommandWithWaitOption() throws Exception {
    final Path src = new Path("/test/testTruncateShellCommandWithWaitOption");
    final int oldLength = 2 * BLOCK_SIZE + 1;
    final int newLength = BLOCK_SIZE + 1;
  public void testTruncate4Symlink() throws IOException {
    final int fileLength = 3 * BLOCK_SIZE;

    fs.mkdirs(parent);
    final byte[] contents = AppendTestUtil.initBuffer(fileLength);
    final Path file = new Path(parent, "testTruncate4Symlink");

