hadoop-tools/hadoop-azure/src/main/java/org/apache/hadoop/fs/azure/AzureNativeFileSystemStore.java
    throws AzureException {
    if (blob instanceof CloudPageBlobWrapper) {
      try {
        return PageBlobInputStream.getPageBlobDataSize((CloudPageBlobWrapper) blob,
            getInstrumentedContext(
                isConcurrentOOBAppendAllowed()));
      } catch (Exception e) {

hadoop-tools/hadoop-azure/src/main/java/org/apache/hadoop/fs/azure/PageBlobInputStream.java
  public static long getPageBlobDataSize(CloudPageBlobWrapper blob,
      OperationContext opContext) throws IOException, StorageException {
    }
    if (pageBlobSize == -1) {
      try {
        pageBlobSize = getPageBlobDataSize(blob, opContext);
      } catch (StorageException e) {
        throw new IOException("Unable to get page blob size.", e);
      }

  private synchronized boolean ensureDataInBuffer() throws IOException {
  @Override
  public synchronized int read(byte[] outputBuffer, int offset, int len)
      throws IOException {
    if (len == 0) {
      return 0;
    }

    int numberOfBytesRead = 0;
    while (len > 0) {
      if (!ensureDataInBuffer()) {
        break;
      }
      int bytesRemainingInCurrentPage = getBytesRemainingInCurrentPage();
      int numBytesToRead = Math.min(len, bytesRemainingInCurrentPage);
        currentOffsetInBuffer += numBytesToRead;
      }
    }

    if (numberOfBytesRead == 0) {
      return -1;
    }

    filePosition += numberOfBytesRead;
    return numberOfBytesRead;
  }
  @Override
  public int read() throws IOException {
    byte[] oneByte = new byte[1];
    int result = read(oneByte);
    if (result < 0) {
      return result;
    }
    return oneByte[0];
  }

hadoop-tools/hadoop-azure/src/main/java/org/apache/hadoop/fs/azure/PageBlobOutputStream.java
  private WriteRequest lastQueuedTask;
  private boolean closed = false;

  public static final Log LOG = LogFactory.getLog(AzureNativeFileSystemStore.class);

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    LOG.debug("Closing page blob output stream.");
    flush();
    checkStreamState();
      Thread.currentThread().interrupt();
    }

    closed = true;
  }


hadoop-tools/hadoop-azure/src/test/java/org/apache/hadoop/fs/azure/NativeAzureFileSystemBaseTest.java
    }
  }

  @Test
  public void testInputStreamReadWithZeroSizeBuffer() throws Exception {
    Path newFile = new Path("zeroSizeRead");
    OutputStream output = fs.create(newFile);
    output.write(10);
    output.close();

    InputStream input = fs.open(newFile);
    int result = input.read(new byte[2], 0, 0);
    assertEquals(0, result);
  }

  @Test
  public void testInputStreamReadWithBufferReturnsMinusOneOnEof() throws Exception {
    Path newFile = new Path("eofRead");
    OutputStream output = fs.create(newFile);
    output.write(10);
    output.close();

    InputStream input = fs.open(newFile);
    byte[] buff = new byte[1];
    int result = input.read(buff, 0, 1);
    assertEquals(1, result);
    assertEquals(10, buff[0]);

    buff[0] = 2;
    result = input.read(buff, 0, 1);
    assertEquals(-1, result);
    assertEquals(2, buff[0]);
  }

  @Test
  public void testInputStreamReadWithBufferReturnsMinusOneOnEofForLargeBuffer() throws Exception {
    Path newFile = new Path("eofRead2");
    OutputStream output = fs.create(newFile);
    byte[] outputBuff = new byte[97331];
    for(int i = 0; i < outputBuff.length; ++i) {
      outputBuff[i] = (byte)(Math.random() * 255);
    }
    output.write(outputBuff);
    output.close();

    InputStream input = fs.open(newFile);
    byte[] buff = new byte[131072];
    int result = input.read(buff, 0, buff.length);
    assertEquals(outputBuff.length, result);
    for(int i = 0; i < outputBuff.length; ++i) {
      assertEquals(outputBuff[i], buff[i]);
    }

    buff = new byte[131072];
    result = input.read(buff, 0, buff.length);
    assertEquals(-1, result);
  }

  @Test
  public void testInputStreamReadIntReturnsMinusOneOnEof() throws Exception {
    Path newFile = new Path("eofRead3");
    OutputStream output = fs.create(newFile);
    output.write(10);
    output.close();

    InputStream input = fs.open(newFile);
    int value = input.read();
    assertEquals(10, value);

    value = input.read();
    assertEquals(-1, value);
  }

  @Test
  public void testSetPermissionOnFile() throws Exception {
    Path newFile = new Path("testPermission");

hadoop-tools/hadoop-azure/src/test/java/org/apache/hadoop/fs/azure/TestNativeAzureFileSystemContractPageBlobLive.java
++ b/hadoop-tools/hadoop-azure/src/test/java/org/apache/hadoop/fs/azure/TestNativeAzureFileSystemContractPageBlobLive.java

package org.apache.hadoop.fs.azure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.junit.Ignore;

public class TestNativeAzureFileSystemContractPageBlobLive extends
    FileSystemContractBaseTest {
  private AzureBlobStorageTestAccount testAccount;

  private AzureBlobStorageTestAccount createTestAccount()
      throws Exception {
    Configuration conf = new Configuration();

    conf.set(AzureNativeFileSystemStore.KEY_PAGE_BLOB_DIRECTORIES, "/");

    conf.set(AzureNativeFileSystemStore.KEY_ATOMIC_RENAME_DIRECTORIES, "/");
    return AzureBlobStorageTestAccount.create(conf);
  }

  @Override
  protected void setUp() throws Exception {
    testAccount = createTestAccount();
    if (testAccount != null) {
      fs = testAccount.getFileSystem();
    }
  }

  @Override
  protected void tearDown() throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
      fs = null;
    }
  }

  @Override
  protected void runTest() throws Throwable {
    if (testAccount != null) {
      super.runTest();
    }
  }
  
  @Ignore
  public void testMoveFileUnderParent() throws Throwable {
  }

  @Ignore
  public void testRenameFileToSelf() throws Throwable {
  }
  
  @Ignore
  public void testRenameChildDirForbidden() throws Exception {
  }
  
  @Ignore
  public void testMoveDirUnderParent() throws Throwable {
  }
  
  @Ignore
  public void testRenameDirToSelf() throws Throwable {
  }
}

