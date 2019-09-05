hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/ReadaheadPool.java
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.nativeio.NativeIO;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_WILLNEED;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

      try {
        NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(identifier,
            fd, off, len, POSIX_FADV_WILLNEED);
      } catch (IOException ioe) {
        if (canceled) {

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/nativeio/NativeIO.java
@InterfaceStability.Unstable
public class NativeIO {
  public static class POSIX {
    public static int O_RDONLY = -1;
    public static int O_WRONLY = -1;
    public static int O_RDWR = -1;
    public static int O_CREAT = -1;
    public static int O_EXCL = -1;
    public static int O_NOCTTY = -1;
    public static int O_TRUNC = -1;
    public static int O_APPEND = -1;
    public static int O_NONBLOCK = -1;
    public static int O_SYNC = -1;

    public static int POSIX_FADV_NORMAL = -1;
    public static int POSIX_FADV_RANDOM = -1;
    public static int POSIX_FADV_SEQUENTIAL = -1;
    public static int POSIX_FADV_WILLNEED = -1;
    public static int POSIX_FADV_DONTNEED = -1;
    public static int POSIX_FADV_NOREUSE = -1;


       in the range before performing the
       write.  */
    public static int SYNC_FILE_RANGE_WAIT_BEFORE = 1;
       dirty pages in the range which are
       not presently under writeback.  */
    public static int SYNC_FILE_RANGE_WRITE = 2;
       the range after performing the
       write.  */
    public static int SYNC_FILE_RANGE_WAIT_AFTER = 4;

    private static final Log LOG = LogFactory.getLog(NativeIO.class);

    public static boolean fadvisePossible = false;

    private static boolean nativeLoaded = false;
    private static boolean syncFileRangePossible = true;

    static final String WORKAROUND_NON_THREADSAFE_CALLS_KEY =
      if (nativeLoaded && fadvisePossible) {
        try {
          posix_fadvise(fd, offset, len, flags);
        } catch (UnsatisfiedLinkError ule) {
          fadvisePossible = false;
        }
      private String owner, group;
      private int mode;

      public static int S_IFMT = -1;    /* type of file */
      public static int S_IFIFO  = -1;  /* named pipe (fifo) */
      public static int S_IFCHR  = -1;  /* character special */
      public static int S_IFDIR  = -1;  /* directory */
      public static int S_IFBLK  = -1;  /* block special */
      public static int S_IFREG  = -1;  /* regular */
      public static int S_IFLNK  = -1;  /* symbolic link */
      public static int S_IFSOCK = -1;  /* socket */
      public static int S_ISUID = -1;  /* set user id on execution */
      public static int S_ISGID = -1;  /* set group id on execution */
      public static int S_ISVTX = -1;  /* save swapped text even after use */
      public static int S_IRUSR = -1;  /* read permission, owner */
      public static int S_IWUSR = -1;  /* write permission, owner */
      public static int S_IXUSR = -1;  /* execute/search permission, owner */

      Stat(int ownerId, int groupId, int mode) {
        this.ownerId = ownerId;

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/io/nativeio/TestNativeIO.java
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.*;
import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat.*;

public class TestNativeIO {
  static final Log LOG = LogFactory.getLog(TestNativeIO.class);

    assertEquals(expectedOwner, owner);
    assertNotNull(stat.getGroup());
    assertTrue(!stat.getGroup().isEmpty());
    assertEquals("Stat mode field should indicate a regular file", S_IFREG,
      stat.getMode() & S_IFMT);
  }

              assertNotNull(stat.getGroup());
              assertTrue(!stat.getGroup().isEmpty());
              assertEquals("Stat mode field should indicate a regular file",
                S_IFREG, stat.getMode() & S_IFMT);
            } catch (Throwable t) {
              thrown.set(t);
            }
    LOG.info("Open a missing file without O_CREAT and it should fail");
    try {
      FileDescriptor fd = NativeIO.POSIX.open(
        new File(TEST_DIR, "doesntexist").getAbsolutePath(), O_WRONLY, 0700);
      fail("Able to open a new file without O_CREAT");
    } catch (NativeIOException nioe) {
      LOG.info("Got expected exception", nioe);
    LOG.info("Test creating a file with O_CREAT");
    FileDescriptor fd = NativeIO.POSIX.open(
      new File(TEST_DIR, "testWorkingOpen").getAbsolutePath(),
      O_WRONLY | O_CREAT, 0700);
    assertNotNull(true);
    assertTrue(fd.valid());
    FileOutputStream fos = new FileOutputStream(fd);
    try {
      fd = NativeIO.POSIX.open(
        new File(TEST_DIR, "testWorkingOpen").getAbsolutePath(),
        O_WRONLY | O_CREAT | O_EXCL, 0700);
      fail("Was able to create existing file with O_EXCL");
    } catch (NativeIOException nioe) {
      LOG.info("Got expected exception for failed exclusive create", nioe);
    for (int i = 0; i < 10000; i++) {
      FileDescriptor fd = NativeIO.POSIX.open(
        new File(TEST_DIR, "testNoFdLeak").getAbsolutePath(),
        O_WRONLY | O_CREAT, 0700);
      assertNotNull(true);
      assertTrue(fd.valid());
      FileOutputStream fos = new FileOutputStream(fd);
    FileInputStream fis = new FileInputStream("/dev/zero");
    try {
      NativeIO.POSIX.posix_fadvise(
          fis.getFD(), 0, 0, POSIX_FADV_SEQUENTIAL);
    } catch (UnsupportedOperationException uoe) {
    }

    try {
      NativeIO.POSIX.posix_fadvise(fis.getFD(), 0, 1024, POSIX_FADV_SEQUENTIAL);
      fail("Did not throw on bad file");
    } catch (NativeIOException nioe) {
      assertEquals(Errno.EBADF, nioe.getErrno());
    }
    
    try {
      NativeIO.POSIX.posix_fadvise(null, 0, 1024, POSIX_FADV_SEQUENTIAL);
      fail("Did not throw on null file");
    } catch (NullPointerException npe) {
      new File(TEST_DIR, "testSyncFileRange"));
    try {
      fos.write("foo".getBytes());
      NativeIO.POSIX.sync_file_range(fos.getFD(), 0, 1024,
        SYNC_FILE_RANGE_WRITE);
    } catch (UnsupportedOperationException uoe) {
      fos.close();
    }
    try {
      NativeIO.POSIX.sync_file_range(fos.getFD(), 0, 1024,
	   SYNC_FILE_RANGE_WRITE);
      fail("Did not throw on bad file");
    } catch (NativeIOException nioe) {
      assertEquals(Errno.EBADF, nioe.getErrno());
      FileUtils.deleteQuietly(TEST_DIR);
    }
  }

  @Test (timeout=10000)
  public void testNativePosixConsts() {
    assumeTrue("Native POSIX constants not required for Windows",
      !Path.WINDOWS);
    assertTrue("Native 0_RDONLY const not set", O_RDONLY >= 0);
    assertTrue("Native 0_WRONLY const not set", O_WRONLY >= 0);
    assertTrue("Native 0_RDWR const not set", O_RDWR >= 0);
    assertTrue("Native 0_CREAT const not set", O_CREAT >= 0);
    assertTrue("Native 0_EXCL const not set", O_EXCL >= 0);
    assertTrue("Native 0_NOCTTY const not set", O_NOCTTY >= 0);
    assertTrue("Native 0_TRUNC const not set", O_TRUNC >= 0);
    assertTrue("Native 0_APPEND const not set", O_APPEND >= 0);
    assertTrue("Native 0_NONBLOCK const not set", O_NONBLOCK >= 0);
    assertTrue("Native 0_SYNC const not set", O_SYNC >= 0);
    assertTrue("Native S_IFMT const not set", S_IFMT >= 0);
    assertTrue("Native S_IFIFO const not set", S_IFIFO >= 0);
    assertTrue("Native S_IFCHR const not set", S_IFCHR >= 0);
    assertTrue("Native S_IFDIR const not set", S_IFDIR >= 0);
    assertTrue("Native S_IFBLK const not set", S_IFBLK >= 0);
    assertTrue("Native S_IFREG const not set", S_IFREG >= 0);
    assertTrue("Native S_IFLNK const not set", S_IFLNK >= 0);
    assertTrue("Native S_IFSOCK const not set", S_IFSOCK >= 0);
    assertTrue("Native S_ISUID const not set", S_ISUID >= 0);
    assertTrue("Native S_ISGID const not set", S_ISGID >= 0);
    assertTrue("Native S_ISVTX const not set", S_ISVTX >= 0);
    assertTrue("Native S_IRUSR const not set", S_IRUSR >= 0);
    assertTrue("Native S_IWUSR const not set", S_IWUSR >= 0);
    assertTrue("Native S_IXUSR const not set", S_IXUSR >= 0);
  }

  @Test (timeout=10000)
  public void testNativeFadviseConsts() {
    assumeTrue("Fadvise constants not supported", fadvisePossible);
    assertTrue("Native POSIX_FADV_NORMAL const not set",
      POSIX_FADV_NORMAL >= 0);
    assertTrue("Native POSIX_FADV_RANDOM const not set",
      POSIX_FADV_RANDOM >= 0);
    assertTrue("Native POSIX_FADV_SEQUENTIAL const not set",
      POSIX_FADV_SEQUENTIAL >= 0);
    assertTrue("Native POSIX_FADV_WILLNEED const not set",
      POSIX_FADV_WILLNEED >= 0);
    assertTrue("Native POSIX_FADV_DONTNEED const not set",
      POSIX_FADV_DONTNEED >= 0);
    assertTrue("Native POSIX_FADV_NOREUSE const not set",
      POSIX_FADV_NOREUSE >= 0);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockReceiver.java
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;
import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.SYNC_FILE_RANGE_WRITE;

import com.google.common.annotations.VisibleForTesting;

            this.datanode.getFSDataset().submitBackgroundSyncFileRangeRequest(
                block, outFd, lastCacheManagementOffset,
                offsetInBlock - lastCacheManagementOffset,
                SYNC_FILE_RANGE_WRITE);
          } else {
            NativeIO.POSIX.syncFileRangeIfPossible(outFd,
                lastCacheManagementOffset,
                offsetInBlock - lastCacheManagementOffset,
                SYNC_FILE_RANGE_WRITE);
          }
        }
        long dropPos = lastCacheManagementOffset - CACHE_DROP_LAG_BYTES;
        if (dropPos > 0 && dropCacheBehindWrites) {
          NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
              block.getBlockName(), outFd, 0, dropPos, POSIX_FADV_DONTNEED);
        }
        lastCacheManagementOffset = offsetInBlock;
        long duration = Time.monotonicNow() - begin;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockSender.java
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;
import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_SEQUENTIAL;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

      try {
        NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
            block.getBlockName(), blockInFd, lastCacheDropOffset,
            offset - lastCacheDropOffset, POSIX_FADV_DONTNEED);
      } catch (Exception e) {
        LOG.warn("Unable to drop cache on file close", e);
      }
    if (isLongRead() && blockInFd != null) {
      NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
          block.getBlockName(), blockInFd, 0, 0, POSIX_FADV_SEQUENTIAL);
    }
    
        long dropLength = offset - lastCacheDropOffset;
        NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
            block.getBlockName(), blockInFd, lastCacheDropOffset,
            dropLength, POSIX_FADV_DONTNEED);
        lastCacheDropOffset = offset;
      }
    }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestCachingStrategy.java
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator;
import org.apache.hadoop.io.nativeio.NativeIOException;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
    synchronized void fadvise(int offset, int len, int flags) {
      LOG.debug("got fadvise(offset=" + offset + ", len=" + len +
          ",flags=" + flags + ")");
      if (flags == POSIX_FADV_DONTNEED) {
        for (int i = 0; i < len; i++) {
          dropped[(offset + i)] = true;
        }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-shuffle/src/main/java/org/apache/hadoop/mapred/FadvisedChunkedFile.java
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.io.nativeio.NativeIO;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;

import org.jboss.netty.handler.stream.ChunkedFile;

public class FadvisedChunkedFile extends ChunkedFile {
        NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(identifier,
            fd,
            getStartOffset(), getEndOffset() - getStartOffset(),
            POSIX_FADV_DONTNEED);
      } catch (Throwable t) {
        LOG.warn("Failed to manage OS cache for " + identifier, t);
      }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-shuffle/src/main/java/org/apache/hadoop/mapred/FadvisedFileRegion.java
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.io.nativeio.NativeIO;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;

import org.jboss.netty.channel.DefaultFileRegion;

import com.google.common.annotations.VisibleForTesting;
    if (manageOsCache && getCount() > 0) {
      try {
        NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(identifier,
            fd, getPosition(), getCount(), POSIX_FADV_DONTNEED);
      } catch (Throwable t) {
        LOG.warn("Failed to manage OS cache for " + identifier, t);
      }

