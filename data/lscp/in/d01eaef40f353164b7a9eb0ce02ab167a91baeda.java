hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/RawLocalFileSystem.java
import java.io.FileDescriptor;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.StringTokenizer;
      return !super.getOwner().isEmpty(); 
    }
    
    DeprecatedRawLocalFileStatus(File f, long defaultBlockSize, FileSystem fs)
      throws IOException {
      super(f.length(), f.isDirectory(), 1, defaultBlockSize,
          f.lastModified(),
          Files.readAttributes(f.toPath(),
            BasicFileAttributes.class).lastAccessTime().toMillis(),
          null, null, null,
          new Path(f.getPath()).makeQualified(fs.getUri(),
            fs.getWorkingDirectory()));
    }
    
  }
 
  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    BasicFileAttributeView view = Files.getFileAttributeView(
        pathToFile(p).toPath(), BasicFileAttributeView.class);
    FileTime fmtime = (mtime >= 0) ? FileTime.fromMillis(mtime) : null;
    FileTime fatime = (atime >= 0) ? FileTime.fromMillis(atime) : null;
    view.setTimes(fmtime, fatime, null);
  }

  @Override

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/SymlinkBaseTest.java
  }

  @Test(timeout=10000)
  public void testSetTimesSymlinkToFile() throws IOException {
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    wrapper.createSymlink(file, link, false);
    long at = wrapper.getFileLinkStatus(link).getAccessTime();
    wrapper.setTimes(link, 2000L, 3000L);
    assertEquals(at, wrapper.getFileLinkStatus(link).getAccessTime());
    assertEquals(2000, wrapper.getFileStatus(file).getModificationTime());
    assertEquals(3000, wrapper.getFileStatus(file).getAccessTime());
  }

  @Test(timeout=10000)
  public void testSetTimesSymlinkToDir() throws IOException {
    Path dir = new Path(testBaseDir1(), "dir");
    Path link = new Path(testBaseDir1(), "linkToDir");
    wrapper.mkdir(dir, FileContext.DEFAULT_PERM, false);
    wrapper.createSymlink(dir, link, false);
    long at = wrapper.getFileLinkStatus(link).getAccessTime();
    wrapper.setTimes(link, 2000L, 3000L);
    assertEquals(at, wrapper.getFileLinkStatus(link).getAccessTime());
    assertEquals(2000, wrapper.getFileStatus(dir).getModificationTime());
    assertEquals(3000, wrapper.getFileStatus(dir).getAccessTime());
  }

  @Test(timeout=10000)
  public void testSetTimesDanglingLink() throws IOException {
    Path file = new Path("/noSuchFile");
    Path link = new Path(testBaseDir1()+"/link");
    wrapper.createSymlink(file, link, false);
    long at = wrapper.getFileLinkStatus(link).getAccessTime();
    try {
      wrapper.setTimes(link, 2000L, 3000L);
      fail("set times to non-existant file");
    } catch (IOException e) {
    }
    assertEquals(at, wrapper.getFileLinkStatus(link).getAccessTime());
  }
}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/TestLocalFileSystem.java
    assertTrue(checksumFileFound);
  }

  private void checkTimesStatus(Path path,
    long expectedModTime, long expectedAccTime) throws IOException {
    FileStatus status = fileSys.getFileStatus(path);
    assertEquals(expectedModTime, status.getModificationTime());
    assertEquals(expectedAccTime, status.getAccessTime());
  }

  @Test(timeout = 1000)
  public void testSetTimes() throws Exception {
    Path path = new Path(TEST_ROOT_DIR, "set-times");
    long newModTime = 12345000;
    long newAccTime = 23456000;

    FileStatus status = fileSys.getFileStatus(path);
    assertTrue("check we're actually changing something", newModTime != status.getModificationTime());
    assertTrue("check we're actually changing something", newAccTime != status.getAccessTime());

    fileSys.setTimes(path, newModTime, newAccTime);
    checkTimesStatus(path, newModTime, newAccTime);

    newModTime = 34567000;

    fileSys.setTimes(path, newModTime, -1);
    checkTimesStatus(path, newModTime, newAccTime);

    newAccTime = 45678000;

    fileSys.setTimes(path, -1, newAccTime);
    checkTimesStatus(path, newModTime, newAccTime);
  }


hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/TestSymlinkLocalFS.java
    }
  }

  @Override
  public void testSetTimesSymlinkToFile() throws IOException {
    assumeTrue(!Path.WINDOWS);
    super.testSetTimesSymlinkToFile();
  }

  @Override
  public void testSetTimesSymlinkToDir() throws IOException {
    assumeTrue(!Path.WINDOWS);
    super.testSetTimesSymlinkToDir();
  }

  @Override
  public void testSetTimesDanglingLink() throws IOException {
    assumeTrue(!Path.WINDOWS);
    super.testSetTimesDanglingLink();
  }
}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/shell/TestCopyPreserveFlag.java
package org.apache.hadoop.fs.shell;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

public class TestCopyPreserveFlag {
  private static final int MODIFICATION_TIME = 12345000;
  private static final int ACCESS_TIME = 23456000;
  private static final Path DIR_FROM = new Path("d0");
  private static final Path DIR_TO1 = new Path("d1");
  private static final Path DIR_TO2 = new Path("d2");
  private static final Path FROM = new Path(DIR_FROM, "f0");
  private static final Path TO = new Path(DIR_TO1, "f1");
  private static final FsPermission PERMISSIONS = new FsPermission(
    FsAction.ALL,
    FsAction.EXECUTE,

    FileSystem.setDefaultUri(conf, fs.getUri());
    fs.setWorkingDirectory(testDir);
    fs.mkdirs(DIR_FROM);
    fs.mkdirs(DIR_TO1);
    fs.createNewFile(FROM);

    FSDataOutputStream output = fs.create(FROM, true);
        output.writeChar('\n');
    }
    output.close();
    fs.setTimes(FROM, MODIFICATION_TIME, ACCESS_TIME);
    fs.setPermission(FROM, PERMISSIONS);
    fs.setTimes(DIR_FROM, MODIFICATION_TIME, ACCESS_TIME);
    fs.setPermission(DIR_FROM, PERMISSIONS);
  }

  @After
    fs.close();
  }

  private void assertAttributesPreserved(Path to) throws IOException {
    FileStatus status = fs.getFileStatus(to);
    assertEquals(MODIFICATION_TIME, status.getModificationTime());
    assertEquals(ACCESS_TIME, status.getAccessTime());
    assertEquals(PERMISSIONS, status.getPermission());
  }

  private void assertAttributesChanged(Path to) throws IOException {
    FileStatus status = fs.getFileStatus(to);
    assertNotEquals(MODIFICATION_TIME, status.getModificationTime());
    assertNotEquals(ACCESS_TIME, status.getAccessTime());
    assertNotEquals(PERMISSIONS, status.getPermission());
  }

  private void run(CommandWithDestination cmd, String... args) {
  @Test(timeout = 10000)
  public void testPutWithP() throws Exception {
    run(new Put(), "-p", FROM.toString(), TO.toString());
    assertAttributesPreserved(TO);
  }

  @Test(timeout = 10000)
  public void testPutWithoutP() throws Exception {
    run(new Put(), FROM.toString(), TO.toString());
    assertAttributesChanged(TO);
  }

  @Test(timeout = 10000)
  public void testGetWithP() throws Exception {
    run(new Get(), "-p", FROM.toString(), TO.toString());
    assertAttributesPreserved(TO);
  }

  @Test(timeout = 10000)
  public void testGetWithoutP() throws Exception {
    run(new Get(), FROM.toString(), TO.toString());
    assertAttributesChanged(TO);
  }

  @Test(timeout = 10000)
  public void testCpWithP() throws Exception {
      run(new Cp(), "-p", FROM.toString(), TO.toString());
      assertAttributesPreserved(TO);
  }

  @Test(timeout = 10000)
  public void testCpWithoutP() throws Exception {
      run(new Cp(), FROM.toString(), TO.toString());
      assertAttributesChanged(TO);
  }

  @Test(timeout = 10000)
  public void testDirectoryCpWithP() throws Exception {
    run(new Cp(), "-p", DIR_FROM.toString(), DIR_TO2.toString());
    assertAttributesPreserved(DIR_TO2);
  }

  @Test(timeout = 10000)
  public void testDirectoryCpWithoutP() throws Exception {
    run(new Cp(), DIR_FROM.toString(), DIR_TO2.toString());
    assertAttributesChanged(DIR_TO2);
  }
}

