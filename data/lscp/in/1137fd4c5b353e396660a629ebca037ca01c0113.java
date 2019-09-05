hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/DiskChecker.java

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.Files;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
  public static void checkDirs(File dir) throws DiskErrorException {
    checkDir(dir);
    IOException ex = null;
    try (DirectoryStream<java.nio.file.Path> stream =
        Files.newDirectoryStream(dir.toPath())) {
      for (java.nio.file.Path entry: stream) {
        File child = entry.toFile();
        if (child.isDirectory()) {
          checkDirs(child);
        }
      }
    } catch (DirectoryIteratorException de) {
      ex = de.getCause();
    } catch (IOException ie) {
      ex = ie;
    }
    if (ex != null) {
      throw new DiskErrorException("I/O error when open a directory: "
          + dir.toString(), ex);
    }
  }


hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/util/TestDiskChecker.java
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.Shell;

    System.out.println("checkDir success: " + success);

  }

  @Test (timeout = 30000)
  public void testCheckDirsIOException() throws Throwable {
    Path path = new Path("target", TestDiskChecker.class.getSimpleName());
    File localDir = new File(path.toUri().getRawPath());
    localDir.mkdir();
    File localFile = new File(localDir, "test");
    localFile.createNewFile();
    File spyLocalDir = spy(localDir);
    doReturn(localFile.toPath()).when(spyLocalDir).toPath();
    try {
      DiskChecker.checkDirs(spyLocalDir);
      fail("Expected exception for I/O error");
    } catch (DiskErrorException e) {
      GenericTestUtils.assertExceptionContains("I/O error", e);
      assertTrue(e.getCause() instanceof IOException);
    } finally {
      localFile.delete();
      localDir.delete();
    }
  }
}

