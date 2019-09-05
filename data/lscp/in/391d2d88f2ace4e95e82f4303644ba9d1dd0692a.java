hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/RawLocalFileSystem.java
    if (!localf.exists()) {
      throw new FileNotFoundException("File " + f + " does not exist");
    }

    if (localf.isDirectory()) {
      String[] names = localf.list();
      if (names == null) {
        return null;
        try {
          results[j] = getFileStatus(new Path(f, new Path(null, null,
                                                          names[i])));
          j++;
        } catch (FileNotFoundException e) {
        }
      }
      if (j == names.length) {
      return Arrays.copyOf(results, j);
    }

    if (!useDeprecatedFileStatus) {
      return new FileStatus[] { getFileStatus(f) };
    }
    return new FileStatus[] {
        new DeprecatedRawLocalFileStatus(localf,
        getDefaultBlockSize(f), this) };
  }
  
  protected boolean mkOneDir(File p2f) throws IOException {
    return mkOneDirWithMode(new Path(p2f.getAbsolutePath()), p2f, null);
  }

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/TestLocalFileSystem.java

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.*;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;


    }
  }

  @Test
  public void testFileStatusPipeFile() throws Exception {
    RawLocalFileSystem origFs = new RawLocalFileSystem();
    RawLocalFileSystem fs = spy(origFs);
    Configuration conf = mock(Configuration.class);
    fs.setConf(conf);
    Whitebox.setInternalState(fs, "useDeprecatedFileStatus", false);
    Path path = new Path("/foo");
    File pipe = mock(File.class);
    when(pipe.isFile()).thenReturn(false);
    when(pipe.isDirectory()).thenReturn(false);
    when(pipe.exists()).thenReturn(true);

    FileStatus stat = mock(FileStatus.class);
    doReturn(pipe).when(fs).pathToFile(path);
    doReturn(stat).when(fs).getFileStatus(path);
    FileStatus[] stats = fs.listStatus(path);
    assertTrue(stats != null && stats.length == 1 && stats[0] == stat);
  }
}

