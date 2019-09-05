hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileStatus.java
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileStatus implements Writable, Comparable<FileStatus> {

  private Path path;
  private long length;
  }

  @Override
  public int compareTo(FileStatus o) {
    return this.getPath().compareTo(o.getPath());
  }
  

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/LocatedFileStatus.java
  }
  
  @Override
  public int compareTo(FileStatus o) {
    return super.compareTo(o);
  }
  

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/viewfs/ViewFsLocatedFileStatus.java
package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
  }

  @Override
  public int compareTo(FileStatus o) {
    return super.compareTo(o);
  }


hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/TestFileStatus.java
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.apache.commons.logging.Log;
    validateToString(fileStatus);
  }
  
  @Test
  public void testCompareTo() throws IOException {
    Path path1 = new Path("path1");
    Path path2 = new Path("path2");
    FileStatus fileStatus1 =
        new FileStatus(1, true, 1, 1, 1, 1, FsPermission.valueOf("-rw-rw-rw-"),
            "one", "one", null, path1);
    FileStatus fileStatus2 =
        new FileStatus(1, true, 1, 1, 1, 1, FsPermission.valueOf("-rw-rw-rw-"),
            "one", "one", null, path2);
    assertTrue(fileStatus1.compareTo(fileStatus2) < 0);
    assertTrue(fileStatus2.compareTo(fileStatus1) > 0);

    List<FileStatus> statList = new ArrayList<>();
    statList.add(fileStatus1);
    statList.add(fileStatus2);
    assertTrue(Collections.binarySearch(statList, fileStatus1) > -1);
  }


