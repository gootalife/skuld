hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirectory.java
    if (checkQuota) {
      final String parentPath = existing.getPath();
      verifyMaxComponentLength(inode.getLocalNameBytes(), parentPath);
      verifyMaxDirItems(parent, parentPath);
    }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFsLimits.java

import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

        HadoopIllegalArgumentException.class);
  }

  @Test
  public void testParentDirectoryNameIsCorrect() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 20);
    mkdirs("/user", null);
    mkdirs("/user/testHome", null);
    mkdirs("/user/testHome/FileNameLength", null);

    mkdirCheckParentDirectory(
      "/user/testHome/FileNameLength/really_big_name_0003_fail",
      "/user/testHome/FileNameLength", PathComponentTooLongException.class);

    renameCheckParentDirectory("/user/testHome/FileNameLength",
      "/user/testHome/really_big_name_0003_fail", "/user/testHome/",
      PathComponentTooLongException.class);

  }


  private void mkdirCheckParentDirectory(String name, String ParentDirName,
                                         Class<?> expected)
    throws Exception {
    verify(mkdirs(name, expected), ParentDirName);
  }

  private void renameCheckParentDirectory(String name, String dst,
                                          String ParentDirName,
                                          Class<?> expected)
    throws Exception {
    verify(rename(name, dst, expected), ParentDirName);
  }

  private void verify(String message, String ParentDirName) {
    boolean found = false;
    if (message != null) {
      String[] tokens = message.split("\\s+");
      for (String token : tokens) {
        if (token != null && token.equals(ParentDirName)) {
          found = true;
          break;
        }
      }
    }
    assertTrue(found);
  }

  private String mkdirs(String name, Class<?> expected)
  throws Exception {
    lazyInitFSDirectory();
    Class<?> generated = null;
    String errorString = null;
    try {
      fs.mkdirs(name, perms, false);
    } catch (Throwable e) {
      generated = e.getClass();
      e.printStackTrace();
      errorString = e.getMessage();
    }
    assertEquals(expected, generated);
    return errorString;
  }

  private String rename(String src, String dst, Class<?> expected)
      throws Exception {
    lazyInitFSDirectory();
    Class<?> generated = null;
    String errorString = null;
    try {
      fs.renameTo(src, dst, false, new Rename[] { });
    } catch (Throwable e) {
      generated = e.getClass();
      errorString = e.getMessage();
    }
    assertEquals(expected, generated);
    return errorString;
  }

  @SuppressWarnings("deprecation")

