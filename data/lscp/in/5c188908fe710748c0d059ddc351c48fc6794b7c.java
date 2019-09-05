hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/AbstractFileSystem.java
        + " doesn't support removeXAttr");
  }

  public Path createSnapshot(final Path path, final String snapshotName)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support createSnapshot");
  }

  public void renameSnapshot(final Path path, final String snapshotOldName,
      final String snapshotNewName) throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support renameSnapshot");
  }

  public void deleteSnapshot(final Path snapshotDir, final String snapshotName)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support deleteSnapshot");
  }

  @Override //Object
  public int hashCode() {
    return myUri.hashCode();

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileContext.java
      }
    }.resolve(this, absF);
  }

  public final Path createSnapshot(Path path) throws IOException {
    return createSnapshot(path, null);
  }

  public Path createSnapshot(final Path path, final String snapshotName)
      throws IOException {
    final Path absF = fixRelativePart(path);
    return new FSLinkResolver<Path>() {

      @Override
      public Path next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        return fs.createSnapshot(p, snapshotName);
      }
    }.resolve(this, absF);
  }

  public void renameSnapshot(final Path path, final String snapshotOldName,
      final String snapshotNewName) throws IOException {
    final Path absF = fixRelativePart(path);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        fs.renameSnapshot(p, snapshotOldName, snapshotNewName);
        return null;
      }
    }.resolve(this, absF);
  }

  public void deleteSnapshot(final Path path, final String snapshotName)
      throws IOException {
    final Path absF = fixRelativePart(path);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        fs.deleteSnapshot(p, snapshotName);
        return null;
      }
    }.resolve(this, absF);
  }
}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FilterFs.java
  public void removeXAttr(Path path, String name) throws IOException {
    myFs.removeXAttr(path, name);
  }

  @Override
  public Path createSnapshot(final Path path, final String snapshotName)
      throws IOException {
    return myFs.createSnapshot(path, snapshotName);
  }

  @Override
  public void renameSnapshot(final Path path, final String snapshotOldName,
      final String snapshotNewName) throws IOException {
    myFs.renameSnapshot(path, snapshotOldName, snapshotNewName);
  }

  @Override
  public void deleteSnapshot(final Path path, final String snapshotName)
      throws IOException {
    myFs.deleteSnapshot(path, snapshotName);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/fs/Hdfs.java
      throws InvalidToken, IOException {
    dfs.cancelDelegationToken((Token<DelegationTokenIdentifier>) token);
  }

  @Override
  public Path createSnapshot(final Path path, final String snapshotName)
      throws IOException {
    return new Path(dfs.createSnapshot(getUriPath(path), snapshotName));
  }

  @Override
  public void renameSnapshot(final Path path, final String snapshotOldName,
      final String snapshotNewName) throws IOException {
    dfs.renameSnapshot(getUriPath(path), snapshotOldName, snapshotNewName);
  }

  @Override
  public void deleteSnapshot(final Path snapshotDir, final String snapshotName)
      throws IOException {
    dfs.deleteSnapshot(getUriPath(snapshotDir), snapshotName);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestFileContextSnapshot.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestFileContextSnapshot.java
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFileContextSnapshot {

  private static final short REPLICATION = 3;
  private static final int BLOCKSIZE = 1024;
  private static final long SEED = 0;
  private Configuration conf;
  private MiniDFSCluster cluster;
  private FileContext fileContext;
  private DistributedFileSystem dfs;

  private final String snapshotRoot = "/snapshot";
  private final Path filePath = new Path(snapshotRoot, "file1");
  private Path snapRootPath;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();

    fileContext = FileContext.getFileContext(conf);
    dfs = (DistributedFileSystem) cluster.getFileSystem();
    snapRootPath = new Path(snapshotRoot);
    dfs.mkdirs(snapRootPath);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 60000)
  public void testCreateAndDeleteSnapshot() throws Exception {
    DFSTestUtil.createFile(dfs, filePath, BLOCKSIZE, REPLICATION, SEED);
    dfs.disallowSnapshot(snapRootPath);
    try {
      fileContext.createSnapshot(snapRootPath, "s1");
    } catch (SnapshotException e) {
      GenericTestUtils.assertExceptionContains(
          "Directory is not a snapshottable directory: " + snapRootPath, e);
    }

    dfs.allowSnapshot(snapRootPath);
    Path ssPath = fileContext.createSnapshot(snapRootPath, "s1");
    assertTrue("Failed to create snapshot", dfs.exists(ssPath));
    fileContext.deleteSnapshot(snapRootPath, "s1");
    assertFalse("Failed to delete snapshot", dfs.exists(ssPath));
  }

  @Test(timeout = 60000)
  public void testRenameSnapshot() throws Exception {
    DFSTestUtil.createFile(dfs, filePath, BLOCKSIZE, REPLICATION, SEED);
    dfs.allowSnapshot(snapRootPath);
    Path snapPath1 = fileContext.createSnapshot(snapRootPath, "s1");
    Path ssPath = new Path(snapPath1, filePath.getName());
    assertTrue("Failed to create snapshot", dfs.exists(ssPath));
    FileStatus statusBeforeRename = dfs.getFileStatus(ssPath);

    fileContext.renameSnapshot(snapRootPath, "s1", "s2");
    assertFalse("Old snapshot still exists after rename!", dfs.exists(ssPath));
    Path snapshotRoot = SnapshotTestHelper.getSnapshotRoot(snapRootPath, "s2");
    ssPath = new Path(snapshotRoot, filePath.getName());

    assertTrue("Snapshot doesn't exists!", dfs.exists(ssPath));
    FileStatus statusAfterRename = dfs.getFileStatus(ssPath);

    assertFalse("Filestatus of the snapshot matches",
        statusBeforeRename.equals(statusAfterRename));
    statusBeforeRename.setPath(statusAfterRename.getPath());
    assertEquals("FileStatus of the snapshot mismatches!",
        statusBeforeRename.toString(), statusAfterRename.toString());
  }
}
\No newline at end of file

