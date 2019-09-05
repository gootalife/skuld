hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/LocatedFileStatus.java
public class LocatedFileStatus extends FileStatus {
  private BlockLocation[] locations;


  public LocatedFileStatus() {
    super();
  }

        stat.getBlockSize(), stat.getModificationTime(),
        stat.getAccessTime(), stat.getPermission(), stat.getOwner(),
        stat.getGroup(), null, stat.getPath(), locations);
    if (stat.isSymlink()) {
      setSymlink(stat.getSymlink());
    }
  }

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/viewfs/ChRootedFileSystem.java
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
    return super.listStatus(fullPath(f));
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
      throws IOException {
    return super.listLocatedStatus(fullPath(f));
  }

  @Override
  public boolean mkdirs(final Path f, final FsPermission permission)
      throws IOException {

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/viewfs/InodeTree.java
hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/viewfs/ViewFileSystem.java
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
  private String getUriPath(final Path p) {
    checkPath(p);
    return makeAbsolute(p).toUri().getPath();
  }
  
  private Path makeAbsolute(final Path f) {
    return res.targetFileSystem.getFileChecksum(res.remainingPath);
  }


  private static FileStatus fixFileStatus(FileStatus orig,
      Path qualified) throws IOException {

    if ("file".equals(orig.getPath().toUri().getScheme())) {
      orig = wrapLocalFileStatus(orig, qualified);
    }

    orig.setPath(qualified);
    return orig;
  }

  private static FileStatus wrapLocalFileStatus(FileStatus orig,
      Path qualified) {
    return orig instanceof LocatedFileStatus
        ? new ViewFsLocatedFileStatus((LocatedFileStatus)orig, qualified)
        : new ViewFsFileStatus(orig, qualified);
  }


  @Override
  public FileStatus getFileStatus(final Path f) throws AccessControlException,
      FileNotFoundException, IOException {
    InodeTree.ResolveResult<FileSystem> res =
      fsState.resolve(getUriPath(f), true);
    FileStatus status =  res.targetFileSystem.getFileStatus(res.remainingPath);
    return fixFileStatus(status, this.makeQualified(f));
  }
  
  @Override
    if (!res.isInternalDir()) {
      int i = 0;
      for (FileStatus status : statusLst) {
          statusLst[i++] = fixFileStatus(status,
              getChrootedPath(res, status, f));
      }
    }
    return statusLst;
  }

  @Override
  public RemoteIterator<LocatedFileStatus>listLocatedStatus(final Path f,
      final PathFilter filter) throws FileNotFoundException, IOException {
    final InodeTree.ResolveResult<FileSystem> res = fsState
        .resolve(getUriPath(f), true);
    final RemoteIterator<LocatedFileStatus> statusIter = res.targetFileSystem
        .listLocatedStatus(res.remainingPath);

    if (res.isInternalDir()) {
      return statusIter;
    }

    return new RemoteIterator<LocatedFileStatus>() {
      @Override
      public boolean hasNext() throws IOException {
        return statusIter.hasNext();
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        final LocatedFileStatus status = statusIter.next();
        return (LocatedFileStatus)fixFileStatus(status,
            getChrootedPath(res, status, f));
      }
    };
  }

  private Path getChrootedPath(InodeTree.ResolveResult<FileSystem> res,
      FileStatus status, Path f) throws IOException {
    final String suffix = ((ChRootedFileSystem)res.targetFileSystem)
        .stripOutRoot(status.getPath());
    return this.makeQualified(
        suffix.length() == 0 ? f : new Path(res.resolvedPath, suffix));
  }

  @Override
  public boolean mkdirs(final Path dir, final FsPermission permission)
      throws IOException {

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/viewfs/ViewFsLocatedFileStatus.java
++ b/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/viewfs/ViewFsLocatedFileStatus.java
package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

class ViewFsLocatedFileStatus extends LocatedFileStatus {
  final LocatedFileStatus myFs;
  Path modifiedPath;

  ViewFsLocatedFileStatus(LocatedFileStatus locatedFileStatus, Path path) {
    myFs = locatedFileStatus;
    modifiedPath = path;
  }

  @Override
  public long getLen() {
    return myFs.getLen();
  }

  @Override
  public boolean isFile() {
    return myFs.isFile();
  }

  @Override
  public boolean isDirectory() {
    return myFs.isDirectory();
  }

  @Override
  @SuppressWarnings("deprecation")
  public boolean isDir() {
    return myFs.isDirectory();
  }

  @Override
  public boolean isSymlink() {
    return myFs.isSymlink();
  }

  @Override
  public long getBlockSize() {
    return myFs.getBlockSize();
  }

  @Override
  public short getReplication() {
    return myFs.getReplication();
  }

  @Override
  public long getModificationTime() {
    return myFs.getModificationTime();
  }

  @Override
  public long getAccessTime() {
    return myFs.getAccessTime();
  }

  @Override
  public FsPermission getPermission() {
    return myFs.getPermission();
  }

  @Override
  public String getOwner() {
    return myFs.getOwner();
  }

  @Override
  public String getGroup() {
    return myFs.getGroup();
  }

  @Override
  public Path getPath() {
    return modifiedPath;
  }

  @Override
  public void setPath(final Path p) {
    modifiedPath = p;
  }

  @Override
  public Path getSymlink() throws IOException {
    return myFs.getSymlink();
  }

  @Override
  public void setSymlink(Path p) {
    myFs.setSymlink(p);
  }

  @Override
  public BlockLocation[] getBlockLocations() {
    return myFs.getBlockLocations();
  }

  @Override
  public int compareTo(Object o) {
    return super.compareTo(o);
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/viewfs/TestChRootedFileSystem.java
    verify(mockFs).getAclStatus(rawPath);
  }

  @Test
  public void testListLocatedFileStatus() throws IOException {
    final Path mockMount = new Path("mockfs://foo/user");
    final Path mockPath = new Path("/usermock");
    final Configuration conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    ConfigUtil.addLink(conf, mockPath.toString(), mockMount.toUri());
    FileSystem vfs = FileSystem.get(URI.create("viewfs:///"), conf);
    vfs.listLocatedStatus(mockPath);
    final FileSystem mockFs = ((MockFileSystem)mockMount.getFileSystem(conf))
        .getRawFileSystem();
    verify(mockFs).listLocatedStatus(new Path(mockMount.toUri().getPath()));
  }

  static class MockFileSystem extends FilterFileSystem {
    MockFileSystem() {
      super(mock(FileSystem.class));

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/viewfs/ViewFileSystemBaseTest.java

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
        fsView.makeQualified(new Path("/foo/bar")));
  }

  @Test
  public void testLocatedOperationsThroughMountLinks() throws IOException {
    testOperationsThroughMountLinksInternal(true);
  }

  @Test
  public void testOperationsThroughMountLinks() throws IOException {
    testOperationsThroughMountLinksInternal(false);
  }

  private void testOperationsThroughMountLinksInternal(boolean located)
      throws IOException {
    fileSystemTestHelper.createFile(fsView, "/user/foo");
    Assert.assertTrue("Created file should be type file",
    fsView.mkdirs(new Path("/targetRoot/dirFoo"));
    Assert.assertTrue(fsView.exists(new Path("/targetRoot/dirFoo")));
    boolean dirFooPresent = false;
    for (FileStatus fileStatus :
        listStatusInternal(located, new Path("/targetRoot/"))) {
      if (fileStatus.getPath().getName().equals("dirFoo")) {
        dirFooPresent = true;
      }
    } 
  }

  @Test
  public void testLocatedListOnInternalDirsOfMountTable() throws IOException {
    testListOnInternalDirsOfMountTableInternal(true);
  }


  @Test
  public void testListOnInternalDirsOfMountTable() throws IOException {
    testListOnInternalDirsOfMountTableInternal(false);
  }

  private void testListOnInternalDirsOfMountTableInternal(boolean located)
      throws IOException {
    

    FileStatus[] dirPaths = listStatusInternal(located, new Path("/"));
    FileStatus fs;
    verifyRootChildren(dirPaths);

    dirPaths = listStatusInternal(located, new Path("/internalDir"));
    Assert.assertEquals(2, dirPaths.length);

    fs = fileSystemTestHelper.containsPath(fsView, "/internalDir/internalDir2", dirPaths);
  
  @Test
  public void testListOnMountTargetDirs() throws IOException {
    testListOnMountTargetDirsInternal(false);
  }

  @Test
  public void testLocatedListOnMountTargetDirs() throws IOException {
    testListOnMountTargetDirsInternal(true);
  }

  private void testListOnMountTargetDirsInternal(boolean located)
      throws IOException {
    final Path dataPath = new Path("/data");

    FileStatus[] dirPaths = listStatusInternal(located, dataPath);

    FileStatus fs;
    Assert.assertEquals(0, dirPaths.length);
    
    long len = fileSystemTestHelper.createFile(fsView, "/data/foo");
    dirPaths = listStatusInternal(located, dataPath);
    Assert.assertEquals(1, dirPaths.length);
    fs = fileSystemTestHelper.containsPath(fsView, "/data/foo", dirPaths);
    Assert.assertNotNull(fs);
    
    fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/data/dirX"));
    dirPaths = listStatusInternal(located, dataPath);
    Assert.assertEquals(2, dirPaths.length);
    fs = fileSystemTestHelper.containsPath(fsView, "/data/foo", dirPaths);
    Assert.assertNotNull(fs);
    Assert.assertTrue("Created dir should appear as a dir", fs.isDirectory()); 
  }

  private FileStatus[] listStatusInternal(boolean located, Path dataPath) throws IOException {
    FileStatus[] dirPaths = new FileStatus[0];
    if (located) {
      RemoteIterator<LocatedFileStatus> statIter =
          fsView.listLocatedStatus(dataPath);
      ArrayList<LocatedFileStatus> tmp = new ArrayList<LocatedFileStatus>(10);
      while (statIter.hasNext()) {
        tmp.add(statIter.next());
      }
      dirPaths = tmp.toArray(dirPaths);
    } else {
      dirPaths = fsView.listStatus(dataPath);
    }
    return dirPaths;
  }

  @Test
  public void testFileStatusOnMountLink() throws IOException {
    Assert.assertTrue(fsView.getFileStatus(new Path("/")).isDirectory());

  @Test
  public void testRootReadableExecutable() throws IOException {
    testRootReadableExecutableInternal(false);
  }

  @Test
  public void testLocatedRootReadableExecutable() throws IOException {
    testRootReadableExecutableInternal(true);
  }

  private void testRootReadableExecutableInternal(boolean located)
      throws IOException {
    Assert.assertFalse("In root before cd",

    verifyRootChildren(listStatusInternal(located,
        fsView.getWorkingDirectory()));


