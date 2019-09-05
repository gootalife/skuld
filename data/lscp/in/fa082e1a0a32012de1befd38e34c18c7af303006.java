hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/viewfs/ChRootedFileSystem.java
    super.removeXAttr(fullPath(path), name);
  }

  @Override
  public Path createSnapshot(Path path, String name) throws IOException {
    return super.createSnapshot(fullPath(path), name);
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName,
      String snapshotNewName) throws IOException {
    super.renameSnapshot(fullPath(path), snapshotOldName, snapshotNewName);
  }

  @Override
  public void deleteSnapshot(Path snapshotDir, String snapshotName)
      throws IOException {
    super.deleteSnapshot(fullPath(snapshotDir), snapshotName);
  }

  @Override
  public Path resolvePath(final Path p) throws IOException {
    return super.resolvePath(fullPath(p));

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/viewfs/ChRootedFs.java
    myFs.removeXAttr(fullPath(path), name);
  }

  @Override
  public Path createSnapshot(Path path, String name) throws IOException {
    return myFs.createSnapshot(fullPath(path), name);
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName,
      String snapshotNewName) throws IOException {
    myFs.renameSnapshot(fullPath(path), snapshotOldName, snapshotNewName);
  }

  @Override
  public void deleteSnapshot(Path snapshotDir, String snapshotName)
      throws IOException {
    myFs.deleteSnapshot(fullPath(snapshotDir), snapshotName);
  }

  @Override
  public void setVerifyChecksum(final boolean verifyChecksum)
      throws IOException, UnresolvedLinkException {

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/viewfs/ViewFileSystem.java
    return result;
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res = fsState.resolve(getUriPath(path),
        true);
    return res.targetFileSystem.createSnapshot(res.remainingPath, snapshotName);
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName,
      String snapshotNewName) throws IOException {
    InodeTree.ResolveResult<FileSystem> res = fsState.resolve(getUriPath(path),
        true);
    res.targetFileSystem.renameSnapshot(res.remainingPath, snapshotOldName,
        snapshotNewName);
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res = fsState.resolve(getUriPath(path),
        true);
    res.targetFileSystem.deleteSnapshot(res.remainingPath, snapshotName);
  }

      checkPathIsSlash(path);
      throw readOnlyMountTable("removeXAttr", path);
    }

    @Override
    public Path createSnapshot(Path path, String snapshotName)
        throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("createSnapshot", path);
    }

    @Override
    public void renameSnapshot(Path path, String snapshotOldName,
        String snapshotNewName) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("renameSnapshot", path);
    }

    @Override
    public void deleteSnapshot(Path path, String snapshotName)
        throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("deleteSnapshot", path);
    }
  }
}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/viewfs/ViewFs.java

  @Override
  public boolean isValidName(String src) {
    return true;
  }

    res.targetFileSystem.removeXAttr(res.remainingPath, name);
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName)
      throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = fsState.resolve(
        getUriPath(path), true);
    return res.targetFileSystem.createSnapshot(res.remainingPath, snapshotName);
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName,
      String snapshotNewName) throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = fsState.resolve(
        getUriPath(path), true);
    res.targetFileSystem.renameSnapshot(res.remainingPath, snapshotOldName,
        snapshotNewName);
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName) throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res = fsState.resolve(
        getUriPath(path), true);
    res.targetFileSystem.deleteSnapshot(res.remainingPath, snapshotName);
  }

      checkPathIsSlash(path);
      throw readOnlyMountTable("removeXAttr", path);
    }

    @Override
    public Path createSnapshot(Path path, String snapshotName)
        throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("createSnapshot", path);
    }

    @Override
    public void renameSnapshot(Path path, String snapshotOldName,
        String snapshotNewName) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("renameSnapshot", path);
    }

    @Override
    public void deleteSnapshot(Path path, String snapshotName)
        throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("deleteSnapshot", path);
    }
  }
}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/viewfs/TestChRootedFileSystem.java
    @Override
    public void initialize(URI name, Configuration conf) throws IOException {}
  }

  @Test(timeout = 30000)
  public void testCreateSnapshot() throws Exception {
    Path snapRootPath = new Path("/snapPath");
    Path chRootedSnapRootPath = new Path("/a/b/snapPath");

    Configuration conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);

    URI chrootUri = URI.create("mockfs://foo/a/b");
    ChRootedFileSystem chrootFs = new ChRootedFileSystem(chrootUri, conf);
    FileSystem mockFs = ((FilterFileSystem) chrootFs.getRawFileSystem())
        .getRawFileSystem();

    chrootFs.createSnapshot(snapRootPath, "snap1");
    verify(mockFs).createSnapshot(chRootedSnapRootPath, "snap1");
  }

  @Test(timeout = 30000)
  public void testDeleteSnapshot() throws Exception {
    Path snapRootPath = new Path("/snapPath");
    Path chRootedSnapRootPath = new Path("/a/b/snapPath");

    Configuration conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);

    URI chrootUri = URI.create("mockfs://foo/a/b");
    ChRootedFileSystem chrootFs = new ChRootedFileSystem(chrootUri, conf);
    FileSystem mockFs = ((FilterFileSystem) chrootFs.getRawFileSystem())
        .getRawFileSystem();

    chrootFs.deleteSnapshot(snapRootPath, "snap1");
    verify(mockFs).deleteSnapshot(chRootedSnapRootPath, "snap1");
  }

  @Test(timeout = 30000)
  public void testRenameSnapshot() throws Exception {
    Path snapRootPath = new Path("/snapPath");
    Path chRootedSnapRootPath = new Path("/a/b/snapPath");

    Configuration conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);

    URI chrootUri = URI.create("mockfs://foo/a/b");
    ChRootedFileSystem chrootFs = new ChRootedFileSystem(chrootUri, conf);
    FileSystem mockFs = ((FilterFileSystem) chrootFs.getRawFileSystem())
        .getRawFileSystem();

    chrootFs.renameSnapshot(snapRootPath, "snapOldName", "snapNewName");
    verify(mockFs).renameSnapshot(chRootedSnapRootPath, "snapOldName",
        "snapNewName");
  }
}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/viewfs/TestChRootedFs.java
    Assert.assertFalse(chRootedFs.isValidName("/test"));
    Mockito.verify(baseFs).isValidName("/chroot/test");
  }

  @Test(timeout = 30000)
  public void testCreateSnapshot() throws Exception {
    Path snapRootPath = new Path("/snapPath");
    Path chRootedSnapRootPath = new Path(
        Path.getPathWithoutSchemeAndAuthority(chrootedTo), "snapPath");
    AbstractFileSystem baseFs = Mockito.spy(fc.getDefaultFileSystem());
    ChRootedFs chRootedFs = new ChRootedFs(baseFs, chrootedTo);
    Mockito.doReturn(snapRootPath).when(baseFs)
        .createSnapshot(chRootedSnapRootPath, "snap1");
    Assert.assertEquals(snapRootPath,
        chRootedFs.createSnapshot(snapRootPath, "snap1"));
    Mockito.verify(baseFs).createSnapshot(chRootedSnapRootPath, "snap1");
  }

  @Test(timeout = 30000)
  public void testDeleteSnapshot() throws Exception {
    Path snapRootPath = new Path("/snapPath");
    Path chRootedSnapRootPath = new Path(
        Path.getPathWithoutSchemeAndAuthority(chrootedTo), "snapPath");
    AbstractFileSystem baseFs = Mockito.spy(fc.getDefaultFileSystem());
    ChRootedFs chRootedFs = new ChRootedFs(baseFs, chrootedTo);
    Mockito.doNothing().when(baseFs)
        .deleteSnapshot(chRootedSnapRootPath, "snap1");
    chRootedFs.deleteSnapshot(snapRootPath, "snap1");
    Mockito.verify(baseFs).deleteSnapshot(chRootedSnapRootPath, "snap1");
  }

  @Test(timeout = 30000)
  public void testRenameSnapshot() throws Exception {
    Path snapRootPath = new Path("/snapPath");
    Path chRootedSnapRootPath = new Path(
        Path.getPathWithoutSchemeAndAuthority(chrootedTo), "snapPath");
    AbstractFileSystem baseFs = Mockito.spy(fc.getDefaultFileSystem());
    ChRootedFs chRootedFs = new ChRootedFs(baseFs, chrootedTo);
    Mockito.doNothing().when(baseFs)
        .renameSnapshot(chRootedSnapRootPath, "snapOldName", "snapNewName");
    chRootedFs.renameSnapshot(snapRootPath, "snapOldName", "snapNewName");
    Mockito.verify(baseFs).renameSnapshot(chRootedSnapRootPath, "snapOldName",
        "snapNewName");
  }
}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/viewfs/ViewFileSystemBaseTest.java
    fsView.removeXAttr(new Path("/internalDir"), "xattrName");
  }

  @Test(expected = AccessControlException.class)
  public void testInternalCreateSnapshot1() throws IOException {
    fsView.createSnapshot(new Path("/internalDir"));
  }

  @Test(expected = AccessControlException.class)
  public void testInternalCreateSnapshot2() throws IOException {
    fsView.createSnapshot(new Path("/internalDir"), "snap1");
  }

  @Test(expected = AccessControlException.class)
  public void testInternalRenameSnapshot() throws IOException {
    fsView.renameSnapshot(new Path("/internalDir"), "snapOldName",
        "snapNewName");
  }

  @Test(expected = AccessControlException.class)
  public void testInternalDeleteSnapshot() throws IOException {
    fsView.deleteSnapshot(new Path("/internalDir"), "snap1");
  }
}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/viewfs/ViewFsBaseTest.java
  public void testInternalRemoveXAttr() throws IOException {
    fcView.removeXAttr(new Path("/internalDir"), "xattrName");
  }

  @Test(expected = AccessControlException.class)
  public void testInternalCreateSnapshot1() throws IOException {
    fcView.createSnapshot(new Path("/internalDir"));
  }

  @Test(expected = AccessControlException.class)
  public void testInternalCreateSnapshot2() throws IOException {
    fcView.createSnapshot(new Path("/internalDir"), "snap1");
  }

  @Test(expected = AccessControlException.class)
  public void testInternalRenameSnapshot() throws IOException {
    fcView.renameSnapshot(new Path("/internalDir"), "snapOldName",
        "snapNewName");
  }

  @Test(expected = AccessControlException.class)
  public void testInternalDeleteSnapshot() throws IOException {
    fcView.deleteSnapshot(new Path("/internalDir"), "snap1");
  }
}

