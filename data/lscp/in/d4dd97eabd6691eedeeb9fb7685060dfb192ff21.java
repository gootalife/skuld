hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/DistCpSync.java
    List<Path> sourcePaths = inputOptions.getSourcePaths();
    if (sourcePaths.size() != 1) {
      throw new IllegalArgumentException(sourcePaths.size()
          + " source paths are provided");
    }
    final Path sourceDir = sourcePaths.get(0);
    final Path targetDir = inputOptions.getTargetPath();
    if (!(sfs instanceof DistributedFileSystem) ||
        !(tfs instanceof DistributedFileSystem)) {
      throw new IllegalArgumentException("The FileSystems needs to" +
          " be DistributedFileSystem for using snapshot-diff-based distcp");
    }
    final DistributedFileSystem sourceFs = (DistributedFileSystem) sfs;
    final DistributedFileSystem targetFs= (DistributedFileSystem) tfs;

    if (!checkNoChange(inputOptions, targetFs, targetDir)) {
      inputOptions.setSourcePaths(Arrays.asList(getSourceSnapshotPath(sourceDir,
          inputOptions.getToSnapshot())));
      return false;
    }


hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/TestDistCpSync.java
  public void testFallback() throws Exception {
    Assert.assertFalse(DistCpSync.sync(options, conf));
    final Path spath = new Path(source,
        HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + "s2");
    Assert.assertEquals(spath, options.getSourcePaths().get(0));

    options.setSourcePaths(Arrays.asList(source));
    dfs.allowSnapshot(source);
    dfs.allowSnapshot(target);
    Assert.assertFalse(DistCpSync.sync(options, conf));
    Assert.assertEquals(spath, options.getSourcePaths().get(0));

    options.setSourcePaths(Arrays.asList(source));
    dfs.createSnapshot(source, "s1");
    dfs.createSnapshot(source, "s2");
    dfs.createSnapshot(target, "s1");
    final Path subTarget = new Path(target, "sub");
    dfs.mkdirs(subTarget);
    Assert.assertFalse(DistCpSync.sync(options, conf));
    Assert.assertEquals(spath, options.getSourcePaths().get(0));

    options.setSourcePaths(Arrays.asList(source));
    dfs.delete(subTarget, true);
    Assert.assertTrue(DistCpSync.sync(options, conf));
  }

