hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/mover/Mover.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.HdfsConfiguration;
  private ExitStatus run() {
    try {
      init();
      return new Processor().processNamespace().getExitStatus();
    } catch (IllegalArgumentException e) {
      System.out.println(e + ".  Exiting ...");
      return ExitStatus.ILLEGAL_ARGUMENTS;
    private Result processNamespace() throws IOException {
      getSnapshottableDirs();
      Result result = new Result();
      for (Path target : targetPaths) {
        processPath(target.toUri().getPath(), result);
      }
      boolean hasFailed = Dispatcher.waitForMoveCompletion(storages.targets
        retryCount.set(0);
      }
      result.updateHasRemaining(hasFailed);
      return result;
    }

    private void processPath(String fullPath, Result result) {
      for (byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME;;) {
        final DirectoryListing children;
        try {
        } catch(IOException e) {
          LOG.warn("Failed to list directory " + fullPath
              + ". Ignore the directory and continue.", e);
          return;
        }
        if (children == null) {
          return;
        }
        for (HdfsFileStatus child : children.getPartialListing()) {
          processRecursively(fullPath, child, result);
        }
        if (children.hasMore()) {
          lastReturnedName = children.getLastName();
        } else {
          return;
        }
      }
    }

    private void processRecursively(String parent, HdfsFileStatus status,
        Result result) {
      String fullPath = status.getFullName(parent);
      if (status.isDir()) {
        if (!fullPath.endsWith(Path.SEPARATOR)) {
          fullPath = fullPath + Path.SEPARATOR;
        }

        processPath(fullPath, result);
        if (snapshottableDirs.contains(fullPath)) {
          final String dirSnapshot = fullPath + HdfsConstants.DOT_SNAPSHOT_DIR;
          processPath(dirSnapshot, result);
        }
      } else if (!status.isSymlink()) { // file
        try {
          if (!isSnapshotPathInCurrent(fullPath)) {
            processFile(fullPath, (HdfsLocatedFileStatus) status, result);
          }
        } catch (IOException e) {
          LOG.warn("Failed to check the status of " + parent
              + ". Ignore it and continue.", e);
        }
      }
    }

    private void processFile(String fullPath, HdfsLocatedFileStatus status,
        Result result) {
      final byte policyId = status.getStoragePolicy();
      if (policyId == HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
        return;
      }
      final BlockStoragePolicy policy = blockStoragePolicies[policyId];
      if (policy == null) {
        LOG.warn("Failed to get the storage policy of file " + fullPath);
        return;
      }
      final List<StorageType> types = policy.chooseStorageTypes(
          status.getReplication());

      final LocatedBlocks locatedBlocks = status.getBlockLocations();
      final boolean lastBlkComplete = locatedBlocks.isLastBlockComplete();
      List<LocatedBlock> lbs = locatedBlocks.getLocatedBlocks();
      for (int i = 0; i < lbs.size(); i++) {
            lb.getStorageTypes());
        if (!diff.removeOverlap(true)) {
          if (scheduleMoves4Block(diff, lb)) {
            result.updateHasRemaining(diff.existing.size() > 1
                && diff.expected.size() > 1);
            result.setNoBlockMoved(false);
          } else {
            result.updateHasRemaining(true);
          }
        }
      }
    }

    boolean scheduleMoves4Block(StorageTypeDiff diff, LocatedBlock lb) {
    }
  }

  private static class Result {

    private boolean hasRemaining;
    private boolean noBlockMoved;

    Result() {
      hasRemaining = false;
      noBlockMoved = true;
    }

    boolean isHasRemaining() {
      return hasRemaining;
    }

    boolean isNoBlockMoved() {
      return noBlockMoved;
    }

    void updateHasRemaining(boolean hasRemaining) {
      this.hasRemaining |= hasRemaining;
    }

    void setNoBlockMoved(boolean noBlockMoved) {
      this.noBlockMoved = noBlockMoved;
    }

    ExitStatus getExitStatus() {
      return !isHasRemaining() ? ExitStatus.SUCCESS
          : isNoBlockMoved() ? ExitStatus.NO_MOVE_BLOCK
              : ExitStatus.IN_PROGRESS;
    }

  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/mover/TestMover.java
    }
  }

  @Test(timeout = 300000)
  public void testMoveWhenStoragePolicyNotSatisfying() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .storageTypes(
            new StorageType[][] { { StorageType.DISK }, { StorageType.DISK },
                { StorageType.DISK } }).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testMoveWhenStoragePolicyNotSatisfying";
      final FSDataOutputStream out = dfs.create(new Path(file));
      out.writeChars("testMoveWhenStoragePolicyNotSatisfying");
      out.close();

      dfs.setStoragePolicy(new Path(file), "COLD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] { "-p", file.toString() });
      int exitcode = ExitStatus.NO_MOVE_BLOCK.getExitCode();
      Assert.assertEquals("Exit code should be " + exitcode, exitcode, rc);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testMoverFailedRetry() throws Exception {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/mover/TestStorageMover.java
        verify(true);

        setStoragePolicy();
        migrate(ExitStatus.SUCCESS);
        verify(true);
      } finally {
        if (shutdown) {
    void migrate(ExitStatus expectedExitCode) throws Exception {
      runMover(expectedExitCode);
      Thread.sleep(5000); // let the NN finish deletion
    }

      }
    }

    private void runMover(ExitStatus expectedExitCode) throws Exception {
      Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
      Map<URI, List<Path>> nnMap = Maps.newHashMap();
      for (URI nn : namenodes) {
        nnMap.put(nn, null);
      }
      int result = Mover.run(nnMap, conf);
      Assert.assertEquals(expectedExitCode.getExitCode(), result);
    }

    private void verifyNamespace() throws Exception {
    try {
      banner("start data migration");
      test.setStoragePolicy(); // set /foo to COLD
      test.migrate(ExitStatus.SUCCESS);

      LocatedBlocks lbs = test.dfs.getClient().getLocatedBlocks(
    try {
      test.runBasicTest(false);
      pathPolicyMap.moveAround(test.dfs);
      test.migrate(ExitStatus.SUCCESS);

      test.verify(true);
    } finally {
      final Path file1 = new Path(pathPolicyMap.hot, "file1");
      test.dfs.rename(file1, pathPolicyMap.warm);
      test.migrate(ExitStatus.NO_MOVE_BLOCK);
      test.verifyFile(new Path(pathPolicyMap.warm, "file1"), WARM.getId());
    } finally {
      test.shutdownCluster();
      { //test move a cold file to warm
        final Path file1 = new Path(pathPolicyMap.cold, "file1");
        test.dfs.rename(file1, pathPolicyMap.warm);
        test.migrate(ExitStatus.SUCCESS);
        test.verify(true);
      }
    } finally {

