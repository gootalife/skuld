hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/AbstractFileSystem.java
        + " doesn't support setStoragePolicy");
  }

  public BlockStoragePolicySpi getStoragePolicy(final Path src)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support getStoragePolicy");
  }


hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileContext.java
    }.resolve(this, absF);
  }

  public BlockStoragePolicySpi getStoragePolicy(Path path) throws IOException {
    final Path absF = fixRelativePart(path);
    return new FSLinkResolver<BlockStoragePolicySpi>() {
      @Override
      public BlockStoragePolicySpi next(final AbstractFileSystem fs,
          final Path p)
          throws IOException {
        return fs.getStoragePolicy(p);
      }
    }.resolve(this, absF);
  }


hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileSystem.java
        + " doesn't support setStoragePolicy");
  }

  public BlockStoragePolicySpi getStoragePolicy(final Path src)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support getStoragePolicy");
  }


hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FilterFileSystem.java
    fs.setStoragePolicy(src, policyName);
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(final Path src)
      throws IOException {
    return fs.getStoragePolicy(src);
  }

  @Override
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FilterFs.java
    myFs.setStoragePolicy(path, policyName);
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(final Path src)
      throws IOException {
    return myFs.getStoragePolicy(src);
  }

  @Override
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/viewfs/ChRootedFs.java
    myFs.setStoragePolicy(fullPath(path), policyName);
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(final Path src)
      throws IOException {
    return myFs.getStoragePolicy(src);
  }

  @Override
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/viewfs/ViewFs.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
    res.targetFileSystem.setStoragePolicy(res.remainingPath, policyName);
  }

  public BlockStoragePolicySpi getStoragePolicy(final Path src)
      throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(src), true);
    return res.targetFileSystem.getStoragePolicy(res.remainingPath);
  }


hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/TestHarFileSystem.java
    public void setStoragePolicy(Path src, String policyName)
        throws IOException;

    public BlockStoragePolicySpi getStoragePolicy(final Path src)
        throws IOException;

    public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
        throws IOException;
  }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/fs/Hdfs.java
    dfs.setStoragePolicy(getUriPath(path), policyName);
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(Path src) throws IOException {
    return dfs.getStoragePolicy(getUriPath(src));
  }

  @Override
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
    }
  }

  public BlockStoragePolicy getStoragePolicy(String path) throws IOException {
    HdfsFileStatus status = getFileInfo(path);
    if (status == null) {
      throw new FileNotFoundException("File does not exist: " + path);
    }
    byte storagePolicyId = status.getStoragePolicy();
    BlockStoragePolicy[] policies = getStoragePolicies();
    for (BlockStoragePolicy policy : policies) {
      if (policy.getId() == storagePolicyId) {
        return policy;
      }
    }
    return null;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DistributedFileSystem.java
    }.resolve(this, absF);
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(Path path) throws IOException {
    statistics.incrementReadOps(1);
    Path absF = fixRelativePart(path);

    return new FileSystemLinkResolver<BlockStoragePolicySpi>() {
      @Override
      public BlockStoragePolicySpi doCall(final Path p) throws IOException {
        return getClient().getStoragePolicy(getPathName(p));
      }

      @Override
      public BlockStoragePolicySpi next(final FileSystem fs, final Path p)
          throws IOException, UnresolvedLinkException {
        return fs.getStoragePolicy(p);
      }
    }.resolve(this, absF);
  }

  @Override
  public Collection<BlockStoragePolicy> getAllStoragePolicies()
      throws IOException {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockStoragePolicy.java
        GenericTestUtils.assertExceptionContains(invalidPath.toString(), e);
      }

      try {
        fs.getStoragePolicy(invalidPath);
        Assert.fail("Should throw a FileNotFoundException");
      } catch (FileNotFoundException e) {
        GenericTestUtils.assertExceptionContains(invalidPath.toString(), e);
      }

      fs.setStoragePolicy(fooFile, HdfsServerConstants.COLD_STORAGE_POLICY_NAME);
      fs.setStoragePolicy(barDir, HdfsServerConstants.WARM_STORAGE_POLICY_NAME);
      fs.setStoragePolicy(barFile2, HdfsServerConstants.HOT_STORAGE_POLICY_NAME);
      Assert.assertEquals("File storage policy should be COLD",
          HdfsServerConstants.COLD_STORAGE_POLICY_NAME,
          fs.getStoragePolicy(fooFile).getName());
      Assert.assertEquals("File storage policy should be WARM",
          HdfsServerConstants.WARM_STORAGE_POLICY_NAME,
          fs.getStoragePolicy(barDir).getName());
      Assert.assertEquals("File storage policy should be HOT",
          HdfsServerConstants.HOT_STORAGE_POLICY_NAME,
          fs.getStoragePolicy(barFile2).getName());

      dirList = fs.getClient().listPaths(dir.toString(),
          HdfsFileStatus.EMPTY_NAME).getPartialListing();

