hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FilterFileSystem.java
      flags, bufferSize, replication, blockSize, progress, checksumOpt);
  }

  @Override
  protected RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f,
      final PathFilter filter)
  throws FileNotFoundException, IOException {
    return fs.listLocatedStatus(f, filter);
  }


  @Override
  @Deprecated
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FilterFs.java
    return myFs.listStatus(f);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f)
      throws AccessControlException, FileNotFoundException,
             UnresolvedLinkException, IOException {
    checkPath(f);
    return myFs.listLocatedStatus(f);
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path)
    throws IOException {

