hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DistributedFileSystem.java
  }
  
  public void createEncryptionZone(final Path path, final String keyName)
    throws IOException {
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException,
          UnresolvedLinkException {
        dfs.createEncryptionZone(getPathName(p), keyName);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p) throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem) fs;
          myDfs.createEncryptionZone(p, keyName);
          return null;
        } else {
          throw new UnsupportedOperationException(
              "Cannot call createEncryptionZone"
                  + " on a symlink to a non-DistributedFileSystem: " + path
                  + " -> " + p);
        }
      }
    }.resolve(this, absF);
  }

  public EncryptionZone getEZForPath(final Path path)
          throws IOException {
    Preconditions.checkNotNull(path);
    Path absF = fixRelativePart(path);
    return new FileSystemLinkResolver<EncryptionZone>() {
      @Override
      public EncryptionZone doCall(final Path p) throws IOException,
          UnresolvedLinkException {
        return dfs.getEZForPath(getPathName(p));
      }

      @Override
      public EncryptionZone next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof DistributedFileSystem) {
          DistributedFileSystem myDfs = (DistributedFileSystem) fs;
          return myDfs.getEZForPath(p);
        } else {
          throw new UnsupportedOperationException(
              "Cannot call getEZForPath"
                  + " on a symlink to a non-DistributedFileSystem: " + path
                  + " -> " + p);
        }
      }
    }.resolve(this, absF);
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestEncryptionZones.java
        true, fs.getFileStatus(zoneFile).isEncrypted());
    DFSTestUtil.verifyFilesNotEqual(fs, zoneFile, rawFile, len);
  }

  @Test(timeout = 60000)
  public void testEncryptionZonesOnRelativePath() throws Exception {
    final int len = 8196;
    final Path baseDir = new Path("/somewhere/base");
    final Path zoneDir = new Path("zone");
    final Path zoneFile = new Path("file");
    fs.setWorkingDirectory(baseDir);
    fs.mkdirs(zoneDir);
    dfsAdmin.createEncryptionZone(zoneDir, TEST_KEY);
    DFSTestUtil.createFile(fs, zoneFile, len, (short) 1, 0xFEED);

    assertNumZones(1);
    assertZonePresent(TEST_KEY, "/somewhere/base/zone");

    assertEquals("Got unexpected ez path", "/somewhere/base/zone", dfsAdmin
        .getEncryptionZoneForPath(zoneDir).getPath().toString());
  }
}

