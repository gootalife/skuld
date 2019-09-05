hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileSystem.java
  public long getUsed() throws IOException{
    long used = 0;
    RemoteIterator<LocatedFileStatus> files = listFiles(new Path("/"), true);
    while (files.hasNext()) {
      used += files.next().getLen();
    }
    return used;
  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSShell.java
      cluster.shutdown();
    }
  }

  @Test(timeout = 30000)
  public void testTotalSizeOfAllFiles() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      FileSystem fs = cluster.getFileSystem();
      FSDataOutputStream File1 = fs.create(new Path("/File1"));
      File1.write("hi".getBytes());
      File1.close();
      FSDataOutputStream File2 = fs.create(new Path("/Folder1/File2"));
      File2.write("hi".getBytes());
      File2.close();
      assertEquals(4, fs.getUsed());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }

  private static void runCount(String path, long dirs, long files, FsShell shell
    ) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(); 

