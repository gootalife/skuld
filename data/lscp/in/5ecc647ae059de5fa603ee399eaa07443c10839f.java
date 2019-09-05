hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/filecache/ClientDistributedCacheManager.java
      Map<URI, FileStatus> statCache) throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);
    Path current = new Path(uri.getPath());
    current = fs.makeQualified(current);
    if (!checkPermissionOfOther(fs, current, FsAction.READ, statCache)) {
      return false;

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/test/java/org/apache/hadoop/mapreduce/filecache/TestClientDistributedCacheManager.java
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
      new File(System.getProperty("test.build.data", "/tmp")).toURI()
      .toString().replace(' ', '+');
  
  private static final String TEST_VISIBILITY_DIR =
      new File(TEST_ROOT_DIR, "TestCacheVisibility").toURI()
      .toString().replace(' ', '+');
  private FileSystem fs;
  private Path firstCacheFile;
  private Path secondCacheFile;
  private Path thirdCacheFile;
  private Configuration conf;
  
  @Before
    fs = FileSystem.get(conf);
    firstCacheFile = new Path(TEST_ROOT_DIR, "firstcachefile");
    secondCacheFile = new Path(TEST_ROOT_DIR, "secondcachefile");
    thirdCacheFile = new Path(TEST_VISIBILITY_DIR,"thirdCachefile");
    createTempFile(firstCacheFile, conf);
    createTempFile(secondCacheFile, conf);
    createTempFile(thirdCacheFile, conf);
  }
  
  @After
    if (!fs.delete(secondCacheFile, false)) {
      LOG.warn("Failed to delete secondcachefile");
    }
    if (!fs.delete(thirdCacheFile, false)) {
      LOG.warn("Failed to delete thirdCachefile");
    }
  }
  
  @Test
    Assert.assertEquals(expected, jobConf.get(MRJobConfig.CACHE_FILE_TIMESTAMPS));
  }
  
  @Test
  public void testDetermineCacheVisibilities() throws IOException {
    Path workingdir = new Path(TEST_VISIBILITY_DIR);
    fs.setWorkingDirectory(workingdir);
    fs.setPermission(workingdir, new FsPermission((short)00777));
    fs.setPermission(new Path(TEST_ROOT_DIR), new FsPermission((short)00700));
    Job job = Job.getInstance(conf);
    Path relativePath = new Path("thirdCachefile");
    job.addCacheFile(relativePath.toUri());
    Configuration jobConf = job.getConfiguration();

    Map<URI, FileStatus> statCache = new HashMap<URI, FileStatus>();
    ClientDistributedCacheManager.
        determineCacheVisibilities(jobConf, statCache);
    Assert.assertFalse(jobConf.
               getBoolean(MRJobConfig.CACHE_FILE_VISIBILITIES,true));
  }

  @SuppressWarnings("deprecation")
  void createTempFile(Path p, Configuration conf) throws IOException {
    SequenceFile.Writer writer = null;

