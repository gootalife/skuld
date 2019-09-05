hadoop-tools/hadoop-extras/src/main/java/org/apache/hadoop/tools/DistCpV1.java
@Deprecated
public class DistCpV1 implements Tool {
  public static final Log LOG = LogFactory.getLog(DistCpV1.class);


hadoop-tools/hadoop-extras/src/main/java/org/apache/hadoop/tools/Logalyzer.java
@Deprecated
public class Logalyzer {
  private static Configuration fsConfig = new Configuration();
  @SuppressWarnings("deprecation")
  public void	
    doArchive(String logListURI, String archiveDirectory)
    throws IOException

hadoop-tools/hadoop-extras/src/test/java/org/apache/hadoop/tools/TestCopyFiles.java
@SuppressWarnings("deprecation")
public class TestCopyFiles extends TestCase {
  {
    ((Log4JLogger)LogFactory.getLog("org.apache.hadoop.hdfs.StateChange")
  }
  
  @SuppressWarnings("deprecation")
  public void testCopyFromLocalToLocal() throws Exception {
    Configuration conf = new Configuration();
    FileSystem localfs = FileSystem.get(LOCAL_FS, conf);
  }
  
  @SuppressWarnings("deprecation")
  public void testCopyFromDfsToDfs() throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;
  }

  @SuppressWarnings("deprecation")
  public void testEmptyDir() throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;
  }
  
  @SuppressWarnings("deprecation")
  public void testCopyFromLocalToDfs() throws Exception {
    MiniDFSCluster cluster = null;
    try {
  }

  @SuppressWarnings("deprecation")
  public void testCopyFromDfsToLocal() throws Exception {
    MiniDFSCluster cluster = null;
    try {
    }
  }

  @SuppressWarnings("deprecation")
  public void testCopyDfsToDfsUpdateOverwrite() throws Exception {
    MiniDFSCluster cluster = null;
    try {
    }
  }

  @SuppressWarnings("deprecation")
  public void testCopyDfsToDfsUpdateWithSkipCRC() throws Exception {
    MiniDFSCluster cluster = null;
    try {
    }
  }

  @SuppressWarnings("deprecation")
  public void testCopyDuplication() throws Exception {
    final FileSystem localfs = FileSystem.get(LOCAL_FS, new Configuration());
    try {    
    }
  }

  @SuppressWarnings("deprecation")
  public void testCopySingleFile() throws Exception {
    FileSystem fs = FileSystem.get(LOCAL_FS, new Configuration());
    Path root = new Path(TEST_ROOT_DIR+"/srcdat");
  }

  @SuppressWarnings("deprecation")
  public void testBasedir() throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;
    }
  }

  @SuppressWarnings("deprecation")
  public void testPreserveOption() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    }
  }

  @SuppressWarnings("deprecation")
  public void testMapCount() throws Exception {
    String namenode = null;
    MiniDFSCluster dfs = null;
    }
  }

  @SuppressWarnings("deprecation")
  public void testLimits() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
  }

  @SuppressWarnings("deprecation")
  public void testDelete() throws Exception {
    final Configuration conf = new Configuration();
    conf.setInt("fs.trash.interval", 60);
  @SuppressWarnings("deprecation")
  public void testDeleteLocal() throws Exception {
    MiniDFSCluster cluster = null;
    try {
  }

  @SuppressWarnings("deprecation")
  public void testGlobbing() throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;

hadoop-tools/hadoop-extras/src/test/java/org/apache/hadoop/tools/TestLogalyzer.java
      + File.separator + "out");

  @Test
  @SuppressWarnings("deprecation")
  public void testLogalyzer() throws Exception {
    Path f = createLogFile();


