hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/SecondaryNameNode.java
      System.exit(0);
    }

    try {
      StringUtils.startupShutdownMessage(SecondaryNameNode.class, argv, LOG);
      Configuration tconf = new HdfsConfiguration();
      SecondaryNameNode secondary = null;
      secondary = new SecondaryNameNode(tconf, opts);

      if (opts != null && opts.getCommand() != null) {
        int ret = secondary.processStartupCommand(opts);
        secondary.startCheckpointThread();
        secondary.join();
      }
    } catch (Throwable e) {
      LOG.fatal("Failed to start secondary namenode", e);
      terminate(1);
    }
  }

  public void startCheckpointThread() {
    Preconditions.checkState(checkpointThread == null,

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestStartup.java
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.ExitUtil.ExitException;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.junit.After;

  @Before
  public void setUp() throws Exception {
    ExitUtil.disableSystemExit();
    ExitUtil.resetFirstExitException();
    config = new HdfsConfiguration();
    hdfsDir = new File(MiniDFSCluster.getBaseDirectory());

    }
  }

  @Test(timeout = 30000)
  public void testSNNStartupWithRuntimeException() throws Exception {
    String[] argv = new String[] { "-checkpoint" };
    try {
      SecondaryNameNode.main(argv);
      fail("Failed to handle runtime exceptions during SNN startup!");
    } catch (ExitException ee) {
      GenericTestUtils.assertExceptionContains("ExitException", ee);
      assertTrue("Didn't termiated properly ", ExitUtil.terminateCalled());
    }
  }

  @Test
  public void testCompression() throws IOException {
    LOG.info("Test compressing image.");

