hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/HistoryFileManager.java
    private JobIndexInfo jobIndexInfo;
    private HistoryInfoState state;

    @VisibleForTesting
    protected HistoryFileInfo(Path historyFile, Path confFile,
        Path summaryFile, JobIndexInfo jobIndexInfo, boolean isInDone) {
      this.historyFile = historyFile;
      this.confFile = confFile;
      this.summaryFile = summaryFile;
             + " historyFile = " + historyFile;
    }

    @VisibleForTesting
    synchronized void moveToDone() throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("moveToDone: " + historyFile);
      }
          paths.add(confFile);
        }

        if (summaryFile == null || !intermediateDoneDirFc.util().exists(
            summaryFile)) {
          LOG.info("No summary file for job: " + jobId);
        } else {
          String jobSummaryString = getJobSummary(intermediateDoneDirFc,

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/test/java/org/apache/hadoop/mapreduce/v2/hs/TestHistoryFileManager.java
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.test.CoreTestDriver;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

  }

  @Test
  public void testHistoryFileInfoSummaryFileNotExist() throws Exception {
    HistoryFileManagerTest hmTest = new HistoryFileManagerTest();
    String job = "job_1410889000000_123456";
    Path summaryFile = new Path(job + ".summary");
    JobIndexInfo jobIndexInfo = new JobIndexInfo();
    jobIndexInfo.setJobId(TypeConverter.toYarn(JobID.forName(job)));
    Configuration conf = dfsCluster.getConfiguration(0);
    conf.set(JHAdminConfig.MR_HISTORY_DONE_DIR,
        "/" + UUID.randomUUID());
    conf.set(JHAdminConfig.MR_HISTORY_INTERMEDIATE_DONE_DIR,
        "/" + UUID.randomUUID());
    hmTest.serviceInit(conf);
    HistoryFileInfo info = hmTest.getHistoryFileInfo(null, null,
        summaryFile, jobIndexInfo, false);
    info.moveToDone();
    Assert.assertFalse(info.didMoveFail());
  }

  static class HistoryFileManagerTest extends HistoryFileManager {
    public HistoryFileManagerTest() {
      super();
    }
    public HistoryFileInfo getHistoryFileInfo(Path historyFile,
        Path confFile, Path summaryFile, JobIndexInfo jobIndexInfo,
        boolean isInDone) {
      return new HistoryFileInfo(historyFile, confFile, summaryFile,
          jobIndexInfo, isInDone);
    }
  }
}

