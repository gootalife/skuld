hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/jobhistory/JobHistoryEventHandler.java
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.jobhistory.FileNameIndexUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.security.UserGroupInformation;
      if (mi.getHistoryFile() != null) {
        Path historyFile = mi.getHistoryFile();
        Path qualifiedLogFile = stagingDirFS.makeQualified(historyFile);
        int jobNameLimit =
            getConfig().getInt(JHAdminConfig.MR_HS_JOBNAME_LIMIT,
            JHAdminConfig.DEFAULT_MR_HS_JOBNAME_LIMIT);
        String doneJobHistoryFileName =
            getTempFileName(FileNameIndexUtils.getDoneFileName(mi
                .getJobIndexInfo(), jobNameLimit));
        qualifiedDoneFile =
            doneDirFS.makeQualified(new Path(doneDirPrefixPath,
                doneJobHistoryFileName));

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-common/src/main/java/org/apache/hadoop/mapreduce/v2/jobhistory/FileNameIndexUtils.java

public class FileNameIndexUtils {

  static final String DELIMITER = "-";
  static final String DELIMITER_ESCAPE = "%2D";
  public static String getDoneFileName(JobIndexInfo indexInfo) throws IOException {
    return getDoneFileName(indexInfo,
        JHAdminConfig.DEFAULT_MR_HS_JOBNAME_LIMIT);
  }

  public static String getDoneFileName(JobIndexInfo indexInfo,
      int jobNameLimit) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(escapeDelimiters(TypeConverter.fromYarn(indexInfo.getJobId()).toString()));
    sb.append(DELIMITER);
    
    sb.append(escapeDelimiters(trimJobName(
        getJobName(indexInfo), jobNameLimit)));
    sb.append(DELIMITER);
    
  private static String trimJobName(String jobName, int jobNameLimit) {
    if (jobName.length() > jobNameLimit) {
      jobName = jobName.substring(0, jobNameLimit);
    }
    return jobName;
  }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-common/src/main/java/org/apache/hadoop/mapreduce/v2/jobhistory/JHAdminConfig.java
  public static boolean DEFAULT_MR_HISTORY_MINICLUSTER_FIXED_PORTS = false;

  public static final String MR_HS_JOBNAME_LIMIT = MR_HISTORY_PREFIX
      + "jobname.limit";
  public static final int DEFAULT_MR_HS_JOBNAME_LIMIT = 50;

}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-common/src/test/java/org/apache/hadoop/mapreduce/v2/jobhistory/TestFileNameIndexUtils.java
        jobHistoryFile.contains(USER_NAME_WITH_DELIMITER_ESCAPE));
  }

  @Test
  public void testTrimJobName() throws IOException {
    int jobNameTrimLength = 5;
    JobIndexInfo info = new JobIndexInfo();
    JobID oldJobId = JobID.forName(JOB_ID);
    JobId jobId = TypeConverter.toYarn(oldJobId);
    info.setJobId(jobId);
    info.setSubmitTime(Long.parseLong(SUBMIT_TIME));
    info.setUser(USER_NAME);
    info.setJobName(JOB_NAME);
    info.setFinishTime(Long.parseLong(FINISH_TIME));
    info.setNumMaps(Integer.parseInt(NUM_MAPS));
    info.setNumReduces(Integer.parseInt(NUM_REDUCES));
    info.setJobStatus(JOB_STATUS);
    info.setQueueName(QUEUE_NAME);
    info.setJobStartTime(Long.parseLong(JOB_START_TIME));

    String jobHistoryFile =
         FileNameIndexUtils.getDoneFileName(info, jobNameTrimLength);
    JobIndexInfo parsedInfo = FileNameIndexUtils.getIndexInfo(jobHistoryFile);

    Assert.assertEquals("Job name did not get trimmed correctly",
        info.getJobName().substring(0, jobNameTrimLength),
        parsedInfo.getJobName());
  }

  @Test
  public void testUserNamePercentDecoding() throws IOException {
    String jobHistoryFile = String.format(JOB_HISTORY_FILE_FORMATTER,

