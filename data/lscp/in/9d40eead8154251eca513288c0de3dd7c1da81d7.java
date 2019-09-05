hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/MRAppMaster.java
  JobStateInternal forcedState = null;
  private final ScheduledExecutorService logSyncer;

  private long recoveredJobStartTime = -1L;
  private static boolean mainStarted = false;

  @VisibleForTesting

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/job/event/JobStartEvent.java
  long recoveredJobStartTime;

  public JobStartEvent(JobId jobID) {
    this(jobID, -1L);
  }

  public JobStartEvent(JobId jobID, long recoveredJobStartTime) {

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/job/impl/JobImpl.java
    @Override
    public void transition(JobImpl job, JobEvent event) {
      JobStartEvent jse = (JobStartEvent) event;
      if (jse.getRecoveredJobStartTime() != -1L) {
        job.startTime = jse.getRecoveredJobStartTime();
      } else {
        job.startTime = job.clock.getTime();

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/TestMRAppMaster.java

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.EventType;
import org.apache.hadoop.mapreduce.jobhistory.EventWriter;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEventHandler;
import org.apache.hadoop.mapreduce.jobhistory.JobInitedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobUnsuccessfulCompletionEvent;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEvent;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.AccessControlException;
    verifyFailedStatus((MRAppMasterTest)appMaster, "FAILED");
  }

  @Test
  public void testMRAppMasterJobLaunchTime() throws IOException,
      InterruptedException {
    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
    String containerIdStr = "container_1317529182569_0004_000002_1";
    String userName = "TestAppMasterUser";
    JobConf conf = new JobConf();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    conf.setInt(MRJobConfig.NUM_REDUCES, 0);
    conf.set(JHAdminConfig.MR_HS_JHIST_FORMAT, "json");
    ApplicationAttemptId applicationAttemptId = ConverterUtils
        .toApplicationAttemptId(applicationAttemptIdStr);
    JobId jobId = TypeConverter.toYarn(
        TypeConverter.fromYarn(applicationAttemptId.getApplicationId()));

    File dir = new File(MRApps.getStagingAreaDir(conf, userName).toString(),
        jobId.toString());
    dir.mkdirs();
    File historyFile = new File(JobHistoryUtils.getStagingJobHistoryFile(
        new Path(dir.toURI().toString()), jobId,
        (applicationAttemptId.getAttemptId() - 1)).toUri().getRawPath());
    historyFile.createNewFile();
    FSDataOutputStream out = new FSDataOutputStream(
        new FileOutputStream(historyFile), null);
    EventWriter writer = new EventWriter(out, EventWriter.WriteMode.JSON);
    writer.close();
    FileSystem fs = FileSystem.get(conf);
    JobSplitWriter.createSplitFiles(new Path(dir.getAbsolutePath()), conf,
        fs, new org.apache.hadoop.mapred.InputSplit[0]);
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    MRAppMasterTestLaunchTime appMaster =
        new MRAppMasterTestLaunchTime(applicationAttemptId, containerId,
            "host", -1, -1, System.currentTimeMillis());
    MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
    appMaster.stop();
    assertTrue("Job launch time should not be negative.",
            appMaster.jobLaunchTime.get() >= 0);
  }

  @Test
  public void testMRAppMasterSuccessLock() throws IOException,
      InterruptedException {
    return spyHistoryService;
  }
}

class MRAppMasterTestLaunchTime extends MRAppMasterTest {
  final AtomicLong jobLaunchTime = new AtomicLong(0L);
  public MRAppMasterTestLaunchTime(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String host, int port, int httpPort,
      long submitTime) {
    super(applicationAttemptId, containerId, host, port, httpPort,
        submitTime, false, false);
  }

  @Override
  protected EventHandler<CommitterEvent> createCommitterEventHandler(
      AppContext context, OutputCommitter committer) {
    return new CommitterEventHandler(context, committer,
        getRMHeartbeatHandler()) {
      @Override
      public void handle(CommitterEvent event) {
      }
    };
  }

  @Override
  protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
      AppContext context) {
    return new JobHistoryEventHandler(context, getStartCount()) {
      @Override
      public void handle(JobHistoryEvent event) {
        if (event.getHistoryEvent().getEventType() == EventType.JOB_INITED) {
          JobInitedEvent jie = (JobInitedEvent) event.getHistoryEvent();
          jobLaunchTime.set(jie.getLaunchTime());
        }
        super.handle(event);
      }
    };
  }
}
\No newline at end of file

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/EventWriter.java
import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class EventWriter {
  static final String VERSION = "Avro-Json";
  static final String VERSION_BINARY = "Avro-Binary";

    new SpecificDatumWriter<Event>(Event.class);
  private Encoder encoder;
  private static final Log LOG = LogFactory.getLog(EventWriter.class);

  public enum WriteMode { JSON, BINARY }
  private final WriteMode writeMode;
  private final boolean jsonOutput;  // Cache value while we have 2 modes

  @VisibleForTesting
  public EventWriter(FSDataOutputStream out, WriteMode mode)
      throws IOException {
    this.out = out;
    this.writeMode = mode;
    if (this.writeMode==WriteMode.JSON) {
    out.hflush();
  }

  @VisibleForTesting
  public void close() throws IOException {
    try {
      encoder.flush();
      out.close();

