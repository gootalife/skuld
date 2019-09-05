hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/MRAppMaster.java
  private final ScheduledExecutorService logSyncer;

  private long recoveredJobStartTime = 0;
  private static boolean mainStarted = false;

  @VisibleForTesting
  protected AtomicBoolean successfullyUnregistered =
      clientService.stop();
    } catch (Throwable t) {
      LOG.warn("Graceful stop failed. Exiting.. ", t);
      exitMRAppMaster(1, t);
    }
    exitMRAppMaster(0, null);
  }

  private void exitMRAppMaster(int status, Throwable t) {
    if (!mainStarted) {
      ExitUtil.disableSystemExit();
    }
    try {
      if (t != null) {
        ExitUtil.terminate(status, t);
      } else {
        ExitUtil.terminate(status);
      }
    } catch (ExitUtil.ExitException ee) {
    }
  }

  private class JobFinishEventHandler implements EventHandler<JobFinishEvent> {

  public static void main(String[] args) {
    try {
      mainStarted = true;
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      String containerIdStr =
          System.getenv(Environment.CONTAINER_ID.name());

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/TestMRAppMaster.java
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;

  }

  @Test
  public void testMRAppMasterShutDownJob() throws Exception,
      InterruptedException {
    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
    String containerIdStr = "container_1317529182569_0004_000002_1";
    String userName = "TestAppMasterUser";
    ApplicationAttemptId applicationAttemptId = ConverterUtils
        .toApplicationAttemptId(applicationAttemptIdStr);
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    JobConf conf = new JobConf();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);

    File stagingDir =
        new File(MRApps.getStagingAreaDir(conf, userName).toString());
    stagingDir.mkdirs();
    MRAppMasterTest appMaster =
        spy(new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
            System.currentTimeMillis(), false, true));
    MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
    doReturn(conf).when(appMaster).getConfig();
    appMaster.isLastAMRetry = true;
    doNothing().when(appMaster).serviceStop();
    appMaster.shutDownJob();
    Assert.assertTrue("Expected shutDownJob to terminate.",
                      ExitUtil.terminateCalled());
    Assert.assertEquals("Expected shutDownJob to exit with status code of 0.",
        0, ExitUtil.getFirstExitException().status);

    ExitUtil.resetFirstExitException();
    String msg = "Injected Exception";
    doThrow(new RuntimeException(msg))
            .when(appMaster).notifyIsLastAMRetry(anyBoolean());
    appMaster.shutDownJob();
    assertTrue("Expected message from ExitUtil.ExitException to be " + msg,
        ExitUtil.getFirstExitException().getMessage().contains(msg));
    Assert.assertEquals("Expected shutDownJob to exit with status code of 1.",
        1, ExitUtil.getFirstExitException().status);
  }

  private void verifyFailedStatus(MRAppMasterTest appMaster,
      String expectedJobState) {
    ArgumentCaptor<JobHistoryEvent> captor = ArgumentCaptor

