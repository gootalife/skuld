hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/event/AsyncDispatcher.java
      synchronized (waitForDrained) {
        while (!drained && eventHandlingThread.isAlive()) {
          waitForDrained.wait(1000);
          LOG.info("Waiting for AsyncDispatcher to drain. Thread state is :" +
              eventHandlingThread.getState());
        }
      }
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/MockAM.java
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;
import org.junit.Assert;

public class MockAM {

  private static final Logger LOG = Logger.getLogger(MockAM.class);

  private volatile int responseId = 0;
  private final ApplicationAttemptId attemptId;
  private RMContext context;
  public void waitForState(RMAppAttemptState finalState) throws Exception {
    RMApp app = context.getRMApps().get(attemptId.getApplicationId());
    RMAppAttempt attempt = app.getRMAppAttempt(attemptId);
    final int timeoutMsecs = 40000;
    final int minWaitMsecs = 1000;
    final int waitMsPerLoop = 500;
    int loop = 0;
    while (!finalState.equals(attempt.getAppAttemptState())
        && waitMsPerLoop * loop < timeoutMsecs) {
      LOG.info("AppAttempt : " + attemptId + " State is : " +
          attempt.getAppAttemptState() + " Waiting for state : " +
          finalState);
      Thread.yield();
      Thread.sleep(waitMsPerLoop);
      loop++;
    }
    int waitedMsecs = waitMsPerLoop * loop;
    if (minWaitMsecs > waitedMsecs) {
      Thread.sleep(minWaitMsecs - waitedMsecs);
    }
    LOG.info("Attempt State is : " + attempt.getAppAttemptState());
    if (waitedMsecs >= timeoutMsecs) {
      Assert.fail("Attempt state is not correct (timedout): expected: "
          + finalState + " actual: " + attempt.getAppAttemptState());
    }
  }

  public RegisterApplicationMasterResponse registerAppAttempt()

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/MockRM.java
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
@SuppressWarnings("unchecked")
public class MockRM extends ResourceManager {

  static final Logger LOG = Logger.getLogger(MockRM.class);
  static final String ENABLE_WEBAPP = "mockrm.webapp.enabled";

  public MockRM() {
      throws Exception {
    RMApp app = getRMContext().getRMApps().get(appId);
    Assert.assertNotNull("app shouldn't be null", app);
    final int timeoutMsecs = 80000;
    final int waitMsPerLoop = 500;
    int loop = 0;
    while (!finalState.equals(app.getState()) &&
        ((waitMsPerLoop * loop) < timeoutMsecs)) {
      LOG.info("App : " + appId + " State is : " + app.getState() +
          " Waiting for state : " + finalState);
      Thread.yield();
      Thread.sleep(waitMsPerLoop);
      loop++;
    }
    int waitedMsecs = waitMsPerLoop * loop;
    LOG.info("App State is : " + app.getState());
    if (waitedMsecs >= timeoutMsecs) {
      Assert.fail("App state is not correct (timedout): expected: " +
          finalState + " actual: " + app.getState());
    }
  }
  
  public void waitForState(ApplicationAttemptId attemptId, 
    RMApp app = getRMContext().getRMApps().get(attemptId.getApplicationId());
    Assert.assertNotNull("app shouldn't be null", app);
    RMAppAttempt attempt = app.getRMAppAttempt(attemptId);
    final int timeoutMsecs = 40000;
    final int minWaitMsecs = 1000;
    final int waitMsPerLoop = 10;
    int loop = 0;
    while (!finalState.equals(attempt.getAppAttemptState())
        && waitMsPerLoop * loop < timeoutMsecs) {
      LOG.info("AppAttempt : " + attemptId + " State is : " +
          attempt.getAppAttemptState() + " Waiting for state : " + finalState);
      Thread.yield();
      Thread.sleep(waitMsPerLoop);
      loop++;
    }
    int waitedMsecs = waitMsPerLoop * loop;
    if (minWaitMsecs > waitedMsecs) {
      Thread.sleep(minWaitMsecs - waitedMsecs);
    }
    LOG.info("Attempt State is : " + attempt.getAppAttemptState());
    if (waitedMsecs >= timeoutMsecs) {
      Assert.fail("Attempt state is not correct (timedout): expected: "
          + finalState + " actual: " + attempt.getAppAttemptState());
    }
  }

  public void waitForContainerAllocated(MockNM nm, ContainerId containerId)

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestApplicationMasterService.java

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMRestart.java
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Supplier;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;

    rm3.waitForState(latestAppAttemptId, RMAppAttemptState.FAILED);
    rm3.waitForState(rmApp.getApplicationId(), RMAppState.ACCEPTED);
    final int maxRetry = 10;
    final RMApp rmAppForCheck = rmApp;
    GenericTestUtils.waitFor(
        new Supplier<Boolean>() {
          @Override
          public Boolean get() {
            return new Boolean(rmAppForCheck.getAppAttempts().size() == 4);
          }
        },
        100, maxRetry);
    Assert.assertEquals(RMAppAttemptState.FAILED,
        rmApp.getAppAttempts().get(latestAppAttemptId).getAppAttemptState());
    

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/applicationsmanager/TestAMRestart.java
    rm2.stop();
  }

  @Test (timeout = 120000)
  public void testRMAppAttemptFailuresValidityInterval() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        new MockNM("127.0.0.1:1234", 8000, rm1.getResourceTrackerService());
    nm1.registerNode();

    RMApp app = rm1.submitApp(200, 60000);
    
    MockAM am = MockRM.launchAM(app, rm1, nm1);
    rm1.waitForState(app.getApplicationId(), RMAppState.FAILED);

    ControlledClock clock = new ControlledClock(new SystemClock());
    RMAppImpl app1 = (RMAppImpl)rm1.submitApp(200, 10000);;
    app1.setSystemClock(clock);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    MockAM am2 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    am2.waitForState(RMAppAttemptState.RUNNING);

    clock.setTime(System.currentTimeMillis() + 10*1000);
    nm1.nodeHeartbeat(am2.getApplicationAttemptId(),
      1, ContainerState.COMPLETE);
    MockAM am4 =
        rm2.waitForNewAMToLaunchAndRegister(app1.getApplicationId(), 4, nm1);

    clock.setTime(System.currentTimeMillis() + 10*1000);
    nm1
      .nodeHeartbeat(am4.getApplicationAttemptId(), 1, ContainerState.COMPLETE);

