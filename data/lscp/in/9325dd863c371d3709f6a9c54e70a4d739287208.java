hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/RMAppManager.java
    RMAppImpl application =
        createAndPopulateNewRMApp(submissionContext, submitTime, user, false);
    ApplicationId appId = submissionContext.getApplicationId();
    Credentials credentials = null;
    try {
      credentials = parseCredentials(submissionContext);
      if (UserGroupInformation.isSecurityEnabled()) {
        this.rmContext.getDelegationTokenRenewer().addApplicationAsync(appId,
            credentials, submissionContext.getCancelTokensWhenComplete(),
            application.getUser());
      } else {
        this.rmContext.getDispatcher().getEventHandler()
            .handle(new RMAppEvent(applicationId, RMAppEventType.START));
      }
    } catch (Exception e) {
      LOG.warn("Unable to parse credentials.", e);
          .handle(new RMAppRejectedEvent(applicationId, e.getMessage()));
      throw RPCUtil.getRemoteException(e);
    }
  }

  protected void recoverApplication(ApplicationStateData appState,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/amlauncher/AMLauncher.java

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
    return container;
  }

  @Private
  @VisibleForTesting
  protected void setupTokens(
      ContainerLaunchContext container, ContainerId containerID)
      throws IOException {
    Map<String, String> environment = container.getEnvironment();

    Credentials credentials = new Credentials();
    DataInputByteBuffer dibb = new DataInputByteBuffer();
    ByteBuffer tokens = container.getTokens();
    if (tokens != null) {
      dibb.reset(tokens);
      credentials.readTokenStorageStream(dibb);
      tokens.rewind();
    }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestAppManager.java

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
        getAppEventType());
  }

  @Test
  public void testRMAppSubmitWithInvalidTokens() throws Exception {
    DataOutputBuffer dob = new DataOutputBuffer();
    ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0,
        dob.getLength());
    asContext.getAMContainerSpec().setTokens(securityTokens);
    try {
      appMonitor.submitApplication(asContext, "test");
      Assert.fail("Application submission should fail because" +
          " Tokens are invalid.");
    } catch (YarnException e) {
      Assert.assertTrue("The thrown exception is not" +
          " java.io.EOFException",
          e.getMessage().contains("java.io.EOFException"));
    }
    int timeoutSecs = 0;
    while ((getAppEventType() == RMAppEventType.KILL) &&
        timeoutSecs++ < 20) {
      Thread.sleep(1000);
    }
    Assert.assertEquals("app event type sent is wrong",
        RMAppEventType.APP_REJECTED, getAppEventType());
    asContext.getAMContainerSpec().setTokens(null);
  }

  @Test
  public void testRMAppSubmitWithValidTokens() throws Exception {
    DataOutputBuffer dob = new DataOutputBuffer();
    Credentials credentials = new Credentials();
    credentials.writeTokenStorageToStream(dob);
    ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0,
        dob.getLength());
    asContext.getAMContainerSpec().setTokens(securityTokens);
    appMonitor.submitApplication(asContext, "test");
    RMApp app = rmContext.getRMApps().get(appId);
    Assert.assertNotNull("app is null", app);
    Assert.assertEquals("app id doesn't match", appId,
        app.getApplicationId());
    Assert.assertEquals("app state doesn't match", RMAppState.NEW,
        app.getState());
    verify(metricsPublisher).appACLsUpdated(
        any(RMApp.class), any(String.class), anyLong());

    int timeoutSecs = 0;
    while ((getAppEventType() == RMAppEventType.KILL) &&
        timeoutSecs++ < 20) {
      Thread.sleep(1000);
    }
    Assert.assertEquals("app event type sent is wrong", RMAppEventType.START,
        getAppEventType());
    asContext.getAMContainerSpec().setTokens(null);
  }

  @Test (timeout = 30000)
  public void testRMAppSubmitMaxAppAttempts() throws Exception {
    int[] globalMaxAppAttempts = new int[] { 10, 1 };

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestApplicationMasterLauncher.java

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
    } catch (ApplicationAttemptNotFoundException e) {
    }
  }

  @Test
  public void testSetupTokens() throws Exception {
    MockRM rm = new MockRM();
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 5000);
    RMApp app = rm.submitApp(2000);
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MyAMLauncher launcher = new MyAMLauncher(rm.getRMContext(),
        attempt, AMLauncherEventType.LAUNCH, rm.getConfig());
    DataOutputBuffer dob = new DataOutputBuffer();
    Credentials ts = new Credentials();
    ts.writeTokenStorageToStream(dob);
    ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(),
        0, dob.getLength());
    ContainerLaunchContext amContainer =
        ContainerLaunchContext.newInstance(null, null,
            null, null, securityTokens, null);
    ContainerId containerId = ContainerId.newContainerId(
        attempt.getAppAttemptId(), 0L);

    try {
      launcher.setupTokens(amContainer, containerId);
    } catch (Exception e) {
    }
    try {
      launcher.setupTokens(amContainer, containerId);
    } catch (java.io.EOFException e) {
      Assert.fail("EOFException should not happen.");
    }
  }

  static class MyAMLauncher extends AMLauncher {
    int count;
    public MyAMLauncher(RMContext rmContext, RMAppAttempt application,
        AMLauncherEventType eventType, Configuration conf) {
      super(rmContext, application, eventType, conf);
      count = 0;
    }

    protected org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>
        createAndSetAMRMToken() {
      count++;
      if (count == 1) {
        throw new RuntimeException("createAndSetAMRMToken failure");
      }
      return null;
    }

    protected void setupTokens(ContainerLaunchContext container,
        ContainerId containerID) throws IOException {
      super.setupTokens(container, containerID);
    }
  }
}

