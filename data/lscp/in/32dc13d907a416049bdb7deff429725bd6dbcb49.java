hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/local/LocalContainerAllocator.java

package org.apache.hadoop.mapreduce.v2.app.local;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMCommunicator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;

        AllocateRequest.newInstance(this.lastResponseID,
          super.getApplicationProgress(), new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>(), null);
    AllocateResponse allocateResponse = null;
    try {
      allocateResponse = scheduler.allocate(allocateRequest);
      retrystartTime = System.currentTimeMillis();
    } catch (ApplicationAttemptNotFoundException e) {
      throw e;
    }

    if (allocateResponse != null) {
      this.lastResponseID = allocateResponse.getResponseId();
      Token token = allocateResponse.getAMRMToken();
      if (token != null) {
        updateAMRMToken(token);
      }
    }
  }

  private void updateAMRMToken(Token token) throws IOException {
    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> amrmToken =
        new org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>(token
          .getIdentifier().array(), token.getPassword().array(), new Text(
          token.getKind()), new Text(token.getService()));
    UserGroupInformation currentUGI = UserGroupInformation.getCurrentUser();
    currentUGI.addToken(amrmToken);
    amrmToken.setService(ClientRMProxy.getAMRMTokenService(getConfig()));
  }

  @SuppressWarnings("unchecked")

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/local/TestLocalContainerAllocator.java
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.ClusterInfo;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerResourceDecrease;
import org.apache.hadoop.yarn.api.records.ContainerResourceIncrease;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

  public void testRMConnectionRetry() throws Exception {
    ApplicationMasterProtocol mockScheduler =
        mock(ApplicationMasterProtocol.class);
    when(mockScheduler.allocate(isA(AllocateRequest.class)))
      .thenThrow(RPCUtil.getRemoteException(new IOException("forcefail")));
    Configuration conf = new Configuration();
    LocalContainerAllocator lca =
        new StubbedLocalContainerAllocator(mockScheduler);
    lca.init(conf);
    lca.start();
    try {

    conf.setLong(MRJobConfig.MR_AM_TO_RM_WAIT_INTERVAL_MS, 0);
    lca = new StubbedLocalContainerAllocator(mockScheduler);
    lca.init(conf);
    lca.start();
    try {
    }
  }

  @Test
  public void testAllocResponseId() throws Exception {
    ApplicationMasterProtocol scheduler = new MockScheduler();
    Configuration conf = new Configuration();
    LocalContainerAllocator lca =
        new StubbedLocalContainerAllocator(scheduler);
    lca.init(conf);
    lca.start();

    lca.heartbeat();
    lca.heartbeat();
    lca.close();
  }

  @Test
  public void testAMRMTokenUpdate() throws Exception {
    Configuration conf = new Configuration();
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(1, 1), 1);
    AMRMTokenIdentifier oldTokenId = new AMRMTokenIdentifier(attemptId, 1);
    AMRMTokenIdentifier newTokenId = new AMRMTokenIdentifier(attemptId, 2);
    Token<AMRMTokenIdentifier> oldToken = new Token<AMRMTokenIdentifier>(
        oldTokenId.getBytes(), "oldpassword".getBytes(), oldTokenId.getKind(),
        new Text());
    Token<AMRMTokenIdentifier> newToken = new Token<AMRMTokenIdentifier>(
        newTokenId.getBytes(), "newpassword".getBytes(), newTokenId.getKind(),
        new Text());

    MockScheduler scheduler = new MockScheduler();
    scheduler.amToken = newToken;

    final LocalContainerAllocator lca =
        new StubbedLocalContainerAllocator(scheduler);
    lca.init(conf);
    lca.start();

    UserGroupInformation testUgi = UserGroupInformation.createUserForTesting(
        "someuser", new String[0]);
    testUgi.addToken(oldToken);
    testUgi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            lca.heartbeat();
            return null;
          }
    });
    lca.close();

    int tokenCount = 0;
    Token<? extends TokenIdentifier> ugiToken = null;
    for (Token<? extends TokenIdentifier> token : testUgi.getTokens()) {
      if (AMRMTokenIdentifier.KIND_NAME.equals(token.getKind())) {
        ugiToken = token;
        ++tokenCount;
      }
    }

    Assert.assertEquals("too many AMRM tokens", 1, tokenCount);
    Assert.assertArrayEquals("token identifier not updated",
        newToken.getIdentifier(), ugiToken.getIdentifier());
    Assert.assertArrayEquals("token password not updated",
        newToken.getPassword(), ugiToken.getPassword());
    Assert.assertEquals("AMRM token service not updated",
        new Text(ClientRMProxy.getAMRMTokenService(conf)),
        ugiToken.getService());
  }

  private static class StubbedLocalContainerAllocator
    extends LocalContainerAllocator {
    private ApplicationMasterProtocol scheduler;

    public StubbedLocalContainerAllocator(ApplicationMasterProtocol scheduler) {
      super(mock(ClientService.class), createAppContext(),
          "nmhost", 1, 2, null);
      this.scheduler = scheduler;
    }

    @Override

    @Override
    protected ApplicationMasterProtocol createSchedulerProxy() {
      return scheduler;
    }

      return ctx;
    }
  }

  private static class MockScheduler implements ApplicationMasterProtocol {
    int responseId = 0;
    Token<AMRMTokenIdentifier> amToken = null;

    @Override
    public RegisterApplicationMasterResponse registerApplicationMaster(
        RegisterApplicationMasterRequest request) throws YarnException,
        IOException {
      return null;
    }

    @Override
    public FinishApplicationMasterResponse finishApplicationMaster(
        FinishApplicationMasterRequest request) throws YarnException,
        IOException {
      return null;
    }

    @Override
    public AllocateResponse allocate(AllocateRequest request)
        throws YarnException, IOException {
      Assert.assertEquals("response ID mismatch",
          responseId, request.getResponseId());
      ++responseId;
      org.apache.hadoop.yarn.api.records.Token yarnToken = null;
      if (amToken != null) {
        yarnToken = org.apache.hadoop.yarn.api.records.Token.newInstance(
            amToken.getIdentifier(), amToken.getKind().toString(),
            amToken.getPassword(), amToken.getService().toString());
      }
      return AllocateResponse.newInstance(responseId,
          Collections.<ContainerStatus>emptyList(),
          Collections.<Container>emptyList(),
          Collections.<NodeReport>emptyList(),
          Resources.none(), null, 1, null,
          Collections.<NMToken>emptyList(),
          yarnToken,
          Collections.<ContainerResourceIncrease>emptyList(),
          Collections.<ContainerResourceDecrease>emptyList());
    }
  }
}

