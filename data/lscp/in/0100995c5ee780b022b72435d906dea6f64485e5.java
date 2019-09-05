hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/client/ServerProxy.java
    long maxWaitTime = conf.getLong(maxWaitTimeStr, defMaxWaitTime);
    long retryIntervalMS =
        conf.getLong(connectRetryIntervalStr, defRetryInterval);

    Preconditions.checkArgument((maxWaitTime == -1 || maxWaitTime > 0),
        "Invalid Configuration. " + maxWaitTimeStr + " should be either"
            + " positive value or -1.");
    Preconditions.checkArgument(retryIntervalMS > 0, "Invalid Configuration. "
        + connectRetryIntervalStr + "should be a positive value.");

    RetryPolicy retryPolicy = null;
    if (maxWaitTime == -1) {
      retryPolicy = RetryPolicies.RETRY_FOREVER;
    } else {
      retryPolicy =
          RetryPolicies.retryUpToMaximumTimeWithFixedSleep(maxWaitTime,
              retryIntervalMS, TimeUnit.MILLISECONDS);
    }

    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
        new HashMap<Class<? extends Exception>, RetryPolicy>();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/TestNMProxy.java
import java.net.InetSocketAddress;

import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.retry.UnreliableInterface;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

  @Before
  public void setUp() throws Exception {
    containerManager.start();
    containerManager.setBlockNewContainerRequests(false);
  }

  @Override
          if (shouldThrowNMNotYetReadyException) {
            containerManager.setBlockNewContainerRequests(true);
          } else {
            if (isRetryPolicyRetryForEver()) {
              throw new IOException(
                  new UnreliableInterface.UnreliableException());
            } else {
              throw new java.net.ConnectException("start container exception");
            }
          }
        } else {
          containerManager.setBlockNewContainerRequests(false);
        return super.startContainers(requests);
      }

      private boolean isRetryPolicyRetryForEver() {
        return conf.getLong(
            YarnConfiguration.CLIENT_NM_CONNECT_MAX_WAIT_MS, 1000) == -1;
      }

      @Override
      public StopContainersResponse stopContainers(
          StopContainersRequest requests) throws YarnException, IOException {

  @Test(timeout = 20000)
   public void testNMProxyRetry() throws Exception {
     conf.setLong(YarnConfiguration.CLIENT_NM_CONNECT_MAX_WAIT_MS, 10000);
     conf.setLong(YarnConfiguration.CLIENT_NM_CONNECT_RETRY_INTERVAL_MS, 100);
     StartContainersRequest allRequests =
         Records.newRecord(StartContainersRequest.class);

    ContainerManagementProtocol proxy = getNMProxy();

    retryCount = 0;
    shouldThrowNMNotYetReadyException = false;
    proxy.startContainers(allRequests);
    Assert.assertEquals(5, retryCount);
  }

  @Test(timeout = 20000, expected = IOException.class)
  public void testShouldNotRetryForeverForNonNetworkExceptionsOnNMConnections()
      throws Exception {
    conf.setLong(YarnConfiguration.CLIENT_NM_CONNECT_MAX_WAIT_MS, -1);
    StartContainersRequest allRequests =
        Records.newRecord(StartContainersRequest.class);

    ContainerManagementProtocol proxy = getNMProxy();

    shouldThrowNMNotYetReadyException = false;
    retryCount = 0;
    proxy.startContainers(allRequests);
  }

  private ContainerManagementProtocol getNMProxy() {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);

    org.apache.hadoop.yarn.api.records.Token nmToken =
        context.getNMTokenSecretManager().createNMToken(attemptId,
            context.getNodeId(), user);
    final InetSocketAddress address =
        conf.getSocketAddr(YarnConfiguration.NM_BIND_HOST,
            YarnConfiguration.NM_ADDRESS, YarnConfiguration.DEFAULT_NM_ADDRESS,
            YarnConfiguration.DEFAULT_NM_PORT);
    Token<NMTokenIdentifier> token =
        ConverterUtils.convertFromYarn(nmToken,
            SecurityUtil.buildTokenService(address));
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    ugi.addToken(token);
    return NMProxy.createNMProxy(conf, ContainerManagementProtocol.class, ugi,
        YarnRPC.create(conf), address);
  }
}

