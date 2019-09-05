hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/client/ServerProxy.java
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.exceptions.NMNotYetReadyException;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import com.google.common.base.Preconditions;
    exceptionToPolicyMap.put(UnknownHostException.class, retryPolicy);
    exceptionToPolicyMap.put(RetriableException.class, retryPolicy);
    exceptionToPolicyMap.put(SocketException.class, retryPolicy);
    exceptionToPolicyMap.put(NMNotYetReadyException.class, retryPolicy);

    return RetryPolicies.retryByException(RetryPolicies.TRY_ONCE_THEN_FAIL,
      exceptionToPolicyMap);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/TestNMProxy.java
  }

  int retryCount = 0;
  boolean shouldThrowNMNotYetReadyException = false;

  @Before
  public void setUp() throws Exception {
          StartContainersRequest requests) throws YarnException, IOException {
        if (retryCount < 5) {
          retryCount++;
          if (shouldThrowNMNotYetReadyException) {
            containerManager.setBlockNewContainerRequests(true);
          } else {
            throw new java.net.ConnectException("start container exception");
          }
        } else {
          containerManager.setBlockNewContainerRequests(false);
        }
        return super.startContainers(requests);
      }

        NMProxy.createNMProxy(conf, ContainerManagementProtocol.class, ugi,
          YarnRPC.create(conf), address);

    retryCount = 0;
    shouldThrowNMNotYetReadyException = false;
    proxy.startContainers(allRequests);
    Assert.assertEquals(5, retryCount);

    retryCount = 0;
    shouldThrowNMNotYetReadyException = false;
    proxy.stopContainers(Records.newRecord(StopContainersRequest.class));
    Assert.assertEquals(5, retryCount);

    retryCount = 0;
    shouldThrowNMNotYetReadyException = false;
    proxy.getContainerStatuses(Records
      .newRecord(GetContainerStatusesRequest.class));
    Assert.assertEquals(5, retryCount);

    retryCount = 0;
    shouldThrowNMNotYetReadyException = true;
    proxy.startContainers(allRequests);
    Assert.assertEquals(5, retryCount);
  }
}

