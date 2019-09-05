hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/ContainerManagerImpl.java

  private static final Log LOG = LogFactory.getLog(ContainerManagerImpl.class);

  static final String INVALID_NMTOKEN_MSG = "Invalid NMToken";
  static final String INVALID_CONTAINERTOKEN_MSG =
      "Invalid ContainerToken";

  final Context context;
  private final ContainersMonitor containersMonitor;
  private Server server;

  protected void authorizeUser(UserGroupInformation remoteUgi,
      NMTokenIdentifier nmTokenIdentifier) throws YarnException {
    if (nmTokenIdentifier == null) {
      throw RPCUtil.getRemoteException(INVALID_NMTOKEN_MSG);
    }
    if (!remoteUgi.getUserName().equals(
      nmTokenIdentifier.getApplicationAttemptId().toString())) {
      throw RPCUtil.getRemoteException("Expected applicationAttemptId: "
  @VisibleForTesting
  protected void authorizeStartRequest(NMTokenIdentifier nmTokenIdentifier,
      ContainerTokenIdentifier containerTokenIdentifier) throws YarnException {
    if (nmTokenIdentifier == null) {
      throw RPCUtil.getRemoteException(INVALID_NMTOKEN_MSG);
    }
    if (containerTokenIdentifier == null) {
      throw RPCUtil.getRemoteException(INVALID_CONTAINERTOKEN_MSG);
    }
    ContainerId containerId = containerTokenIdentifier.getContainerID();
    String containerIDStr = containerId.toString();
    boolean unauthorized = false;
    for (StartContainerRequest request : requests.getStartContainerRequests()) {
      ContainerId containerId = null;
      try {
        if (request.getContainerToken() == null ||
            request.getContainerToken().getIdentifier() == null) {
          throw new IOException(INVALID_CONTAINERTOKEN_MSG);
        }
        ContainerTokenIdentifier containerTokenIdentifier =
            BuilderUtils.newContainerTokenIdentifier(request.getContainerToken());
        verifyAndGetContainerTokenIdentifier(request.getContainerToken(),
        new HashMap<ContainerId, SerializedException>();
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier identifier = selectNMTokenIdentifier(remoteUgi);
    if (identifier == null) {
      throw RPCUtil.getRemoteException(INVALID_NMTOKEN_MSG);
    }
    for (ContainerId id : requests.getContainerIds()) {
      try {
        stopContainerInternal(identifier, id);
        new HashMap<ContainerId, SerializedException>();
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier identifier = selectNMTokenIdentifier(remoteUgi);
    if (identifier == null) {
      throw RPCUtil.getRemoteException(INVALID_NMTOKEN_MSG);
    }
    for (ContainerId id : request.getContainerIds()) {
      try {
        ContainerStatus status = getContainerStatusInternal(id, identifier);
  protected void authorizeGetAndStopContainerRequest(ContainerId containerId,
      Container container, boolean stopRequest, NMTokenIdentifier identifier)
      throws YarnException {
    if (identifier == null) {
      throw RPCUtil.getRemoteException(INVALID_NMTOKEN_MSG);
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/BaseContainerManagerTest.java
            ByteBuffer.wrap("AuxServiceMetaData2".getBytes()));
        return serviceData;
      }

      @Override
      protected NMTokenIdentifier selectNMTokenIdentifier(
          UserGroupInformation remoteUgi) {
        return new NMTokenIdentifier();
      }
    };
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/TestContainerManager.java
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerStatusesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainersRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StopContainersRequestPBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestContainerManager extends BaseContainerManagerTest {

        .contains("The auxService:" + serviceName + " does not exist"));
  }

  @Test
  public void testNullTokens() throws Exception {
    ContainerManagerImpl cMgrImpl =
        new ContainerManagerImpl(context, exec, delSrvc, nodeStatusUpdater,
        metrics, new ApplicationACLsManager(conf), dirsHandler);
    String strExceptionMsg = "";
    try {
      cMgrImpl.authorizeStartRequest(null, new ContainerTokenIdentifier());
    } catch(YarnException ye) {
      strExceptionMsg = ye.getMessage();
    }
    Assert.assertEquals(strExceptionMsg,
        ContainerManagerImpl.INVALID_NMTOKEN_MSG);

    strExceptionMsg = "";
    try {
      cMgrImpl.authorizeStartRequest(new NMTokenIdentifier(), null);
    } catch(YarnException ye) {
      strExceptionMsg = ye.getMessage();
    }
    Assert.assertEquals(strExceptionMsg,
        ContainerManagerImpl.INVALID_CONTAINERTOKEN_MSG);

    strExceptionMsg = "";
    try {
      cMgrImpl.authorizeGetAndStopContainerRequest(null, null, true, null);
    } catch(YarnException ye) {
      strExceptionMsg = ye.getMessage();
    }
    Assert.assertEquals(strExceptionMsg,
        ContainerManagerImpl.INVALID_NMTOKEN_MSG);

    strExceptionMsg = "";
    try {
      cMgrImpl.authorizeUser(null, null);
    } catch(YarnException ye) {
      strExceptionMsg = ye.getMessage();
    }
    Assert.assertEquals(strExceptionMsg,
        ContainerManagerImpl.INVALID_NMTOKEN_MSG);

    ContainerManagerImpl spyContainerMgr = Mockito.spy(cMgrImpl);
    UserGroupInformation ugInfo = UserGroupInformation.createRemoteUser("a");
    Mockito.when(spyContainerMgr.getRemoteUgi()).thenReturn(ugInfo);
    Mockito.when(spyContainerMgr.
        selectNMTokenIdentifier(ugInfo)).thenReturn(null);

    strExceptionMsg = "";
    try {
      spyContainerMgr.stopContainers(new StopContainersRequestPBImpl());
    } catch(YarnException ye) {
      strExceptionMsg = ye.getMessage();
    }
    Assert.assertEquals(strExceptionMsg,
        ContainerManagerImpl.INVALID_NMTOKEN_MSG);

    strExceptionMsg = "";
    try {
      spyContainerMgr.getContainerStatuses(
          new GetContainerStatusesRequestPBImpl());
    } catch(YarnException ye) {
      strExceptionMsg = ye.getMessage();
    }
    Assert.assertEquals(strExceptionMsg,
        ContainerManagerImpl.INVALID_NMTOKEN_MSG);

    Mockito.doNothing().when(spyContainerMgr).authorizeUser(ugInfo, null);
    List<StartContainerRequest> reqList
        = new ArrayList<StartContainerRequest>();
    reqList.add(StartContainerRequest.newInstance(null, null));
    StartContainersRequest reqs = new StartContainersRequestPBImpl();
    reqs.setStartContainerRequests(reqList);
    strExceptionMsg = "";
    try {
      spyContainerMgr.startContainers(reqs);
    } catch(YarnException ye) {
      strExceptionMsg = ye.getCause().getMessage();
    }
    Assert.assertEquals(strExceptionMsg,
        ContainerManagerImpl.INVALID_CONTAINERTOKEN_MSG);
  }

  public static Token createContainerToken(ContainerId cId, long rmIdentifier,
      NodeId nodeId, String user,
      NMContainerTokenSecretManager containerTokenSecretManager)

