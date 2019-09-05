hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/ZKRMStateStore.java
    String versionNodePath = getNodePath(zkRootNodePath, VERSION_NODE);
    byte[] data =
        ((VersionPBImpl) CURRENT_VERSION_INFO).getProto().toByteArray();
    if (existsWithRetries(versionNodePath, false) != null) {
      setDataWithRetries(versionNodePath, data, -1);
    } else {
      createWithRetries(versionNodePath, data, zkAcl, CreateMode.PERSISTENT);
  protected synchronized Version loadVersion() throws Exception {
    String versionNodePath = getNodePath(zkRootNodePath, VERSION_NODE);

    if (existsWithRetries(versionNodePath, false) != null) {
      byte[] data = getDataWithRetries(versionNodePath, false);
      Version version =
          new VersionPBImpl(VersionProto.parseFrom(data));
      return version;
  public synchronized long getAndIncrementEpoch() throws Exception {
    String epochNodePath = getNodePath(zkRootNodePath, EPOCH_NODE);
    long currentEpoch = 0;
    if (existsWithRetries(epochNodePath, false) != null) {
      byte[] data = getDataWithRetries(epochNodePath, false);
      Epoch epoch = new EpochPBImpl(EpochProto.parseFrom(data));
      currentEpoch = epoch.getEpoch();

  private void loadAMRMTokenSecretManagerState(RMState rmState)
      throws Exception {
    byte[] data = getDataWithRetries(amrmTokenSecretManagerRoot, false);
    if (data == null) {
      LOG.warn("There is no data saved");
      return;

  private void loadRMDelegationKeyState(RMState rmState) throws Exception {
    List<String> childNodes =
        getChildrenWithRetries(dtMasterKeysRootPath, false);
    for (String childNodeName : childNodes) {
      String childNodePath = getNodePath(dtMasterKeysRootPath, childNodeName);
      byte[] childData = getDataWithRetries(childNodePath, false);

      if (childData == null) {
        LOG.warn("Content of " + childNodePath + " is broken.");

  private void loadRMDelegationTokenState(RMState rmState) throws Exception {
    List<String> childNodes =
        getChildrenWithRetries(delegationTokensRootPath, false);
    for (String childNodeName : childNodes) {
      String childNodePath =
          getNodePath(delegationTokensRootPath, childNodeName);
      byte[] childData = getDataWithRetries(childNodePath, false);

      if (childData == null) {
        LOG.warn("Content of " + childNodePath + " is broken.");
  }

  private synchronized void loadRMAppState(RMState rmState) throws Exception {
    List<String> childNodes = getChildrenWithRetries(rmAppRoot, false);
    for (String childNodeName : childNodes) {
      String childNodePath = getNodePath(rmAppRoot, childNodeName);
      byte[] childData = getDataWithRetries(childNodePath, false);
      if (childNodeName.startsWith(ApplicationId.appIdStrPrefix)) {
        if (LOG.isDebugEnabled()) {
    for (String attemptIDStr : attempts) {
      if (attemptIDStr.startsWith(ApplicationAttemptId.appAttemptIdStrPrefix)) {
        String attemptPath = getNodePath(appPath, attemptIDStr);
        byte[] attemptData = getDataWithRetries(attemptPath, false);

        ApplicationAttemptStateDataPBImpl attemptState =
            new ApplicationAttemptStateDataPBImpl(
    }
    byte[] appStateData = appStateDataPB.getProto().toByteArray();

    if (existsWithRetries(nodeUpdatePath, false) != null) {
      setDataWithRetries(nodeUpdatePath, appStateData, -1);
    } else {
      createWithRetries(nodeUpdatePath, appStateData, zkAcl,
    }
    byte[] attemptStateData = attemptStateDataPB.getProto().toByteArray();

    if (existsWithRetries(nodeUpdatePath, false) != null) {
      setDataWithRetries(nodeUpdatePath, attemptStateData, -1);
    } else {
      createWithRetries(nodeUpdatePath, attemptStateData, zkAcl,
      LOG.debug("Removing RMDelegationToken_"
          + rmDTIdentifier.getSequenceNumber());
    }
    if (existsWithRetries(nodeRemovePath, false) != null) {
      opList.add(Op.delete(nodeRemovePath, -1));
    } else {
      LOG.debug("Attempted to delete a non-existing znode " + nodeRemovePath);
    String nodeRemovePath =
        getNodePath(delegationTokensRootPath, DELEGATION_TOKEN_PREFIX
            + rmDTIdentifier.getSequenceNumber());
    if (existsWithRetries(nodeRemovePath, false) == null) {
      addStoreOrUpdateOps(opList, rmDTIdentifier, renewDate, false);
      LOG.debug("Attempted to update a non-existing znode " + nodeRemovePath);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing RMDelegationKey_" + delegationKey.getKeyId());
    }
    if (existsWithRetries(nodeRemovePath, false) != null) {
      doMultiWithRetries(Op.delete(nodeRemovePath, -1));
    } else {
      LOG.debug("Attempted to delete a non-existing znode " + nodeRemovePath);

  @Override
  public synchronized void deleteStore() throws Exception {
    if (existsWithRetries(zkRootNodePath, false) != null) {
      deleteWithRetries(zkRootNodePath, false);
    }
  }


