hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ha/ActiveStandbyElector.java

  private Lock sessionReestablishLockForTests = new ReentrantLock();
  private boolean wantToBeInElection;
  private boolean monitorLockNodePending = false;
  private ZooKeeper monitorLockNodeClient;

  public synchronized void processResult(int rc, String path, Object ctx,
      Stat stat) {
    if (isStaleClient(ctx)) return;
    monitorLockNodePending = false;

    assert wantToBeInElection :
        "Got a StatNode result after quitting election";
    return state;
  }

  @VisibleForTesting
  synchronized boolean isMonitorLockNodePending() {
    return monitorLockNodePending;
  }

  private boolean reEstablishSession() {
    int connectionRetryCount = 0;
    boolean success = false;
  }

  private void monitorLockNodeAsync() {
    if (monitorLockNodePending && monitorLockNodeClient == zkClient) {
      LOG.info("Ignore duplicate monitor lock-node request.");
      return;
    }
    monitorLockNodePending = true;
    monitorLockNodeClient = zkClient;
    zkClient.exists(zkLockFilePath,
        watcher, this,
        zkClient);

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/ha/TestActiveStandbyElector.java
        Event.KeeperState.SyncConnected);
    elector.processWatchEvent(mockZK, mockEvent);
    verifyExistCall(1);
    Assert.assertTrue(elector.isMonitorLockNodePending());
    elector.processResult(Code.SESSIONEXPIRED.intValue(), ZK_LOCK_NAME,
        mockZK, new Stat());
    Assert.assertFalse(elector.isMonitorLockNodePending());

        ZK_LOCK_NAME);
    Mockito.verify(mockApp, Mockito.times(1)).becomeStandby();
    verifyExistCall(1);
    Assert.assertTrue(elector.isMonitorLockNodePending());

    Stat stat = new Stat();
    stat.setEphemeralOwner(0L);
    Mockito.when(mockZK.getSessionId()).thenReturn(1L);
    elector.processResult(Code.OK.intValue(), ZK_LOCK_NAME, mockZK, stat);
    Assert.assertFalse(elector.isMonitorLockNodePending());

    WatchedEvent mockEvent = Mockito.mock(WatchedEvent.class);
    Mockito.when(mockEvent.getPath()).thenReturn(ZK_LOCK_NAME);
        Event.EventType.NodeDataChanged);
    elector.processWatchEvent(mockZK, mockEvent);
    verifyExistCall(2);
    Assert.assertTrue(elector.isMonitorLockNodePending());
    elector.processResult(Code.OK.intValue(), ZK_LOCK_NAME, mockZK, stat);
    Assert.assertFalse(elector.isMonitorLockNodePending());

    Mockito.when(mockEvent.getType()).thenReturn(
        Event.EventType.NodeChildrenChanged);
    elector.processWatchEvent(mockZK, mockEvent);
    verifyExistCall(3);
    Assert.assertTrue(elector.isMonitorLockNodePending());
    elector.processResult(Code.OK.intValue(), ZK_LOCK_NAME, mockZK, stat);
    Assert.assertFalse(elector.isMonitorLockNodePending());

        ZK_LOCK_NAME);
    Mockito.verify(mockApp, Mockito.times(1)).becomeActive();
    verifyExistCall(4);
    Assert.assertTrue(elector.isMonitorLockNodePending());
    stat.setEphemeralOwner(1L);
    elector.processResult(Code.OK.intValue(), ZK_LOCK_NAME, mockZK, stat);
    Assert.assertFalse(elector.isMonitorLockNodePending());

        ZK_LOCK_NAME);
    Mockito.verify(mockApp, Mockito.times(2)).becomeActive();
    verifyExistCall(5);
    Assert.assertTrue(elector.isMonitorLockNodePending());
    elector.processResult(Code.OK.intValue(), ZK_LOCK_NAME, mockZK, stat);
    Assert.assertFalse(elector.isMonitorLockNodePending());

    Mockito.when(mockEvent.getPath()).thenReturn(null);
        ZK_LOCK_NAME);
    Mockito.verify(mockApp, Mockito.times(1)).becomeStandby();
    verifyExistCall(1);
    Assert.assertTrue(elector.isMonitorLockNodePending());

    Stat stat = new Stat();
    stat.setEphemeralOwner(0L);
    Mockito.when(mockZK.getSessionId()).thenReturn(1L);
    elector.processResult(Code.OK.intValue(), ZK_LOCK_NAME, mockZK, stat);
    Assert.assertFalse(elector.isMonitorLockNodePending());

    WatchedEvent mockEvent = Mockito.mock(WatchedEvent.class);
    Mockito.when(mockEvent.getPath()).thenReturn(ZK_LOCK_NAME);

