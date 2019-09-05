hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/ZKRMStateStore.java
    createRootDir(amrmTokenSecretManagerRoot);
  }

  protected void createRootDir(final String rootPath) throws Exception {
    new ZKAction<String>() {
      @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/TestZKRMStateStore.java
        return workingZnode + "/" + ROOT_ZNODE_NAME + "/" + RM_APP_ROOT + "/"
            + appId;
      }

      public void testRetryingCreateRootDir() throws Exception {
        createRootDir(znodeWorkingPath);
      }

    }

    public RMStateStore getRMStateStore() throws Exception {
    testDeleteStore(zkTester);
    testRemoveApplication(zkTester);
    testAMRMTokenSecretManagerStateStore(zkTester);
    ((TestZKRMStateStoreTester.TestZKRMStateStoreInternal)
        zkTester.getRMStateStore()).testRetryingCreateRootDir();
  }

  @Test (timeout = 60000)

