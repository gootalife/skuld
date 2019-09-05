hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/ZKRMStateStore.java
  public synchronized void removeApplication(ApplicationId removeAppId)
      throws Exception {
    String appIdRemovePath = getNodePath(rmAppRoot, removeAppId.toString());
    if (existsWithRetries(appIdRemovePath, false) != null) {
      deleteWithRetries(appIdRemovePath, false);
    }
  }


