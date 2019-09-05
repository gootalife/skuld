hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/LinuxContainerExecutor.java
  private LCEResourcesHandler resourcesHandler;
  private boolean containerSchedPriorityIsSet = false;
  private int containerSchedPriorityAdjustment = 0;
  private boolean containerLimitUsers;

  @Override
  public void setConf(Configuration conf) {
    containerLimitUsers = conf.getBoolean(
      YarnConfiguration.NM_NONSECURE_MODE_LIMIT_USERS,
      YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LIMIT_USERS);
    if (!containerLimitUsers) {
      LOG.warn(YarnConfiguration.NM_NONSECURE_MODE_LIMIT_USERS +
          ": impersonation without authentication enabled");
    }
  }

  void verifyUsernamePattern(String user) {

