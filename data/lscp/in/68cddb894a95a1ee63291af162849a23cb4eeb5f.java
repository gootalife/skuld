hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
  public static final String APPLICATION_HISTORY_MAX_APPS =
      APPLICATION_HISTORY_PREFIX + "max-applications";
  public static final long DEFAULT_APPLICATION_HISTORY_MAX_APPS = 10000;

  public static final String TIMELINE_SERVICE_STORE =

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/ApplicationHistoryManagerOnTimelineStore.java
    serverHttpAddress = WebAppUtils.getHttpSchemePrefix(conf) +
        WebAppUtils.getAHSWebAppURLWithoutScheme(conf);
    maxLoadedApplications =
        conf.getLong(YarnConfiguration.APPLICATION_HISTORY_MAX_APPS,
          YarnConfiguration.DEFAULT_APPLICATION_HISTORY_MAX_APPS);
    super.serviceInit(conf);
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/applicationhistoryservice/TestApplicationHistoryClientService.java
    Configuration conf = new YarnConfiguration();
    conf.setLong(YarnConfiguration.APPLICATION_HISTORY_MAX_APPS, 1);
    ApplicationHistoryManagerOnTimelineStore historyManager2 =
        new ApplicationHistoryManagerOnTimelineStore(dataManager,
          new ApplicationACLsManager(conf));

