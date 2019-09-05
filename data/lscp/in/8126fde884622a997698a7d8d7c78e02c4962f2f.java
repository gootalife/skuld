hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/ApplicationReport.java
      long startTime, long finishTime, FinalApplicationStatus finalStatus,
      ApplicationResourceUsageReport appResources, String origTrackingUrl,
      float progress, String applicationType, Token amRmToken,
      Set<String> tags, boolean unmanagedApplication) {
    ApplicationReport report =
        newInstance(applicationId, applicationAttemptId, user, queue, name,
          host, rpcPort, clientToAMToken, state, diagnostics, url, startTime,
          finishTime, finalStatus, appResources, origTrackingUrl, progress,
          applicationType, amRmToken);
    report.setApplicationTags(tags);
    report.setUnmanagedApp(unmanagedApplication);
    return report;
  }

  @Unstable
  public abstract void setLogAggregationStatus(
      LogAggregationStatus logAggregationStatus);

  @Public
  @Unstable
  public abstract boolean isUnmanagedApp();

  @Public
  @Unstable
  public abstract void setUnmanagedApp(boolean unmanagedApplication);
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/cli/ApplicationCLI.java
      appReportStr.println(appReport.getLogAggregationStatus() == null ? "N/A"
          : appReport.getLogAggregationStatus());
      appReportStr.print("\tDiagnostics : ");
      appReportStr.println(appReport.getDiagnostics());
      appReportStr.print("\tUnmanaged Application : ");
      appReportStr.print(appReport.isUnmanagedApp());
    } else {
      appReportStr.print("Application with id '" + applicationId
          + "' doesn't exist in RM.");

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/api/impl/TestYarnClient.java
      rmClient.start();

      ApplicationId appId = createApp(rmClient, false);
      waitTillAccepted(rmClient, appId, false);
      Assert.assertNull(rmClient.getAMRMToken(appId));

      appId = createApp(rmClient, true);
      waitTillAccepted(rmClient, appId, true);
      long start = System.currentTimeMillis();
      while (rmClient.getAMRMToken(appId) == null) {
        if (System.currentTimeMillis() - start > 20 * 1000) {
            rmClient.init(yarnConf);
            rmClient.start();
            ApplicationId appId = createApp(rmClient, true);
          waitTillAccepted(rmClient, appId, true);
            long start = System.currentTimeMillis();
            while (rmClient.getAMRMToken(appId) == null) {
              if (System.currentTimeMillis() - start > 20 * 1000) {
    return appId;
  }
  
  private void waitTillAccepted(YarnClient rmClient, ApplicationId appId,
      boolean unmanagedApplication)
    throws Exception {
    try {
      long start = System.currentTimeMillis();
        Thread.sleep(200);
        report = rmClient.getApplicationReport(appId);
      }
      Assert.assertEquals(unmanagedApplication, report.isUnmanagedApp());
    } catch (Exception ex) {
      throw new Exception(ex);
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/cli/TestYarnCLI.java
          "user", "queue", "appname", "host", 124, null,
          YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0,
          FinalApplicationStatus.SUCCEEDED, usageReport, "N/A", 0.53789f, "YARN",
          null, null, false);
      newApplicationReport.setLogAggregationStatus(LogAggregationStatus.SUCCEEDED);
      when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
          newApplicationReport);
          (i == 0 ? "N/A" : "123456 MB-seconds, 4567 vcore-seconds"));
      pw.println("\tLog Aggregation Status : SUCCEEDED");
      pw.println("\tDiagnostics : diagnostics");
      pw.println("\tUnmanaged Application : false");
      pw.close();
      String appReportStr = baos.toString("UTF-8");
      Assert.assertEquals(appReportStr, sysOutStream.toString());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/ApplicationReportPBImpl.java
      convertToProtoFormat(LogAggregationStatus s) {
    return ProtoUtils.convertToProtoFormat(s);
  }

  @Override
  public boolean isUnmanagedApp() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getUnmanagedApplication();
  }

  @Override
  public void setUnmanagedApp(boolean unmanagedApplication) {
    maybeInitBuilder();
    builder.setUnmanagedApplication(unmanagedApplication);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/api/TestApplicatonReport.java
        ApplicationReport.newInstance(appId, appAttemptId, "user", "queue",
          "appname", "host", 124, null, YarnApplicationState.FINISHED,
          "diagnostics", "url", 0, 0, FinalApplicationStatus.SUCCEEDED, null,
          "N/A", 0.53789f, YarnConfiguration.DEFAULT_APPLICATION_TYPE, null,
          null,false);
    return appReport;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/ApplicationHistoryManagerOnTimelineStore.java
    String queue = null;
    String name = null;
    String type = null;
    boolean unmanagedApplication = false;
    long createdTime = 0;
    long finishedTime = 0;
    float progress = 0.0f;
            ConverterUtils.toApplicationId(entity.getEntityId()),
            latestApplicationAttemptId, user, queue, name, null, -1, null, state,
            diagnosticsInfo, null, createdTime, finishedTime, finalStatus, null,
            null, progress, type, null, appTags,
            unmanagedApplication), appViewACLs);
      }
      if (entityInfo.containsKey(ApplicationMetricsConstants.QUEUE_ENTITY_INFO)) {
        queue =
            entityInfo.get(ApplicationMetricsConstants.TYPE_ENTITY_INFO)
                .toString();
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO)) {
        unmanagedApplication =
            Boolean.parseBoolean(entityInfo.get(
                ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO)
                .toString());
      }
      if (entityInfo.containsKey(ApplicationMetricsConstants.APP_CPU_METRICS)) {
        long vcoreSeconds=Long.parseLong(entityInfo.get(
                ApplicationMetricsConstants.APP_CPU_METRICS).toString());
        ConverterUtils.toApplicationId(entity.getEntityId()),
        latestApplicationAttemptId, user, queue, name, null, -1, null, state,
        diagnosticsInfo, null, createdTime, finishedTime, finalStatus, appResources,
        null, progress, type, null, appTags, unmanagedApplication), appViewACLs);
  }

  private static ApplicationAttemptReport convertToApplicationAttemptReport(

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/applicationhistoryservice/TestApplicationHistoryManagerOnTimelineStore.java
        "test app type");
    entityInfo.put(ApplicationMetricsConstants.USER_ENTITY_INFO, "user1");
    entityInfo.put(ApplicationMetricsConstants.QUEUE_ENTITY_INFO, "test queue");
    entityInfo.put(
        ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO, "false");
    entityInfo.put(ApplicationMetricsConstants.SUBMITTED_TIME_ENTITY_INFO,
        Integer.MAX_VALUE + 1L);
    entityInfo.put(ApplicationMetricsConstants.APP_MEM_METRICS,123);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/metrics/ApplicationMetricsConstants.java
      "YARN_APPLICATION_LATEST_APP_ATTEMPT";

  public static final String APP_TAGS_INFO = "YARN_APPLICATION_TAGS";

  public static final String UNMANAGED_APPLICATION_ENTITY_INFO =
      "YARN_APPLICATION_UNMANAGED_APPLICATION";
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/AppBlock.java
    }
    overviewTable._("Diagnostics:",
        app.getDiagnosticsInfo() == null ? "" : app.getDiagnosticsInfo());
    overviewTable._("Unmanaged Application:", app.isUnmanagedApp());

    Collection<ApplicationAttemptReport> attempts;
    try {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/dao/AppInfo.java
  protected String applicationTags;
  private int allocatedCpuVcores;
  private int allocatedMemoryMB;
  protected boolean unmanagedApplication;

  public AppInfo() {
    if (app.getApplicationTags() != null && !app.getApplicationTags().isEmpty()) {
      this.applicationTags = CSV_JOINER.join(app.getApplicationTags());
    }
    unmanagedApplication = app.isUnmanagedApp();
  }

  public String getAppId() {
  public String getApplicationTags() {
    return applicationTags;
  }

  public boolean isUnmanagedApp() {
    return unmanagedApplication;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/metrics/ApplicationCreatedEvent.java
  private String queue;
  private long submittedTime;
  private Set<String> appTags;
  private boolean unmanagedApplication;

  public ApplicationCreatedEvent(ApplicationId appId,
      String name,
      String queue,
      long submittedTime,
      long createdTime,
      Set<String> appTags,
      boolean unmanagedApplication) {
    super(SystemMetricsEventType.APP_CREATED, createdTime);
    this.appId = appId;
    this.name = name;
    this.queue = queue;
    this.submittedTime = submittedTime;
    this.appTags = appTags;
    this.unmanagedApplication = unmanagedApplication;
  }

  @Override
  public Set<String> getAppTags() {
    return appTags;
  }

  public boolean isUnmanagedApp() {
    return unmanagedApplication;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/metrics/SystemMetricsPublisher.java
              app.getUser(),
              app.getQueue(),
              app.getSubmitTime(),
              createdTime, app.getApplicationTags(),
              app.getApplicationSubmissionContext().getUnmanagedAM()));
    }
  }

        event.getSubmittedTime());
    entityInfo.put(ApplicationMetricsConstants.APP_TAGS_INFO,
        event.getAppTags());
    entityInfo.put(
        ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO,
        event.isUnmanagedApp());
    entity.setOtherInfo(entityInfo);
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/RMAppImpl.java
          appUsageReport, origTrackingUrl, progress, this.applicationType, 
              amrmToken, applicationTags);
      report.setLogAggregationStatus(logAggregationStatus);
      report.setUnmanagedApp(submissionContext.getUnmanagedAM());
      return report;
    } finally {
      this.readLock.unlock();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/AppInfo.java
  protected List<ResourceRequest> resourceRequests;

  protected LogAggregationStatus logAggregationStatus;
  protected boolean unmanagedApplication;

  public AppInfo() {
  } // JAXB needs this
          appMetrics.getResourcePreempted().getVirtualCores();
      memorySeconds = appMetrics.getMemorySeconds();
      vcoreSeconds = appMetrics.getVcoreSeconds();
      unmanagedApplication =
          app.getApplicationSubmissionContext().getUnmanagedAM();
    }
  }

  public LogAggregationStatus getLogAggregationStatus() {
    return this.logAggregationStatus;
  }

  public boolean isUnmanagedApp() {
    return unmanagedApplication;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/metrics/TestSystemMetricsPublisher.java

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
      Assert.assertEquals(app.getQueue(),
          entity.getOtherInfo()
              .get(ApplicationMetricsConstants.QUEUE_ENTITY_INFO));

      Assert.assertEquals(
          app.getApplicationSubmissionContext().getUnmanagedAM(),
          entity.getOtherInfo().get(
              ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO));

      Assert
          .assertEquals(
              app.getUser(),
    appTags.add("test");
    appTags.add("tags");
    when(app.getApplicationTags()).thenReturn(appTags);
    ApplicationSubmissionContext asc = mock(ApplicationSubmissionContext.class);
    when(asc.getUnmanagedAM()).thenReturn(false);
    when(app.getApplicationSubmissionContext()).thenReturn(asc);
    return app;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServicesApps.java
          WebServicesTestUtils.getXmlInt(element, "preemptedResourceVCores"),
          WebServicesTestUtils.getXmlInt(element, "numNonAMContainerPreempted"),
          WebServicesTestUtils.getXmlInt(element, "numAMContainerPreempted"),
          WebServicesTestUtils.getXmlString(element, "logAggregationStatus"),
          WebServicesTestUtils.getXmlBoolean(element, "unmanagedApplication"));
    }
  }

  public void verifyAppInfo(JSONObject info, RMApp app) throws JSONException,
      Exception {

    assertEquals("incorrect number of elements", 29, info.length());

    verifyAppInfoGeneric(app, info.getString("id"), info.getString("user"),
        info.getString("name"), info.getString("applicationType"),
        info.getInt("preemptedResourceVCores"),
        info.getInt("numNonAMContainerPreempted"),
        info.getInt("numAMContainerPreempted"),
        info.getString("logAggregationStatus"),
        info.getBoolean("unmanagedApplication"));
  }

  public void verifyAppInfoGeneric(RMApp app, String id, String user,
      int allocatedMB, int allocatedVCores, int numContainers,
      int preemptedResourceMB, int preemptedResourceVCores,
      int numNonAMContainerPreempted, int numAMContainerPreempted,
      String logAggregationStatus, boolean unmanagedApplication)
      throws JSONException,
      Exception {

    WebServicesTestUtils.checkStringMatch("id", app.getApplicationId()
    assertEquals("Log aggregation Status doesn't match", app
        .getLogAggregationStatusForAppReport().toString(),
        logAggregationStatus);
    assertEquals("unmanagedApplication doesn't match", app
        .getApplicationSubmissionContext().getUnmanagedAM(),
        unmanagedApplication);
  }

  @Test

