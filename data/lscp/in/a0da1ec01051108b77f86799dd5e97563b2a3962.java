hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/ApplicationReport.java
  @Public
  @Unstable
  public abstract void setUnmanagedApp(boolean unmanagedApplication);

  @Public
  @Stable
  public abstract Priority getPriority();

  @Private
  @Unstable
  public abstract void setPriority(Priority priority);
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/cli/ApplicationCLI.java
      appReportStr.println(appReport.getUser());
      appReportStr.print("\tQueue : ");
      appReportStr.println(appReport.getQueue());
      appReportStr.print("\tApplication Priority : ");
      appReportStr.println(appReport.getPriority());
      appReportStr.print("\tStart-Time : ");
      appReportStr.println(appReport.getStartTime());
      appReportStr.print("\tFinish-Time : ");

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/cli/TestYarnCLI.java
          FinalApplicationStatus.SUCCEEDED, usageReport, "N/A", 0.53789f, "YARN",
          null, null, false);
      newApplicationReport.setLogAggregationStatus(LogAggregationStatus.SUCCEEDED);
      newApplicationReport.setPriority(Priority.newInstance(0));
      when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
          newApplicationReport);
      int result = cli.run(new String[] { "application", "-status", applicationId.toString() });
      pw.println("\tApplication-Type : YARN");
      pw.println("\tUser : user");
      pw.println("\tQueue : queue");
      pw.println("\tApplication Priority : 0");
      pw.println("\tStart-Time : 0");
      pw.println("\tFinish-Time : 0");
      pw.println("\tProgress : 53.79%");

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/ApplicationReportPBImpl.java
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationResourceUsageReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.FinalApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LogAggregationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationStateProto;

import com.google.protobuf.TextFormat;
  private Token clientToAMToken = null;
  private Token amRmToken = null;
  private Set<String> applicationTags = null;
  private Priority priority = null;

  public ApplicationReportPBImpl() {
    builder = ApplicationReportProto.newBuilder();
      builder.clearApplicationTags();
      builder.addAllApplicationTags(this.applicationTags);
    }
    if (this.priority != null
        && !((PriorityPBImpl) this.priority).getProto().equals(
            builder.getPriority())) {
      builder.setPriority(convertToProtoFormat(this.priority));
    }
  }

  private void mergeLocalToProto() {
    return ((TokenPBImpl)t).getProto();
  }

  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority t) {
    return ((PriorityPBImpl)t).getProto();
  }

  @Override
  public LogAggregationStatus getLogAggregationStatus() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    maybeInitBuilder();
    builder.setUnmanagedApplication(unmanagedApplication);
  }

  @Override
  public Priority getPriority() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.priority != null) {
      return this.priority;
    }
    if (!p.hasPriority()) {
      return null;
    }
    this.priority = convertFromProtoFormat(p.getPriority());
    return this.priority;
  }

  @Override
  public void setPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null)
      builder.clearPriority();
    this.priority = priority;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/utils/BuilderUtils.java
      String url, long startTime, long finishTime,
      FinalApplicationStatus finalStatus,
      ApplicationResourceUsageReport appResources, String origTrackingUrl,
      float progress, String appType, Token amRmToken, Set<String> tags,
      Priority priority) {
    ApplicationReport report = recordFactory
        .newRecordInstance(ApplicationReport.class);
    report.setApplicationId(applicationId);
    report.setApplicationType(appType);
    report.setAMRMToken(amRmToken);
    report.setApplicationTags(tags);
    report.setPriority(priority);
    return report;
  }
  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/AppBlock.java
      ._("Application Type:", app.getType())
      ._("Application Tags:",
        app.getApplicationTags() == null ? "" : app.getApplicationTags())
      ._("Application Priority:", clarifyAppPriority(app.getPriority()))
      ._(
        "YarnApplicationState:",
        app.getAppState() == null ? UNAVAILABLE : clarifyAppState(app
    }
  }

  private String clarifyAppPriority(int priority) {
    return priority + " (Higher Integer value indicates higher priority)";
  }

  private String clairfyAppFinalStatus(FinalApplicationStatus status) {
    if (status == FinalApplicationStatus.UNDEFINED) {
      return "Application has not completed yet.";

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/WebPageUtils.java
      .append("{'sType':'string', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }")
      .append("\n, {'sType':'numeric', 'aTargets': " +
          (isFairSchedulerPage ? "[6, 7]": "[6, 7]"))
      .append(", 'mRender': renderHadoopDate }")
      .append("\n, {'sType':'numeric', bSearchable:false, 'aTargets':");
    if (isFairSchedulerPage) {
      sb.append("[13]");
    } else if (isResourceManager) {
      sb.append("[13]");
    } else {
      sb.append("[9]");
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/dao/AppInfo.java
  protected long finishedTime;
  protected long elapsedTime;
  protected String applicationTags;
  protected int priority;
  private int allocatedCpuVcores;
  private int allocatedMemoryMB;
  protected boolean unmanagedApplication;
    finishedTime = app.getFinishTime();
    elapsedTime = Times.elapsed(startedTime, finishedTime);
    finalAppStatus = app.getFinalApplicationStatus();
    priority = 0;
    if (app.getPriority() != null) {
      priority = app.getPriority().getPriority();
    }
    if (app.getApplicationResourceUsageReport() != null) {
      runningContainers = app.getApplicationResourceUsageReport()
          .getNumUsedContainers();
  public boolean isUnmanagedApp() {
    return unmanagedApplication;
  }

  public int getPriority() {
    return priority;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/RMAppImpl.java
      ApplicationReport report = BuilderUtils.newApplicationReport(
          this.applicationId, currentApplicationAttemptId, this.user,
          this.queue, this.name, host, rpcPort, clientToAMToken,
          createApplicationState(), diags, trackingUrl, this.startTime,
          this.finishTime, finishState, appUsageReport, origTrackingUrl,
          progress, this.applicationType, amrmToken, applicationTags,
          this.submissionContext.getPriority());
      report.setLogAggregationStatus(logAggregationStatus);
      report.setUnmanagedApp(submissionContext.getUnmanagedAM());
      return report;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppsBlock.java
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#apps").thead().tr().th(".id", "ID").th(".user", "User")
          .th(".name", "Name").th(".type", "Application Type")
          .th(".queue", "Queue").th(".priority", "Application Priority")
          .th(".starttime", "StartTime")
          .th(".finishtime", "FinishTime").th(".state", "State")
          .th(".finalstatus", "FinalStatus")
          .th(".runningcontainer", "Running Containers")
        .append("\",\"")
        .append(
          StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(app
             .getQueue()))).append("\",\"").append(String
             .valueOf(app.getPriority()))
        .append("\",\"").append(app.getStartedTime())
        .append("\",\"").append(app.getFinishedTime())
        .append("\",\"")
        .append(app.getAppState() == null ? UNAVAILABLE : app.getAppState())

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/AppInfo.java
  protected long clusterId;
  protected String applicationType;
  protected String applicationTags = "";
  protected int priority;

  protected long startedTime;
      this.user = app.getUser().toString();
      this.name = app.getName().toString();
      this.queue = app.getQueue().toString();
      this.priority = 0;
      if (app.getApplicationSubmissionContext().getPriority() != null) {
        this.priority = app.getApplicationSubmissionContext().getPriority()
            .getPriority();
      }
      this.progress = app.getProgress() * 100;
      this.diagnostics = app.getDiagnostics().toString();
      if (diagnostics == null || diagnostics.isEmpty()) {
  public boolean isUnmanagedApp() {
    return unmanagedApplication;
  }

  public int getPriority() {
    return this.priority;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServicesApps.java
          WebServicesTestUtils.getXmlString(element, "name"),
          WebServicesTestUtils.getXmlString(element, "applicationType"),
          WebServicesTestUtils.getXmlString(element, "queue"),
          WebServicesTestUtils.getXmlInt(element, "priority"),
          WebServicesTestUtils.getXmlString(element, "state"),
          WebServicesTestUtils.getXmlString(element, "finalStatus"),
          WebServicesTestUtils.getXmlFloat(element, "progress"),
  public void verifyAppInfo(JSONObject info, RMApp app) throws JSONException,
      Exception {

    assertEquals("incorrect number of elements", 30, info.length());

    verifyAppInfoGeneric(app, info.getString("id"), info.getString("user"),
        info.getString("name"), info.getString("applicationType"),
        info.getString("queue"), info.getInt("priority"),
        info.getString("state"), info.getString("finalStatus"),
        (float) info.getDouble("progress"), info.getString("trackingUI"),
        info.getString("diagnostics"), info.getLong("clusterId"),
        info.getLong("startedTime"), info.getLong("finishedTime"),
        info.getLong("elapsedTime"), info.getString("amHostHttpAddress"),
        info.getString("amContainerLogs"), info.getInt("allocatedMB"),
        info.getInt("allocatedVCores"), info.getInt("runningContainers"),
        info.getInt("preemptedResourceMB"),
        info.getInt("preemptedResourceVCores"),
        info.getInt("numNonAMContainerPreempted"),
  }

  public void verifyAppInfoGeneric(RMApp app, String id, String user,
      String name, String applicationType, String queue, int prioirty,
      String state, String finalStatus, float progress, String trackingUI,
      String diagnostics, long clusterId, long startedTime, long finishedTime,
      long elapsedTime, String amHostHttpAddress, String amContainerLogs,
      int allocatedMB, int allocatedVCores, int numContainers,
    WebServicesTestUtils.checkStringMatch("applicationType",
      app.getApplicationType(), applicationType);
    WebServicesTestUtils.checkStringMatch("queue", app.getQueue(), queue);
    assertEquals("priority doesn't match", 0, prioirty);
    WebServicesTestUtils.checkStringMatch("state", app.getState().toString(),
        state);
    WebServicesTestUtils.checkStringMatch("finalStatus", app

