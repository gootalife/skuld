hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/ApplicationAttemptReport.java
  public static ApplicationAttemptReport newInstance(
      ApplicationAttemptId applicationAttemptId, String host, int rpcPort,
      String url, String oUrl, String diagnostics,
      YarnApplicationAttemptState state, ContainerId amContainerId,
      long startTime, long finishTime) {
    ApplicationAttemptReport report =
        Records.newRecord(ApplicationAttemptReport.class);
    report.setApplicationAttemptId(applicationAttemptId);
    report.setDiagnostics(diagnostics);
    report.setYarnApplicationAttemptState(state);
    report.setAMContainerId(amContainerId);
    report.setStartTime(startTime);
    report.setFinishTime(finishTime);
    return report;
  }

  public static ApplicationAttemptReport newInstance(
      ApplicationAttemptId applicationAttemptId, String host, int rpcPort,
      String url, String oUrl, String diagnostics,
      YarnApplicationAttemptState state, ContainerId amContainerId) {
    return newInstance(applicationAttemptId, host, rpcPort, url, oUrl,
        diagnostics, state, amContainerId, 0L, 0L);
  }

  @Private
  @Unstable
  public abstract void setAMContainerId(ContainerId amContainerId);

  @Public
  @Unstable
  public abstract long getStartTime();

  @Private
  @Unstable
  public abstract void setStartTime(long startTime);

  @Public
  @Unstable
  public abstract long getFinishTime();

  @Private
  @Unstable
  public abstract void setFinishTime(long finishTime);
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/ProtocolHATestBase.java
    public ApplicationAttemptReport createFakeApplicationAttemptReport() {
      return ApplicationAttemptReport.newInstance(
          createFakeApplicationAttemptId(), "localhost", 0, "", "", "",
          YarnApplicationAttemptState.RUNNING, createFakeContainerId(), 1000l,
          1200l);
    }

    public List<ApplicationAttemptReport>

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/api/impl/TestYarnClient.java
          "diagnostics",
          YarnApplicationAttemptState.FINISHED,
          ContainerId.newContainerId(
                  newApplicationReport.getCurrentApplicationAttemptId(), 1), 0,
              0);
      appAttempts.add(attempt);
      ApplicationAttemptReport attempt1 = ApplicationAttemptReport.newInstance(
          ApplicationAttemptId.newInstance(applicationId, 2),

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/cli/TestYarnCLI.java
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        applicationId, 1);
    ApplicationAttemptReport attemptReport =
        ApplicationAttemptReport.newInstance(attemptId, "host", 124, "url",
            "oUrl", "diagnostics", YarnApplicationAttemptState.FINISHED,
            ContainerId.newContainerId(attemptId, 1), 1000l, 2000l);
    when(
        client
            .getApplicationAttemptReport(any(ApplicationAttemptId.class)))

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/ApplicationAttemptReportPBImpl.java
      builder.clearAmContainerId();
    this.amContainerId = amContainerId;
  }

  @Override
  public void setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime(startTime);
  }

  @Override
  public void setFinishTime(long finishTime) {
    maybeInitBuilder();
    builder.setFinishTime(finishTime);
  }

  @Override
  public long getStartTime() {
    ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getStartTime();
  }

  @Override
  public long getFinishTime() {
    ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getFinishTime();
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/dao/AppAttemptInfo.java
  protected String diagnosticsInfo;
  protected YarnApplicationAttemptState appAttemptState;
  protected String amContainerId;
  protected long startedTime;
  protected long finishedTime;

  public AppAttemptInfo() {
    if (appAttempt.getAMContainerId() != null) {
      amContainerId = appAttempt.getAMContainerId().toString();
    }
    startedTime = appAttempt.getStartTime();
    finishedTime = appAttempt.getFinishTime();
  }

  public String getAppAttemptId() {
    return amContainerId;
  }

  public long getStartedTime() {
    return startedTime;
  }

  public long getFinishedTime() {
    return finishedTime;
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/attempt/RMAppAttemptImpl.java
      attemptReport = ApplicationAttemptReport.newInstance(this
          .getAppAttemptId(), this.getHost(), this.getRpcPort(), this
          .getTrackingUrl(), this.getOriginalTrackingUrl(), this.getDiagnostics(),
              YarnApplicationAttemptState.valueOf(this.getState().toString()),
              amId, this.startTime, this.finishTime);
    } finally {
      this.readLock.unlock();
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppAttemptBlock.java
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.AppAttemptBlock;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
        "Application Attempt State:",
        appAttempt.getAppAttemptState() == null ? UNAVAILABLE : appAttempt
          .getAppAttemptState())
        ._("Started:", Times.format(appAttempt.getStartedTime()))
        ._("Elapsed:",
            org.apache.hadoop.util.StringUtils.formatTime(Times.elapsed(
                appAttempt.getStartedTime(), appAttempt.getFinishedTime())))
      ._(
        "AM Container:",
        appAttempt.getAmContainerId() == null || containers == null

