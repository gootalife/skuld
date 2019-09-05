hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/WebPageUtils.java
public class WebPageUtils {

  public static String appsTableInit() {
    return appsTableInit(false, true);
  }

  public static String appsTableInit(
      boolean isFairSchedulerPage, boolean isResourceManager) {
    return tableInit()
      .append(", bDeferRender: true")
      .append(", bProcessing: true")
      .append("\n, aoColumnDefs: ")
      .append(getAppsTableColumnDefs(isFairSchedulerPage, isResourceManager))
      .append(", aaSorting: [[0, 'desc']]}").toString();
  }

  private static String getAppsTableColumnDefs(
      boolean isFairSchedulerPage, boolean isResourceManager) {
    StringBuilder sb = new StringBuilder();
    sb.append("[\n")
      .append("{'sType':'string', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }")
      .append("\n, {'sType':'numeric', 'aTargets': " +
          (isFairSchedulerPage ? "[6, 7]": "[5, 6]"))
      .append(", 'mRender': renderHadoopDate }")
      .append("\n, {'sType':'numeric', bSearchable:false, 'aTargets':");
    if (isFairSchedulerPage) {
      sb.append("[11]");
    } else if (isResourceManager) {
      sb.append("[10]");
    } else {
      sb.append("[9]");
    }
    sb.append(", 'mRender': parseHadoopProgress }]");
    return sb.toString();
  }

  public static String attemptsTableInit() {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/dao/AppInfo.java
  protected String host;
  protected int rpcPort;
  protected YarnApplicationState appState;
  protected int runningContainers;
  protected float progress;
  protected String diagnosticsInfo;
  protected String originalTrackingUrl;
    finishedTime = app.getFinishTime();
    elapsedTime = Times.elapsed(startedTime, finishedTime);
    finalAppStatus = app.getFinalApplicationStatus();
    if (app.getApplicationResourceUsageReport() != null) {
      runningContainers =
          app.getApplicationResourceUsageReport().getNumUsedContainers();
    }
    progress = app.getProgress() * 100; // in percent
    if (app.getApplicationTags() != null && !app.getApplicationTags().isEmpty()) {
      this.applicationTags = CSV_JOINER.join(app.getApplicationTags());
    return appState;
  }

  public int getRunningContainers() {
    return runningContainers;
  }

  public float getProgress() {
    return progress;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/FairSchedulerAppsBlock.java
            th(".finishtime", "FinishTime").
            th(".state", "State").
            th(".finalstatus", "FinalStatus").
            th(".runningcontainer", "Running Containers").
            th(".progress", "Progress").
            th(".ui", "Tracking UI")._()._().
        tbody();
      .append(appInfo.getFinishTime()).append("\",\"")
      .append(appInfo.getState()).append("\",\"")
      .append(appInfo.getFinalStatus()).append("\",\"")
      .append(appInfo.getRunningContainers()).append("\",\"")
      .append("<br title='").append(percent)
      .append("'> <div class='").append(C_PROGRESSBAR).append("' title='")

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/FairSchedulerPage.java

  @Override
  protected String initAppsTable() {
    return WebPageUtils.appsTableInit(true, false);
  }

  static String percent(float f) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppsBlock.java
          .th(".name", "Name").th(".type", "Application Type")
          .th(".queue", "Queue").th(".starttime", "StartTime")
          .th(".finishtime", "FinishTime").th(".state", "State")
          .th(".finalstatus", "FinalStatus")
          .th(".runningcontainer", "Running Containers")
          .th(".progress", "Progress")
          .th(".ui", "Tracking UI").th(".blacklisted", "Blacklisted Nodes")._()
          ._().tbody();

        .append("\",\"")
        .append(app.getFinalAppStatus())
        .append("\",\"")
        .append(String.valueOf(app.getRunningContainers()))
        .append("\",\"")
        .append("<br title='").append(percent).append("'> <div class='")
        .append(C_PROGRESSBAR).append("' title='").append(join(percent, '%'))

