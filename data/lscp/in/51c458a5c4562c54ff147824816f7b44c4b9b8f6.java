hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/TasksPage.java
      .append(", bProcessing: true")

      .append("\n, aoColumnDefs: [\n")
      .append("{'sType':'string', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }")

      .append("\n, {'sType':'numeric', bSearchable:false, 'aTargets': [1]")

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/webapp/HsTasksPage.java
    .append(", bProcessing: true")

    .append("\n, aoColumnDefs: [\n")
    .append("{'sType':'string', 'aTargets': [ 0 ]")
    .append(", 'mRender': parseHadoopID }")

    .append(", {'sType':'numeric', 'aTargets': [ 4")

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/AppAttemptPage.java

  protected String getContainersTableColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb.append("[\n").append("{'sType':'string', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }]").toString();
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/AppPage.java

  protected String getAttemptsTableColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb.append("[\n").append("{'sType':'string', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }")

      .append("\n, {'sType':'numeric', 'aTargets': [1]")

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/AppAttemptBlock.java
    StringBuilder containersTableData = new StringBuilder("[\n");
    for (ContainerReport containerReport : containers) {
      ContainerInfo container = new ContainerInfo(containerReport);
      containersTableData
        .append("[\"<a href='")
        .append(url("container", container.getContainerId()))

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/AppBlock.java
        logsLink = containerReport.getLogUrl();
        nodeLink = containerReport.getNodeHttpAddress();
      }
      attemptsTableData
        .append("[\"<a href='")
        .append(url("appattempt", appAttempt.getAppAttemptId()))

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/AppsBlock.java
      }
      AppInfo app = new AppInfo(appReport);
      String percent = String.format("%.1f", app.getProgress());
      appsTableData
        .append("[\"<a href='")
        .append(url("app", app.getAppId()))

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/FairSchedulerAppsBlock.java
        continue;
      }
      appsTableData.append("[\"<a href='")
      .append(url("app", appInfo.getAppId())).append("'>")
      .append(appInfo.getAppId()).append("</a>\",\"")

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppBlock.java
        blacklistedNodesCount = String.valueOf(nodes.size());
      }

      attemptsTableData
          .append("[\"<a href='")
          .append(url("appattempt", appAttempt.getAppAttemptId()))

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppsBlock.java
        blacklistedNodesCount = String.valueOf(nodes.size());
      }
      String percent = String.format("%.1f", app.getProgress());
      appsTableData
        .append("[\"<a href='")
        .append(url("app", app.getAppId()))

