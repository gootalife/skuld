hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/UserMetricsInfo.java
      this.userMetricsAvailable = true;

      this.appsSubmitted = userMetrics.getAppsSubmitted();
      this.appsCompleted = userMetrics.getAppsCompleted();
      this.appsPending = userMetrics.getAppsPending();
      this.appsRunning = userMetrics.getAppsRunning();
      this.appsFailed = userMetrics.getAppsFailed();
      this.appsKilled = userMetrics.getAppsKilled();

      this.runningContainers = userMetrics.getAllocatedContainers();
      this.pendingContainers = userMetrics.getPendingContainers();

