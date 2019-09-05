hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/logaggregation/AppLogAggregatorImpl.java
        finalReport.setApplicationId(appId);
        finalReport.setLogAggregationStatus(renameTemporaryLogFileFailed
            ? LogAggregationStatus.FAILED : LogAggregationStatus.SUCCEEDED);
        this.context.getLogAggregationStatusForApps().add(finalReport);
      }
    } finally {
      if (writer != null) {

