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

