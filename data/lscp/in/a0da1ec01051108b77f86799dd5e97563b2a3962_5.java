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
   

