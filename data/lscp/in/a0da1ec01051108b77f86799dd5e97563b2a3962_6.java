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

