hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/AppInfo.java
   protected long clusterId;
   protected String applicationType;
   protected String applicationTags = "";
   protected int priority;
 
   // these are only allowed if acls allow
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

