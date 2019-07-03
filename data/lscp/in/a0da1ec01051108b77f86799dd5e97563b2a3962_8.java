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

