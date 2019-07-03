hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/ApplicationReport.java
   @Public
   @Unstable
   public abstract void setUnmanagedApp(boolean unmanagedApplication);
 
   /**
    * Get priority of the application
    *
    * @return Application's priority
    */
   @Public
   @Stable
   public abstract Priority getPriority();
 
   @Private
   @Unstable
   public abstract void setPriority(Priority priority);
 }

