hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/RMAppManager.java
        success = true;
        break;
      default:
        break;
    }
    
    if (success) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/FileSystemRMStateStore.java
  private Path dtSequenceNumberPath = null;
  private int fsNumRetries;
  private long fsRetryInterval;
  private volatile boolean isHDFS;

  @VisibleForTesting
  Path fsWorkingPath;

