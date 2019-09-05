hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeManager.java
    if (isStopping.getAndSet(true)) {
      return;
    }
    try {
      super.serviceStop();
      DefaultMetricsSystem.shutdown();
    } finally {
      stopRecoveryStore();
    }
  }

  public String getName() {

