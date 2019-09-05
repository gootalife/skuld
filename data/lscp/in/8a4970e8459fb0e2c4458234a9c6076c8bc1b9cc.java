hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/MRAppMaster.java
    }
  }

  protected void shutdownTaskLog() {
    TaskLog.syncLogsShutdown(logSyncer);
  }

  @Override
  public void stop() {
    super.stop();
    shutdownTaskLog();
  }

  private boolean isRecoverySupported() throws IOException {
    T call(Configuration conf) throws Exception;
  }

  protected void shutdownLogManager() {
    LogManager.shutdown();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    shutdownLogManager();
  }

  public ClientService getClientService() {

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/MRApp.java
            new Text(containerToken.getService()));
    return token.decodeIdentifier();
  }

  @Override
  protected void shutdownTaskLog() {
  }

  @Override
  protected void shutdownLogManager() {
  }

}
 

