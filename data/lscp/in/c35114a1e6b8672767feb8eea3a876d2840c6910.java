hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/attempt/RMAppAttemptImpl.java
      finishEvent.getApplicationAttemptId()).append(
      " exited with ").append(" exitCode: ").append(status.getExitStatus()).
      append("\n");
    diagnosticsBuilder.append("Failing this attempt.").append("Diagnostics: ")
        .append(status.getDiagnostics());
    if (this.getTrackingUrl() != null) {
      diagnosticsBuilder.append("For more detailed output,").append(
        " check application tracking page: ").append(
        this.getTrackingUrl()).append(
        " Then, click on links to logs of each attempt.\n");
    }
    return diagnosticsBuilder.toString();
  }


