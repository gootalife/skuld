hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/RMAppImpl.java
              + " failed due to " + failedEvent.getDiagnostics()
              + ". Failing the application.";
    } else if (this.isNumAttemptsBeyondThreshold) {
      int globalLimit = conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      msg = String.format(
        "Application %s failed %d times%s%s due to %s. Failing the application.",
          getApplicationId(),
          maxAppAttempts,
          (attemptFailuresValidityInterval <= 0 ? ""
               : (" in previous " + attemptFailuresValidityInterval
                  + " milliseconds")),
          (globalLimit == maxAppAttempts) ? ""
              : (" (global limit =" + globalLimit
                 + "; local limit is =" + maxAppAttempts + ")"),
          failedEvent.getDiagnostics());
    }
    return msg;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/attempt/RMAppAttemptImpl.java
        .append(status.getDiagnostics());
    if (this.getTrackingUrl() != null) {
      diagnosticsBuilder.append("For more detailed output,").append(
        " check the application tracking page: ").append(
        this.getTrackingUrl()).append(
        " Then click on links to logs of each attempt.\n");
    }
    return diagnosticsBuilder.toString();
  }

