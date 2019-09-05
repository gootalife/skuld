hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppsBlock.java

      String trackingURL =
          app.getTrackingUrl() == null
              || app.getTrackingUrl().equals(UNAVAILABLE)
              || app.getAppState() == YarnApplicationState.NEW ? null : app
              .getTrackingUrl();

      String trackingUI =
          app.getTrackingUrl() == null
              || app.getTrackingUrl().equals(UNAVAILABLE)
              || app.getAppState() == YarnApplicationState.NEW ? "Unassigned"
              : app.getAppState() == YarnApplicationState.FINISHED
              || app.getAppState() == YarnApplicationState.FAILED
              || app.getAppState() == YarnApplicationState.KILLED ? "History"
              : "ApplicationMaster";

