hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppAttemptBlock.java
        "AM Container:",
        appAttempt.getAmContainerId() == null || containers == null
            || !hasAMContainer(appAttemptReport.getAMContainerId(), containers)
            ? null : root_url("container", appAttempt.getAmContainerId()),
        appAttempt.getAmContainerId() == null ? "N/A" :
          String.valueOf(appAttempt.getAmContainerId()))
      ._("Node:", node)
      ._(

