hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/MRAppMaster.java
            " because a commit was started.");
        copyHistory = true;
        if (commitSuccess) {
          shutDownMessage =
              "Job commit succeeded in a prior MRAppMaster attempt " +
              "before it crashed. Recovering.";
          forcedState = JobStateInternal.SUCCEEDED;
        } else if (commitFailure) {
          shutDownMessage =
              "Job commit failed in a prior MRAppMaster attempt " +
              "before it crashed. Not retrying.";
          forcedState = JobStateInternal.FAILED;
        } else {
          shutDownMessage =
              "Job commit from a prior MRAppMaster attempt is " +
              "potentially in progress. Preventing multiple commit executions";
          forcedState = JobStateInternal.ERROR;
        }
      }
      if ( !isLastAMRetry){
        if (((JobImpl)job).getInternalState() != JobStateInternal.REBOOT) {
          LOG.info("Job finished cleanly, recording last MRAppMaster retry");
          isLastAMRetry = true;
        }
      }

