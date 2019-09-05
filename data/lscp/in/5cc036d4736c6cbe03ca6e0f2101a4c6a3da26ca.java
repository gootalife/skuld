hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/job/impl/JobImpl.java
  private final ApplicationAttemptId applicationAttemptId;
  private final Clock clock;
  private final JobACLsManager aclsManager;
  private final String reporterUserName;
  private final Map<JobACL, AccessControlList> jobACLs;
  private float setupWeight = 0.05f;
  private float cleanupWeight = 0.05f;
    this.jobTokenSecretManager = jobTokenSecretManager;

    this.aclsManager = new JobACLsManager(conf);
    this.reporterUserName = System.getProperty("user.name");
    this.jobACLs = aclsManager.constructJobACLs(conf);

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
      }

      if (getInternalState() == JobStateInternal.NEW) {
        return MRBuilderUtils.newJobReport(jobId, jobName, reporterUserName,
            state, appSubmitTime, startTime, finishTime, setupProgress, 0.0f,
            0.0f, cleanupProgress, jobFile, amInfos, isUber, diagsb.toString());
      }

      computeProgress();
      JobReport report = MRBuilderUtils.newJobReport(jobId, jobName,
          reporterUserName,
          state, appSubmitTime, startTime, finishTime, setupProgress,
          this.mapProgress, this.reduceProgress,
          cleanupProgress, jobFile, amInfos, isUber, diagsb.toString());

