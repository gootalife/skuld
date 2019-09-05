hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/CompletedJob.java
    String historyUrl = "N/A";
    try {
      historyUrl =
          MRWebAppUtil.getApplicationWebURLOnJHSWithScheme(conf,
              jobId.getAppId());
    } catch (UnknownHostException e) {
        LOG.error("Problem determining local host: " + e.getMessage());
    }
    report.setTrackingUrl(historyUrl);
    report.setAMInfos(getAMInfos());

