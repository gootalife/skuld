hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/api/impl/TestYarnClient.java
            ReservationSystemTestUtil.reservationQ);
    return request;
  }

  @Test(timeout = 30000, expected = ApplicationNotFoundException.class)
  public void testShouldNotRetryForeverForNonNetworkExceptions() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, -1);

    ResourceManager rm = null;
    YarnClient yarnClient = null;
    try {
      rm = new ResourceManager();
      rm.init(conf);
      rm.start();

      yarnClient = YarnClient.createYarnClient();
      yarnClient.init(conf);
      yarnClient.start();

      ApplicationId appId = ApplicationId.newInstance(1430126768L, 10645);

      yarnClient.getApplicationReport(appId);
    } finally {
      if (yarnClient != null) {
        yarnClient.stop();
      }
      if (rm != null) {
        rm.stop();
      }
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/client/RMProxy.java
          failoverSleepBaseMs, failoverSleepMaxMs);
    }

    if (rmConnectionRetryIntervalMS < 0) {
      throw new YarnRuntimeException("Invalid Configuration. " +
          YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS +
          " should not be negative.");
    }

    RetryPolicy retryPolicy = null;
    if (waitForEver) {
      retryPolicy = RetryPolicies.RETRY_FOREVER;
    } else {
      retryPolicy =
          RetryPolicies.retryUpToMaximumTimeWithFixedSleep(rmConnectWaitMS,
              rmConnectionRetryIntervalMS, TimeUnit.MILLISECONDS);
    }

    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
        new HashMap<Class<? extends Exception>, RetryPolicy>();

