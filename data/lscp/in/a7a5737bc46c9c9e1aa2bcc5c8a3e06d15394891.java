hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/MockRM.java
      nm.nodeHeartbeat(true);
      Thread.sleep(200);
    }
    Assert.assertNotNull("Failed in waiting for " + containerId + " " +
        "allocation.", getResourceScheduler().getRMContainer(containerId));
  }

  public void waitForContainerToComplete(RMAppAttempt attempt,
      }
      tick++;
    }
    Assert.assertNotNull("Timed out waiting for SchedulerApplicationAttempt=" +
      attemptId + " to be added.", ((AbstractYarnScheduler)
        rm.getResourceScheduler()).getApplicationAttempt(attemptId));
  }

  public static MockAM launchAM(RMApp app, MockRM rm, MockNM nm)
    rm.waitForState(app.getApplicationId(), RMAppState.ACCEPTED);
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    waitForSchedulerAppAttemptAdded(attempt.getAppAttemptId(), rm);
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.SCHEDULED);
    System.out.println("Launch AM " + attempt.getAppAttemptId());
    nm.nodeHeartbeat(true);
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());

