hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/QueuePlacementRule.java
  }

  protected String cleanName(String name) {
    name = name.trim();
    if (name.contains(".")) {
      String converted = name.replaceAll("\\.", "_dot_");
      LOG.warn("Name " + name + " is converted to " + converted

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/TestFairScheduler.java
          "Failed to initialize FairScheduler"));
    }
  }

  @Test
  public void testUserAsDefaultQueueWithLeadingTrailingSpaceUserName()
      throws Exception {
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    ApplicationAttemptId appAttemptId = createAppAttemptId(1, 1);
    createApplicationWithAMResource(appAttemptId, "default", "  user1", null);
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getNumRunnableApps());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("default", true)
        .getNumRunnableApps());
    assertEquals("root.user1", resourceManager.getRMContext().getRMApps()
        .get(appAttemptId.getApplicationId()).getQueue());

    ApplicationAttemptId attId2 = createAppAttemptId(2, 1);
    createApplicationWithAMResource(attId2, "default", "user1  ", null);
    assertEquals(2, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getNumRunnableApps());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("default", true)
        .getNumRunnableApps());
    assertEquals("root.user1", resourceManager.getRMContext().getRMApps()
        .get(attId2.getApplicationId()).getQueue());

    ApplicationAttemptId attId3 = createAppAttemptId(3, 1);
    createApplicationWithAMResource(attId3, "default", "user1", null);
    assertEquals(3, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getNumRunnableApps());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("default", true)
        .getNumRunnableApps());
    assertEquals("root.user1", resourceManager.getRMContext().getRMApps()
        .get(attId3.getApplicationId()).getQueue());
  }
}

