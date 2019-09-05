hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacityScheduler.java
    for (Map.Entry<String, CSQueue> e : queues.entrySet()) {
      if (!(e.getValue() instanceof ReservationQueue)) {
        String queueName = e.getKey();
        CSQueue oldQueue = e.getValue();
        CSQueue newQueue = newQueues.get(queueName); 
        if (null == newQueue) {
          throw new IOException(queueName + " cannot be found during refresh!");
        } else if (!oldQueue.getQueuePath().equals(newQueue.getQueuePath())) {
          throw new IOException(queueName + " is moved from:"
              + oldQueue.getQueuePath() + " to:" + newQueue.getQueuePath()
              + " after refresh, which is not allowed.");
        }
      }
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestQueueParsing.java
    capacityScheduler.start();
    ServiceOperations.stopQuietly(capacityScheduler);
  }
  
  @Test(expected = IOException.class)
  public void testQueueParsingWithMoveQueue()
      throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    csConf.setQueues("root", new String[] { "a" });
    csConf.setQueues("root.a", new String[] { "x", "y" });
    csConf.setCapacity("root.a", 100);
    csConf.setCapacity("root.a.x", 50);
    csConf.setCapacity("root.a.y", 50);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    
    csConf.setQueues("root", new String[] { "a", "x" });
    csConf.setQueues("root.a", new String[] { "y" });
    csConf.setCapacity("root.x", 50);
    csConf.setCapacity("root.a", 50);
    csConf.setCapacity("root.a.y", 100);
    
    capacityScheduler.reinitialize(csConf, rmContext);
  }
}

