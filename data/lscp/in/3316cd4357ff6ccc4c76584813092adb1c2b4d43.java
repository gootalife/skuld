hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacityScheduler.java
    return queue;
  }

  public CSQueue getQueue(String queueName) {
    if (queueName == null) {
      return null;
    }
      boolean includeChildQueues, boolean recursive) 
  throws IOException {
    CSQueue queue = null;
    queue = this.queues.get(queueName);
    if (queue == null) {
      throw new IOException("Unknown queue: " + queueName);
    }

