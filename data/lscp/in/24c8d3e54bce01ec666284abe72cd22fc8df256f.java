hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacitySchedulerConfiguration.java
     PREFIX + "node-locality-delay";

  @Private 
  public static final int DEFAULT_NODE_LOCALITY_DELAY = 40;

  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_PREFIX =
  }

  public int getNodeLocalityDelay() {
    return getInt(NODE_LOCALITY_DELAY, DEFAULT_NODE_LOCALITY_DELAY);
  }
  
  public ResourceCalculator getResourceCalculator() {

