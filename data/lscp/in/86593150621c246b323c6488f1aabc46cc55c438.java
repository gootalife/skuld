hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/resource/Resources.java
    return multiplyTo(clone(lhs), by);
  }

  public static Resource multiplyAndAddTo(
      Resource lhs, Resource rhs, double by) {
    lhs.setMemory(lhs.getMemory() + (int)(rhs.getMemory() * by));
    lhs.setVirtualCores(lhs.getVirtualCores()
        + (int)(rhs.getVirtualCores() * by));
    return lhs;
  }

  public static Resource multiplyAndNormalizeUp(
      ResourceCalculator calculator,Resource lhs, double by, Resource factor) {
    return calculator.multiplyAndNormalizeUp(lhs, by, factor);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FSAppAttempt.java
    synchronized (this) {
      for (Priority p : getPriorities()) {
        for (ResourceRequest r : getResourceRequests(p).values()) {
          Resources.multiplyAndAddTo(demand,
              r.getCapability(), r.getNumContainers());
        }
      }
    }

