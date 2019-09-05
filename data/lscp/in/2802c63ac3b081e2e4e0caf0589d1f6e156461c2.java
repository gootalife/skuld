hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/WindowsResourceCalculatorPlugin.java
  @Override
  public long getCpuFrequency() {
    refreshIfNeeded();
    return cpuFrequencyKhz;
  }


