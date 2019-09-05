hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/server/tasktracker/TTConfig.java
    "mapreduce.tasktracker.resourcecalculatorplugin";
  public static final String TT_REDUCE_SLOTS = 
    "mapreduce.tasktracker.reduce.tasks.maximum";
  public static final String TT_LOCAL_CACHE_SIZE = 
    "mapreduce.tasktracker.cache.local.size";
  public static final String TT_LOCAL_CACHE_SUBDIRS_LIMIT =

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/util/ConfigUtil.java
        TTConfig.TT_RESOURCE_CALCULATOR_PLUGIN),
      new DeprecationDelta("mapred.tasktracker.reduce.tasks.maximum",
        TTConfig.TT_REDUCE_SLOTS),
      new DeprecationDelta(
        "mapred.tasktracker.tasks.sleeptime-before-sigkill",
        TTConfig.TT_SLEEP_TIME_BEFORE_SIG_KILL),

