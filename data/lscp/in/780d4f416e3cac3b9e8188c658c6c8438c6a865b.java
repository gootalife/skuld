hadoop-mapreduce-project/hadoop-mapreduce-examples/src/main/java/org/apache/hadoop/examples/terasort/TeraSort.java
    }
    
    job.getConfiguration().setInt("dfs.replication", getOutputReplication(job));
    int ret = job.waitForCompletion(true) ? 0 : 1;
    LOG.info("done");
    return ret;

hadoop-mapreduce-project/hadoop-mapreduce-examples/src/main/java/org/apache/hadoop/examples/terasort/TeraSortConfigKeys.java
  public static final long DEFAULT_NUM_ROWS = 0L;
  public static final int DEFAULT_NUM_PARTITIONS = 10;
  public static final long DEFAULT_SAMPLE_SIZE = 100000L;
  public static final boolean DEFAULT_FINAL_SYNC_ATTRIBUTE = true;
  public static final boolean DEFAULT_USE_TERA_SCHEDULER = true;
  public static final boolean DEFAULT_USE_SIMPLE_PARTITIONER = false;
  public static final int DEFAULT_OUTPUT_REPLICATION = 1;

