hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapred/TestJobCounters.java
    jobConf.setOutputValueClass(Text.class);
    jobConf.setMaxMapAttempts(1);
    jobConf.setMaxReduceAttempts(1);
    jobConf.set(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, heapOptions);
    jobConf.set(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS, heapOptions);

    jobConf.setLong(MemoryLoaderMapper.TARGET_VALUE, targetMapValue);

