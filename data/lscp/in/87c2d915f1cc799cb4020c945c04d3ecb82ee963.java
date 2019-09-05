hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/task/reduce/MergeManagerImpl.java
  Set<CompressAwarePath> onDiskMapOutputs = new TreeSet<CompressAwarePath>();
  private final OnDiskMerger onDiskMerger;

  @VisibleForTesting
  final long memoryLimit;

  private long usedMemory;
  private long commitMemory;
  private final long maxSingleShuffleLimit;
    }

    this.memoryLimit = (long)(jobConf.getLong(
        MRJobConfig.REDUCE_MEMORY_TOTAL_BYTES,
        Runtime.getRuntime().maxMemory()) * maxInMemCopyUse);

    this.ioSortFactor = jobConf.getInt(MRJobConfig.IO_SORT_FACTOR, 100);

    }
  }

  @VisibleForTesting
  final long getMaxInMemReduceLimit() {
    final float maxRedPer =
        jobConf.getFloat(MRJobConfig.REDUCE_INPUT_BUFFER_PERCENT, 0f);
    if (maxRedPer > 1.0 || maxRedPer < 0.0) {
      throw new RuntimeException(maxRedPer + ": "
          + MRJobConfig.REDUCE_INPUT_BUFFER_PERCENT
          + " must be a float between 0 and 1.0");
    }
    return (long)(memoryLimit * maxRedPer);
  }

  private RawKeyValueIterator finalMerge(JobConf job, FileSystem fs,
                                       List<InMemoryMapOutput<K,V>> inMemoryMapOutputs,
                                       List<CompressAwarePath> onDiskMapOutputs
    LOG.info("finalMerge called with " +
        inMemoryMapOutputs.size() + " in-memory map-outputs and " +
        onDiskMapOutputs.size() + " on-disk map-outputs");
    final long maxInMemReduce = getMaxInMemReduceLimit();
    Class<K> keyClass = (Class<K>)job.getMapOutputKeyClass();
    Class<V> valueClass = (Class<V>)job.getMapOutputValueClass();

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/test/java/org/apache/hadoop/mapreduce/task/reduce/TestMergeManager.java
    }

  }

  @Test
  public void testLargeMemoryLimits() throws Exception {
    final JobConf conf = new JobConf();
    conf.setLong(MRJobConfig.REDUCE_MEMORY_TOTAL_BYTES,
        8L * 1024 * 1024 * 1024);

    conf.setFloat(MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT, 1.0f);

    conf.setFloat(MRJobConfig.SHUFFLE_MEMORY_LIMIT_PERCENT, 0.95f);

    conf.setFloat(MRJobConfig.SHUFFLE_MERGE_PERCENT, 1.0f);

    conf.setFloat(MRJobConfig.REDUCE_INPUT_BUFFER_PERCENT, 1.0f);

    final MergeManagerImpl<Text, Text> mgr = new MergeManagerImpl<Text, Text>(
        null, conf, mock(LocalFileSystem.class), null, null, null, null, null,
        null, null, null, null, null, new MROutputFiles());
    assertTrue("Large shuffle area unusable: " + mgr.memoryLimit,
        mgr.memoryLimit > Integer.MAX_VALUE);
    final long maxInMemReduce = mgr.getMaxInMemReduceLimit();
    assertTrue("Large in-memory reduce area unusable: " + maxInMemReduce,
        maxInMemReduce > Integer.MAX_VALUE);
  }
}

