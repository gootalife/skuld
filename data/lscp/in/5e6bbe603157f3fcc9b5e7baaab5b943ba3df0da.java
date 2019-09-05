hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileSystem.java
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
      volatile int readOps;
      volatile int largeReadOps;
      volatile int writeOps;

    private final ThreadLocal<StatisticsData> threadData;

    private final Set<StatisticsDataReference> allData;

    private static final ReferenceQueue<Thread> STATS_DATA_REF_QUEUE;
    private static final Thread STATS_DATA_CLEANER;

    static {
      STATS_DATA_REF_QUEUE = new ReferenceQueue<Thread>();
      STATS_DATA_CLEANER = new Thread(new StatisticsDataReferenceCleaner());
      STATS_DATA_CLEANER.
          setName(StatisticsDataReferenceCleaner.class.getName());
      STATS_DATA_CLEANER.setDaemon(true);
      STATS_DATA_CLEANER.start();
    }

    public Statistics(String scheme) {
      this.scheme = scheme;
      this.rootData = new StatisticsData();
      this.threadData = new ThreadLocal<StatisticsData>();
      this.allData = new HashSet<StatisticsDataReference>();
    }

    public Statistics(Statistics other) {
      this.scheme = other.scheme;
      this.rootData = new StatisticsData();
      other.visitAll(new StatisticsAggregator<Void>() {
        @Override
        public void accept(StatisticsData data) {
        }
      });
      this.threadData = new ThreadLocal<StatisticsData>();
      this.allData = new HashSet<StatisticsDataReference>();
    }

    private class StatisticsDataReference extends PhantomReference<Thread> {
      private final StatisticsData data;

      public StatisticsDataReference(StatisticsData data, Thread thread) {
        super(thread, STATS_DATA_REF_QUEUE);
        this.data = data;
      }

      public StatisticsData getData() {
        return data;
      }

      public void cleanUp() {
        synchronized (Statistics.this) {
          rootData.add(data);
          allData.remove(this);
        }
      }
    }

    private static class StatisticsDataReferenceCleaner implements Runnable {
      @Override
      public void run() {
        while (true) {
          try {
            StatisticsDataReference ref =
                (StatisticsDataReference)STATS_DATA_REF_QUEUE.remove();
            ref.cleanUp();
          } catch (Throwable th) {
            LOG.warn("exception in the cleaner thread but it will continue to "
                + "run", th);
          }
        }
      }
    }

    public StatisticsData getThreadStatistics() {
      StatisticsData data = threadData.get();
      if (data == null) {
        data = new StatisticsData();
        threadData.set(data);
        StatisticsDataReference ref =
            new StatisticsDataReference(data, Thread.currentThread());
        synchronized(this) {
          allData.add(ref);
        }
      }
      return data;
    private synchronized <T> T visitAll(StatisticsAggregator<T> visitor) {
      visitor.accept(rootData);
      for (StatisticsDataReference ref: allData) {
        StatisticsData data = ref.getData();
        visitor.accept(data);
      }
      return visitor.aggregate();
    }
    @Override
    public String toString() {
      return visitAll(new StatisticsAggregator<String>() {
        private StatisticsData total = new StatisticsData();

        @Override
        public void accept(StatisticsData data) {
    public void reset() {
      visitAll(new StatisticsAggregator<Void>() {
        private StatisticsData total = new StatisticsData();

        @Override
        public void accept(StatisticsData data) {
    public String getScheme() {
      return scheme;
    }

    @VisibleForTesting
    synchronized int getAllThreadLocalDataSize() {
      return allData.size();
    }
  }
  

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/FCStatisticsBaseTest.java

package org.apache.hadoop.fs;

import static org.apache.hadoop.fs.FileContextTestHelper.createFile;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Uninterruptibles;

    fc.delete(filePath, true);
  }

  @Test(timeout=60000)
  public void testStatisticsThreadLocalDataCleanUp() throws Exception {
    final Statistics stats = new Statistics("test");
    final int size = 2;
    ExecutorService es = Executors.newFixedThreadPool(size);
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>(size);
    for (int i = 0; i < size; i++) {
      tasks.add(new Callable<Boolean>() {
        public Boolean call() {
          stats.incrementReadOps(1);
          return true;
        }
      });
    }
    es.invokeAll(tasks);
    final AtomicInteger allDataSize = new AtomicInteger(0);
    allDataSize.set(stats.getAllThreadLocalDataSize());
    Assert.assertEquals(size, allDataSize.get());
    Assert.assertEquals(size, stats.getReadOps());
    es.shutdownNow();
    es.awaitTermination(1, TimeUnit.MINUTES);
    es = null;
    System.gc();

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
          @Override
          public Boolean get() {
            int size = stats.getAllThreadLocalDataSize();
            allDataSize.set(size);
            return size == 0;
          }
        }, 1000, 10*1000);
    Assert.assertEquals(0, allDataSize.get());
    Assert.assertEquals(size, stats.getReadOps());
  }


