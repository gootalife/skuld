hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/metrics2/impl/MetricsSinkAdapter.java
  void stop() {
    stopping = true;
    sinkThread.interrupt();
    if (sink instanceof Closeable) {
      IOUtils.cleanup(LOG, (Closeable)sink);
    }
    try {
      sinkThread.join();
    }
    catch (InterruptedException e) {
      LOG.warn("Stop interrupted", e);
    }
  }

  String name() {

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/metrics2/impl/TestMetricsSystemImpl.java

package org.apache.hadoop.metrics2.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
    }
  }

  private static class TestClosableSink implements MetricsSink, Closeable {

    boolean closed = false;
    CountDownLatch collectingLatch;

    public TestClosableSink(CountDownLatch collectingLatch) {
      this.collectingLatch = collectingLatch;
    }

    @Override
    public void init(SubsetConfiguration conf) {
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }

    @Override
    public void putMetrics(MetricsRecord record) {
      while (!closed) {
        collectingLatch.countDown();
      }
    }

    @Override
    public void flush() {
    }
  }

  @Test(timeout = 5000)
  public void testHangOnSinkRead() throws Exception {
    new ConfigBuilder().add("*.period", 8)
        .add("test.sink.test.class", TestSink.class.getName())
        .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();
    try {
      CountDownLatch collectingLatch = new CountDownLatch(1);
      MetricsSink sink = new TestClosableSink(collectingLatch);
      ms.registerSink("closeableSink",
          "The sink will be used to test closeability", sink);
      ms.onTimerEvent();
      assertTrue(collectingLatch.await(1, TimeUnit.SECONDS));
    } finally {
      ms.stop();
    }
  }

  @Metrics(context="test")
  private static class TestSource {
    @Metric("C1 desc") MutableCounterLong c1;

