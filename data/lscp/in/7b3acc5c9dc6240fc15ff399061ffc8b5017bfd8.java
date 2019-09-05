hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/Time.java
  public static long monotonicNow() {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BPOfferService.java
  void scheduleBlockReport(long delay) {
    for (BPServiceActor actor : bpServices) {
      actor.getScheduler().scheduleBlockReport(delay);
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BPServiceActor.java
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;


  final BPOfferService bpos;
  
  volatile long lastCacheReport = 0;
  private final Scheduler scheduler;

  Thread bpThread;
  DatanodeProtocolClientSideTranslatorPB bpNamenode;

  static enum RunningState {
    CONNECTING, INIT_FAILED, RUNNING, EXITED, FAILED;
    this.nnAddr = nnAddr;
    this.dnConf = dn.getDnConf();
    prevBlockReportId = DFSUtil.getRandom().nextLong();
    scheduler = new Scheduler(dnConf.heartBeatInterval, dnConf.blockReportInterval);
  }

  boolean isAlive() {
    register(nsInfo);
  }

  @VisibleForTesting
  void triggerBlockReportForTests() {
    synchronized (pendingIncrementalBRperStorage) {
      scheduler.scheduleHeartbeat();
      long nextBlockReportTime = scheduler.scheduleBlockReport(0);
      pendingIncrementalBRperStorage.notifyAll();
      while (nextBlockReportTime - scheduler.nextBlockReportTime >= 0) {
        try {
          pendingIncrementalBRperStorage.wait(100);
        } catch (InterruptedException e) {
  @VisibleForTesting
  void triggerHeartbeatForTests() {
    synchronized (pendingIncrementalBRperStorage) {
      final long nextHeartbeatTime = scheduler.scheduleHeartbeat();
      pendingIncrementalBRperStorage.notifyAll();
      while (nextHeartbeatTime - scheduler.nextHeartbeatTime >= 0) {
        try {
          pendingIncrementalBRperStorage.wait(100);
        } catch (InterruptedException e) {
  List<DatanodeCommand> blockReport() throws IOException {
    if (!scheduler.isBlockReportDue()) {
      return null;
    }

                  (nCmds + " commands: " + Joiner.on("; ").join(cmds)))) +
          ".");
    }
    scheduler.scheduleNextBlockReport();
    return cmds.size() == 0 ? null : cmds;
  }

  DatanodeCommand cacheReport() throws IOException {
    if (dn.getFSDataset().getCacheCapacity() == 0) {
    while (shouldRun()) {
      try {
        final long startTime = scheduler.monotonicNow();

        final boolean sendHeartbeat = scheduler.isHeartbeatDue(startTime);
        if (sendHeartbeat) {
          scheduler.scheduleNextHeartbeat();
          if (!dn.areHeartbeatsDisabledForTests()) {
            HeartbeatResponse resp = sendHeartBeat();
            assert resp != null;
            dn.getMetrics().addHeartbeat(scheduler.monotonicNow() - startTime);

        long waitTime = scheduler.getHeartbeatWaitTime();
        synchronized(pendingIncrementalBRperStorage) {
          if (waitTime > 0 && !sendImmediateIBR) {
            try {
    bpos.registrationSucceeded(this, bpRegistration);

    scheduler.scheduleBlockReport(dnConf.initialBlockReportDelay);
  }


      NamespaceInfo nsInfo = retrieveNamespaceInfo();
      register(nsInfo);
      scheduler.scheduleHeartbeat();
    }
  }

    } else {
      LOG.info(bpos.toString() + ": scheduling a full block report.");
      synchronized(pendingIncrementalBRperStorage) {
        scheduler.scheduleBlockReport(0);
        pendingIncrementalBRperStorage.notifyAll();
      }
    }
      }
    }
  }

  Scheduler getScheduler() {
    return scheduler;
  }

  static class Scheduler {
    @VisibleForTesting
    volatile long nextBlockReportTime = monotonicNow();

    @VisibleForTesting
    volatile long nextHeartbeatTime = monotonicNow();

    @VisibleForTesting
    boolean resetBlockReportTime = true;

    private final long heartbeatIntervalMs;
    private final long blockReportIntervalMs;

    Scheduler(long heartbeatIntervalMs, long blockReportIntervalMs) {
      this.heartbeatIntervalMs = heartbeatIntervalMs;
      this.blockReportIntervalMs = blockReportIntervalMs;
    }

    long scheduleHeartbeat() {
      nextHeartbeatTime = monotonicNow();
      return nextHeartbeatTime;
    }

    long scheduleNextHeartbeat() {
      nextHeartbeatTime += heartbeatIntervalMs;
      return nextHeartbeatTime;
    }

    boolean isHeartbeatDue(long startTime) {
      return (nextHeartbeatTime - startTime <= 0);
    }

    boolean isBlockReportDue() {
      return nextBlockReportTime - monotonicNow() <= 0;
    }

    long scheduleBlockReport(long delay) {
      if (delay > 0) { // send BR after random delay
        nextBlockReportTime =
            monotonicNow() + DFSUtil.getRandom().nextInt((int) (delay));
      } else { // send at next heartbeat
        nextBlockReportTime = monotonicNow();
      }
      resetBlockReportTime = true; // reset future BRs for randomness
      return nextBlockReportTime;
    }

    void scheduleNextBlockReport() {
      if (resetBlockReportTime) {
        nextBlockReportTime = monotonicNow() +
            DFSUtil.getRandom().nextInt((int)(blockReportIntervalMs));
        resetBlockReportTime = false;
      } else {
        nextBlockReportTime +=
              (((monotonicNow() - nextBlockReportTime + blockReportIntervalMs) /
                  blockReportIntervalMs)) * blockReportIntervalMs;
      }
    }

    long getHeartbeatWaitTime() {
      return nextHeartbeatTime - monotonicNow();
    }

    @VisibleForTesting
    public long monotonicNow() {
      return Time.monotonicNow();
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestBpServiceActorScheduler.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestBpServiceActorScheduler.java

package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static java.lang.Math.abs;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import org.apache.hadoop.hdfs.server.datanode.BPServiceActor.Scheduler;

import java.util.Arrays;
import java.util.List;
import java.util.Random;


public class TestBpServiceActorScheduler {
  protected static final Log LOG = LogFactory.getLog(TestBpServiceActorScheduler.class);

  @Rule
  public Timeout timeout = new Timeout(300000);

  private static final long HEARTBEAT_INTERVAL_MS = 5000;      // 5 seconds
  private static final long BLOCK_REPORT_INTERVAL_MS = 10000;  // 10 seconds
  private final Random random = new Random(System.nanoTime());

  @Test
  public void testInit() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      assertTrue(scheduler.isHeartbeatDue(now));
      assertTrue(scheduler.isBlockReportDue());
    }
  }

  @Test
  public void testScheduleBlockReportImmediate() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      scheduler.scheduleBlockReport(0);
      assertTrue(scheduler.resetBlockReportTime);
      assertThat(scheduler.nextBlockReportTime, is(now));
    }
  }

  @Test
  public void testScheduleBlockReportDelayed() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      final long delayMs = 10;
      scheduler.scheduleBlockReport(delayMs);
      assertTrue(scheduler.resetBlockReportTime);
      assertTrue(scheduler.nextBlockReportTime - now >= 0);
      assertTrue(scheduler.nextBlockReportTime - (now + delayMs) < 0);
    }
  }

  @Test
  public void testScheduleNextBlockReport() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      assertTrue(scheduler.resetBlockReportTime);
      scheduler.scheduleNextBlockReport();
      assertTrue(scheduler.nextBlockReportTime - (now + BLOCK_REPORT_INTERVAL_MS) < 0);
    }
  }

  @Test
  public void testScheduleNextBlockReport2() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      scheduler.resetBlockReportTime = false;
      scheduler.scheduleNextBlockReport();
      assertThat(scheduler.nextBlockReportTime, is(now + BLOCK_REPORT_INTERVAL_MS));
    }
  }

  @Test
  public void testScheduleNextBlockReport3() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      scheduler.resetBlockReportTime = false;

      final long blockReportDelay =
          BLOCK_REPORT_INTERVAL_MS + random.nextInt(2 * (int) BLOCK_REPORT_INTERVAL_MS);
      final long origBlockReportTime = now - blockReportDelay;
      scheduler.nextBlockReportTime = origBlockReportTime;
      scheduler.scheduleNextBlockReport();
      assertTrue(scheduler.nextBlockReportTime - now < BLOCK_REPORT_INTERVAL_MS);
      assertTrue(((scheduler.nextBlockReportTime - origBlockReportTime) % BLOCK_REPORT_INTERVAL_MS) == 0);
    }
  }

  @Test
  public void testScheduleHeartbeat() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      scheduler.scheduleNextHeartbeat();
      assertFalse(scheduler.isHeartbeatDue(now));
      scheduler.scheduleHeartbeat();
      assertTrue(scheduler.isHeartbeatDue(now));
    }
  }

  private Scheduler makeMockScheduler(long now) {
    LOG.info("Using now = " + now);
    Scheduler mockScheduler = spy(new Scheduler(HEARTBEAT_INTERVAL_MS, BLOCK_REPORT_INTERVAL_MS));
    doReturn(now).when(mockScheduler).monotonicNow();
    mockScheduler.nextBlockReportTime = now;
    mockScheduler.nextHeartbeatTime = now;
    return mockScheduler;
  }

  List<Long> getTimestamps() {
    return Arrays.asList(
        0L, Long.MIN_VALUE, Long.MAX_VALUE, // test boundaries
        Long.MAX_VALUE - 1,                 // test integer overflow
        abs(random.nextLong()),             // positive random
        -abs(random.nextLong()));           // negative random
  }
}

