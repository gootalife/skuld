hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/api/records/TestTaskAttemptReport.java
++ b/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/api/records/TestTaskAttemptReport.java

package org.apache.hadoop.mapreduce.v2.api.records;

import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskAttemptReportPBImpl;
import org.apache.hadoop.mapreduce.v2.app.MockJobs;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos;
import org.apache.hadoop.yarn.util.Records;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestTaskAttemptReport {

  @Test
  public void testSetRawCounters() {
    TaskAttemptReport report = Records.newRecord(TaskAttemptReport.class);
    org.apache.hadoop.mapreduce.Counters rCounters = MockJobs.newCounters();
    report.setRawCounters(rCounters);
    Counters counters = report.getCounters();
    assertNotEquals(null, counters);
  }

  @Test
  public void testBuildImplicitRawCounters() {
    TaskAttemptReportPBImpl report = new TaskAttemptReportPBImpl();
    org.apache.hadoop.mapreduce.Counters rCounters = MockJobs.newCounters();
    report.setRawCounters(rCounters);
    MRProtos.TaskAttemptReportProto protoVal = report.getProto();
    Counters counters = report.getCounters();
    assertTrue(protoVal.hasCounters());
  }

  @Test
  public void testCountersOverRawCounters() {
    TaskAttemptReport report = Records.newRecord(TaskAttemptReport.class);
    org.apache.hadoop.mapreduce.Counters rCounters = MockJobs.newCounters();
    Counters altCounters = TypeConverter.toYarn(rCounters);
    report.setRawCounters(rCounters);
    report.setCounters(altCounters);
    Counters counters = report.getCounters();
    assertNotEquals(null, counters);
    assertNotEquals(rCounters, altCounters);
    assertEquals(counters, altCounters);
  }

  @Test
  public void testUninitializedCounters() {
    TaskAttemptReport report = Records.newRecord(TaskAttemptReport.class);
    assertEquals(null, report.getCounters());
    assertEquals(null, report.getRawCounters());
  }

  @Test
  public void testSetRawCountersToNull() {
    TaskAttemptReport report = Records.newRecord(TaskAttemptReport.class);
    report.setRawCounters(null);
    assertEquals(null, report.getCounters());
    assertEquals(null, report.getRawCounters());

  }

  @Test
  public void testSetCountersToNull() {
    TaskAttemptReport report = Records.newRecord(TaskAttemptReport.class);
    report.setCounters(null);
    assertEquals(null, report.getCounters());
    assertEquals(null, report.getRawCounters());
  }

  @Test
  public void testSetNonNullCountersToNull() {
    TaskAttemptReport report = Records.newRecord(TaskAttemptReport.class);
    org.apache.hadoop.mapreduce.Counters rCounters = MockJobs.newCounters();
    report.setRawCounters(rCounters);
    Counters counters = report.getCounters();
    assertNotEquals(null, counters);
    report.setCounters(null);
    assertEquals(null, report.getCounters());
    assertEquals(null, report.getRawCounters());
  }

  @Test
  public void testSetNonNullRawCountersToNull() {
    TaskAttemptReport report = Records.newRecord(TaskAttemptReport.class);
    org.apache.hadoop.mapreduce.Counters rCounters = MockJobs.newCounters();
    report.setRawCounters(rCounters);
    Counters counters = report.getCounters();
    assertNotEquals(null, counters);
    report.setRawCounters(null);
    assertEquals(null, report.getCounters());
    assertEquals(null, report.getRawCounters());
  }
}


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/api/records/TestTaskReport.java
++ b/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/api/records/TestTaskReport.java

package org.apache.hadoop.mapreduce.v2.api.records;

import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskReportPBImpl;
import org.apache.hadoop.mapreduce.v2.app.MockJobs;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos;
import org.apache.hadoop.yarn.util.Records;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestTaskReport {

  @Test
  public void testSetRawCounters() {
    TaskReport report = Records.newRecord(TaskReport.class);
    org.apache.hadoop.mapreduce.Counters rCounters = MockJobs.newCounters();
    report.setRawCounters(rCounters);
    Counters counters = report.getCounters();
    assertNotEquals(null, counters);
  }

  @Test
  public void testBuildImplicitRawCounters() {
    TaskReportPBImpl report = new TaskReportPBImpl();
    org.apache.hadoop.mapreduce.Counters rCounters = MockJobs.newCounters();
    report.setRawCounters(rCounters);
    MRProtos.TaskReportProto protoVal = report.getProto();
    assertTrue(protoVal.hasCounters());
  }

  @Test
  public void testCountersOverRawCounters() {
    TaskReport report = Records.newRecord(TaskReport.class);
    org.apache.hadoop.mapreduce.Counters rCounters = MockJobs.newCounters();
    Counters altCounters = TypeConverter.toYarn(rCounters);
    report.setRawCounters(rCounters);
    report.setCounters(altCounters);
    Counters counters = report.getCounters();
    assertNotEquals(null, counters);
    assertNotEquals(rCounters, altCounters);
    assertEquals(counters, altCounters);
  }

  @Test
  public void testUninitializedCounters() {
    TaskReport report = Records.newRecord(TaskReport.class);
    assertEquals(null, report.getCounters());
    assertEquals(null, report.getRawCounters());
  }

  @Test
  public void testSetRawCountersToNull() {
    TaskReport report = Records.newRecord(TaskReport.class);
    report.setRawCounters(null);
    assertEquals(null, report.getCounters());
    assertEquals(null, report.getRawCounters());

  }

  @Test
  public void testSetCountersToNull() {
    TaskReport report = Records.newRecord(TaskReport.class);
    report.setCounters(null);
    assertEquals(null, report.getCounters());
    assertEquals(null, report.getRawCounters());
  }

  @Test
  public void testSetNonNullCountersToNull() {
    TaskReport report = Records.newRecord(TaskReport.class);
    org.apache.hadoop.mapreduce.Counters rCounters = MockJobs.newCounters();
    report.setRawCounters(rCounters);
    Counters counters = report.getCounters();
    assertNotEquals(null, counters);
    report.setCounters(null);
    assertEquals(null, report.getCounters());
    assertEquals(null, report.getRawCounters());
  }

  @Test
  public void testSetNonNullRawCountersToNull() {
    TaskReport report = Records.newRecord(TaskReport.class);
    org.apache.hadoop.mapreduce.Counters rCounters = MockJobs.newCounters();
    report.setRawCounters(rCounters);
    Counters counters = report.getCounters();
    assertNotEquals(null, counters);
    report.setRawCounters(null);
    assertEquals(null, report.getCounters());
    assertEquals(null, report.getRawCounters());
  }
}
\No newline at end of file

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-common/src/main/java/org/apache/hadoop/mapreduce/v2/api/records/TaskAttemptReport.java
  public abstract long getSortFinishTime();
  public abstract Counters getCounters();
  public abstract org.apache.hadoop.mapreduce.Counters getRawCounters();
  public abstract String getDiagnosticInfo();
  public abstract String getStateString();
  public abstract Phase getPhase();
  public abstract void setStartTime(long startTime);
  public abstract void setFinishTime(long finishTime);
  public abstract void setCounters(Counters counters);
  public abstract void
      setRawCounters(org.apache.hadoop.mapreduce.Counters rCounters);
  public abstract void setDiagnosticInfo(String diagnosticInfo);
  public abstract void setStateString(String stateString);
  public abstract void setPhase(Phase phase);

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-common/src/main/java/org/apache/hadoop/mapreduce/v2/api/records/TaskReport.java
  public abstract long getStartTime();
  public abstract long getFinishTime();
  public abstract Counters getCounters();
  public abstract org.apache.hadoop.mapreduce.Counters getRawCounters();
  public abstract List<TaskAttemptId> getRunningAttemptsList();
  public abstract TaskAttemptId getRunningAttempt(int index);
  public abstract int getRunningAttemptsCount();
  public abstract void setStartTime(long startTime);
  public abstract void setFinishTime(long finishTime);
  public abstract void setCounters(Counters counters);
  public abstract void
      setRawCounters(org.apache.hadoop.mapreduce.Counters rCounters);

  public abstract void addAllRunningAttempts(List<TaskAttemptId> taskAttempts);
  public abstract void addRunningAttempt(TaskAttemptId taskAttempt);

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-common/src/main/java/org/apache/hadoop/mapreduce/v2/api/records/impl/pb/TaskAttemptReportPBImpl.java

package org.apache.hadoop.mapreduce.v2.api.records.impl.pb;

import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

  private TaskAttemptId taskAttemptId = null;
  private Counters counters = null;
  private org.apache.hadoop.mapreduce.Counters rawCounters = null;
  private ContainerId containerId = null;

  public TaskAttemptReportPBImpl() {
    builder = TaskAttemptReportProto.newBuilder();
  }
    if (this.taskAttemptId != null) {
      builder.setTaskAttemptId(convertToProtoFormat(this.taskAttemptId));
    }
    convertRawCountersToCounters();
    if (this.counters != null) {
      builder.setCounters(convertToProtoFormat(this.counters));
    }
  @Override
  public Counters getCounters() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    convertRawCountersToCounters();
    if (this.counters != null) {
      return this.counters;
    }
  @Override
  public void setCounters(Counters counters) {
    maybeInitBuilder();
    if (counters == null) {
      builder.clearCounters();
    }
    this.counters = counters;
    this.rawCounters = null;
  }

  @Override
  public org.apache.hadoop.mapreduce.Counters
        getRawCounters() {
    return this.rawCounters;
  }

  @Override
  public void setRawCounters(org.apache.hadoop.mapreduce.Counters rCounters) {
    setCounters(null);
    this.rawCounters = rCounters;
  }

  private void convertRawCountersToCounters() {
    if (this.counters == null && this.rawCounters != null) {
      this.counters = TypeConverter.toYarn(rawCounters);
      this.rawCounters = null;
    }
  }

  @Override
  public long getStartTime() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-common/src/main/java/org/apache/hadoop/mapreduce/v2/api/records/impl/pb/TaskReportPBImpl.java
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;

  private TaskId taskId = null;
  private Counters counters = null;
  private org.apache.hadoop.mapreduce.Counters rawCounters = null;
  private List<TaskAttemptId> runningAttempts = null;
  private TaskAttemptId successfulAttemptId = null;
  private List<String> diagnostics = null;
    if (this.taskId != null) {
      builder.setTaskId(convertToProtoFormat(this.taskId));
    }
    convertRawCountersToCounters();
    if (this.counters != null) {
      builder.setCounters(convertToProtoFormat(this.counters));
    }
  @Override
  public Counters getCounters() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    convertRawCountersToCounters();
    if (this.counters != null) {
      return this.counters;
    }
  @Override
  public void setCounters(Counters counters) {
    maybeInitBuilder();
    if (counters == null) {
      builder.clearCounters();
    }
    this.counters = counters;
    this.rawCounters = null;
  }

  @Override
  public org.apache.hadoop.mapreduce.Counters
      getRawCounters() {
    return this.rawCounters;
  }

  @Override
  public void setRawCounters(org.apache.hadoop.mapreduce.Counters rCounters) {
    setCounters(null);
    this.rawCounters = rCounters;
  }

  private void convertRawCountersToCounters() {
    if (this.counters == null && this.rawCounters != null) {
      this.counters = TypeConverter.toYarn(rawCounters);
      this.rawCounters = null;
    }
  }

  @Override
  public long getStartTime() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/CompletedTask.java
    if (counters == null) {
      counters = EMPTY_COUNTERS;
    }
    report.setRawCounters(counters);
    if (successfulAttempt != null) {
      report.setSuccessfulAttempt(successfulAttempt);
    }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/CompletedTaskAttempt.java
    }
    report.setStateString(attemptInfo.getState());
    report.setRawCounters(getCounters());
    report.setContainerId(attemptInfo.getContainerId());
    if (attemptInfo.getHostname() == null) {
      report.setNodeManagerHost("UNKNOWN");

