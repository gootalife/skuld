hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/job/event/JobTaskAttemptFetchFailureEvent.java

  private final TaskAttemptId reduce;
  private final List<TaskAttemptId> maps;
  private final String hostname;

  public JobTaskAttemptFetchFailureEvent(TaskAttemptId reduce, 
      List<TaskAttemptId> maps, String host) {
    super(reduce.getTaskId().getJobId(),
        JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE);
    this.reduce = reduce;
    this.maps = maps;
    this.hostname = host;
  }

  public List<TaskAttemptId> getMaps() {
    return reduce;
  }

  public String getHost() {
    return hostname;
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/job/event/TaskAttemptTooManyFetchFailureEvent.java
++ b/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/job/event/TaskAttemptTooManyFetchFailureEvent.java

package org.apache.hadoop.mapreduce.v2.app.job.event;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

public class TaskAttemptTooManyFetchFailureEvent extends TaskAttemptEvent {
  private TaskAttemptId reduceID;
  private String  reduceHostname;

  public TaskAttemptTooManyFetchFailureEvent(TaskAttemptId attemptId,
      TaskAttemptId reduceId, String reduceHost) {
      super(attemptId, TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURE);
    this.reduceID = reduceId;
    this.reduceHostname = reduceHost;
  }

  public TaskAttemptId getReduceId() {
    return reduceID;
  }

  public String getReduceHost() {
    return reduceHostname;
  }  
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/job/impl/JobImpl.java
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskAttemptFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobUpdatedNodesEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptTooManyFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskRecoverEvent;
            && failureRate >= job.getMaxAllowedFetchFailuresFraction()) {
          LOG.info("Too many fetch-failures for output of task attempt: " + 
              mapId + " ... raising fetch failure to map");
          job.eventHandler.handle(new TaskAttemptTooManyFetchFailureEvent(mapId,
              fetchfailureEvent.getReduce(), fetchfailureEvent.getHost()));
          job.fetchFailuresMapping.remove(mapId);
        }
      }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/job/impl/TaskAttemptImpl.java
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptRecoverEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptTooManyFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent event) {
      TaskAttemptTooManyFetchFailureEvent fetchFailureEvent =
          (TaskAttemptTooManyFetchFailureEvent) event;
      Preconditions
          .checkArgument(taskAttempt.getID().getTaskId().getTaskType() == TaskType.MAP);
      taskAttempt.addDiagnosticInfo("Too many fetch failures."
          + " Failing the attempt. Last failure reported by " +
          fetchFailureEvent.getReduceId() +
          " from host " + fetchFailureEvent.getReduceHost());

      if (taskAttempt.getLaunchTime() != 0) {
        taskAttempt.eventHandler
      if (taskAttempt.reportedStatus.fetchFailedMaps != null && 
          taskAttempt.reportedStatus.fetchFailedMaps.size() > 0) {
        String hostname = taskAttempt.container == null ? "UNKNOWN"
            : taskAttempt.container.getNodeId().getHost();
        taskAttempt.eventHandler.handle(new JobTaskAttemptFetchFailureEvent(
            taskAttempt.attemptId, taskAttempt.reportedStatus.fetchFailedMaps,
                hostname));
      }
    }
  }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/TestFetchFailure.java
    app.waitForState(reduceAttempt, TaskAttemptState.RUNNING);
    
    sendFetchFailure(app, reduceAttempt, mapAttempt1, "host");
    sendFetchFailure(app, reduceAttempt, mapAttempt1, "host");
    sendFetchFailure(app, reduceAttempt, mapAttempt1, "host");

    app.waitForState(mapTask, TaskState.RUNNING);
    app.waitForState(reduceAttempt, TaskAttemptState.RUNNING);

    sendFetchFailure(app, reduceAttempt, mapAttempt1, "host");
    sendFetchFailure(app, reduceAttempt, mapAttempt1, "host");
    sendFetchFailure(app, reduceAttempt, mapAttempt1, "host");

    app.waitForState(mapTask, TaskState.RUNNING);
    updateStatus(app, reduceAttempt3, Phase.SHUFFLE);

    sendFetchFailure(app, reduceAttempt, mapAttempt1, "host1");
    sendFetchFailure(app, reduceAttempt2, mapAttempt1, "host2");

    assertEquals(TaskState.SUCCEEDED, mapTask.getState());
    updateStatus(app, reduceAttempt3, Phase.REDUCE);

    sendFetchFailure(app, reduceAttempt3, mapAttempt1, "host3");

    app.waitForState(mapTask, TaskState.RUNNING);
    Assert.assertEquals("Map TaskAttempt state not correct",
        TaskAttemptState.FAILED, mapAttempt1.getState());

    Assert.assertEquals(mapAttempt1.getDiagnostics().get(0),
            "Too many fetch failures. Failing the attempt. "
            + "Last failure reported by "
            + reduceAttempt3.getID().toString() + " from host host3");

    Assert.assertEquals("Num attempts in Map Task not correct",
        2, mapTask.getAttempts().size());
    
  }

  private void sendFetchFailure(MRApp app, TaskAttempt reduceAttempt, 
      TaskAttempt mapAttempt, String hostname) {
    app.getContext().getEventHandler().handle(
        new JobTaskAttemptFetchFailureEvent(
            reduceAttempt.getID(), 
            Arrays.asList(new TaskAttemptId[] {mapAttempt.getID()}),
                hostname));
  }
  
  static class MRAppWithHistory extends MRApp {

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/job/impl/TestTaskAttempt.java
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptTooManyFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerRequestEvent;
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
    TaskId reduceTaskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.REDUCE);
    TaskAttemptId reduceTAId =
        MRBuilderUtils.newTaskAttemptId(reduceTaskId, 0);
    Path jobFile = mock(Path.class);

    MockEventHandler eventHandler = new MockEventHandler();

    assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    taImpl.handle(new TaskAttemptTooManyFetchFailureEvent(attemptId,
        reduceTAId, "Host"));
    assertEquals("Task attempt is not in FAILED state", taImpl.getState(),
        TaskAttemptState.FAILED);
    taImpl.handle(new TaskAttemptEvent(attemptId,
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
    TaskId reducetaskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.REDUCE);
    TaskAttemptId reduceTAId =
        MRBuilderUtils.newTaskAttemptId(reducetaskId, 0);
    Path jobFile = mock(Path.class);

    MockEventHandler eventHandler = new MockEventHandler();

    Long finishTime = taImpl.getFinishTime();
    Thread.sleep(5);
    taImpl.handle(new TaskAttemptTooManyFetchFailureEvent(attemptId,
        reduceTAId, "Host"));

    assertEquals("Task attempt is not in Too Many Fetch Failure state",
        taImpl.getState(), TaskAttemptState.FAILED);

