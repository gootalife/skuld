hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapred/LocalContainerLauncher.java
          context.getEventHandler().handle(
              new TaskAttemptEvent(taId,
                  TaskAttemptEventType.TA_CONTAINER_CLEANED));
        } else if (event.getType() == EventType.CONTAINER_COMPLETED) {
          LOG.debug("Container completed " + event.toString());
        } else {
          LOG.warn("Ignoring unexpected event " + event.toString());
        }
        runSubtask(remoteTask, ytask.getType(), attemptID, numMapTasks,
                   (numReduceTasks > 0), localMapFiles);

        context.getEventHandler().handle(new TaskAttemptEvent(attemptID,
            TaskAttemptEventType.TA_CONTAINER_COMPLETED));

      } catch (RuntimeException re) {
        JobCounterUpdateEvent jce = new JobCounterUpdateEvent(attemptID.getTaskId().getJobId());
        jce.addCounterUpdate(JobCounter.NUM_FAILED_UBERTASKS, 1);

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/AppContext.java
  boolean hasSuccessfullyUnregistered();

  String getNMHostname();

  TaskAttemptFinishingMonitor getTaskAttemptFinishingMonitor();
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/MRAppMaster.java
  private JobHistoryEventHandler jobHistoryEventHandler;
  private SpeculatorEventDispatcher speculatorEventDispatcher;

  private TaskAttemptFinishingMonitor taskAttemptFinishingMonitor;

  private Job job;
  private Credentials jobCredentials = new Credentials(); // Filled during init
  protected UserGroupInformation currentUser; // Will be setup during init
    logSyncer = TaskLog.createLogSyncer();
    LOG.info("Created MRAppMaster for application " + applicationAttemptId);
  }
  protected TaskAttemptFinishingMonitor createTaskAttemptFinishingMonitor(
      EventHandler eventHandler) {
    TaskAttemptFinishingMonitor monitor =
        new TaskAttemptFinishingMonitor(eventHandler);
    return monitor;
  }

  @Override
  protected void serviceInit(final Configuration conf) throws Exception {

    initJobCredentialsAndUGI(conf);

    dispatcher = createDispatcher();
    addIfService(dispatcher);
    taskAttemptFinishingMonitor = createTaskAttemptFinishingMonitor(dispatcher.getEventHandler());
    addIfService(taskAttemptFinishingMonitor);
    context = new RunningAppContext(conf, taskAttemptFinishingMonitor);

    }
    
    if (errorHappenedShutDown) {
      NoopEventHandler eater = new NoopEventHandler();
      dispatcher.register(JobEventType.class, eater);
    } else {
      committer = createOutputCommitter(conf);

      clientService = createClientService(context);
    private final ClusterInfo clusterInfo = new ClusterInfo();
    private final ClientToAMTokenSecretManager clientToAMTokenSecretManager;

    private final TaskAttemptFinishingMonitor taskAttemptFinishingMonitor;

    public RunningAppContext(Configuration config,
        TaskAttemptFinishingMonitor taskAttemptFinishingMonitor) {
      this.conf = config;
      this.clientToAMTokenSecretManager =
          new ClientToAMTokenSecretManager(appAttemptID, null);
      this.taskAttemptFinishingMonitor = taskAttemptFinishingMonitor;
    }

    @Override
    public String getNMHostname() {
      return nmHost;
    }

    @Override
    public TaskAttemptFinishingMonitor getTaskAttemptFinishingMonitor() {
      return taskAttemptFinishingMonitor;
    }

  }

  @SuppressWarnings("unchecked")

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/TaskAttemptFinishingMonitor.java
++ b/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/TaskAttemptFinishingMonitor.java

package org.apache.hadoop.mapreduce.v2.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.SystemClock;

@SuppressWarnings({"unchecked", "rawtypes"})
public class TaskAttemptFinishingMonitor extends
    AbstractLivelinessMonitor<TaskAttemptId> {

  private EventHandler eventHandler;

  public TaskAttemptFinishingMonitor(EventHandler eventHandler) {
    super("TaskAttemptFinishingMonitor", new SystemClock());
    this.eventHandler = eventHandler;
  }

  public void init(Configuration conf) {
    super.init(conf);
    int expireIntvl = conf.getInt(MRJobConfig.TASK_EXIT_TIMEOUT,
        MRJobConfig.TASK_EXIT_TIMEOUT_DEFAULT);
    int checkIntvl = conf.getInt(
        MRJobConfig.TASK_EXIT_TIMEOUT_CHECK_INTERVAL_MS,
        MRJobConfig.TASK_EXIT_TIMEOUT_CHECK_INTERVAL_MS_DEFAULT);

    setExpireInterval(expireIntvl);
    setMonitorInterval(checkIntvl);
  }

  @Override
  protected void expire(TaskAttemptId id) {
    eventHandler.handle(
        new TaskAttemptEvent(id,
        TaskAttemptEventType.TA_TIMED_OUT));
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/client/MRClientService.java
          new TaskAttemptDiagnosticsUpdateEvent(taskAttemptId, message));
      appContext.getEventHandler().handle(
          new TaskAttemptEvent(taskAttemptId, 
              TaskAttemptEventType.TA_FAILMSG_BY_CLIENT));
      FailTaskAttemptResponse response = recordFactory.
        newRecordInstance(FailTaskAttemptResponse.class);
      return response;

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/job/TaskAttemptStateInternal.java
  ASSIGNED, 
  RUNNING, 
  COMMIT_PENDING,


  SUCCESS_FINISHING_CONTAINER,


  FAIL_FINISHING_CONTAINER,

  SUCCESS_CONTAINER_CLEANUP,
  SUCCEEDED,
  FAIL_CONTAINER_CLEANUP, 

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/job/event/TaskAttemptEventType.java
  TA_UPDATE,
  TA_TIMED_OUT,

  TA_FAILMSG_BY_CLIENT,

  TA_CLEANUP_DONE,


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/job/impl/TaskAttemptImpl.java
  private Locality locality;
  private Avataar avataar;

  private static final CleanupContainerTransition
      CLEANUP_CONTAINER_TRANSITION = new CleanupContainerTransition();
  private static final MoveContainerToSucceededFinishingTransition
      SUCCEEDED_FINISHING_TRANSITION =
          new MoveContainerToSucceededFinishingTransition();
  private static final MoveContainerToFailedFinishingTransition
      FAILED_FINISHING_TRANSITION =
          new MoveContainerToFailedFinishingTransition();
  private static final ExitFinishingOnTimeoutTransition
      FINISHING_ON_TIMEOUT_TRANSITION =
          new ExitFinishingOnTimeoutTransition();

  private static final FinalizeFailedTransition FINALIZE_FAILED_TRANSITION =
      new FinalizeFailedTransition();

  private static final DiagnosticInformationUpdater 
    DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION 
      TaskAttemptEventType.TA_COMMIT_PENDING,
      TaskAttemptEventType.TA_DONE,
      TaskAttemptEventType.TA_FAILMSG,
      TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
      TaskAttemptEventType.TA_TIMED_OUT,
      TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURE);

  private static final StateMachineFactory
     .addTransition(TaskAttemptStateInternal.NEW, TaskAttemptStateInternal.KILLED,
         TaskAttemptEventType.TA_KILL, new KilledTransition())
     .addTransition(TaskAttemptStateInternal.NEW, TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_FAILMSG_BY_CLIENT, new FailedTransition())
     .addTransition(TaskAttemptStateInternal.NEW,
         EnumSet.of(TaskAttemptStateInternal.FAILED,
             TaskAttemptStateInternal.KILLED,
         TaskAttemptEventType.TA_KILL, new DeallocateContainerTransition(
         TaskAttemptStateInternal.KILLED, true))
     .addTransition(TaskAttemptStateInternal.UNASSIGNED, TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_FAILMSG_BY_CLIENT, new DeallocateContainerTransition(
             TaskAttemptStateInternal.FAILED, true))
     .addTransition(TaskAttemptStateInternal.UNASSIGNED,
         TaskAttemptStateInternal.UNASSIGNED,
         TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED,
         new DeallocateContainerTransition(TaskAttemptStateInternal.FAILED, false))
     .addTransition(TaskAttemptStateInternal.ASSIGNED,
         TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_CONTAINER_COMPLETED,
         FINALIZE_FAILED_TRANSITION)
     .addTransition(TaskAttemptStateInternal.ASSIGNED, 
         TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_KILL, CLEANUP_CONTAINER_TRANSITION)
     .addTransition(TaskAttemptStateInternal.ASSIGNED,
         TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
         TaskAttemptEventType.TA_FAILMSG, FAILED_FINISHING_TRANSITION)
     .addTransition(TaskAttemptStateInternal.ASSIGNED,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             CLEANUP_CONTAINER_TRANSITION)

     .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.RUNNING,
     .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.RUNNING,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptEventType.TA_DONE, SUCCEEDED_FINISHING_TRANSITION)
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptEventType.TA_COMMIT_PENDING, new CommitPendingTransition())
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
         TaskAttemptEventType.TA_FAILMSG, FAILED_FINISHING_TRANSITION)
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_FAILMSG_BY_CLIENT, CLEANUP_CONTAINER_TRANSITION)
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_CONTAINER_COMPLETED,
         FINALIZE_FAILED_TRANSITION)
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_CONTAINER_CLEANED, new KilledTransition())
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_KILL,
         CLEANUP_CONTAINER_TRANSITION)

     .addTransition(TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptStateInternal.SUCCEEDED,
         TaskAttemptEventType.TA_CONTAINER_COMPLETED,
         new ExitFinishingOnContainerCompletedTransition())
     .addTransition(TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptStateInternal.SUCCEEDED,
         TaskAttemptEventType.TA_CONTAINER_CLEANED,
         new ExitFinishingOnContainerCleanedupTransition())
     .addTransition(TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         EnumSet.of(TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP,
             TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP),
         TaskAttemptEventType.TA_KILL,
         new KilledAfterSucceededFinishingTransition())
     .addTransition(TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_TIMED_OUT, FINISHING_ON_TIMEOUT_TRANSITION)
     .addTransition(TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     .addTransition(TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         EnumSet.of(TaskAttemptEventType.TA_UPDATE,
             TaskAttemptEventType.TA_DONE,
             TaskAttemptEventType.TA_COMMIT_PENDING,
             TaskAttemptEventType.TA_FAILMSG,
             TaskAttemptEventType.TA_FAILMSG_BY_CLIENT))

    .addTransition(TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
        TaskAttemptStateInternal.FAILED,
        TaskAttemptEventType.TA_CONTAINER_COMPLETED,
        new ExitFinishingOnContainerCompletedTransition())
    .addTransition(TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
        TaskAttemptStateInternal.FAILED,
        TaskAttemptEventType.TA_CONTAINER_CLEANED,
        new ExitFinishingOnContainerCleanedupTransition())
    .addTransition(TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
        TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
        TaskAttemptEventType.TA_TIMED_OUT, FINISHING_ON_TIMEOUT_TRANSITION)
    .addTransition(TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
        TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
        TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
        DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
    .addTransition(TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
        TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
        EnumSet.of(TaskAttemptEventType.TA_KILL,
            TaskAttemptEventType.TA_UPDATE,
            TaskAttemptEventType.TA_DONE,
            TaskAttemptEventType.TA_COMMIT_PENDING,
            TaskAttemptEventType.TA_FAILMSG,
            TaskAttemptEventType.TA_FAILMSG_BY_CLIENT))

     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptEventType.TA_UPDATE,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptEventType.TA_DONE, SUCCEEDED_FINISHING_TRANSITION)
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_KILL,
         CLEANUP_CONTAINER_TRANSITION)
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.KILLED,
         TaskAttemptEventType.TA_CONTAINER_CLEANED, new KilledTransition())
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
         TaskAttemptEventType.TA_FAILMSG, FAILED_FINISHING_TRANSITION)
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             CLEANUP_CONTAINER_TRANSITION)
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_CONTAINER_COMPLETED,
         FINALIZE_FAILED_TRANSITION)
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_TIMED_OUT, CLEANUP_CONTAINER_TRANSITION)
     .addTransition(TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP,
         TaskAttemptStateInternal.SUCCEEDED,
         TaskAttemptEventType.TA_CONTAINER_CLEANED)
     .addTransition(
          TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP,
          TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP,
         TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP,
         EnumSet.of(TaskAttemptEventType.TA_KILL,
             TaskAttemptEventType.TA_FAILMSG,
             TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             TaskAttemptEventType.TA_TIMED_OUT,
             TaskAttemptEventType.TA_CONTAINER_COMPLETED))

             TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED,
             TaskAttemptEventType.TA_DONE,
             TaskAttemptEventType.TA_FAILMSG,
             TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             TaskAttemptEventType.TA_TIMED_OUT))

             TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED,
             TaskAttemptEventType.TA_DONE,
             TaskAttemptEventType.TA_FAILMSG,
             TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             TaskAttemptEventType.TA_TIMED_OUT))

             TaskAttemptEventType.TA_COMMIT_PENDING,
             TaskAttemptEventType.TA_DONE,
             TaskAttemptEventType.TA_FAILMSG,
             TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             TaskAttemptEventType.TA_CONTAINER_CLEANED,
             TaskAttemptEventType.TA_CONTAINER_LAUNCHED,
             TaskAttemptEventType.TA_COMMIT_PENDING,
             TaskAttemptEventType.TA_DONE,
             TaskAttemptEventType.TA_FAILMSG,
             TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             TaskAttemptEventType.TA_CONTAINER_CLEANED,
             TaskAttemptEventType.TA_CONTAINER_LAUNCHED,
     .addTransition(TaskAttemptStateInternal.SUCCEEDED,
         TaskAttemptStateInternal.SUCCEEDED,
         EnumSet.of(TaskAttemptEventType.TA_FAILMSG,
             TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             TaskAttemptEventType.TA_TIMED_OUT,
             TaskAttemptEventType.TA_CONTAINER_CLEANED,
             TaskAttemptEventType.TA_CONTAINER_COMPLETED))

      return TaskAttemptState.STARTING;
    case COMMIT_PENDING:
      return TaskAttemptState.COMMIT_PENDING;
    case FAIL_CONTAINER_CLEANUP:
    case FAIL_TASK_CLEANUP:
    case FAIL_FINISHING_CONTAINER:
    case FAILED:
      return TaskAttemptState.FAILED;
    case KILL_CONTAINER_CLEANUP:
    case KILL_TASK_CLEANUP:
    case KILLED:
      return TaskAttemptState.KILLED;
    case RUNNING:
      return TaskAttemptState.RUNNING;
    case NEW:
      return TaskAttemptState.NEW;
    case SUCCESS_CONTAINER_CLEANUP:
    case SUCCESS_FINISHING_CONTAINER:
    case SUCCEEDED:
      return TaskAttemptState.SUCCEEDED;
    default:
    }
  }

  private static void finalizeProgress(TaskAttemptImpl taskAttempt) {
    taskAttempt.taskAttemptListener.unregister(
        taskAttempt.attemptId, taskAttempt.jvmID);
    taskAttempt.reportedStatus.progress = 1.0f;
    taskAttempt.updateProgressSplits();
  }


  static class RequestContainerTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    private final boolean rescheduled;
    }
  }

  private static class ExitFinishingOnContainerCompletedTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
       TaskAttemptEvent event) {
      taskAttempt.appContext.getTaskAttemptFinishingMonitor().unregister(
          taskAttempt.attemptId);
      sendContainerCompleted(taskAttempt);
    }
  }

  private static class ExitFinishingOnContainerCleanedupTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      taskAttempt.appContext.getTaskAttemptFinishingMonitor().unregister(
          taskAttempt.attemptId);
    }
  }

      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      taskAttempt.setFinishTime();
      notifyTaskAttemptFailed(taskAttempt);
    }
  }

  private static class FinalizeFailedTransition extends FailedTransition {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      finalizeProgress(taskAttempt);
      sendContainerCompleted(taskAttempt);
      super.transition(taskAttempt, event);
    }
  }

  @SuppressWarnings("unchecked")
  private static void sendContainerCompleted(TaskAttemptImpl taskAttempt) {
    taskAttempt.eventHandler.handle(new ContainerLauncherEvent(
        taskAttempt.attemptId,
        taskAttempt.container.getId(), StringInterner
        .weakIntern(taskAttempt.container.getNodeId().toString()),
        taskAttempt.container.getContainerToken(),
        ContainerLauncher.EventType.CONTAINER_COMPLETED));
  }

  private static class RecoverTransition implements
    }
  }

  private static class KilledAfterSucceededFinishingTransition
      implements MultipleArcTransition<TaskAttemptImpl, TaskAttemptEvent,
      TaskAttemptStateInternal> {

    @SuppressWarnings("unchecked")
    @Override
    public TaskAttemptStateInternal transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      taskAttempt.appContext.getTaskAttemptFinishingMonitor().unregister(
          taskAttempt.attemptId);
      sendContainerCleanup(taskAttempt, event);
      if(taskAttempt.getID().getTaskId().getTaskType() == TaskType.REDUCE) {
        LOG.info("Ignoring killed event for successful reduce task attempt" +
            taskAttempt.getID().toString());
        return TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP;
      } else {
        return TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP;
      }
    }
  }

  private static class KilledTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    }
  }

  private static class ExitFinishingOnTimeoutTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      taskAttempt.appContext.getTaskAttemptFinishingMonitor().unregister(
          taskAttempt.attemptId);
      String msg = "Task attempt " + taskAttempt.getID() + " is done from " +
          "TaskUmbilicalProtocol's point of view. However, it stays in " +
          "finishing state for too long";
      LOG.warn(msg);
      taskAttempt.addDiagnosticInfo(msg);
      sendContainerCleanup(taskAttempt, event);
    }
  }

  private static class CleanupContainerTransition implements
       SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    public void transition(TaskAttemptImpl taskAttempt, 
        TaskAttemptEvent event) {
      finalizeProgress(taskAttempt);
      sendContainerCleanup(taskAttempt, event);
    }
  }

  @SuppressWarnings("unchecked")
  private static void sendContainerCleanup(TaskAttemptImpl taskAttempt,
      TaskAttemptEvent event) {
    if (event instanceof TaskAttemptKillEvent) {
      taskAttempt.addDiagnosticInfo(
          ((TaskAttemptKillEvent) event).getMessage());
    }
    taskAttempt.eventHandler.handle(new ContainerLauncherEvent(
        taskAttempt.attemptId,
        taskAttempt.container.getContainerToken(),
        ContainerLauncher.EventType.CONTAINER_REMOTE_CLEANUP));
  }

  private static class MoveContainerToSucceededFinishingTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      finalizeProgress(taskAttempt);

      taskAttempt.appContext.getTaskAttemptFinishingMonitor().register(
          taskAttempt.attemptId);

      taskAttempt.setFinishTime();

      taskAttempt.eventHandler.handle(
          createJobCounterUpdateEventTASucceeded(taskAttempt));
      taskAttempt.logAttemptFinishedEvent(TaskAttemptStateInternal.SUCCEEDED);

      taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
          taskAttempt.attemptId,
          TaskEventType.T_ATTEMPT_SUCCEEDED));
      taskAttempt.eventHandler.handle
          (new SpeculatorEvent
              (taskAttempt.reportedStatus, taskAttempt.clock.getTime()));

    }
  }

  private static class MoveContainerToFailedFinishingTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      finalizeProgress(taskAttempt);
      taskAttempt.appContext.getTaskAttemptFinishingMonitor().register(
          taskAttempt.attemptId);
      notifyTaskAttemptFailed(taskAttempt);
    }
  }

  @SuppressWarnings("unchecked")
  private static void notifyTaskAttemptFailed(TaskAttemptImpl taskAttempt) {
    taskAttempt.setFinishTime();

    if (taskAttempt.getLaunchTime() != 0) {
      taskAttempt.eventHandler
          .handle(createJobCounterUpdateEventTAFailed(taskAttempt, false));
      TaskAttemptUnsuccessfulCompletionEvent tauce =
          createTaskAttemptUnsuccessfulCompletionEvent(taskAttempt,
              TaskAttemptStateInternal.FAILED);
      taskAttempt.eventHandler.handle(new JobHistoryEvent(
          taskAttempt.attemptId.getTaskId().getJobId(), tauce));
    }else {
      LOG.debug("Not generating HistoryFinish event since start event not " +
          "generated for taskAttempt: " + taskAttempt.getID());
    }
    taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
        taskAttempt.attemptId, TaskEventType.T_ATTEMPT_FAILED));

  }

  private void addDiagnosticInfo(String diag) {

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/launcher/ContainerLauncher.java

  enum EventType {
    CONTAINER_REMOTE_LAUNCH,
    CONTAINER_REMOTE_CLEANUP,
    CONTAINER_COMPLETED
  }

}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/launcher/ContainerLauncherImpl.java
      return state == ContainerState.DONE || state == ContainerState.FAILED;
    }

    public synchronized void done() {
      state = ContainerState.DONE;
    }

    @SuppressWarnings("unchecked")
    public synchronized void launch(ContainerRemoteLaunchEvent event) {
      LOG.info("Launching " + taskAttemptID);
      case CONTAINER_REMOTE_CLEANUP:
        c.kill();
        break;

      case CONTAINER_COMPLETED:
        c.done();
        break;

      }
      removeContainerIfDone(containerID);
    }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapred/TestTaskAttemptFinishingMonitor.java
++ b/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapred/TestTaskAttemptFinishingMonitor.java
package org.apache.hadoop.mapred;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptFinishingMonitor;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.SystemClock;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestTaskAttemptFinishingMonitor {

  @Test
  public void testFinshingAttemptTimeout()
      throws IOException, InterruptedException {
    SystemClock clock = new SystemClock();
    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.TASK_EXIT_TIMEOUT, 100);
    conf.setInt(MRJobConfig.TASK_EXIT_TIMEOUT_CHECK_INTERVAL_MS, 10);

    AppContext appCtx = mock(AppContext.class);
    JobTokenSecretManager secret = mock(JobTokenSecretManager.class);
    RMHeartbeatHandler rmHeartbeatHandler =
        mock(RMHeartbeatHandler.class);
    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptFinishingMonitor taskAttemptFinishingMonitor =
        new TaskAttemptFinishingMonitor(eventHandler);
    taskAttemptFinishingMonitor.init(conf);
    taskAttemptFinishingMonitor.start();

    when(appCtx.getEventHandler()).thenReturn(eventHandler);
    when(appCtx.getNMHostname()).thenReturn("0.0.0.0");
    when(appCtx.getTaskAttemptFinishingMonitor()).thenReturn(
        taskAttemptFinishingMonitor);
    when(appCtx.getClock()).thenReturn(clock);

    TaskAttemptListenerImpl listener =
        new TaskAttemptListenerImpl(appCtx, secret, rmHeartbeatHandler);

    listener.init(conf);
    listener.start();

    JobId jid = MRBuilderUtils.newJobId(12345, 1, 1);
    TaskId tid = MRBuilderUtils.newTaskId(jid, 0,
        org.apache.hadoop.mapreduce.v2.api.records.TaskType.MAP);
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(tid, 0);
    appCtx.getTaskAttemptFinishingMonitor().register(attemptId);
    int check = 0;
    while ( !eventHandler.timedOut &&  check++ < 10 ) {
      Thread.sleep(100);
    }
    taskAttemptFinishingMonitor.stop();

    assertTrue("Finishing attempt didn't time out.", eventHandler.timedOut);

  }

  public static class MockEventHandler implements EventHandler {
    public boolean timedOut = false;

    @Override
    public void handle(Event event) {
      if (event instanceof TaskAttemptEvent) {
        TaskAttemptEvent attemptEvent = ((TaskAttemptEvent) event);
        if (TaskAttemptEventType.TA_TIMED_OUT == attemptEvent.getType()) {
          timedOut = true;
        }
      }
    }
  };

}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/MRApp.java
    return newJob;
  }

  @Override
  protected TaskAttemptFinishingMonitor
      createTaskAttemptFinishingMonitor(
      EventHandler eventHandler) {
    return new TaskAttemptFinishingMonitor(eventHandler) {
      @Override
      public synchronized void register(TaskAttemptId attemptID) {
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(attemptID,
                TaskAttemptEventType.TA_CONTAINER_COMPLETED));
      }
    };
  }

  @Override
    protected TaskAttemptListener createTaskAttemptListener(AppContext context) {
    return new TaskAttemptListener(){
            new TaskAttemptEvent(event.getTaskAttemptID(),
                TaskAttemptEventType.TA_CONTAINER_CLEANED));
        break;
      case CONTAINER_COMPLETED:
        break;
      }
    }
  }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/MockAppContext.java
    return null;
  }

  @Override
  public TaskAttemptFinishingMonitor getTaskAttemptFinishingMonitor() {
      return null;
  }

}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/TestFail.java
                new TaskAttemptEvent(event.getTaskAttemptID(),
                    TaskAttemptEventType.TA_CONTAINER_CLEANED));
            break;
          case CONTAINER_COMPLETED:
            super.handle(event);
          }
        }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/TestKill.java
              super.dispatch(new TaskAttemptEvent(taID,
                TaskAttemptEventType.TA_DONE));
              super.dispatch(new TaskAttemptEvent(taID,
                TaskAttemptEventType.TA_CONTAINER_COMPLETED));
              super.dispatch(new TaskTAttemptEvent(taID,
                TaskEventType.T_ATTEMPT_SUCCEEDED));
              this.cachedKillEvent = killEvent;
    app.waitForInternalState((JobImpl) job, JobStateInternal.KILLED);
  }

  @Test
  public void testKillTaskWaitKillJobAfterTA_DONE() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    TaskAttempt reduceAttempt = reduceTask.getAttempts().values().iterator().next();
    app.waitForState(reduceAttempt, TaskAttemptState.RUNNING);


    app.getContext().getEventHandler().handle(
    app.waitForInternalState((JobImpl)job, JobStateInternal.KILLED);
  }


  @Test
  public void testKillTaskWaitKillJobBeforeTA_DONE() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    final Dispatcher dispatcher = new MyAsyncDispatch(latch, JobEventType.JOB_KILL);
    MRApp app = new MRApp(1, 1, false, this.getClass().getName(), true) {
      @Override
      public Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    Job job = app.submit(new Configuration());
    JobId jobId = app.getJobId();
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("Num tasks not correct", 2, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask = it.next();
    Task reduceTask = it.next();
    app.waitForState(mapTask, TaskState.RUNNING);
    app.waitForState(reduceTask, TaskState.RUNNING);
    TaskAttempt mapAttempt = mapTask.getAttempts().values().iterator().next();
    app.waitForState(mapAttempt, TaskAttemptState.RUNNING);
    TaskAttempt reduceAttempt = reduceTask.getAttempts().values().iterator().next();
    app.waitForState(reduceAttempt, TaskAttemptState.RUNNING);


    app.getContext().getEventHandler()
        .handle(new JobEvent(jobId, JobEventType.JOB_KILL));

    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            mapAttempt.getID(),
            TaskAttemptEventType.TA_DONE));

    latch.countDown();

    app.waitForInternalState((JobImpl)job, JobStateInternal.KILLED);
  }

  static class MyAsyncDispatch extends AsyncDispatcher {
    private CountDownLatch latch;
    private TaskAttemptEventType attemptEventTypeToWait;
    private JobEventType jobEventTypeToWait;
    MyAsyncDispatch(CountDownLatch latch, TaskAttemptEventType attemptEventTypeToWait) {
      super();
      this.latch = latch;
      this.attemptEventTypeToWait = attemptEventTypeToWait;
    }

    MyAsyncDispatch(CountDownLatch latch, JobEventType jobEventTypeToWait) {
      super();
      this.latch = latch;
      this.jobEventTypeToWait = jobEventTypeToWait;
    }

    @Override
    protected void dispatch(Event event) {
      if (event instanceof TaskAttemptEvent) {
        TaskAttemptEvent attemptEvent = (TaskAttemptEvent) event;
        TaskAttemptId attemptID = ((TaskAttemptEvent) event).getTaskAttemptID();
        if (attemptEvent.getType() == this.attemptEventTypeToWait
            && attemptID.getTaskId().getId() == 0 && attemptID.getId() == 0 ) {
          try {
            latch.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      } else if ( event instanceof JobEvent) {
        JobEvent jobEvent = (JobEvent) event;
        if (jobEvent.getType() == this.jobEventTypeToWait) {
          try {
            latch.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }

      super.dispatch(event);
    }
  }

  @Test
  public void testKillTaskAttempt() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/TestRuntimeEstimators.java
      return null;
    }

    @Override
    public TaskAttemptFinishingMonitor getTaskAttemptFinishingMonitor() {
      return null;
    }
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/job/impl/TestTaskAttempt.java
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.ClusterInfo;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptFinishingMonitor;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
    Resource resource = mock(Resource.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
    when(resource.getMemory()).thenReturn(1024);
    setupTaskAttemptFinishingMonitor(eventHandler, jobConf, appCtx);

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
    Resource resource = mock(Resource.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
    when(resource.getMemory()).thenReturn(1024);
    setupTaskAttemptFinishingMonitor(eventHandler, jobConf, appCtx);

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
    Resource resource = mock(Resource.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
    when(resource.getMemory()).thenReturn(1024);
    setupTaskAttemptFinishingMonitor(eventHandler, jobConf, appCtx);

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_DONE));
    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_CONTAINER_COMPLETED));

    assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    Resource resource = mock(Resource.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
    when(resource.getMemory()).thenReturn(1024);
    setupTaskAttemptFinishingMonitor(eventHandler, jobConf, appCtx);

    TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler,
        jobFile, 1, splits, jobConf, taListener,
    Resource resource = mock(Resource.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
    when(resource.getMemory()).thenReturn(1024);
    setupTaskAttemptFinishingMonitor(eventHandler, jobConf, appCtx);

    TaskAttemptImpl taImpl =
      new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
    taImpl.handle(new TaskAttemptEvent(attemptId,
      TaskAttemptEventType.TA_DONE));
    taImpl.handle(new TaskAttemptEvent(attemptId,
      TaskAttemptEventType.TA_CONTAINER_COMPLETED));

    assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
      TaskAttemptState.SUCCEEDED);
    Resource resource = mock(Resource.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
    when(resource.getMemory()).thenReturn(1024);
    setupTaskAttemptFinishingMonitor(eventHandler, jobConf, appCtx);

    TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler,
        jobFile, 1, splits, jobConf, taListener,
	AppContext appCtx = mock(AppContext.class);
	ClusterInfo clusterInfo = mock(ClusterInfo.class);
	when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
  setupTaskAttemptFinishingMonitor(eventHandler, jobConf, appCtx);

	TaskAttemptImpl taImpl =
	  new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
	taImpl.handle(new TaskAttemptEvent(attemptId,
	    TaskAttemptEventType.TA_DONE));
	taImpl.handle(new TaskAttemptEvent(attemptId,
	    TaskAttemptEventType.TA_CONTAINER_COMPLETED));
	    
	assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
		      TaskAttemptState.SUCCEEDED);
        taImpl.getInternalState());
  }


  @Test
  public void testKillMapTaskWhileSuccessFinishing() throws Exception {
    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptImpl taImpl = createTaskAttemptImpl(eventHandler);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in SUCCEEDED state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    assertEquals("Task attempt's internal state is not " +
        "SUCCESS_FINISHING_CONTAINER", taImpl.getInternalState(),
        TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_KILL));
    assertEquals("Task attempt is not in KILLED state", taImpl.getState(),
        TaskAttemptState.KILLED);
    assertEquals("Task attempt's internal state is not KILL_CONTAINER_CLEANUP",
        taImpl.getInternalState(),
        TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_CONTAINER_CLEANED));
    assertEquals("Task attempt's internal state is not KILL_TASK_CLEANUP",
        taImpl.getInternalState(),
        TaskAttemptStateInternal.KILL_TASK_CLEANUP);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_CLEANUP_DONE));

    assertEquals("Task attempt is not in KILLED state", taImpl.getState(),
        TaskAttemptState.KILLED);

    assertFalse("InternalError occurred", eventHandler.internalError);
  }

  @Test
  public void testKillMapTaskWhileFailFinishing() throws Exception {
    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptImpl taImpl = createTaskAttemptImpl(eventHandler);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_FAILMSG));

    assertEquals("Task attempt is not in FAILED state", taImpl.getState(),
        TaskAttemptState.FAILED);
    assertEquals("Task attempt's internal state is not " +
        "FAIL_FINISHING_CONTAINER", taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_KILL));
    assertEquals("Task attempt is not in RUNNING state", taImpl.getState(),
        TaskAttemptState.FAILED);
    assertEquals("Task attempt's internal state is not " +
        "FAIL_FINISHING_CONTAINER", taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_TIMED_OUT));
    assertEquals("Task attempt's internal state is not FAIL_CONTAINER_CLEANUP",
        taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_CONTAINER_CLEANED));
    assertEquals("Task attempt's internal state is not FAIL_TASK_CLEANUP",
        taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_TASK_CLEANUP);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_CLEANUP_DONE));

    assertEquals("Task attempt is not in KILLED state", taImpl.getState(),
        TaskAttemptState.FAILED);

    assertFalse("InternalError occurred", eventHandler.internalError);
  }

  @Test
  public void testFailMapTaskByClient() throws Exception {
    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptImpl taImpl = createTaskAttemptImpl(eventHandler);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_FAILMSG_BY_CLIENT));

    assertEquals("Task attempt is not in RUNNING state", taImpl.getState(),
        TaskAttemptState.FAILED);
    assertEquals("Task attempt's internal state is not " +
        "FAIL_CONTAINER_CLEANUP", taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_CONTAINER_CLEANED));
    assertEquals("Task attempt's internal state is not FAIL_TASK_CLEANUP",
        taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_TASK_CLEANUP);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_CLEANUP_DONE));

    assertEquals("Task attempt is not in KILLED state", taImpl.getState(),
        TaskAttemptState.FAILED);

    assertFalse("InternalError occurred", eventHandler.internalError);
  }

  @Test
  public void testTaskAttemptDiagnosticEventOnFinishing() throws Exception {
    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptImpl taImpl = createTaskAttemptImpl(eventHandler);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in RUNNING state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    assertEquals("Task attempt's internal state is not " +
        "SUCCESS_FINISHING_CONTAINER", taImpl.getInternalState(),
        TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER);

    taImpl.handle(new TaskAttemptDiagnosticsUpdateEvent(taImpl.getID(),
        "Task got updated"));
    assertEquals("Task attempt is not in RUNNING state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    assertEquals("Task attempt's internal state is not " +
        "SUCCESS_FINISHING_CONTAINER", taImpl.getInternalState(),
        TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER);

    assertFalse("InternalError occurred", eventHandler.internalError);
  }

  @Test
  public void testTimeoutWhileSuccessFinishing() throws Exception {
    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptImpl taImpl = createTaskAttemptImpl(eventHandler);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in RUNNING state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    assertEquals("Task attempt's internal state is not " +
        "SUCCESS_FINISHING_CONTAINER", taImpl.getInternalState(),
        TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_TIMED_OUT));
    assertEquals("Task attempt is not in RUNNING state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    assertEquals("Task attempt's internal state is not " +
        "SUCCESS_CONTAINER_CLEANUP", taImpl.getInternalState(),
        TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP);

    assertFalse("InternalError occurred", eventHandler.internalError);
  }

  @Test
  public void testTimeoutWhileFailFinishing() throws Exception {
    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptImpl taImpl = createTaskAttemptImpl(eventHandler);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_FAILMSG));

    assertEquals("Task attempt is not in RUNNING state", taImpl.getState(),
        TaskAttemptState.FAILED);
    assertEquals("Task attempt's internal state is not " +
        "FAIL_FINISHING_CONTAINER", taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER);

    taImpl.handle(new TaskAttemptEvent(taImpl.getID(),
        TaskAttemptEventType.TA_TIMED_OUT));
    assertEquals("Task attempt's internal state is not FAIL_CONTAINER_CLEANUP",
        taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP);

    assertFalse("InternalError occurred", eventHandler.internalError);
  }

  private void setupTaskAttemptFinishingMonitor(
      EventHandler eventHandler, JobConf jobConf, AppContext appCtx) {
    TaskAttemptFinishingMonitor taskAttemptFinishingMonitor =
        new TaskAttemptFinishingMonitor(eventHandler);
    taskAttemptFinishingMonitor.init(jobConf);
    when(appCtx.getTaskAttemptFinishingMonitor()).
        thenReturn(taskAttemptFinishingMonitor);
  }

  private TaskAttemptImpl createTaskAttemptImpl(
      MockEventHandler eventHandler) {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 0);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
    Path jobFile = mock(Path.class);

    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(new InetSocketAddress("localhost", 0));

    JobConf jobConf = new JobConf();
    jobConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    jobConf.setBoolean("fs.file.impl.disable.cache", true);
    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "");
    jobConf.set(MRJobConfig.APPLICATION_ATTEMPT_ID, "10");

    TaskSplitMetaInfo splits = mock(TaskSplitMetaInfo.class);
    when(splits.getLocations()).thenReturn(new String[] {"127.0.0.1"});

    AppContext appCtx = mock(AppContext.class);
    ClusterInfo clusterInfo = mock(ClusterInfo.class);
    when(appCtx.getClusterInfo()).thenReturn(clusterInfo);
    setupTaskAttemptFinishingMonitor(eventHandler, jobConf, appCtx);

    TaskAttemptImpl taImpl =
        new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
            splits, jobConf, taListener,
            mock(Token.class), new Credentials(),
            new SystemClock(), appCtx);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    taImpl.handle(new TaskAttemptEvent(attemptId,
        TaskAttemptEventType.TA_SCHEDULE));
    taImpl.handle(new TaskAttemptContainerAssignedEvent(attemptId,
        container, mock(Map.class)));
    taImpl.handle(new TaskAttemptContainerLaunchedEvent(attemptId, 0));
    return taImpl;
  }

  public static class MockEventHandler implements EventHandler {
    public boolean internalError;


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/MRJobConfig.java

  public static final String TASK_TIMEOUT_CHECK_INTERVAL_MS = "mapreduce.task.timeout.check-interval-ms";

  public static final String TASK_EXIT_TIMEOUT = "mapreduce.task.exit.timeout";

  public static final int TASK_EXIT_TIMEOUT_DEFAULT = 60 * 1000;

  public static final String TASK_EXIT_TIMEOUT_CHECK_INTERVAL_MS = "mapreduce.task.exit.timeout.check-interval-ms";

  public static final int TASK_EXIT_TIMEOUT_CHECK_INTERVAL_MS_DEFAULT = 20 * 1000;

  public static final String TASK_ID = "mapreduce.task.id";

  public static final String TASK_OUTPUT_DIR = "mapreduce.task.output.dir";

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/JobHistory.java
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.ClusterInfo;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptFinishingMonitor;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
    return null;
  }

  @Override
  public TaskAttemptFinishingMonitor getTaskAttemptFinishingMonitor() {
    return null;
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/v2/TestSpeculativeExecutionWithMRApp.java
          appEventHandler.handle(new TaskAttemptEvent(taskAttempt.getKey(),
            TaskAttemptEventType.TA_DONE));
          appEventHandler.handle(new TaskAttemptEvent(taskAttempt.getKey(),
            TaskAttemptEventType.TA_CONTAINER_COMPLETED));
          app.waitForState(taskAttempt.getValue(), TaskAttemptState.SUCCEEDED);
        }
      }
          appEventHandler.handle(new TaskAttemptEvent(taskAttempt.getKey(),
            TaskAttemptEventType.TA_DONE));
          appEventHandler.handle(new TaskAttemptEvent(taskAttempt.getKey(),
            TaskAttemptEventType.TA_CONTAINER_COMPLETED));
          numTasksToFinish--;
          app.waitForState(taskAttempt.getValue(), TaskAttemptState.SUCCEEDED);
        } else {
    appEventHandler.handle(
        new TaskAttemptEvent(ta[0].getID(), TaskAttemptEventType.TA_DONE));
    appEventHandler.handle(new TaskAttemptEvent(ta[0].getID(),
        TaskAttemptEventType.TA_CONTAINER_COMPLETED));
    return ta;
  }


