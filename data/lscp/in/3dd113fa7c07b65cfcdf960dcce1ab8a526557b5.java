hadoop-tools/hadoop-sls/src/main/java/org/apache/hadoop/yarn/sls/scheduler/ResourceSchedulerWrapper.java
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
      ContainerStatus containerStatus, RMContainerEventType event) {
  }

  @Override
  public Priority checkAndGetApplicationPriority(Priority priority,
      String user, String queueName, ApplicationId applicationId)
      throws YarnException {
    return Priority.newInstance(0);
  }

}


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
  public static final String DEFAULT_NODELABEL_CONFIGURATION_TYPE =
      CENTALIZED_NODELABEL_CONFIGURATION_TYPE;

  public static final String MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY =
      YARN_PREFIX + "cluster.max-application-priority";

  public static final int DEFAULT_CLUSTER_LEVEL_APPLICATION_PRIORITY = 0;

  @Private
  public static boolean isDistributedNodeLabelConfiguration(Configuration conf) {
    return DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE.equals(conf.get(

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/RMAppManager.java
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
    ResourceRequest amReq =
        validateAndCreateResourceRequest(submissionContext, isRecovery);

    Priority appPriority = rmContext.getScheduler()
        .checkAndGetApplicationPriority(submissionContext.getPriority(), user,
            submissionContext.getQueue(), applicationId);
    submissionContext.setPriority(appPriority);

    RMAppImpl application = new RMAppImpl(applicationId, rmContext, this.conf,
        submissionContext.getApplicationName(), user,
        submissionContext.getQueue(), submissionContext, this.scheduler,
        this.masterService, submitTime, submissionContext.getApplicationType(),
        submissionContext.getApplicationTags(), amReq);


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/RMAppImpl.java
      if (app.attempts.isEmpty()) {
        app.scheduler.handle(new AppAddedSchedulerEvent(app.user,
            app.submissionContext, false));
        return RMAppState.SUBMITTED;
      }

      app.scheduler.handle(new AppAddedSchedulerEvent(app.user,
          app.submissionContext, true));

      app.recoverAppAttempts();
      RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.handler.handle(new AppAddedSchedulerEvent(app.user,
          app.submissionContext, false));
    }
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/AbstractYarnScheduler.java
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
    }
    return null;
  }

  @Override
  public Priority checkAndGetApplicationPriority(Priority priorityFromContext,
      String user, String queueName, ApplicationId applicationId)
      throws YarnException {
    return Priority.newInstance(0);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/Queue.java
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
  public void decPendingResource(String nodeLabel, Resource resourceToDec);

  public Priority getDefaultApplicationPriority();
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/SchedulerApplication.java

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;

@Private
  private Queue queue;
  private final String user;
  private T currentAttempt;
  private volatile Priority priority;

  public SchedulerApplication(Queue queue, String user) {
    this.queue = queue;
    this.user = user;
    this.priority = null;
  }

  public SchedulerApplication(Queue queue, String user, Priority priority) {
    this.queue = queue;
    this.user = user;
    this.priority = priority;
  }

  public Queue getQueue() {
    queue.getMetrics().finishApp(user, rmAppFinalState);
  }

  public Priority getPriority() {
    return priority;
  }

  public void setPriority(Priority priority) {
    this.priority = priority;

    if (null != currentAttempt) {
      currentAttempt.setPriority(priority);
    }
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/SchedulerApplicationAttempt.java
  private boolean amRunning = false;
  private LogAggregationContext logAggregationContext;

  private Priority appPriority = null;

  protected ResourceUsage attemptResourceUsage = new ResourceUsage();
  private AtomicLong firstAllocationRequestSentTime = new AtomicLong(0);
  private AtomicLong firstContainerAllocatedTime = new AtomicLong(0);
    return this.attemptResourceUsage;
  }

  @Override
  public Priority getPriority() {
    return appPriority;
  }

  public void setPriority(Priority appPriority) {
    this.appPriority = appPriority;
  }

  @Override
  public String getId() {
    return getApplicationId().toString();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/YarnScheduler.java
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
  public EnumSet<SchedulerResourceTypes> getSchedulingResourceTypes();

  public Priority checkAndGetApplicationPriority(Priority priorityFromContext,
      String user, String queueName, ApplicationId applicationId)
      throws YarnException;
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/AbstractCSQueue.java
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
    return false;
  }

  @Override
  public Priority getDefaultApplicationPriority() {
    return null;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacityScheduler.java
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
    new Comparator<FiCaSchedulerApp>() {
    @Override
    public int compare(FiCaSchedulerApp a1, FiCaSchedulerApp a2) {
      if (!a1.getPriority().equals(a2.getPriority())) {
        return a1.getPriority().compareTo(a2.getPriority());
      }
      return a1.getApplicationId().compareTo(a2.getApplicationId());
    }
  };
  private RMNodeLabelsManager labelManager;
  private SchedulerHealth schedulerHealth = new SchedulerHealth();
  long lastNodeUpdateTime;
  private Priority maxClusterLevelAppPriority;
    if (scheduleAsynchronously) {
      asyncSchedulerThread = new AsyncScheduleThread(this);
    }
    maxClusterLevelAppPriority = Priority.newInstance(yarnConf.getInt(
        YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY,
        YarnConfiguration.DEFAULT_CLUSTER_LEVEL_APPLICATION_PRIORITY));

    LOG.info("Initialized CapacityScheduler with " +
        "calculator=" + getResourceCalculator().getClass() + ", " +
  }

  private synchronized void addApplication(ApplicationId applicationId,
      String queueName, String user, boolean isAppRecovering, Priority priority) {

    if (mappings != null && mappings.size() > 0) {
      try {
    queue.getMetrics().submitApp(user);
    SchedulerApplication<FiCaSchedulerApp> application =
        new SchedulerApplication<FiCaSchedulerApp>(queue, user, priority);
    applications.put(applicationId, application);
    LOG.info("Accepted application " + applicationId + " from user: " + user
        + ", in queue: " + queueName);
        applications.get(applicationAttemptId.getApplicationId());
    CSQueue queue = (CSQueue) application.getQueue();

    FiCaSchedulerApp attempt = new FiCaSchedulerApp(applicationAttemptId,
        application.getUser(), queue, queue.getActiveUsersManager(), rmContext,
        application.getPriority());
    if (transferStateFromPreviousAttempt) {
      attempt.transferStateFromPreviousAttempt(application
        .getCurrentAppAttempt());
        addApplication(appAddedEvent.getApplicationId(),
            queueName,
            appAddedEvent.getUser(),
            appAddedEvent.getIsAppRecovering(),
            appAddedEvent.getApplicatonPriority());
      }
    }
    break;
  private synchronized void setLastNodeUpdateTime(long time) {
    this.lastNodeUpdateTime = time;
  }

  @Override
  public Priority checkAndGetApplicationPriority(Priority priorityFromContext,
      String user, String queueName, ApplicationId applicationId)
      throws YarnException {
    Priority appPriority = null;


    if (null == priorityFromContext) {
      priorityFromContext = getDefaultPriorityForQueue(queueName);

      LOG.info("Application '" + applicationId
          + "' is submitted without priority "
          + "hence considering default queue/cluster priority:"
          + priorityFromContext.getPriority());
    }

    if (priorityFromContext.compareTo(getMaxClusterLevelAppPriority()) < 0) {
      priorityFromContext = Priority
          .newInstance(getMaxClusterLevelAppPriority().getPriority());
    }

    appPriority = priorityFromContext;

    LOG.info("Priority '" + appPriority.getPriority()
        + "' is acceptable in queue :" + queueName + "for application:"
        + applicationId + "for the user: " + user);

    return appPriority;
  }

  private Priority getDefaultPriorityForQueue(String queueName) {
    Queue queue = getQueue(queueName);
    if (null == queue) {
      return Priority.newInstance(CapacitySchedulerConfiguration
          .DEFAULT_CONFIGURATION_APPLICATION_PRIORITY);
    }

    return Priority.newInstance(queue.getDefaultApplicationPriority()
        .getPriority());
  }

  public Priority getMaxClusterLevelAppPriority() {
    return maxClusterLevelAppPriority;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacitySchedulerConfiguration.java
  @Private
  public static final String QUEUE_PREEMPTION_DISABLED = "disable_preemption";

  @Private
  public static final String DEFAULT_APPLICATION_PRIORITY = "default-application-priority";

  @Private
  public static final Integer DEFAULT_CONFIGURATION_APPLICATION_PRIORITY = 0;

  @Private
  public static class QueueMapping {

    
    return configuredNodeLabels;
  }

  public Integer getDefaultApplicationPriorityConfPerQueue(String queue) {
    Integer defaultPriority = getInt(getQueuePrefix(queue)
        + DEFAULT_APPLICATION_PRIORITY,
        DEFAULT_CONFIGURATION_APPLICATION_PRIORITY);
    return defaultPriority;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/LeafQueue.java
  Map<ApplicationAttemptId, FiCaSchedulerApp> applicationAttemptMap =
      new HashMap<ApplicationAttemptId, FiCaSchedulerApp>();

  private Priority defaultAppPriorityPerQueue;

  Set<FiCaSchedulerApp> pendingApplications;
  
  private float minimumAllocationFactor;
      }
    }

    defaultAppPriorityPerQueue = Priority.newInstance(conf
        .getDefaultApplicationPriorityConfPerQueue(getQueuePath()));

    LOG.info("Initializing " + queueName + "\n" +
        "capacity = " + queueCapacities.getCapacity() +
        " [= (float) configuredCapacity / 100 ]" + "\n" + 
        "nodeLocalityDelay = " +  nodeLocalityDelay + "\n" +
        "reservationsContinueLooking = " +
        reservationsContinueLooking + "\n" +
        "preemptionDisabled = " + getPreemptionDisabled() + "\n" +
        "defaultAppPriorityPerQueue = " + defaultAppPriorityPerQueue);
  }

  @Override
    this.orderingPolicy = orderingPolicy;
  }

  @Override
  public Priority getDefaultApplicationPriority() {
    return defaultAppPriorityPerQueue;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/common/fica/FiCaSchedulerApp.java
  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId, 
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext) {
    this(applicationAttemptId, user, queue, activeUsersManager, rmContext,
        Priority.newInstance(0));
  }

  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId,
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext, Priority appPriority) {
    super(applicationAttemptId, user, queue, activeUsersManager, rmContext);
    
    RMApp rmApp = rmContext.getRMApps().get(getApplicationId());
    }
    
    setAMResource(amResource);
    setPriority(appPriority);
  }

  synchronized public boolean containerCompleted(RMContainer rmContainer,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/event/AppAddedSchedulerEvent.java
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;

public class AppAddedSchedulerEvent extends SchedulerEvent {
  private final String user;
  private final ReservationId reservationID;
  private final boolean isAppRecovering;
  private final Priority appPriority;

  public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
      String user) {
    this(applicationId, queue, user, false, null, Priority.newInstance(0));
  }

  public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
      String user, ReservationId reservationID, Priority appPriority) {
    this(applicationId, queue, user, false, reservationID, appPriority);
  }

  public AppAddedSchedulerEvent(String user,
      ApplicationSubmissionContext submissionContext, boolean isAppRecovering) {
    this(submissionContext.getApplicationId(), submissionContext.getQueue(),
        user, isAppRecovering, submissionContext.getReservationID(),
        submissionContext.getPriority());
  }

  public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
      String user, boolean isAppRecovering, ReservationId reservationID,
      Priority appPriority) {
    super(SchedulerEventType.APP_ADDED);
    this.applicationId = applicationId;
    this.queue = queue;
    this.user = user;
    this.reservationID = reservationID;
    this.isAppRecovering = isAppRecovering;
    this.appPriority = appPriority;
  }

  public ApplicationId getApplicationId() {
  public ReservationId getReservationID() {
    return reservationID;
  }

  public Priority getApplicatonPriority() {
    return appPriority;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FSQueue.java
  public void decPendingResource(String nodeLabel, Resource resourceToDec) {
  }

  @Override
  public Priority getDefaultApplicationPriority() {
    return null;
  }

  public boolean fitsInMaxShare(Resource additionalResource) {
    Resource usagePlusAddition =
        Resources.add(getResourceUsage(), additionalResource);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fifo/FifoScheduler.java
    @Override
    public void decPendingResource(String nodeLabel, Resource resourceToDec) {
    }

    @Override
    public Priority getDefaultApplicationPriority() {
      return null;
    }
  };

  public FifoScheduler() {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/FifoComparator.java
      
    @Override
  public int compare(SchedulableEntity r1, SchedulableEntity r2) {
    if (r1.getPriority() != null
        && !r1.getPriority().equals(r2.getPriority())) {
      return r1.getPriority().compareTo(r2.getPriority());
    }
    int res = r1.compareInputOrderTo(r2);
    return res;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/SchedulableEntity.java
  public ResourceUsage getSchedulingResourceUsage();
  
  public Priority getPriority();

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/MockRM.java
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
    return submitApp(masterMemory, false);
  }

  public RMApp submitApp(int masterMemory, Priority priority) throws Exception {
    Resource resource = Resource.newInstance(masterMemory, 0);
    return submitApp(resource, "", UserGroupInformation.getCurrentUser()
        .getShortUserName(), null, false, null,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null, true,
        false, false, null, 0, null, true, priority);
  }

  public RMApp submitApp(int masterMemory, boolean unmanaged)
      throws Exception {
    return submitApp(masterMemory, "", UserGroupInformation.getCurrentUser()
    return submitApp(resource, name, user, acls, false, queue,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null,
          true, false, false, null, 0, null, true, null);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
    resource.setMemory(masterMemory);
    return submitApp(resource, name, user, acls, unmanaged, queue,
        maxAppAttempts, ts, appType, waitForAccepted, keepContainers,
        false, null, 0, null, true, Priority.newInstance(0));
  }

  public RMApp submitApp(int masterMemory, long attemptFailuresValidityInterval)
      throws Exception {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(masterMemory);
    Priority priority = Priority.newInstance(0);
    return submitApp(resource, "", UserGroupInformation.getCurrentUser()
      .getShortUserName(), null, false, null,
      super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null, true, false,
      false, null, attemptFailuresValidityInterval, null, true, priority);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
      ApplicationId applicationId) throws Exception {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(masterMemory);
    Priority priority = Priority.newInstance(0);
    return submitApp(resource, name, user, acls, unmanaged, queue,
      maxAppAttempts, ts, appType, waitForAccepted, keepContainers,
      isAppIdProvided, applicationId, 0, null, true, priority);
  }

  public RMApp submitApp(int masterMemory,
      LogAggregationContext logAggregationContext) throws Exception {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(masterMemory);
    Priority priority = Priority.newInstance(0);
    return submitApp(resource, "", UserGroupInformation.getCurrentUser()
      .getShortUserName(), null, false, null,
      super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null, true, false,
      false, null, 0, logAggregationContext, true, priority);
   }

  public RMApp submitApp(Resource capability, String name, String user,
      int maxAppAttempts, Credentials ts, String appType,
      boolean waitForAccepted, boolean keepContainers, boolean isAppIdProvided,
      ApplicationId applicationId, long attemptFailuresValidityInterval,
      LogAggregationContext logAggregationContext,
      boolean cancelTokensWhenComplete, Priority priority)
      throws Exception {
    ApplicationId appId = isAppIdProvided ? applicationId : null;
    ApplicationClientProtocol client = getClientRMService();
    if (queue != null) {
      sub.setQueue(queue);
    }
    if (priority != null) {
      sub.setPriority(priority);
    }
    sub.setApplicationType(appType);
    ContainerLaunchContext clc = Records
        .newRecord(ContainerLaunchContext.class);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestAppManager.java

    rmContext = mockRMContext(1, now - 10);
    ResourceScheduler scheduler = mockResourceScheduler();
    ((RMContextImpl)rmContext).setScheduler(scheduler);
    Configuration conf = new Configuration();
    ApplicationMasterService masterService =
        new ApplicationMasterService(rmContext, scheduler);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestWorkPreservingRMRestart.java
    RMApp app0 = rm1.submitApp(resource, "", UserGroupInformation
        .getCurrentUser().getShortUserName(), null, false, null,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS, null, null, true, true,
        false, null, 0, null, true, null);
    MockAM am0 = MockRM.launchAndRegisterAM(app0, rm1, nm1);

    am0.allocate("127.0.0.1", 1000, 2, new ArrayList<ContainerId>());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/capacity/TestProportionalCapacityPreemptionPolicyForNodePartitions.java
hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestApplicationLimits.java
    doReturn(applicationAttemptId). when(application).getApplicationAttemptId();
    doReturn(user).when(application).getUser();
    doReturn(amResource).when(application).getAMResource();
    doReturn(Priority.newInstance(0)).when(application).getPriority();
    when(application.compareInputOrderTo(any(FiCaSchedulerApp.class))).thenCallRealMethod();
    return application;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestApplicationPriority.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestApplicationPriority.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationIdNotProvidedException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestApplicationPriority {
  private static final Log LOG = LogFactory
      .getLog(TestApplicationPriority.class);
  private final int GB = 1024;

  private YarnConfiguration conf;

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
  }

  @Test
  public void testApplicationOrderingWithPriority() throws Exception {

    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    LeafQueue q = (LeafQueue) cs.getQueue("default");
    Assert.assertNotNull(q);

    String host = "127.0.0.1";
    RMNode node = MockNodes.newNodeInfo(0, MockNodes.newResource(16 * GB), 1,
        host);
    cs.handle(new NodeAddedSchedulerEvent(node));

    ApplicationId appId1 = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId1 = BuilderUtils.newApplicationAttemptId(
        appId1, 1);

    RMAppAttemptMetrics attemptMetric1 = new RMAppAttemptMetrics(appAttemptId1,
        rm.getRMContext());
    RMAppImpl app1 = mock(RMAppImpl.class);
    when(app1.getApplicationId()).thenReturn(appId1);
    RMAppAttemptImpl attempt1 = mock(RMAppAttemptImpl.class);
    when(attempt1.getAppAttemptId()).thenReturn(appAttemptId1);
    when(attempt1.getRMAppAttemptMetrics()).thenReturn(attemptMetric1);
    when(app1.getCurrentAppAttempt()).thenReturn(attempt1);

    rm.getRMContext().getRMApps().put(appId1, app1);

    SchedulerEvent addAppEvent1 = new AppAddedSchedulerEvent(appId1, "default",
        "user", null, Priority.newInstance(5));
    cs.handle(addAppEvent1);
    SchedulerEvent addAttemptEvent1 = new AppAttemptAddedSchedulerEvent(
        appAttemptId1, false);
    cs.handle(addAttemptEvent1);

    ApplicationId appId2 = BuilderUtils.newApplicationId(100, 2);
    ApplicationAttemptId appAttemptId2 = BuilderUtils.newApplicationAttemptId(
        appId2, 1);

    RMAppAttemptMetrics attemptMetric2 = new RMAppAttemptMetrics(appAttemptId2,
        rm.getRMContext());
    RMAppImpl app2 = mock(RMAppImpl.class);
    when(app2.getApplicationId()).thenReturn(appId2);
    RMAppAttemptImpl attempt2 = mock(RMAppAttemptImpl.class);
    when(attempt2.getAppAttemptId()).thenReturn(appAttemptId2);
    when(attempt2.getRMAppAttemptMetrics()).thenReturn(attemptMetric2);
    when(app2.getCurrentAppAttempt()).thenReturn(attempt2);

    rm.getRMContext().getRMApps().put(appId2, app2);

    SchedulerEvent addAppEvent2 = new AppAddedSchedulerEvent(appId2, "default",
        "user", null, Priority.newInstance(8));
    cs.handle(addAppEvent2);
    SchedulerEvent addAttemptEvent2 = new AppAttemptAddedSchedulerEvent(
        appAttemptId2, false);
    cs.handle(addAttemptEvent2);

    assertEquals(q.getApplications().size(), 2);
    assertEquals(q.getApplications().iterator().next()
        .getApplicationAttemptId(), appAttemptId2);

    rm.stop();
  }

  @Test
  public void testApplicationPriorityAllocation() throws Exception {

    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, 10);
    MockRM rm = new MockRM(conf);
    rm.start();

    Priority appPriority1 = Priority.newInstance(5);
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 16 * GB);
    RMApp app1 = rm.submitApp(1 * GB, appPriority1);

    MockAM am1 = MockRM.launchAM(app1, rm, nm1);
    am1.registerAppAttempt();

    am1.addRequests(new String[]{"127.0.0.1", "127.0.0.2"}, 2 * GB, 1, 7);
    AllocateResponse alloc1Response = am1.schedule(); // send the request

    nm1.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(100);
      alloc1Response = am1.schedule();
    }

    List<Container> allocated1 = alloc1Response.getAllocatedContainers();
    Assert.assertEquals(7, allocated1.size());
    Assert.assertEquals(2 * GB, allocated1.get(0).getResource().getMemory());

    SchedulerNodeReport report_nm1 = rm.getResourceScheduler().getNodeReport(
        nm1.getNodeId());
    Assert.assertEquals(15 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(1 * GB, report_nm1.getAvailableResource().getMemory());

    Priority appPriority2 = Priority.newInstance(8);
    RMApp app2 = rm.submitApp(1 * GB, appPriority2);

    nm1.nodeHeartbeat(true);
    MockAM am2 = rm.sendAMLaunched(app2.getCurrentAppAttempt()
        .getAppAttemptId());
    am2.registerAppAttempt();

    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(16 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(0 * GB, report_nm1.getAvailableResource().getMemory());

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    FiCaSchedulerApp schedulerAppAttempt = cs.getSchedulerApplications()
        .get(app1.getApplicationId()).getCurrentAppAttempt();

    int counter = 0;
    for (Container c : allocated1) {
      if (++counter > 2) {
        break;
      }
      cs.killContainer(schedulerAppAttempt.getRMContainer(c.getId()));
    }

    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(12 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(4 * GB, report_nm1.getAvailableResource().getMemory());

    am1.addRequests(new String[]{"127.0.0.1", "127.0.0.2"}, 2 * GB, 1, 10);
    am1.schedule(); // send the request for App1

    am2.addRequests(new String[]{"127.0.0.1", "127.0.0.2"}, 2 * GB, 1, 3);
    AllocateResponse alloc1Response4 = am2.schedule(); // send the request

    nm1.nodeHeartbeat(true);
    while (alloc1Response4.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 2...");
      Thread.sleep(100);
      alloc1Response4 = am2.schedule();
    }

    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(16 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(0 * GB, report_nm1.getAvailableResource().getMemory());

    rm.stop();
  }

  @Test
  public void testPriorityWithPendingApplications() throws Exception {

    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, 10);
    MockRM rm = new MockRM(conf);
    rm.start();

    Priority appPriority1 = Priority.newInstance(5);
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 8 * GB);
    RMApp app1 = rm.submitApp(1 * GB, appPriority1);

    MockAM am1 = MockRM.launchAM(app1, rm, nm1);
    am1.registerAppAttempt();

    am1.addRequests(new String[]{"127.0.0.1", "127.0.0.2"}, 1 * GB, 1, 7);
    AllocateResponse alloc1Response = am1.schedule(); // send the request

    nm1.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(100);
      alloc1Response = am1.schedule();
    }

    List<Container> allocated1 = alloc1Response.getAllocatedContainers();
    Assert.assertEquals(7, allocated1.size());
    Assert.assertEquals(1 * GB, allocated1.get(0).getResource().getMemory());

    SchedulerNodeReport report_nm1 = rm.getResourceScheduler().getNodeReport(
        nm1.getNodeId());
    Assert.assertEquals(8 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(0 * GB, report_nm1.getAvailableResource().getMemory());

    Priority appPriority2 = Priority.newInstance(7);
    RMApp app2 = rm.submitApp(1 * GB, appPriority2);

    Priority appPriority3 = Priority.newInstance(8);
    RMApp app3 = rm.submitApp(1 * GB, appPriority3);

    Priority appPriority4 = Priority.newInstance(6);
    RMApp app4 = rm.submitApp(1 * GB, appPriority4);

    rm.killApp(app1.getApplicationId());

    nm1.nodeHeartbeat(true);
    MockAM am3 = rm.sendAMLaunched(app3.getCurrentAppAttempt()
        .getAppAttemptId());
    am3.registerAppAttempt();

    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(1 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(7 * GB, report_nm1.getAvailableResource().getMemory());

    rm.stop();
  }

  @Test
  public void testMaxPriorityValidation() throws Exception {

    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, 10);
    Priority maxPriority = Priority.newInstance(10);
    MockRM rm = new MockRM(conf);
    rm.start();

    Priority appPriority1 = Priority.newInstance(15);
    rm.registerNode("127.0.0.1:1234", 8 * GB);
    RMApp app1 = rm.submitApp(1 * GB, appPriority1);

    Assert.assertEquals(app1.getApplicationSubmissionContext().getPriority(),
        maxPriority);
    rm.stop();
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestCapacityScheduler.java
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
      ApplicationId id1 = ApplicationId.newInstance(1, 1);
      ApplicationId id2 = ApplicationId.newInstance(1, 2);
      ApplicationId id3 = ApplicationId.newInstance(2, 1);
      Priority priority = Priority.newInstance(0);
      FiCaSchedulerApp app1 = Mockito.mock(FiCaSchedulerApp.class);
      when(app1.getApplicationId()).thenReturn(id1);
      when(app1.getPriority()).thenReturn(priority);
      FiCaSchedulerApp app2 = Mockito.mock(FiCaSchedulerApp.class);
      when(app2.getApplicationId()).thenReturn(id2);
      when(app2.getPriority()).thenReturn(priority);
      FiCaSchedulerApp app3 = Mockito.mock(FiCaSchedulerApp.class);
      when(app3.getApplicationId()).thenReturn(id3);
      when(app3.getPriority()).thenReturn(priority);
      assertTrue(appComparator.compare(app1, app2) < 0);
      assertTrue(appComparator.compare(app1, app3) < 0);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/MockSchedulableEntity.java
  
  private String id;
  private long serial = 0;
  private Priority priority;

  public MockSchedulableEntity() { }
  
    return 1;//let other types go before this, if any
  }

  @Override
  public Priority getPriority() {
    return priority;
  }

  public void setApplicationPriority(Priority priority) {
    this.priority = priority;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/security/TestDelegationTokenRenewer.java
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(200);
    RMApp app1 = rm.submitApp(resource, "name", "user", null, false, null, 2,
        credentials, null, true, false, false, null, 0, null, false, null);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);
    rm.waitForState(app1.getApplicationId(), RMAppState.RUNNING);

    RMApp app2 = rm.submitApp(resource, "name", "user", null, false, null, 2,
        credentials, null, true, false, false, null, 0, null, true, null);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm1);
    rm.waitForState(app2.getApplicationId(), RMAppState.RUNNING);
    MockRM.finishAMAndVerifyAppState(app2, rm, nm1, am2);
    resource.setMemory(200);
    RMApp app1 =
        rm.submitApp(resource, "name", "user", null, false, null, 2, credentials,
          null, true, false, false, null, 0, null, true, null);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);
    rm.waitForState(app1.getApplicationId(), RMAppState.RUNNING);

    Assert.assertNotNull(dttr);
    Assert.assertTrue(dttr.referringAppIds.contains(app1.getApplicationId()));
    RMApp app2 = rm.submitApp(resource, "name", "user", null, false, null, 2,
        credentials, null, true, false, false, null, 0, null, true, null);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm1);
    rm.waitForState(app2.getApplicationId(), RMAppState.RUNNING);
    Assert.assertTrue(renewer.getAllTokens().containsKey(token1));
    Assert.assertFalse(Renewer.cancelled);

    RMApp app3 = rm.submitApp(resource, "name", "user", null, false, null, 2,
        credentials, null, true, false, false, null, 0, null, true, null);
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm, nm1);
    rm.waitForState(app3.getApplicationId(), RMAppState.RUNNING);
    Assert.assertTrue(renewer.getAllTokens().containsKey(token1));

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServicesAppsModification.java
    ApplicationSubmissionContextInfo appInfo = new ApplicationSubmissionContextInfo();
    appInfo.setApplicationId(appId);
    appInfo.setApplicationName(appName);
    appInfo.setMaxAppAttempts(2);
    appInfo.setQueue(queueName);
    appInfo.setApplicationType(appType);
    appInfo.setPriority(0);
    HashMap<String, LocalResourceInfo> lr =  new HashMap<>();
    LocalResourceInfo y = new LocalResourceInfo();
    y.setUrl(new URI("http://www.test.com/file.txt"));

