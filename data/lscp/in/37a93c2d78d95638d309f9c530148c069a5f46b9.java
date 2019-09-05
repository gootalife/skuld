hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/server/api/ContainerContext.java
  private final String user;
  private final ContainerId containerId;
  private final Resource resource;
  private final ContainerType containerType;

  @Private
  @Unstable
  public ContainerContext(String user, ContainerId containerId,
      Resource resource) {
    this(user, containerId, resource, ContainerType.TASK);
  }

  @Private
  @Unstable
  public ContainerContext(String user, ContainerId containerId,
      Resource resource, ContainerType containerType) {
    this.user = user;
    this.containerId = containerId;
    this.resource = resource;
    this.containerType = containerType;
  }

  public Resource getResource() {
    return resource;
  }

  public ContainerType getContainerType() {
    return containerType;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/server/api/ContainerInitializationContext.java
    super(user, containerId, resource);
  }

  @Private
  @Unstable
  public ContainerInitializationContext(String user, ContainerId containerId,
      Resource resource, ContainerType containerType) {
    super(user, containerId, resource, containerType);
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/server/api/ContainerTerminationContext.java
    super(user, containerId, resource);
  }

  @Private
  @Unstable
  public ContainerTerminationContext(String user, ContainerId containerId,
      Resource resource, ContainerType containerType) {
    super(user, containerId, resource, containerType);
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/server/api/ContainerType.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/server/api/ContainerType.java

package org.apache.hadoop.yarn.server.api;

public enum ContainerType {
  APPLICATION_MASTER, TASK
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/ProtoUtils.java
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationRequestInterpreterProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationAttemptStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerTypeProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.server.api.ContainerType;

import com.google.protobuf.ByteString;

    return LogAggregationStatus.valueOf(e.name().replace(
      LOG_AGGREGATION_STATUS_PREFIX, ""));
  }

  public static ContainerTypeProto convertToProtoFormat(ContainerType e) {
    return ContainerTypeProto.valueOf(e.name());
  }
  public static ContainerType convertFromProtoFormat(ContainerTypeProto e) {
    return ContainerType.valueOf(e.name());
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/security/ContainerTokenIdentifier.java
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.LogAggregationContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerTypeProto;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.ContainerTokenIdentifierProto;
import org.apache.hadoop.yarn.server.api.ContainerType;

import com.google.protobuf.TextFormat;

      int masterKeyId, long rmIdentifier, Priority priority, long creationTime) {
    this(containerID, hostName, appSubmitter, r, expiryTimeStamp, masterKeyId,
        rmIdentifier, priority, creationTime, null,
        CommonNodeLabelsManager.NO_LABEL, ContainerType.TASK);
  }

  public ContainerTokenIdentifier(ContainerId containerID, String hostName,
      String appSubmitter, Resource r, long expiryTimeStamp, int masterKeyId,
      long rmIdentifier, Priority priority, long creationTime,
      LogAggregationContext logAggregationContext, String nodeLabelExpression) {
    this(containerID, hostName, appSubmitter, r, expiryTimeStamp, masterKeyId,
        rmIdentifier, priority, creationTime, logAggregationContext,
        nodeLabelExpression, ContainerType.TASK);
  }

  public ContainerTokenIdentifier(ContainerId containerID, String hostName,
      String appSubmitter, Resource r, long expiryTimeStamp, int masterKeyId,
      long rmIdentifier, Priority priority, long creationTime,
      LogAggregationContext logAggregationContext, String nodeLabelExpression,
      ContainerType containerType) {
    ContainerTokenIdentifierProto.Builder builder =
        ContainerTokenIdentifierProto.newBuilder();
    if (containerID != null) {
    if (nodeLabelExpression != null) {
      builder.setNodeLabelExpression(nodeLabelExpression);
    }
    builder.setContainerType(convertToProtoFormat(containerType));

    proto = builder.build();
  }
    return proto.getRmIdentifier();
  }

  public ContainerType getContainerType(){
    if (!proto.hasContainerType()) {
      return null;
    }
    return convertFromProtoFormat(proto.getContainerType());
  }

  public ContainerTokenIdentifierProto getProto() {
    return proto;
  }
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private ContainerTypeProto convertToProtoFormat(ContainerType containerType) {
    return ProtoUtils.convertToProtoFormat(containerType);
  }

  private ContainerType convertFromProtoFormat(
      ContainerTypeProto containerType) {
    return ProtoUtils.convertFromProtoFormat(containerType);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/security/TestYARNTokenIdentifier.java
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.junit.Assert;
import org.junit.Test;

        anotherToken.getCreationTime(), creationTime);
    
    Assert.assertNull(anotherToken.getLogAggregationContext());

    Assert.assertEquals(CommonNodeLabelsManager.NO_LABEL,
        anotherToken.getNodeLabelExpression());

    Assert.assertEquals(ContainerType.TASK,
        anotherToken.getContainerType());
  }
  
  @Test
    Assert.assertEquals(new Text("yarn"), token.getRenewer());
  }

  @Test
  public void testAMContainerTokenIdentifier() throws IOException {
    ContainerId containerID = ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(
            1, 1), 1), 1);
    String hostName = "host0";
    String appSubmitter = "usr0";
    Resource r = Resource.newInstance(1024, 1);
    long expiryTimeStamp = 1000;
    int masterKeyId = 1;
    long rmIdentifier = 1;
    Priority priority = Priority.newInstance(1);
    long creationTime = 1000;

    ContainerTokenIdentifier token =
        new ContainerTokenIdentifier(containerID, hostName, appSubmitter, r,
            expiryTimeStamp, masterKeyId, rmIdentifier, priority, creationTime,
            null, CommonNodeLabelsManager.NO_LABEL, ContainerType.APPLICATION_MASTER);

    ContainerTokenIdentifier anotherToken = new ContainerTokenIdentifier();

    byte[] tokenContent = token.getBytes();
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(tokenContent, tokenContent.length);
    anotherToken.readFields(dib);

    Assert.assertEquals(ContainerType.APPLICATION_MASTER,
        anotherToken.getContainerType());

    token =
        new ContainerTokenIdentifier(containerID, hostName, appSubmitter, r,
            expiryTimeStamp, masterKeyId, rmIdentifier, priority, creationTime,
            null, CommonNodeLabelsManager.NO_LABEL, ContainerType.TASK);

    anotherToken = new ContainerTokenIdentifier();

    tokenContent = token.getBytes();
    dib = new DataInputBuffer();
    dib.reset(tokenContent, tokenContent.length);
    anotherToken.readFields(dib);

    Assert.assertEquals(ContainerType.TASK,
        anotherToken.getContainerType());
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/AuxServices.java
          try {
            serv.initializeContainer(new ContainerInitializationContext(
                event.getUser(), event.getContainer().getContainerId(),
                event.getContainer().getResource(), event.getContainer()
                .getContainerTokenIdentifier().getContainerType()));
          } catch (Throwable th) {
            logWarningWhenAuxServiceThrowExceptions(serv,
                AuxServicesEventType.CONTAINER_INIT, th);
          try {
            serv.stopContainer(new ContainerTerminationContext(
                event.getUser(), event.getContainer().getContainerId(),
                event.getContainer().getResource(), event.getContainer()
                .getContainerTokenIdentifier().getContainerType()));
          } catch (Throwable th) {
            logWarningWhenAuxServiceThrowExceptions(serv,
                AuxServicesEventType.CONTAINER_STOP, th);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/SchedulerApplicationAttempt.java
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AggregateAppResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
      .hasNext();) {
      RMContainer rmContainer = i.next();
      Container container = rmContainer.getContainer();
      ContainerType containerType = ContainerType.TASK;
      RMAppAttempt appAttempt =
          rmContext
              .getRMApps()
              .get(
                  container.getId().getApplicationAttemptId()
                      .getApplicationId()).getCurrentAppAttempt();
      if (appAttempt.getMasterContainer() == null
          && appAttempt.getSubmissionContext().getUnmanagedAM() == false) {
        containerType = ContainerType.APPLICATION_MASTER;
      }
      try {
        container.setContainerToken(rmContext.getContainerTokenSecretManager()
            .createContainerToken(container.getId(), container.getNodeId(),
                getUser(), container.getResource(), container.getPriority(),
                rmContainer.getCreationTime(), this.logAggregationContext,
                rmContainer.getNodeLabelExpression(), containerType));
        NMToken nmToken =
            rmContext.getNMTokenSecretManager().createAndGetNMToken(getUser(),
              getApplicationAttemptId(), container);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/LeafQueue.java
            RMAppAttempt rmAppAttempt =
                csContext.getRMContext().getRMApps()
                    .get(application.getApplicationId()).getCurrentAppAttempt();
            if (rmAppAttempt.getSubmissionContext().getUnmanagedAM() == false
                && null == rmAppAttempt.getMasterContainer()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Skip allocating AM container to app_attempt="
                    + application.getApplicationAttemptId()

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/security/RMContainerTokenSecretManager.java
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;
      String appSubmitter, Resource capability, Priority priority,
      long createTime) {
    return createContainerToken(containerId, nodeId, appSubmitter, capability,
      priority, createTime, null, null, ContainerType.TASK);
  }

  public Token createContainerToken(ContainerId containerId, NodeId nodeId,
      String appSubmitter, Resource capability, Priority priority,
      long createTime, LogAggregationContext logAggregationContext,
      String nodeLabelExpression, ContainerType containerType) {
    byte[] password;
    ContainerTokenIdentifier tokenIdentifier;
    long expiryTimeStamp =
            appSubmitter, capability, expiryTimeStamp, this.currentMasterKey
              .getMasterKey().getKeyId(),
            ResourceManager.getClusterTimeStamp(), priority, createTime,
            logAggregationContext, nodeLabelExpression, containerType);
      password = this.createPassword(tokenIdentifier);

    } finally {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/Application.java
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.Task.State;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
    
    resourceManager.getClientRMService().submitApplication(request);

    RMAppEvent event =
        new RMAppEvent(this.applicationId, RMAppEventType.START);
    resourceManager.getRMContext().getRMApps().get(applicationId).handle(event);
    event =
        new RMAppEvent(this.applicationId, RMAppEventType.APP_NEW_SAVED);
    resourceManager.getRMContext().getRMApps().get(applicationId).handle(event);
    event =
        new RMAppEvent(this.applicationId, RMAppEventType.APP_ACCEPTED);
    resourceManager.getRMContext().getRMApps().get(applicationId).handle(event);

    AppAddedSchedulerEvent addAppEvent =
        new AppAddedSchedulerEvent(this.applicationId, this.queue, "user");

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestContainerAllocation.java
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
        public Token createContainerToken(ContainerId containerId,
            NodeId nodeId, String appSubmitter, Resource capability,
            Priority priority, long createTime,
            LogAggregationContext logAggregationContext, String nodeLabelExp, ContainerType containerType) {
          numRetries++;
          return super.createContainerToken(containerId, nodeId, appSubmitter,
              capability, priority, createTime, logAggregationContext,
              nodeLabelExp, containerType);
        }
      };
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FairSchedulerTestBase.java
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
    when(rmApp.getCurrentAppAttempt()).thenReturn(rmAppAttempt);
    when(rmAppAttempt.getRMAppAttemptMetrics()).thenReturn(
        new RMAppAttemptMetrics(id, resourceManager.getRMContext()));
    ApplicationSubmissionContext submissionContext = mock(ApplicationSubmissionContext.class);
    when(submissionContext.getUnmanagedAM()).thenReturn(false);
    when(rmAppAttempt.getSubmissionContext()).thenReturn(submissionContext);
    Container container = mock(Container.class);
    when(rmAppAttempt.getMasterContainer()).thenReturn(container);
    resourceManager.getRMContext().getRMApps()
        .put(id.getApplicationId(), rmApp);

    when(rmApp.getCurrentAppAttempt()).thenReturn(rmAppAttempt);
    when(rmAppAttempt.getRMAppAttemptMetrics()).thenReturn(
        new RMAppAttemptMetrics(id,resourceManager.getRMContext()));
    ApplicationSubmissionContext submissionContext = mock(ApplicationSubmissionContext.class);
    when(submissionContext.getUnmanagedAM()).thenReturn(false);
    when(rmAppAttempt.getSubmissionContext()).thenReturn(submissionContext);
    resourceManager.getRMContext().getRMApps()
        .put(id.getApplicationId(), rmApp);

  protected void createApplicationWithAMResource(ApplicationAttemptId attId,
      String queue, String user, Resource amResource) {
    RMContext rmContext = resourceManager.getRMContext();
    ApplicationId appId = attId.getApplicationId();
    RMApp rmApp = new RMAppImpl(appId, rmContext, conf,
        null, user, null, ApplicationSubmissionContext.newInstance(appId, null,
        queue, null, null, false, false, 0, amResource, null), null, null,
        0, null, null, null);
    rmContext.getRMApps().put(appId, rmApp);
    RMAppEvent event = new RMAppEvent(appId, RMAppEventType.START);
    resourceManager.getRMContext().getRMApps().get(appId).handle(event);
    event = new RMAppEvent(appId, RMAppEventType.APP_NEW_SAVED);
    resourceManager.getRMContext().getRMApps().get(appId).handle(event);
    event = new RMAppEvent(appId, RMAppEventType.APP_ACCEPTED);
    resourceManager.getRMContext().getRMApps().get(appId).handle(event);
    AppAddedSchedulerEvent appAddedEvent = new AppAddedSchedulerEvent(
        appId, queue, user);
    scheduler.handle(appAddedEvent);
    AppAttemptAddedSchedulerEvent attempAddedEvent =
        new AppAttemptAddedSchedulerEvent(attId, false);
    RMAppAttemptMetrics attemptMetric = mock(RMAppAttemptMetrics.class);
    when(attempt.getRMAppAttemptMetrics()).thenReturn(attemptMetric);
    when(app.getCurrentAppAttempt()).thenReturn(attempt);
    ApplicationSubmissionContext submissionContext = mock(ApplicationSubmissionContext.class);
    when(submissionContext.getUnmanagedAM()).thenReturn(false);
    when(attempt.getSubmissionContext()).thenReturn(submissionContext);
    resourceManager.getRMContext().getRMApps()
        .put(attemptId.getApplicationId(), app);
    return app;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fifo/TestFifoScheduler.java
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
    RMAppAttemptMetrics attemptMetric = mock(RMAppAttemptMetrics.class);
    when(attempt.getRMAppAttemptMetrics()).thenReturn(attemptMetric);
    when(app.getCurrentAppAttempt()).thenReturn(attempt);
    ApplicationSubmissionContext submissionContext = mock(ApplicationSubmissionContext.class);
    when(submissionContext.getUnmanagedAM()).thenReturn(false);
    when(attempt.getSubmissionContext()).thenReturn(submissionContext);
    context.getRMApps().putIfAbsent(attemptId.getApplicationId(), app);
    return app;
  }

