hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/ApplicationReport.java
  @Stable
  public abstract Token getAMRMToken();

  @Public
  @Stable
  public abstract LogAggregationStatus getLogAggregationStatus();

  @Private
  @Unstable
  public abstract void setLogAggregationStatus(
      LogAggregationStatus logAggregationStatus);
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/LogAggregationStatus.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/LogAggregationStatus.java

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

public enum LogAggregationStatus {

  DISABLED,

  NOT_START,

  RUNNING,

  SUCCEEDED,

  FAILED,

  TIME_OUT
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/cli/ApplicationCLI.java
      } else {
        appReportStr.println("N/A");
      }
      appReportStr.print("\tLog Aggregation Status : ");
      appReportStr.println(appReport.getLogAggregationStatus() == null ? "N/A"
          : appReport.getLogAggregationStatus());
      appReportStr.print("\tDiagnostics : ");
      appReportStr.print(appReport.getDiagnostics());
    } else {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/cli/TestYarnCLI.java
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
          YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0,
          FinalApplicationStatus.SUCCEEDED, usageReport, "N/A", 0.53789f, "YARN",
          null);
      newApplicationReport.setLogAggregationStatus(LogAggregationStatus.SUCCEEDED);
      when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
          newApplicationReport);
      int result = cli.run(new String[] { "application", "-status", applicationId.toString() });
      pw.println("\tAM Host : host");
      pw.println("\tAggregate Resource Allocation : " +
          (i == 0 ? "N/A" : "123456 MB-seconds, 4567 vcore-seconds"));
      pw.println("\tLog Aggregation Status : SUCCEEDED");
      pw.println("\tDiagnostics : diagnostics");
      pw.close();
      String appReportStr = baos.toString("UTF-8");

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/ApplicationReportPBImpl.java
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationReportProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationResourceUsageReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.FinalApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LogAggregationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationStateProto;

import com.google.protobuf.TextFormat;
  private TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl)t).getProto();
  }

  @Override
  public LogAggregationStatus getLogAggregationStatus() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasLogAggregationStatus()) {
      return null;
    }
    return convertFromProtoFormat(p.getLogAggregationStatus());
  }

  @Override
  public void setLogAggregationStatus(
      LogAggregationStatus logAggregationStatus) {
    maybeInitBuilder();
    if (logAggregationStatus == null) {
      builder.clearLogAggregationStatus();
      return;
    }
    builder.setLogAggregationStatus(
        convertToProtoFormat(logAggregationStatus));
  }

  private LogAggregationStatus convertFromProtoFormat(
      LogAggregationStatusProto s) {
    return ProtoUtils.convertFromProtoFormat(s);
  }

  private LogAggregationStatusProto
      convertToProtoFormat(LogAggregationStatus s) {
    return ProtoUtils.convertToProtoFormat(s);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/ProtoUtils.java
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.proto.YarnProtos.FinalApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceTypeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceVisibilityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LogAggregationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueACLProto;
    return ReservationRequestInterpreter.valueOf(e.name());
  }

  private static final String LOG_AGGREGATION_STATUS_PREFIX = "LOG_";
  public static LogAggregationStatusProto convertToProtoFormat(
      LogAggregationStatus e) {
    return LogAggregationStatusProto.valueOf(LOG_AGGREGATION_STATUS_PREFIX
        + e.name());
  }

  public static LogAggregationStatus convertFromProtoFormat(
      LogAggregationStatusProto e) {
    return LogAggregationStatus.valueOf(e.name().replace(
      LOG_AGGREGATION_STATUS_PREFIX, ""));
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/LogAggregationReport.java
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Records;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/LogAggregationReportPBImpl.java
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LogAggregationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.LogAggregationReportProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.LogAggregationReportProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;

import com.google.protobuf.TextFormat;

  LogAggregationReportProto.Builder builder = null;
  boolean viaProto = false;

  private ApplicationId applicationId;
  private NodeId nodeId;


  private LogAggregationStatus convertFromProtoFormat(
      LogAggregationStatusProto s) {
    return ProtoUtils.convertFromProtoFormat(s);
  }

  private LogAggregationStatusProto
      convertToProtoFormat(LogAggregationStatus s) {
    return ProtoUtils.convertToProtoFormat(s);
  }

  @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/logaggregation/AppLogAggregatorImpl.java
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRetentionPolicy;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
      report.setDiagnosticMessage(diagnosticMessage);
      if (appFinished) {
        report.setLogAggregationStatus(renameTemporaryLogFileFailed
            ? LogAggregationStatus.FAILED : LogAggregationStatus.SUCCEEDED);
      } else {
        report.setLogAggregationStatus(LogAggregationStatus.RUNNING);
      }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/RMApp.java
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
  ResourceRequest getAMResourceRequest();

  Map<NodeId, LogAggregationReport> getLogAggregationReportsForApp();

  LogAggregationStatus getLogAggregationStatusForAppReport();
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/RMAppImpl.java
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEventType;
  private final long logAggregationStatusTimeout;
  private final Map<NodeId, LogAggregationReport> logAggregationStatus =
      new HashMap<NodeId, LogAggregationReport>();
  private LogAggregationStatus logAggregationStatusForAppReport;

  private RMAppState stateBeforeKilling;
      String trackingUrl = UNAVAILABLE;
      String host = UNAVAILABLE;
      String origTrackingUrl = UNAVAILABLE;
      LogAggregationStatus logAggregationStatus = null;
      int rpcPort = -1;
      ApplicationResourceUsageReport appUsageReport =
          RMServerUtils.DUMMY_APPLICATION_RESOURCE_USAGE_REPORT;
          rpcPort = this.currentAttempt.getRpcPort();
          appUsageReport = currentAttempt.getApplicationResourceUsageReport();
          progress = currentAttempt.getProgress();
          logAggregationStatus = this.getLogAggregationStatusForAppReport();
        }
        diags = this.diagnostics.toString();

                DUMMY_APPLICATION_ATTEMPT_NUMBER);
      }

      ApplicationReport report = BuilderUtils.newApplicationReport(
          this.applicationId, currentApplicationAttemptId, this.user,
          this.queue, this.name, host, rpcPort, clientToAMToken,
          createApplicationState(), diags,
          trackingUrl, this.startTime, this.finishTime, finishState,
          appUsageReport, origTrackingUrl, progress, this.applicationType, 
          amrmToken, applicationTags);
      report.setLogAggregationStatus(logAggregationStatus);
      return report;
    } finally {
      this.readLock.unlock();
    }
      app.ranNodes.add(nodeAddedEvent.getNodeId());

      if (!app.logAggregationStatus.containsKey(nodeAddedEvent.getNodeId())) {
        app.logAggregationStatus.put(nodeAddedEvent.getNodeId(),
          LogAggregationReport.newInstance(app.applicationId, nodeAddedEvent
            .getNodeId(), app.logAggregationEnabled
              ? LogAggregationStatus.NOT_START : LogAggregationStatus.DISABLED,
            ""));
      }
    };
  }

        if (!output.getValue().getLogAggregationStatus()
          .equals(LogAggregationStatus.TIME_OUT)
            && !output.getValue().getLogAggregationStatus()
              .equals(LogAggregationStatus.SUCCEEDED)
            && !output.getValue().getLogAggregationStatus()
              .equals(LogAggregationStatus.FAILED)
            && isAppInFinalState(this)
            && System.currentTimeMillis() > this.logAggregationStartTime
                + this.logAggregationStatusTimeout) {
          if (curReport.getLogAggregationStatus().equals(
            LogAggregationStatus.TIME_OUT)) {
            if (report.getLogAggregationStatus().equals(
              LogAggregationStatus.SUCCEEDED)
                || report.getLogAggregationStatus().equals(
                  LogAggregationStatus.FAILED)) {
              curReport.setLogAggregationStatus(report
                .getLogAggregationStatus());
            }
      this.writeLock.unlock();
    }
  }

  @Override
  public LogAggregationStatus getLogAggregationStatusForAppReport() {
    if (!logAggregationEnabled) {
      return LogAggregationStatus.DISABLED;
    }
    if (this.logAggregationStatusForAppReport == LogAggregationStatus.FAILED
        || this.logAggregationStatusForAppReport == LogAggregationStatus.SUCCEEDED) {
      return this.logAggregationStatusForAppReport;
    }
    try {
      this.readLock.lock();
      Map<NodeId, LogAggregationReport> reports =
          getLogAggregationReportsForApp();
      if (reports.size() == 0) {
        return null;
      }
      int logNotStartCount = 0;
      int logCompletedCount = 0;
      int logTimeOutCount = 0;
      int logFailedCount = 0;
      for (Entry<NodeId, LogAggregationReport> report : reports.entrySet()) {
        switch (report.getValue().getLogAggregationStatus()) {
          case NOT_START:
            logNotStartCount++;
            break;
          case SUCCEEDED:
            logCompletedCount++;
            break;
          case FAILED:
            logFailedCount++;
            logCompletedCount++;
            break;
          case TIME_OUT:
            logTimeOutCount++;
            logCompletedCount++;
            break;
          default:
            break;
        }
      }
      if (logNotStartCount == reports.size()) {
        return LogAggregationStatus.NOT_START;
      } else if (logCompletedCount == reports.size()) {
        if (logFailedCount > 0 && isAppInFinalState(this)) {
          this.logAggregationStatusForAppReport = LogAggregationStatus.FAILED;
          return LogAggregationStatus.FAILED;
        } else if (logTimeOutCount > 0) {
          return LogAggregationStatus.TIME_OUT;
        }
        if (isAppInFinalState(this)) {
          this.logAggregationStatusForAppReport = LogAggregationStatus.SUCCEEDED;
          return LogAggregationStatus.SUCCEEDED;
        }
      }
      return LogAggregationStatus.RUNNING;
    } finally {
      this.readLock.unlock();
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppLogAggregationStatusBlock.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.util.Apps;
      .td("Log Aggregation does not Start.")._();
    table_description.tr().td(LogAggregationStatus.RUNNING.name())
      .td("Log Aggregation is Running.")._();
    table_description.tr().td(LogAggregationStatus.SUCCEEDED.name())
      .td("Log Aggregation is Succeeded. All of the logs have been "
          + "aggregated successfully.")._();
    table_description.tr().td(LogAggregationStatus.FAILED.name())
      .td("Log Aggregation is Failed. At least one of the logs "
          + "have not been aggregated.")._();
    table_description.tr().td(LogAggregationStatus.TIME_OUT.name())
      .td("The application is finished, but the log aggregation status is "
          + "not updated for a long time. Not sure whether the log aggregation "
          + "is finished or not.")._();
    table_description._();
    div_description._();


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/AppInfo.java
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

  protected List<ResourceRequest> resourceRequests;

  protected LogAggregationStatus logAggregationStatus;

  public AppInfo() {
  } // JAXB needs this

        this.finishedTime = app.getFinishTime();
        this.elapsedTime = Times.elapsed(app.getStartTime(),
            app.getFinishTime());
        this.logAggregationStatus = app.getLogAggregationStatusForAppReport();
        RMAppAttempt attempt = app.getCurrentAppAttempt();
        if (attempt != null) {
          Container masterContainer = attempt.getMasterContainer();
  public List<ResourceRequest> getResourceRequests() {
    return this.resourceRequests;
  }

  public LogAggregationStatus getLogAggregationStatus() {
    return this.logAggregationStatus;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/applicationsmanager/MockAsm.java
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
    public Map<NodeId, LogAggregationReport> getLogAggregationReportsForApp() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public LogAggregationStatus getLogAggregationStatusForAppReport() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  public static RMApp newApplication(int i) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/logaggregationstatus/TestRMAppLogAggregationStatus.java
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;

    Map<ApplicationId, LogAggregationReport> node1ReportForApp3 =
        new HashMap<ApplicationId, LogAggregationReport>();
    String messageForNode1_3 =
            + System.currentTimeMillis();
    LogAggregationReport report1_3 =
        LogAggregationReport.newInstance(appId, nodeId1,
          LogAggregationStatus.SUCCEEDED, messageForNode1_3);
    node1ReportForApp3.put(appId, report1_3);
    node1.handle(new RMNodeStatusEvent(node1.getNodeID(), NodeHealthStatus
      .newInstance(true, null, 0), new ArrayList<ContainerStatus>(), null,
    for (Entry<NodeId, LogAggregationReport> report : logAggregationStatus
      .entrySet()) {
      if (report.getKey().equals(node1.getNodeID())) {
        Assert.assertEquals(LogAggregationStatus.SUCCEEDED, report.getValue()
          .getLogAggregationStatus());
        Assert.assertEquals(messageForNode1_1 + messageForNode1_2
            + messageForNode1_3, report.getValue().getDiagnosticMessage());
    }
  }

  @Test (timeout = 10000)
  public void testGetLogAggregationStatusForAppReport() {
    YarnConfiguration conf = new YarnConfiguration();

    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);
    RMAppImpl rmApp = (RMAppImpl)createRMApp(conf);
    Assert.assertEquals(LogAggregationStatus.DISABLED,
      rmApp.getLogAggregationStatusForAppReport());

    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    rmApp = (RMAppImpl)createRMApp(conf);
    Assert.assertNull(rmApp.getLogAggregationStatusForAppReport());

    NodeId nodeId1 = NodeId.newInstance("localhost", 1111);
    NodeId nodeId2 = NodeId.newInstance("localhost", 2222);
    NodeId nodeId3 = NodeId.newInstance("localhost", 3333);
    NodeId nodeId4 = NodeId.newInstance("localhost", 4444);

    rmApp.aggregateLogReport(nodeId1, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.NOT_START, ""));
    rmApp.aggregateLogReport(nodeId2, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.NOT_START, ""));
    rmApp.aggregateLogReport(nodeId3, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.NOT_START, ""));
    rmApp.aggregateLogReport(nodeId4, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.NOT_START, ""));
    Assert.assertEquals(LogAggregationStatus.NOT_START,
      rmApp.getLogAggregationStatusForAppReport());

    rmApp.aggregateLogReport(nodeId1, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.NOT_START, ""));
    rmApp.aggregateLogReport(nodeId2, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.RUNNING, ""));
    rmApp.aggregateLogReport(nodeId3, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.SUCCEEDED, ""));
    rmApp.aggregateLogReport(nodeId4, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.FAILED, ""));
    Assert.assertEquals(LogAggregationStatus.RUNNING,
      rmApp.getLogAggregationStatusForAppReport());

    rmApp.handle(new RMAppEvent(rmApp.getApplicationId(), RMAppEventType.KILL));
    Assert.assertTrue(RMAppImpl.isAppInFinalState(rmApp));

    rmApp.aggregateLogReport(nodeId1, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.SUCCEEDED, ""));
    rmApp.aggregateLogReport(nodeId2, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.TIME_OUT, ""));
    rmApp.aggregateLogReport(nodeId3, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.SUCCEEDED, ""));
    rmApp.aggregateLogReport(nodeId4, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.SUCCEEDED, ""));
    Assert.assertEquals(LogAggregationStatus.TIME_OUT,
      rmApp.getLogAggregationStatusForAppReport());

    rmApp.aggregateLogReport(nodeId1, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.SUCCEEDED, ""));
    rmApp.aggregateLogReport(nodeId2, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.SUCCEEDED, ""));
    rmApp.aggregateLogReport(nodeId3, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.SUCCEEDED, ""));
    rmApp.aggregateLogReport(nodeId4, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.SUCCEEDED, ""));
    Assert.assertEquals(LogAggregationStatus.SUCCEEDED,
      rmApp.getLogAggregationStatusForAppReport());

    rmApp = (RMAppImpl)createRMApp(conf);
    rmApp.handle(new RMAppEvent(rmApp.getApplicationId(), RMAppEventType.KILL));
    Assert.assertTrue(RMAppImpl.isAppInFinalState(rmApp));
    rmApp.aggregateLogReport(nodeId1, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.SUCCEEDED, ""));
    rmApp.aggregateLogReport(nodeId2, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.TIME_OUT, ""));
    rmApp.aggregateLogReport(nodeId3, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.FAILED, ""));
    rmApp.aggregateLogReport(nodeId4, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), nodeId1, LogAggregationStatus.SUCCEEDED, ""));
    Assert.assertEquals(LogAggregationStatus.FAILED,
      rmApp.getLogAggregationStatusForAppReport());

  }

  private RMApp createRMApp(Configuration conf) {
    ApplicationSubmissionContext submissionContext =
        ApplicationSubmissionContext.newInstance(appId, "test", "default",

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/MockRMApp.java
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
  public Map<NodeId, LogAggregationReport> getLogAggregationReportsForApp() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public LogAggregationStatus getLogAggregationStatusForAppReport() {
    return null;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServicesApps.java
          WebServicesTestUtils.getXmlInt(element, "preemptedResourceMB"),
          WebServicesTestUtils.getXmlInt(element, "preemptedResourceVCores"),
          WebServicesTestUtils.getXmlInt(element, "numNonAMContainerPreempted"),
          WebServicesTestUtils.getXmlInt(element, "numAMContainerPreempted"),
          WebServicesTestUtils.getXmlString(element, "logAggregationStatus"));
    }
  }

  public void verifyAppInfo(JSONObject info, RMApp app) throws JSONException,
      Exception {

    assertEquals("incorrect number of elements", 28, info.length());

    verifyAppInfoGeneric(app, info.getString("id"), info.getString("user"),
        info.getString("name"), info.getString("applicationType"),
        info.getInt("preemptedResourceMB"),
        info.getInt("preemptedResourceVCores"),
        info.getInt("numNonAMContainerPreempted"),
        info.getInt("numAMContainerPreempted"),
        info.getString("logAggregationStatus"));
  }

  public void verifyAppInfoGeneric(RMApp app, String id, String user,
      long elapsedTime, String amHostHttpAddress, String amContainerLogs,
      int allocatedMB, int allocatedVCores, int numContainers,
      int preemptedResourceMB, int preemptedResourceVCores,
      int numNonAMContainerPreempted, int numAMContainerPreempted,
      String logAggregationStatus) throws JSONException,
      Exception {

    WebServicesTestUtils.checkStringMatch("id", app.getApplicationId()
    assertEquals("numAMContainerPreempted doesn't match", app
        .getRMAppMetrics().getNumAMContainersPreempted(),
        numAMContainerPreempted);
    assertEquals("Log aggregation Status doesn't match", app
        .getLogAggregationStatusForAppReport().toString(),
        logAggregationStatus);
  }

  @Test

