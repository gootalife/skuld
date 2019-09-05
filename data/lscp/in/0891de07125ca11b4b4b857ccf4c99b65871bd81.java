hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
      YARN_PREFIX + "log-aggregation.retain-check-interval-seconds";
  public static final long DEFAULT_LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS = -1;

  public static final String LOG_AGGREGATION_STATUS_TIME_OUT_MS =
      YARN_PREFIX + "log-aggregation-status.time-out.ms";
  public static final long DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS
      = 10 * 60 * 1000;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/LogAggregationReport.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/LogAggregationReport.java

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.util.Records;

@Public
@Unstable
public abstract class LogAggregationReport {

  @Public
  @Unstable
  public static LogAggregationReport newInstance(ApplicationId appId,
      NodeId nodeId, LogAggregationStatus status, String diagnosticMessage) {
    LogAggregationReport report = Records.newRecord(LogAggregationReport.class);
    report.setApplicationId(appId);
    report.setLogAggregationStatus(status);
    report.setDiagnosticMessage(diagnosticMessage);
    return report;
  }

  @Public
  @Unstable
  public abstract ApplicationId getApplicationId();

  @Public
  @Unstable
  public abstract void setApplicationId(ApplicationId appId);

  @Public
  @Unstable
  public abstract NodeId getNodeId();

  @Public
  @Unstable
  public abstract void setNodeId(NodeId nodeId);

  @Public
  @Unstable
  public abstract LogAggregationStatus getLogAggregationStatus();

  @Public
  @Unstable
  public abstract void setLogAggregationStatus(
      LogAggregationStatus logAggregationStatus);

  @Public
  @Unstable
  public abstract String getDiagnosticMessage();

  @Public
  @Unstable
  public abstract void setDiagnosticMessage(String diagnosticMessage);
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/NodeHeartbeatRequest.java

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.util.Records;
  
  public abstract Set<String> getNodeLabels();
  public abstract void setNodeLabels(Set<String> nodeLabels);

  public abstract Map<ApplicationId, LogAggregationReport>
      getLogAggregationReportsForApps();

  public abstract void setLogAggregationReportsForApps(
      Map<ApplicationId, LogAggregationReport> logAggregationReportsForApps);
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/LogAggregationReportPBImpl.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/LogAggregationReportPBImpl.java

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.LogAggregationStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.LogAggregationReportProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.LogAggregationReportProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.LogAggregationStatus;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class LogAggregationReportPBImpl extends LogAggregationReport {

  LogAggregationReportProto proto = LogAggregationReportProto
    .getDefaultInstance();
  LogAggregationReportProto.Builder builder = null;
  boolean viaProto = false;

  private static final String LOGAGGREGATION_STATUS_PREFIX = "LOG_";

  private ApplicationId applicationId;
  private NodeId nodeId;

  public LogAggregationReportPBImpl() {
    builder = LogAggregationReportProto.newBuilder();
  }

  public LogAggregationReportPBImpl(LogAggregationReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public LogAggregationReportProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void mergeLocalToBuilder() {
    if (this.applicationId != null
        && !((ApplicationIdPBImpl) this.applicationId).getProto().equals(
          builder.getApplicationId())) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }

    if (this.nodeId != null
        && !((NodeIdPBImpl) this.nodeId).getProto().equals(
          builder.getNodeId())) {
      builder.setNodeId(convertToProtoFormat(this.nodeId));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = LogAggregationReportProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ApplicationId getApplicationId() {
    if (this.applicationId != null) {
      return this.applicationId;
    }

    LogAggregationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationId()) {
      return null;
    }
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return this.applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId appId) {
    maybeInitBuilder();
    if (appId == null)
      builder.clearApplicationId();
    this.applicationId = appId;
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }

  private ApplicationIdPBImpl convertFromProtoFormat(
      ApplicationIdProto applicationId) {
    return new ApplicationIdPBImpl(applicationId);
  }

  @Override
  public LogAggregationStatus getLogAggregationStatus() {
    LogAggregationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasLogAggregationStatus()) {
      return null;
    }
    return convertFromProtoFormat(p.getLogAggregationStatus());
  }

  @Override
  public void
      setLogAggregationStatus(LogAggregationStatus logAggregationStatus) {
    maybeInitBuilder();
    if (logAggregationStatus == null) {
      builder.clearLogAggregationStatus();
      return;
    }
    builder.setLogAggregationStatus(convertToProtoFormat(logAggregationStatus));
  }

  private LogAggregationStatus convertFromProtoFormat(
      LogAggregationStatusProto s) {
    return LogAggregationStatus.valueOf(s.name().replace(
      LOGAGGREGATION_STATUS_PREFIX, ""));
  }

  private LogAggregationStatusProto
      convertToProtoFormat(LogAggregationStatus s) {
    return LogAggregationStatusProto.valueOf(LOGAGGREGATION_STATUS_PREFIX
        + s.name());
  }

  @Override
  public String getDiagnosticMessage() {
    LogAggregationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnostics()) {
      return null;
    }
    return p.getDiagnostics();
  }

  @Override
  public void setDiagnosticMessage(String diagnosticMessage) {
    maybeInitBuilder();
    if (diagnosticMessage == null) {
      builder.clearDiagnostics();
      return;
    }
    builder.setDiagnostics(diagnosticMessage);
  }

  @Override
  public NodeId getNodeId() {
    if (this.nodeId != null) {
      return this.nodeId;
    }

    LogAggregationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeId()) {
      return null;
    }
    this.nodeId = convertFromProtoFormat(p.getNodeId());
    return this.nodeId;
  }

  @Override
  public void setNodeId(NodeId nodeId) {
    maybeInitBuilder();
    if (nodeId == null)
      builder.clearNodeId();
    this.nodeId = nodeId;
  }

  private NodeIdProto convertToProtoFormat(NodeId t) {
    return ((NodeIdPBImpl) t).getProto();
  }

  private NodeIdPBImpl convertFromProtoFormat(NodeIdProto nodeId) {
    return new NodeIdPBImpl(nodeId);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/NodeHeartbeatRequestPBImpl.java

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringArrayProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.LogAggregationReportProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.LogAggregationReportsForAppsProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
  private MasterKey lastKnownContainerTokenMasterKey = null;
  private MasterKey lastKnownNMTokenMasterKey = null;
  private Set<String> labels = null;
  private Map<ApplicationId, LogAggregationReport>
      logAggregationReportsForApps = null;
  
  public NodeHeartbeatRequestPBImpl() {
    builder = NodeHeartbeatRequestProto.newBuilder();
      builder.setNodeLabels(StringArrayProto.newBuilder()
          .addAllElements(this.labels).build());
    }
    if (this.logAggregationReportsForApps != null) {
      addLogAggregationStatusForAppsToProto();
    }
  }

  private void addLogAggregationStatusForAppsToProto() {
    maybeInitBuilder();
    builder.clearLogAggregationReportsForApps();
    for (Entry<ApplicationId, LogAggregationReport> entry : logAggregationReportsForApps
      .entrySet()) {
      builder.addLogAggregationReportsForApps(LogAggregationReportsForAppsProto
        .newBuilder().setAppId(convertToProtoFormat(entry.getKey()))
        .setLogAggregationReport(convertToProtoFormat(entry.getValue())));
    }
  }

  private LogAggregationReportProto convertToProtoFormat(
      LogAggregationReport value) {
    return ((LogAggregationReportPBImpl) value).getProto();
  }

  private void mergeLocalToProto() {
    StringArrayProto nodeLabels = p.getNodeLabels();
    labels = new HashSet<String>(nodeLabels.getElementsList());
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }

  @Override
  public Map<ApplicationId, LogAggregationReport>
      getLogAggregationReportsForApps() {
    if (this.logAggregationReportsForApps != null) {
      return this.logAggregationReportsForApps;
    }
    initLogAggregationReportsForApps();
    return logAggregationReportsForApps;
  }

  private void initLogAggregationReportsForApps() {
    NodeHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<LogAggregationReportsForAppsProto> list =
        p.getLogAggregationReportsForAppsList();
    this.logAggregationReportsForApps =
        new HashMap<ApplicationId, LogAggregationReport>();
    for (LogAggregationReportsForAppsProto c : list) {
      ApplicationId appId = convertFromProtoFormat(c.getAppId());
      LogAggregationReport report =
          convertFromProtoFormat(c.getLogAggregationReport());
      this.logAggregationReportsForApps.put(appId, report);
    }
  }

  private LogAggregationReport convertFromProtoFormat(
      LogAggregationReportProto logAggregationReport) {
    return new LogAggregationReportPBImpl(logAggregationReport);
  }

  @Override
  public void setLogAggregationReportsForApps(
      Map<ApplicationId, LogAggregationReport> logAggregationStatusForApps) {
    if (logAggregationStatusForApps == null
        || logAggregationStatusForApps.isEmpty()) {
      return;
    }
    maybeInitBuilder();
    this.logAggregationReportsForApps =
        new HashMap<ApplicationId, LogAggregationReport>();
    this.logAggregationReportsForApps.putAll(logAggregationStatusForApps);
  }
}  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/records/LogAggregationStatus.java
hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/AppBlock.java
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
      html.script().$type("text/javascript")._(script.toString())._();
    }

    ResponseInfo overviewTable = info("Application Overview")
      ._("User:", app.getUser())
      ._("Name:", app.getName())
      ._("Application Type:", app.getType())
          .getAppState() == YarnApplicationState.FINISHED
            || app.getAppState() == YarnApplicationState.FAILED
            || app.getAppState() == YarnApplicationState.KILLED ? "History"
            : "ApplicationMaster");
    if (webUiType != null
        && webUiType.equals(YarnWebParams.RM_WEB_UI)) {
      overviewTable._("Log Aggregation Status",
        root_url("logaggregationstatus", app.getAppId()), "Status");
    }
    overviewTable._("Diagnostics:",
        app.getDiagnosticsInfo() == null ? "" : app.getDiagnosticsInfo());

    Collection<ApplicationAttemptReport> attempts;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/Context.java
package org.apache.hadoop.yarn.server.nodemanager;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
  boolean getDecommissioned();

  void setDecommissioned(boolean isDecommissioned);

  ConcurrentLinkedQueue<LogAggregationReport>
      getLogAggregationStatusForApps();
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeManager.java
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
        .getRecordFactory(null).newRecordInstance(NodeHealthStatus.class);
    private final NMStateStoreService stateStore;
    private boolean isDecommissioned = false;
    private final ConcurrentLinkedQueue<LogAggregationReport>
        logAggregationReportForApps;

    public NMContext(NMContainerTokenSecretManager containerTokenSecretManager,
        NMTokenSecretManagerInNM nmTokenSecretManager,
      this.nodeHealthStatus.setHealthReport("Healthy");
      this.nodeHealthStatus.setLastHealthReportTime(System.currentTimeMillis());
      this.stateStore = stateStore;
      this.logAggregationReportForApps = new ConcurrentLinkedQueue<
          LogAggregationReport>();
    }

        Map<ApplicationId, Credentials> systemCredentials) {
      this.systemCredentials = systemCredentials;
    }

    @Override
    public ConcurrentLinkedQueue<LogAggregationReport>
        getLogAggregationStatusForApps() {
      return this.logAggregationReportForApps;
    }
  }



hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeStatusUpdaterImpl.java
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.yarn.server.api.ResourceManagerConstants;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeLabelsProvider;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

import com.google.common.annotations.VisibleForTesting;
  private long durationToTrackStoppedContainers;

  private boolean logAggregationEnabled;

  private final List<LogAggregationReport> logAggregationReportForAppsTempList;

  private final NodeHealthCheckerService healthChecker;
  private final NodeManagerMetrics metrics;

    this.recentlyStoppedContainers = new LinkedHashMap<ContainerId, Long>();
    this.pendingCompletedContainers =
        new HashMap<ContainerId, ContainerStatus>();
    this.logAggregationReportForAppsTempList =
        new ArrayList<LogAggregationReport>();
  }

  @Override
    LOG.info("Initialized nodemanager for " + nodeId + ":" +
        " physical-memory=" + memoryMb + " virtual-memory=" + virtualMemoryMb +
        " virtual-cores=" + virtualCores);

    this.logAggregationEnabled =
        conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
          YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED);
  }

  @Override
                    NodeStatusUpdaterImpl.this.context
                        .getNMTokenSecretManager().getCurrentKey(),
                    nodeLabelsForHeartbeat);

            if (logAggregationEnabled) {
              Map<ApplicationId, LogAggregationReport> logAggregationReports =
                  getLogAggregationReportsForApps(context
                    .getLogAggregationStatusForApps());
              if (logAggregationReports != null
                  && !logAggregationReports.isEmpty()) {
                request.setLogAggregationReportsForApps(logAggregationReports);
              }
            }

            response = resourceTracker.nodeHeartbeat(request);
            nextHeartBeatInterval = response.getNextHeartBeatInterval();
            removeOrTrackCompletedContainersFromContext(response
                  .getContainersToBeRemovedFromNM());

            logAggregationReportForAppsTempList.clear();
            lastHeartbeatID = response.getResponseId();
            List<ContainerId> containersToCleanup = response
                .getContainersToCleanup();
    statusUpdater.start();
  }

  private Map<ApplicationId, LogAggregationReport>
      getLogAggregationReportsForApps(
          ConcurrentLinkedQueue<LogAggregationReport> lastestLogAggregationStatus) {
    Map<ApplicationId, LogAggregationReport> latestLogAggregationReports =
        new HashMap<ApplicationId, LogAggregationReport>();
    LogAggregationReport status;
    while ((status = lastestLogAggregationStatus.poll()) != null) {
      this.logAggregationReportForAppsTempList.add(status);
    }
    for (LogAggregationReport logAggregationReport
        : this.logAggregationReportForAppsTempList) {
      LogAggregationReport report = null;
      if (latestLogAggregationReports.containsKey(logAggregationReport
        .getApplicationId())) {
        report =
            latestLogAggregationReports.get(logAggregationReport
              .getApplicationId());
        report.setLogAggregationStatus(logAggregationReport
          .getLogAggregationStatus());
        String message = report.getDiagnosticMessage();
        if (logAggregationReport.getDiagnosticMessage() != null
            && !logAggregationReport.getDiagnosticMessage().isEmpty()) {
          if (message != null) {
            message += logAggregationReport.getDiagnosticMessage();
          } else {
            message = logAggregationReport.getDiagnosticMessage();
          }
          report.setDiagnosticMessage(message);
        }
      } else {
        report = Records.newRecord(LogAggregationReport.class);
        report.setApplicationId(logAggregationReport.getApplicationId());
        report.setNodeId(this.nodeId);
        report.setLogAggregationStatus(logAggregationReport
          .getLogAggregationStatus());
        report
          .setDiagnosticMessage(logAggregationReport.getDiagnosticMessage());
      }
      latestLogAggregationReports.put(logAggregationReport.getApplicationId(),
        report);
    }
    return latestLogAggregationReports;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/logaggregation/AppLogAggregatorImpl.java
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogWriter;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRetentionPolicy;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.Times;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
  private final AtomicBoolean waiting = new AtomicBoolean(false);

  private boolean renameTemporaryLogFileFailed = false;

  private final Map<ContainerId, ContainerLogAggregator> containerLogAggregators =
      new HashMap<ContainerId, ContainerLogAggregator>();

        writer.close();
      }

      long currentTime = System.currentTimeMillis();
      final Path renamedPath = this.rollingMonitorInterval <= 0
              ? remoteNodeLogFileForApp : new Path(
                remoteNodeLogFileForApp.getParent(),
                remoteNodeLogFileForApp.getName() + "_"
                    + currentTime);

      String diagnosticMessage = "";
      final boolean rename = uploadedLogsInThisCycle;
      try {
        userUgi.doAs(new PrivilegedExceptionAction<Object>() {
            return null;
          }
        });
        diagnosticMessage =
            "Log uploaded successfully for Application: " + appId
                + " in NodeManager: "
                + LogAggregationUtils.getNodeString(nodeId) + " at "
                + Times.format(currentTime) + "\n";
      } catch (Exception e) {
        LOG.error(
          "Failed to move temporary log file to final location: ["
              + remoteNodeTmpLogFileForApp + "] to ["
              + renamedPath + "]", e);
        diagnosticMessage =
            "Log uploaded failed for Application: " + appId
                + " in NodeManager: "
                + LogAggregationUtils.getNodeString(nodeId) + " at "
                + Times.format(currentTime) + "\n";
        renameTemporaryLogFileFailed = true;
      }

      LogAggregationReport report =
          Records.newRecord(LogAggregationReport.class);
      report.setApplicationId(appId);
      report.setNodeId(nodeId);
      report.setDiagnosticMessage(diagnosticMessage);
      if (appFinished) {
        report.setLogAggregationStatus(renameTemporaryLogFileFailed
            ? LogAggregationStatus.FAILED : LogAggregationStatus.FINISHED);
      } else {
        report.setLogAggregationStatus(LogAggregationStatus.RUNNING);
      }
      this.context.getLogAggregationStatusForApps().add(report);
    } finally {
      if (writer != null) {
        writer.close();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ResourceTrackerService.java
    }

    RMNodeStatusEvent nodeStatusEvent =
        new RMNodeStatusEvent(nodeId, remoteNodeStatus.getNodeHealthStatus(),
          remoteNodeStatus.getContainersStatuses(),
          remoteNodeStatus.getKeepAliveApplications(), nodeHeartBeatResponse);
    if (request.getLogAggregationReportsForApps() != null
        && !request.getLogAggregationReportsForApps().isEmpty()) {
      nodeStatusEvent.setLogAggregationReportsForApps(request
        .getLogAggregationReportsForApps());
    }
    this.rmContext.getDispatcher().getEventHandler().handle(nodeStatusEvent);

    if (isDistributesNodeLabelsConf && request.getNodeLabels() != null) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/RMApp.java
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

  ReservationId getReservationId();
  
  ResourceRequest getAMResourceRequest();

  Map<NodeId, LogAggregationReport> getLogAggregationReportsForApp();
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/RMAppImpl.java
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEventType;
      new AppFinishedTransition();
  private Set<NodeId> ranNodes = new ConcurrentSkipListSet<NodeId>();

  private final boolean logAggregationEnabled;
  private long logAggregationStartTime = 0;
  private final long logAggregationStatusTimeout;
  private final Map<NodeId, LogAggregationReport> logAggregationStatus =
      new HashMap<NodeId, LogAggregationReport>();

  private RMAppState stateBeforeKilling;
  private RMAppState stateBeforeFinalSaving;

    rmContext.getRMApplicationHistoryWriter().applicationStarted(this);
    rmContext.getSystemMetricsPublisher().appCreated(this, startTime);

    long localLogAggregationStatusTimeout =
        conf.getLong(YarnConfiguration.LOG_AGGREGATION_STATUS_TIME_OUT_MS,
          YarnConfiguration.DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS);
    if (localLogAggregationStatusTimeout <= 0) {
      this.logAggregationStatusTimeout =
          YarnConfiguration.DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS;
    } else {
      this.logAggregationStatusTimeout = localLogAggregationStatusTimeout;
    }
    this.logAggregationEnabled =
        conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
          YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED);
  }

  @Override
      
      app.ranNodes.add(nodeAddedEvent.getNodeId());

      app.logAggregationStatus.put(nodeAddedEvent.getNodeId(),
        LogAggregationReport.newInstance(app.applicationId, nodeAddedEvent
          .getNodeId(), app.logAggregationEnabled
            ? LogAggregationStatus.NOT_START : LogAggregationStatus.DISABLED,
          ""));
    };
  }

    }

    public void transition(RMAppImpl app, RMAppEvent event) {
      app.logAggregationStartTime = System.currentTimeMillis();
      for (NodeId nodeId : app.getRanNodes()) {
        app.handler.handle(
            new RMNodeCleanAppEvent(nodeId, app.applicationId));
    }
    return credentials;
  }

  @Override
  public Map<NodeId, LogAggregationReport> getLogAggregationReportsForApp() {
    try {
      this.readLock.lock();
      Map<NodeId, LogAggregationReport> outputs =
          new HashMap<NodeId, LogAggregationReport>();
      outputs.putAll(logAggregationStatus);
      for (Entry<NodeId, LogAggregationReport> output : outputs.entrySet()) {
        if (!output.getValue().getLogAggregationStatus()
          .equals(LogAggregationStatus.TIME_OUT)
            && !output.getValue().getLogAggregationStatus()
              .equals(LogAggregationStatus.FINISHED)
            && isAppInFinalState(this)
            && System.currentTimeMillis() > this.logAggregationStartTime
                + this.logAggregationStatusTimeout) {
          output.getValue().setLogAggregationStatus(
            LogAggregationStatus.TIME_OUT);
        }
      }
      return outputs;
    } finally {
      this.readLock.unlock();
    }
  }

  public void aggregateLogReport(NodeId nodeId, LogAggregationReport report) {
    try {
      this.writeLock.lock();
      if (this.logAggregationEnabled) {
        LogAggregationReport curReport = this.logAggregationStatus.get(nodeId);
        if (curReport == null) {
          this.logAggregationStatus.put(nodeId, report);
        } else {
          if (curReport.getLogAggregationStatus().equals(
            LogAggregationStatus.TIME_OUT)) {
            if (report.getLogAggregationStatus().equals(
              LogAggregationStatus.FINISHED)) {
              curReport.setLogAggregationStatus(report
                .getLogAggregationStatus());
            }
          } else {
            curReport.setLogAggregationStatus(report.getLogAggregationStatus());
          }

          if (report.getDiagnosticMessage() != null
              && !report.getDiagnosticMessage().isEmpty()) {
            curReport
              .setDiagnosticMessage(curReport.getDiagnosticMessage() == null
                  ? report.getDiagnosticMessage() : curReport
                    .getDiagnosticMessage() + report.getDiagnosticMessage());
          }
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmnode/RMNodeImpl.java
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRunningOnNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;

      rmNode.handleContainerStatus(statusEvent.getContainers());

      Map<ApplicationId, LogAggregationReport> logAggregationReportsForApps =
          statusEvent.getLogAggregationReportsForApps();
      if (logAggregationReportsForApps != null
          && !logAggregationReportsForApps.isEmpty()) {
        rmNode.handleLogAggregationStatus(logAggregationReportsForApps);
      }

      if(rmNode.nextHeartBeat) {
        rmNode.nextHeartBeat = false;
        rmNode.context.getDispatcher().getEventHandler().handle(
    }
  }

  private void handleLogAggregationStatus(
      Map<ApplicationId, LogAggregationReport> logAggregationReportsForApps) {
    for (Entry<ApplicationId, LogAggregationReport> report :
        logAggregationReportsForApps.entrySet()) {
      RMApp rmApp = this.context.getRMApps().get(report.getKey());
      if (rmApp != null) {
        ((RMAppImpl)rmApp).aggregateLogReport(this.nodeId, report.getValue());
      }
    }
  }

 }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmnode/RMNodeStatusEvent.java
package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;

  private final List<ContainerStatus> containersCollection;
  private final NodeHeartbeatResponse latestResponse;
  private final List<ApplicationId> keepAliveAppIds;
  private Map<ApplicationId, LogAggregationReport> logAggregationReportsForApps;

  public RMNodeStatusEvent(NodeId nodeId, NodeHealthStatus nodeHealthStatus,
      List<ContainerStatus> collection, List<ApplicationId> keepAliveAppIds,
    this.containersCollection = collection;
    this.keepAliveAppIds = keepAliveAppIds;
    this.latestResponse = latestResponse;
    this.logAggregationReportsForApps = null;
  }

  public RMNodeStatusEvent(NodeId nodeId, NodeHealthStatus nodeHealthStatus,
      List<ContainerStatus> collection, List<ApplicationId> keepAliveAppIds,
      NodeHeartbeatResponse latestResponse,
      Map<ApplicationId, LogAggregationReport> logAggregationReportsForApps) {
    super(nodeId, RMNodeEventType.STATUS_UPDATE);
    this.nodeHealthStatus = nodeHealthStatus;
    this.containersCollection = collection;
    this.keepAliveAppIds = keepAliveAppIds;
    this.latestResponse = latestResponse;
    this.logAggregationReportsForApps = logAggregationReportsForApps;
  }

  public NodeHealthStatus getNodeHealthStatus() {
  public List<ApplicationId> getKeepAliveAppIds() {
    return this.keepAliveAppIds;
  }

  public Map<ApplicationId, LogAggregationReport>
      getLogAggregationReportsForApps() {
    return this.logAggregationReportsForApps;
  }

  public void setLogAggregationReportsForApps(
      Map<ApplicationId, LogAggregationReport> logAggregationReportsForApps) {
    this.logAggregationReportsForApps = logAggregationReportsForApps;
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/AppLogAggregationStatusPage.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/AppLogAggregationStatusPage.java

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

public class AppLogAggregationStatusPage extends RmView{

  @Override
  protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    String appId = $(YarnWebParams.APPLICATION_ID);
    set(
      TITLE,
      appId.isEmpty() ? "Bad request: missing application ID" : join(
        "Application ", $(YarnWebParams.APPLICATION_ID)));
  }

  @Override
  protected Class<? extends SubView> content() {
    return RMAppLogAggregationStatusBlock.class;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppLogAggregationStatusBlock.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppLogAggregationStatusBlock.java

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APPLICATION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

public class RMAppLogAggregationStatusBlock extends HtmlBlock {

  private static final Log LOG = LogFactory
    .getLog(RMAppLogAggregationStatusBlock.class);
  private final ResourceManager rm;
  private final Configuration conf;

  @Inject
  RMAppLogAggregationStatusBlock(ViewContext ctx, ResourceManager rm,
      Configuration conf) {
    super(ctx);
    this.rm = rm;
    this.conf = conf;
  }

  @Override
  protected void render(Block html) {
    String aid = $(APPLICATION_ID);
    if (aid.isEmpty()) {
      puts("Bad request: requires Application ID");
      return;
    }

    ApplicationId appId;
    try {
      appId = Apps.toAppID(aid);
    } catch (Exception e) {
      puts("Invalid Application ID: " + aid);
      return;
    }

    setTitle(join("Application ", aid));

    DIV<Hamlet> div_description = html.div(_INFO_WRAP);
    TABLE<DIV<Hamlet>> table_description =
        div_description.table("#LogAggregationStatusDecription");
    table_description.
      tr().
        th(_TH, "Log Aggregation Status").
        th(_TH, "Description").
      _();
    table_description.tr().td(LogAggregationStatus.DISABLED.name())
      .td("Log Aggregation is Disabled.")._();
    table_description.tr().td(LogAggregationStatus.NOT_START.name())
      .td("Log Aggregation does not Start.")._();
    table_description.tr().td(LogAggregationStatus.RUNNING.name())
      .td("Log Aggregation is Running.")._();
    table_description.tr().td(LogAggregationStatus.FINISHED.name())
      .td("Log Aggregation is Finished. All of the logs have been "
          + "aggregated successfully.")._();
    table_description.tr().td(LogAggregationStatus.FAILED.name())
      .td("Log Aggregation is Failed. At least one of the logs "
          + "have not been aggregated.")._();
    table_description.tr().td(LogAggregationStatus.TIME_OUT.name())
      .td("Does not get the Log aggregation status for a long time. "
          + "Not sure what is the current Log Aggregation Status.")._();
    table_description._();
    div_description._();

    boolean logAggregationEnabled =
        conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
          YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED);
    DIV<Hamlet> div = html.div(_INFO_WRAP);
    TABLE<DIV<Hamlet>> table =
        div.h3(
          "Log Aggregation: "
              + (logAggregationEnabled ? "Enabled" : "Disabled")).table(
          "#LogAggregationStatus");
    table.
      tr().
        th(_TH, "NodeId").
        th(_TH, "Log Aggregation Status").
        th(_TH, "Diagnostis Message").
      _();

    RMApp rmApp = rm.getRMContext().getRMApps().get(appId);
    if (rmApp != null) {
      Map<NodeId, LogAggregationReport> logAggregationReports =
          rmApp.getLogAggregationReportsForApp();
      if (logAggregationReports != null && !logAggregationReports.isEmpty()) {
        for (Entry<NodeId, LogAggregationReport> report :
            logAggregationReports.entrySet()) {
          LogAggregationStatus status =
              report.getValue() == null ? null : report.getValue()
                .getLogAggregationStatus();
          String message =
              report.getValue() == null ? null : report.getValue()
                .getDiagnosticMessage();
          table.tr()
            .td(report.getKey().toString())
            .td(status == null ? "N/A" : status.toString())
            .td(message == null ? "N/A" : message)._();
        }
      }
    }
    table._();
    div._();
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMWebApp.java
      "appattempt");
    route(pajoin("/container", CONTAINER_ID), RmController.class, "container");
    route("/errors-and-warnings", RmController.class, "errorsAndWarnings");
    route(pajoin("/logaggregationstatus", APPLICATION_ID),
      RmController.class, "logaggregationstatus");
  }

  @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RmController.java
  public void errorsAndWarnings() {
    render(RMErrorsAndWarningsPage.class);
  }

  public void logaggregationstatus() {
    render(AppLogAggregationStatusPage.class);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/applicationsmanager/MockAsm.java
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
    public ResourceRequest getAMResourceRequest() {
      return this.amReq; 
    }

    @Override
    public Map<NodeId, LogAggregationReport> getLogAggregationReportsForApp() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  public static RMApp newApplication(int i) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/logaggregationstatus/TestRMAppLogAggregationStatus.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/logaggregationstatus/TestRMAppLogAggregationStatus.java

package org.apache.hadoop.yarn.server.resourcemanager.logaggregationstatus;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRunningOnNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStartedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestRMAppLogAggregationStatus {

  private RMContext rmContext;
  private YarnScheduler scheduler;

  private SchedulerEventType eventType;

  private ApplicationId appId;


  private final class TestSchedulerEventDispatcher implements
  EventHandler<SchedulerEvent> {
    @Override
    public void handle(SchedulerEvent event) {
      scheduler.handle(event);
    }
  }

  @Before
  public void setUp() throws Exception {
    InlineDispatcher rmDispatcher = new InlineDispatcher();

    rmContext =
        new RMContextImpl(rmDispatcher, null, null, null,
          null, null, null, null, null,
          new RMApplicationHistoryWriter());
    rmContext.setSystemMetricsPublisher(new SystemMetricsPublisher());

    scheduler = mock(YarnScheduler.class);
    doAnswer(
        new Answer<Void>() {

          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            final SchedulerEvent event = (SchedulerEvent)(invocation.getArguments()[0]);
            eventType = event.getType();
            if (eventType == SchedulerEventType.NODE_UPDATE) {
            }
            return null;
          }
        }
        ).when(scheduler).handle(any(SchedulerEvent.class));

    rmDispatcher.register(SchedulerEventType.class,
        new TestSchedulerEventDispatcher());

    appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testLogAggregationStatus() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    conf.setLong(YarnConfiguration.LOG_AGGREGATION_STATUS_TIME_OUT_MS, 1500);
    RMApp rmApp = createRMApp(conf);
    this.rmContext.getRMApps().put(appId, rmApp);
    rmApp.handle(new RMAppEvent(this.appId, RMAppEventType.START));
    rmApp.handle(new RMAppEvent(this.appId, RMAppEventType.APP_NEW_SAVED));
    rmApp.handle(new RMAppEvent(this.appId, RMAppEventType.APP_ACCEPTED));

    NodeId nodeId1 = NodeId.newInstance("localhost", 1234);
    Resource capability = Resource.newInstance(4096, 4);
    RMNodeImpl node1 =
        new RMNodeImpl(nodeId1, rmContext, null, 0, 0, null, capability, null);
    node1.handle(new RMNodeStartedEvent(nodeId1, null, null));
    rmApp.handle(new RMAppRunningOnNodeEvent(this.appId, nodeId1));

    NodeId nodeId2 = NodeId.newInstance("localhost", 2345);
    RMNodeImpl node2 =
        new RMNodeImpl(nodeId2, rmContext, null, 0, 0, null, capability, null);
    node2.handle(new RMNodeStartedEvent(node2.getNodeID(), null, null));
    rmApp.handle(new RMAppRunningOnNodeEvent(this.appId, nodeId2));

    Map<NodeId, LogAggregationReport> logAggregationStatus =
        rmApp.getLogAggregationReportsForApp();
    Assert.assertEquals(2, logAggregationStatus.size());
    Assert.assertTrue(logAggregationStatus.containsKey(nodeId1));
    Assert.assertTrue(logAggregationStatus.containsKey(nodeId2));
    for (Entry<NodeId, LogAggregationReport> report : logAggregationStatus
      .entrySet()) {
      Assert.assertEquals(LogAggregationStatus.NOT_START, report.getValue()
        .getLogAggregationStatus());
    }

    Map<ApplicationId, LogAggregationReport> node1ReportForApp =
        new HashMap<ApplicationId, LogAggregationReport>();
    String messageForNode1_1 =
        "node1 logAggregation status updated at " + System.currentTimeMillis();
    LogAggregationReport report1 =
        LogAggregationReport.newInstance(appId, nodeId1,
          LogAggregationStatus.RUNNING, messageForNode1_1);
    node1ReportForApp.put(appId, report1);
    node1.handle(new RMNodeStatusEvent(node1.getNodeID(), NodeHealthStatus
      .newInstance(true, null, 0), new ArrayList<ContainerStatus>(), null,
      null, node1ReportForApp));

    Map<ApplicationId, LogAggregationReport> node2ReportForApp =
        new HashMap<ApplicationId, LogAggregationReport>();
    String messageForNode2_1 =
        "node2 logAggregation status updated at " + System.currentTimeMillis();
    LogAggregationReport report2 =
        LogAggregationReport.newInstance(appId, nodeId2,
          LogAggregationStatus.RUNNING, messageForNode2_1);
    node2ReportForApp.put(appId, report2);
    node2.handle(new RMNodeStatusEvent(node2.getNodeID(), NodeHealthStatus
      .newInstance(true, null, 0), new ArrayList<ContainerStatus>(), null,
      null, node2ReportForApp));
    logAggregationStatus = rmApp.getLogAggregationReportsForApp();
    Assert.assertEquals(2, logAggregationStatus.size());
    Assert.assertTrue(logAggregationStatus.containsKey(nodeId1));
    Assert.assertTrue(logAggregationStatus.containsKey(nodeId2));
    for (Entry<NodeId, LogAggregationReport> report : logAggregationStatus
      .entrySet()) {
      if (report.getKey().equals(node1.getNodeID())) {
        Assert.assertEquals(LogAggregationStatus.RUNNING, report.getValue()
          .getLogAggregationStatus());
        Assert.assertEquals(messageForNode1_1, report.getValue()
          .getDiagnosticMessage());
      } else if (report.getKey().equals(node2.getNodeID())) {
        Assert.assertEquals(LogAggregationStatus.RUNNING, report.getValue()
          .getLogAggregationStatus());
        Assert.assertEquals(messageForNode2_1, report.getValue()
          .getDiagnosticMessage());
      } else {
        Assert
          .fail("should not contain log aggregation report for other nodes");
      }
    }

    Map<ApplicationId, LogAggregationReport> node1ReportForApp2 =
        new HashMap<ApplicationId, LogAggregationReport>();
    String messageForNode1_2 =
        "node1 logAggregation status updated at " + System.currentTimeMillis();
    LogAggregationReport report1_2 =
        LogAggregationReport.newInstance(appId, nodeId1,
          LogAggregationStatus.RUNNING, messageForNode1_2);
    node1ReportForApp2.put(appId, report1_2);
    node1.handle(new RMNodeStatusEvent(node1.getNodeID(), NodeHealthStatus
      .newInstance(true, null, 0), new ArrayList<ContainerStatus>(), null,
      null, node1ReportForApp2));

    logAggregationStatus = rmApp.getLogAggregationReportsForApp();
    Assert.assertEquals(2, logAggregationStatus.size());
    Assert.assertTrue(logAggregationStatus.containsKey(nodeId1));
    Assert.assertTrue(logAggregationStatus.containsKey(nodeId2));
    for (Entry<NodeId, LogAggregationReport> report : logAggregationStatus
      .entrySet()) {
      if (report.getKey().equals(node1.getNodeID())) {
        Assert.assertEquals(LogAggregationStatus.RUNNING, report.getValue()
          .getLogAggregationStatus());
        Assert.assertEquals(messageForNode1_1 + messageForNode1_2, report
          .getValue().getDiagnosticMessage());
      } else if (report.getKey().equals(node2.getNodeID())) {
        Assert.assertEquals(LogAggregationStatus.RUNNING, report.getValue()
          .getLogAggregationStatus());
        Assert.assertEquals(messageForNode2_1, report.getValue()
          .getDiagnosticMessage());
      } else {
        Assert
          .fail("should not contain log aggregation report for other nodes");
      }
    }

    rmApp.handle(new RMAppEvent(appId, RMAppEventType.KILL));
    rmApp.handle(new RMAppEvent(appId, RMAppEventType.ATTEMPT_KILLED));
    rmApp.handle(new RMAppEvent(appId, RMAppEventType.APP_UPDATE_SAVED));
    Assert.assertEquals(RMAppState.KILLED, rmApp.getState());

    Thread.sleep(1500);

    logAggregationStatus = rmApp.getLogAggregationReportsForApp();
    Assert.assertEquals(2, logAggregationStatus.size());
    Assert.assertTrue(logAggregationStatus.containsKey(nodeId1));
    Assert.assertTrue(logAggregationStatus.containsKey(nodeId2));
    for (Entry<NodeId, LogAggregationReport> report : logAggregationStatus
      .entrySet()) {
      Assert.assertEquals(LogAggregationStatus.TIME_OUT, report.getValue()
        .getLogAggregationStatus());
    }

    Map<ApplicationId, LogAggregationReport> node1ReportForApp3 =
        new HashMap<ApplicationId, LogAggregationReport>();
    String messageForNode1_3 =
        "node1 final logAggregation status updated at "
            + System.currentTimeMillis();
    LogAggregationReport report1_3 =
        LogAggregationReport.newInstance(appId, nodeId1,
          LogAggregationStatus.FINISHED, messageForNode1_3);
    node1ReportForApp3.put(appId, report1_3);
    node1.handle(new RMNodeStatusEvent(node1.getNodeID(), NodeHealthStatus
      .newInstance(true, null, 0), new ArrayList<ContainerStatus>(), null,
      null, node1ReportForApp3));

    logAggregationStatus = rmApp.getLogAggregationReportsForApp();
    Assert.assertEquals(2, logAggregationStatus.size());
    Assert.assertTrue(logAggregationStatus.containsKey(nodeId1));
    Assert.assertTrue(logAggregationStatus.containsKey(nodeId2));
    for (Entry<NodeId, LogAggregationReport> report : logAggregationStatus
      .entrySet()) {
      if (report.getKey().equals(node1.getNodeID())) {
        Assert.assertEquals(LogAggregationStatus.FINISHED, report.getValue()
          .getLogAggregationStatus());
        Assert.assertEquals(messageForNode1_1 + messageForNode1_2
            + messageForNode1_3, report.getValue().getDiagnosticMessage());
      } else if (report.getKey().equals(node2.getNodeID())) {
        Assert.assertEquals(LogAggregationStatus.TIME_OUT, report.getValue()
          .getLogAggregationStatus());
      } else {
        Assert
          .fail("should not contain log aggregation report for other nodes");
      }
    }
  }

  private RMApp createRMApp(Configuration conf) {
    ApplicationSubmissionContext submissionContext =
        ApplicationSubmissionContext.newInstance(appId, "test", "default",
          Priority.newInstance(0), null, false, true,
          2, Resource.newInstance(10, 2), "test");
    return new RMAppImpl(this.appId, this.rmContext,
      conf, "test", "test", "default", submissionContext,
      this.rmContext.getScheduler(),
      this.rmContext.getApplicationMasterService(),
      System.currentTimeMillis(), "test",
      null, null);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/MockRMApp.java
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

  public ResourceRequest getAMResourceRequest() {
    return this.amReq; 
  }

  @Override
  public Map<NodeId, LogAggregationReport> getLogAggregationReportsForApp() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}

