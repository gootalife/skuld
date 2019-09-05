hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/LogAggregationStatus.java
  RUNNING,

  RUNNING_WITH_FAILURE,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
      + "proxy-user-privileges.enabled";
  public static boolean DEFAULT_RM_PROXY_USER_PRIVILEGES_ENABLED = false;

  public static final String RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY =
      RM_PREFIX + "max-log-aggregation-diagnostics-in-memory";
  public static final int DEFAULT_RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY =
      10;

  public static final String LOG_AGGREGATION_ENABLED = YARN_PREFIX
      + "log-aggregation-enable";

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/LogAggregationReport.java
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.util.Records;

  @Public
  @Unstable
  public static LogAggregationReport newInstance(ApplicationId appId,
      LogAggregationStatus status, String diagnosticMessage) {
    LogAggregationReport report = Records.newRecord(LogAggregationReport.class);
    report.setApplicationId(appId);
    report.setLogAggregationStatus(status);
  @Unstable
  public abstract void setApplicationId(ApplicationId appId);


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/NodeHeartbeatRequest.java

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.util.Records;
  public abstract Set<String> getNodeLabels();
  public abstract void setNodeLabels(Set<String> nodeLabels);

  public abstract List<LogAggregationReport>
      getLogAggregationReportsForApps();

  public abstract void setLogAggregationReportsForApps(
      List<LogAggregationReport> logAggregationReportsForApps);
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/LogAggregationReportPBImpl.java
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LogAggregationStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.LogAggregationReportProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.LogAggregationReportProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
  boolean viaProto = false;

  private ApplicationId applicationId;

  public LogAggregationReportPBImpl() {
    builder = LogAggregationReportProto.newBuilder();
          builder.getApplicationId())) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
  }

  private void mergeLocalToProto() {
    }
    builder.setDiagnostics(diagnosticMessage);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/NodeHeartbeatRequestPBImpl.java

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.proto.YarnProtos.StringArrayProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.LogAggregationReportProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
  private MasterKey lastKnownContainerTokenMasterKey = null;
  private MasterKey lastKnownNMTokenMasterKey = null;
  private Set<String> labels = null;
  private List<LogAggregationReport> logAggregationReportsForApps = null;

  public NodeHeartbeatRequestPBImpl() {
    builder = NodeHeartbeatRequestProto.newBuilder();
  private void addLogAggregationStatusForAppsToProto() {
    maybeInitBuilder();
    builder.clearLogAggregationReportsForApps();
    if (this.logAggregationReportsForApps == null) {
      return;
    }
    Iterable<LogAggregationReportProto> it =
        new Iterable<LogAggregationReportProto>() {
          @Override
          public Iterator<LogAggregationReportProto> iterator() {
            return new Iterator<LogAggregationReportProto>() {
              private Iterator<LogAggregationReport> iter =
                  logAggregationReportsForApps.iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public LogAggregationReportProto next() {
                return convertToProtoFormat(iter.next());
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        };
    builder.addAllLogAggregationReportsForApps(it);
  }

  private LogAggregationReportProto convertToProtoFormat(
    labels = new HashSet<String>(nodeLabels.getElementsList());
  }

  @Override
  public List<LogAggregationReport> getLogAggregationReportsForApps() {
    if (this.logAggregationReportsForApps != null) {
      return this.logAggregationReportsForApps;
    }

  private void initLogAggregationReportsForApps() {
    NodeHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<LogAggregationReportProto> list =
        p.getLogAggregationReportsForAppsList();
    this.logAggregationReportsForApps = new ArrayList<LogAggregationReport>();
    for (LogAggregationReportProto c : list) {
      this.logAggregationReportsForApps.add(convertFromProtoFormat(c));
    }
  }


  @Override
  public void setLogAggregationReportsForApps(
      List<LogAggregationReport> logAggregationStatusForApps) {
    if(logAggregationStatusForApps == null) {
      builder.clearLogAggregationReportsForApps();
    }
    this.logAggregationReportsForApps = logAggregationStatusForApps;
  }
}  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/AppBlock.java
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
            : "ApplicationMaster");
    if (webUiType != null
        && webUiType.equals(YarnWebParams.RM_WEB_UI)) {
      LogAggregationStatus status = getLogAggregationStatus();
      if (status == null) {
        overviewTable._("Log Aggregation Status", "N/A");
      } else if (status == LogAggregationStatus.DISABLED
          || status == LogAggregationStatus.NOT_START
          || status == LogAggregationStatus.SUCCEEDED) {
        overviewTable._("Log Aggregation Status", status.name());
      } else {
        overviewTable._("Log Aggregation Status",
            root_url("logaggregationstatus", app.getAppId()), status.name());
      }
    }
    overviewTable._("Diagnostics:",
        app.getDiagnosticsInfo() == null ? "" : app.getDiagnosticsInfo());
  protected void createApplicationMetricsTable(Block html) {

  }

  protected LogAggregationStatus getLogAggregationStatus() {
    return null;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeStatusUpdaterImpl.java
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeLabelsProvider;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

import com.google.common.annotations.VisibleForTesting;

            if (logAggregationEnabled) {
              List<LogAggregationReport> logAggregationReports =
                  getLogAggregationReportsForApps(context
                    .getLogAggregationStatusForApps());
              if (logAggregationReports != null
    statusUpdater.start();
  }

  private List<LogAggregationReport> getLogAggregationReportsForApps(
      ConcurrentLinkedQueue<LogAggregationReport> lastestLogAggregationStatus) {
    LogAggregationReport status;
    while ((status = lastestLogAggregationStatus.poll()) != null) {
      this.logAggregationReportForAppsTempList.add(status);
    }
    List<LogAggregationReport> reports = new ArrayList<LogAggregationReport>();
    reports.addAll(logAggregationReportForAppsTempList);
    return reports;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/logaggregation/AppLogAggregatorImpl.java
                    + currentTime);

      String diagnosticMessage = "";
      boolean logAggregationSucceedInThisCycle = true;
      final boolean rename = uploadedLogsInThisCycle;
      try {
        userUgi.doAs(new PrivilegedExceptionAction<Object>() {
                + LogAggregationUtils.getNodeString(nodeId) + " at "
                + Times.format(currentTime) + "\n";
        renameTemporaryLogFileFailed = true;
        logAggregationSucceedInThisCycle = false;
      }

      LogAggregationReport report =
          Records.newRecord(LogAggregationReport.class);
      report.setApplicationId(appId);
      report.setDiagnosticMessage(diagnosticMessage);
      report.setLogAggregationStatus(logAggregationSucceedInThisCycle
          ? LogAggregationStatus.RUNNING
          : LogAggregationStatus.RUNNING_WITH_FAILURE);
      this.context.getLogAggregationStatusForApps().add(report);
      if (appFinished) {
        LogAggregationReport finalReport =
            Records.newRecord(LogAggregationReport.class);
        finalReport.setApplicationId(appId);
        finalReport.setLogAggregationStatus(renameTemporaryLogFileFailed
            ? LogAggregationStatus.FAILED : LogAggregationStatus.SUCCEEDED);
        this.context.getLogAggregationStatusForApps().add(report);
      }
    } finally {
      if (writer != null) {
        writer.close();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmapp/RMAppImpl.java
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
  private final Map<NodeId, LogAggregationReport> logAggregationStatus =
      new HashMap<NodeId, LogAggregationReport>();
  private LogAggregationStatus logAggregationStatusForAppReport;
  private int logAggregationSucceed = 0;
  private int logAggregationFailed = 0;
  private Map<NodeId, List<String>> logAggregationDiagnosticsForNMs =
      new HashMap<NodeId, List<String>>();
  private Map<NodeId, List<String>> logAggregationFailureMessagesForNMs =
      new HashMap<NodeId, List<String>>();
  private final int maxLogAggregationDiagnosticsInMemory;

  private RMAppState stateBeforeKilling;
    this.logAggregationEnabled =
        conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
          YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED);
    if (this.logAggregationEnabled) {
      this.logAggregationStatusForAppReport = LogAggregationStatus.NOT_START;
    } else {
      this.logAggregationStatusForAppReport = LogAggregationStatus.DISABLED;
    }
    maxLogAggregationDiagnosticsInMemory = conf.getInt(
        YarnConfiguration.RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY,
        YarnConfiguration.DEFAULT_RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY);
  }

  @Override

      if (!app.logAggregationStatus.containsKey(nodeAddedEvent.getNodeId())) {
        app.logAggregationStatus.put(nodeAddedEvent.getNodeId(),
          LogAggregationReport.newInstance(app.applicationId,
            app.logAggregationEnabled ? LogAggregationStatus.NOT_START
                : LogAggregationStatus.DISABLED, ""));
      }
    };
  }
      Map<NodeId, LogAggregationReport> outputs =
          new HashMap<NodeId, LogAggregationReport>();
      outputs.putAll(logAggregationStatus);
      if (!isLogAggregationFinished()) {
        for (Entry<NodeId, LogAggregationReport> output : outputs.entrySet()) {
          if (!output.getValue().getLogAggregationStatus()
            .equals(LogAggregationStatus.TIME_OUT)
              LogAggregationStatus.TIME_OUT);
          }
        }
      }
      return outputs;
    } finally {
      this.readLock.unlock();
  public void aggregateLogReport(NodeId nodeId, LogAggregationReport report) {
    try {
      this.writeLock.lock();
      if (this.logAggregationEnabled && !isLogAggregationFinished()) {
        LogAggregationReport curReport = this.logAggregationStatus.get(nodeId);
        boolean stateChangedToFinal = false;
        if (curReport == null) {
          this.logAggregationStatus.put(nodeId, report);
          if (isLogAggregationFinishedForNM(report)) {
            stateChangedToFinal = true;
          }
        } else {
          if (isLogAggregationFinishedForNM(report)) {
            if (!isLogAggregationFinishedForNM(curReport)) {
              stateChangedToFinal = true;
            }
          }
          if (report.getLogAggregationStatus() != LogAggregationStatus.RUNNING
              || curReport.getLogAggregationStatus() !=
                  LogAggregationStatus.RUNNING_WITH_FAILURE) {
            if (curReport.getLogAggregationStatus()
                == LogAggregationStatus.TIME_OUT
                && report.getLogAggregationStatus()
                    == LogAggregationStatus.RUNNING) {
              if (logAggregationFailureMessagesForNMs.get(nodeId) != null &&
                  !logAggregationFailureMessagesForNMs.get(nodeId).isEmpty()) {
                report.setLogAggregationStatus(
                    LogAggregationStatus.RUNNING_WITH_FAILURE);
              }
            }
            curReport.setLogAggregationStatus(report
              .getLogAggregationStatus());
          }
        }
        updateLogAggregationDiagnosticMessages(nodeId, report);
        if (isAppInFinalState(this) && stateChangedToFinal) {
          updateLogAggregationStatus(nodeId);
        }
      }
    } finally {

  @Override
  public LogAggregationStatus getLogAggregationStatusForAppReport() {
    try {
      this.readLock.lock();
      if (! logAggregationEnabled) {
        return LogAggregationStatus.DISABLED;
      }
      if (isLogAggregationFinished()) {
        return this.logAggregationStatusForAppReport;
      }
      Map<NodeId, LogAggregationReport> reports =
          getLogAggregationReportsForApp();
      if (reports.size() == 0) {
        return this.logAggregationStatusForAppReport;
      }
      int logNotStartCount = 0;
      int logCompletedCount = 0;
      int logTimeOutCount = 0;
      int logFailedCount = 0;
      int logRunningWithFailure = 0;
      for (Entry<NodeId, LogAggregationReport> report : reports.entrySet()) {
        switch (report.getValue().getLogAggregationStatus()) {
          case NOT_START:
            logNotStartCount++;
            break;
          case RUNNING_WITH_FAILURE:
            logRunningWithFailure ++;
            break;
          case SUCCEEDED:
            logCompletedCount++;
            break;
        if (logFailedCount > 0 && isAppInFinalState(this)) {
          return LogAggregationStatus.FAILED;
        } else if (logTimeOutCount > 0) {
          return LogAggregationStatus.TIME_OUT;
        }
        if (isAppInFinalState(this)) {
          return LogAggregationStatus.SUCCEEDED;
        }
      } else if (logRunningWithFailure > 0) {
        return LogAggregationStatus.RUNNING_WITH_FAILURE;
      }
      return LogAggregationStatus.RUNNING;
    } finally {
      this.readLock.unlock();
    }
  }

  private boolean isLogAggregationFinished() {
    return this.logAggregationStatusForAppReport
      .equals(LogAggregationStatus.SUCCEEDED)
        || this.logAggregationStatusForAppReport
          .equals(LogAggregationStatus.FAILED);

  }

  private boolean isLogAggregationFinishedForNM(LogAggregationReport report) {
    return report.getLogAggregationStatus() == LogAggregationStatus.SUCCEEDED
        || report.getLogAggregationStatus() == LogAggregationStatus.FAILED;
  }

  private void updateLogAggregationDiagnosticMessages(NodeId nodeId,
      LogAggregationReport report) {
    if (report.getDiagnosticMessage() != null
        && !report.getDiagnosticMessage().isEmpty()) {
      if (report.getLogAggregationStatus()
          == LogAggregationStatus.RUNNING ) {
        List<String> diagnostics = logAggregationDiagnosticsForNMs.get(nodeId);
        if (diagnostics == null) {
          diagnostics = new ArrayList<String>();
          logAggregationDiagnosticsForNMs.put(nodeId, diagnostics);
        } else {
          if (diagnostics.size()
              == maxLogAggregationDiagnosticsInMemory) {
            diagnostics.remove(0);
          }
        }
        diagnostics.add(report.getDiagnosticMessage());
        this.logAggregationStatus.get(nodeId).setDiagnosticMessage(
          StringUtils.join(diagnostics, "\n"));
      } else if (report.getLogAggregationStatus()
          == LogAggregationStatus.RUNNING_WITH_FAILURE) {
        List<String> failureMessages =
            logAggregationFailureMessagesForNMs.get(nodeId);
        if (failureMessages == null) {
          failureMessages = new ArrayList<String>();
          logAggregationFailureMessagesForNMs.put(nodeId, failureMessages);
        } else {
          if (failureMessages.size()
              == maxLogAggregationDiagnosticsInMemory) {
            failureMessages.remove(0);
          }
        }
        failureMessages.add(report.getDiagnosticMessage());
      }
    }
  }

  private void updateLogAggregationStatus(NodeId nodeId) {
    LogAggregationStatus status =
        this.logAggregationStatus.get(nodeId).getLogAggregationStatus();
    if (status.equals(LogAggregationStatus.SUCCEEDED)) {
      this.logAggregationSucceed++;
    } else if (status.equals(LogAggregationStatus.FAILED)) {
      this.logAggregationFailed++;
    }
    if (this.logAggregationSucceed == this.logAggregationStatus.size()) {
      this.logAggregationStatusForAppReport =
          LogAggregationStatus.SUCCEEDED;
      this.logAggregationStatus.clear();
      this.logAggregationDiagnosticsForNMs.clear();
      this.logAggregationFailureMessagesForNMs.clear();
    } else if (this.logAggregationSucceed + this.logAggregationFailed
        == this.logAggregationStatus.size()) {
      this.logAggregationStatusForAppReport = LogAggregationStatus.FAILED;
      for (Iterator<Map.Entry<NodeId, LogAggregationReport>> it =
          this.logAggregationStatus.entrySet().iterator(); it.hasNext();) {
        Map.Entry<NodeId, LogAggregationReport> entry = it.next();
        if (entry.getValue().getLogAggregationStatus()
          .equals(LogAggregationStatus.SUCCEEDED)) {
          it.remove();
        }
      }
      this.logAggregationDiagnosticsForNMs.clear();
    }
  }

  public String getLogAggregationFailureMessagesForNM(NodeId nodeId) {
    try {
      this.readLock.lock();
      List<String> failureMessages =
          this.logAggregationFailureMessagesForNMs.get(nodeId);
      if (failureMessages == null || failureMessages.isEmpty()) {
        return StringUtils.EMPTY;
      }
      return StringUtils.join(failureMessages, "\n");
    } finally {
      this.readLock.unlock();
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmnode/RMNodeImpl.java
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

      rmNode.handleContainerStatus(statusEvent.getContainers());

      List<LogAggregationReport> logAggregationReportsForApps =
          statusEvent.getLogAggregationReportsForApps();
      if (logAggregationReportsForApps != null
          && !logAggregationReportsForApps.isEmpty()) {
  }

  private void handleLogAggregationStatus(
      List<LogAggregationReport> logAggregationReportsForApps) {
    for (LogAggregationReport report : logAggregationReportsForApps) {
      RMApp rmApp = this.context.getRMApps().get(report.getApplicationId());
      if (rmApp != null) {
        ((RMAppImpl)rmApp).aggregateLogReport(this.nodeId, report);
      }
    }
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmnode/RMNodeStatusEvent.java
package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import java.util.List;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
  private final List<ContainerStatus> containersCollection;
  private final NodeHeartbeatResponse latestResponse;
  private final List<ApplicationId> keepAliveAppIds;
  private List<LogAggregationReport> logAggregationReportsForApps;

  public RMNodeStatusEvent(NodeId nodeId, NodeHealthStatus nodeHealthStatus,
      List<ContainerStatus> collection, List<ApplicationId> keepAliveAppIds,
  public RMNodeStatusEvent(NodeId nodeId, NodeHealthStatus nodeHealthStatus,
      List<ContainerStatus> collection, List<ApplicationId> keepAliveAppIds,
      NodeHeartbeatResponse latestResponse,
      List<LogAggregationReport> logAggregationReportsForApps) {
    super(nodeId, RMNodeEventType.STATUS_UPDATE);
    this.nodeHealthStatus = nodeHealthStatus;
    this.containersCollection = collection;
    return this.keepAliveAppIds;
  }

  public List<LogAggregationReport> getLogAggregationReportsForApps() {
    return this.logAggregationReportsForApps;
  }

  public void setLogAggregationReportsForApps(
      List<LogAggregationReport> logAggregationReportsForApps) {
    this.logAggregationReportsForApps = logAggregationReportsForApps;
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppBlock.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.AppBlock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;

    tbody._()._();
  }

  @Override
  protected LogAggregationStatus getLogAggregationStatus() {
    RMApp rmApp = this.rm.getRMContext().getRMApps().get(appID);
    if (rmApp == null) {
      return null;
    }
    return rmApp.getLogAggregationStatusForAppReport();
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppLogAggregationStatusBlock.java
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
      .td("Log Aggregation does not Start.")._();
    table_description.tr().td(LogAggregationStatus.RUNNING.name())
      .td("Log Aggregation is Running.")._();
    table_description.tr().td(LogAggregationStatus.RUNNING_WITH_FAILURE.name())
      .td("Log Aggregation is Running, but has failures "
          + "in previous cycles")._();
    table_description.tr().td(LogAggregationStatus.SUCCEEDED.name())
      .td("Log Aggregation is Succeeded. All of the logs have been "
          + "aggregated successfully.")._();
    table_description._();
    div_description._();

    RMApp rmApp = rm.getRMContext().getRMApps().get(appId);
    DIV<Hamlet> div = html.div(_INFO_WRAP);
    TABLE<DIV<Hamlet>> table =
        div.h3(
          "Log Aggregation: "
              + (rmApp == null ? "N/A" : rmApp
                .getLogAggregationStatusForAppReport() == null ? "N/A" : rmApp
                .getLogAggregationStatusForAppReport().name())).table(
          "#LogAggregationStatus");

    int maxLogAggregationDiagnosticsInMemory = conf.getInt(
      YarnConfiguration.RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY,
      YarnConfiguration.DEFAULT_RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY);
    table
      .tr()
      .th(_TH, "NodeId")
      .th(_TH, "Log Aggregation Status")
      .th(_TH, "Last "
          + maxLogAggregationDiagnosticsInMemory + " Diagnostic Messages")
      .th(_TH, "Last "
          + maxLogAggregationDiagnosticsInMemory + " Failure Messages")._();

    if (rmApp != null) {
      Map<NodeId, LogAggregationReport> logAggregationReports =
          rmApp.getLogAggregationReportsForApp();
          String message =
              report.getValue() == null ? null : report.getValue()
                .getDiagnosticMessage();
          String failureMessage =
              report.getValue() == null ? null : ((RMAppImpl)rmApp)
                  .getLogAggregationFailureMessagesForNM(report.getKey());
          table.tr()
            .td(report.getKey().toString())
            .td(status == null ? "N/A" : status.toString())
            .td(message == null ? "N/A" : message)
            .td(failureMessage == null ? "N/A" : failureMessage)._();
        }
      }
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/logaggregationstatus/TestRMAppLogAggregationStatus.java
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

        .getLogAggregationStatus());
    }

    List<LogAggregationReport> node1ReportForApp =
        new ArrayList<LogAggregationReport>();
    String messageForNode1_1 =
        "node1 logAggregation status updated at " + System.currentTimeMillis();
    LogAggregationReport report1 =
        LogAggregationReport.newInstance(appId, LogAggregationStatus.RUNNING,
          messageForNode1_1);
    node1ReportForApp.add(report1);
    node1.handle(new RMNodeStatusEvent(node1.getNodeID(), NodeHealthStatus
      .newInstance(true, null, 0), new ArrayList<ContainerStatus>(), null,
      null, node1ReportForApp));

    List<LogAggregationReport> node2ReportForApp =
        new ArrayList<LogAggregationReport>();
    String messageForNode2_1 =
        "node2 logAggregation status updated at " + System.currentTimeMillis();
    LogAggregationReport report2 =
        LogAggregationReport.newInstance(appId,
          LogAggregationStatus.RUNNING, messageForNode2_1);
    node2ReportForApp.add(report2);
    node2.handle(new RMNodeStatusEvent(node2.getNodeID(), NodeHealthStatus
      .newInstance(true, null, 0), new ArrayList<ContainerStatus>(), null,
      null, node2ReportForApp));
    }

    List<LogAggregationReport> node1ReportForApp2 =
        new ArrayList<LogAggregationReport>();
    String messageForNode1_2 =
        "node1 logAggregation status updated at " + System.currentTimeMillis();
    LogAggregationReport report1_2 =
        LogAggregationReport.newInstance(appId,
          LogAggregationStatus.RUNNING, messageForNode1_2);
    node1ReportForApp2.add(report1_2);
    node1.handle(new RMNodeStatusEvent(node1.getNodeID(), NodeHealthStatus
      .newInstance(true, null, 0), new ArrayList<ContainerStatus>(), null,
      null, node1ReportForApp2));
      if (report.getKey().equals(node1.getNodeID())) {
        Assert.assertEquals(LogAggregationStatus.RUNNING, report.getValue()
          .getLogAggregationStatus());
        Assert.assertEquals(
          messageForNode1_1 + "\n" + messageForNode1_2, report
            .getValue().getDiagnosticMessage());
      } else if (report.getKey().equals(node2.getNodeID())) {
        Assert.assertEquals(LogAggregationStatus.RUNNING, report.getValue()
    List<LogAggregationReport> node1ReportForApp3 =
        new ArrayList<LogAggregationReport>();
    LogAggregationReport report1_3;
    for (int i = 0; i < 10 ; i ++) {
      report1_3 =
          LogAggregationReport.newInstance(appId,
            LogAggregationStatus.RUNNING, "test_message_" + i);
      node1ReportForApp3.add(report1_3);
    }
    node1ReportForApp3.add(LogAggregationReport.newInstance(appId,
      LogAggregationStatus.SUCCEEDED, ""));
    node1.handle(new RMNodeStatusEvent(node1.getNodeID(), NodeHealthStatus
      .newInstance(true, null, 0), new ArrayList<ContainerStatus>(), null,
      null, node1ReportForApp3));
      if (report.getKey().equals(node1.getNodeID())) {
        Assert.assertEquals(LogAggregationStatus.SUCCEEDED, report.getValue()
          .getLogAggregationStatus());
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 9; i ++) {
          builder.append("test_message_" + i);
          builder.append("\n");
        }
        builder.append("test_message_" + 9);
        Assert.assertEquals(builder.toString(), report.getValue()
          .getDiagnosticMessage());
      } else if (report.getKey().equals(node2.getNodeID())) {
        Assert.assertEquals(LogAggregationStatus.TIME_OUT, report.getValue()
          .getLogAggregationStatus());
          .fail("should not contain log aggregation report for other nodes");
      }
    }

    List<LogAggregationReport> node2ReportForApp2 =
        new ArrayList<LogAggregationReport>();
    LogAggregationReport report2_2 =
        LogAggregationReport.newInstance(appId,
          LogAggregationStatus.RUNNING_WITH_FAILURE, "Fail_Message");
    LogAggregationReport report2_3 =
        LogAggregationReport.newInstance(appId,
          LogAggregationStatus.FAILED, "");
    node2ReportForApp2.add(report2_2);
    node2ReportForApp2.add(report2_3);
    node2.handle(new RMNodeStatusEvent(node2.getNodeID(), NodeHealthStatus
      .newInstance(true, null, 0), new ArrayList<ContainerStatus>(), null,
      null, node2ReportForApp2));
    Assert.assertEquals(LogAggregationStatus.FAILED,
      rmApp.getLogAggregationStatusForAppReport());
    logAggregationStatus = rmApp.getLogAggregationReportsForApp();
    Assert.assertTrue(logAggregationStatus.size() == 1);
    Assert.assertTrue(logAggregationStatus.containsKey(node2.getNodeID()));
    Assert.assertTrue(!logAggregationStatus.containsKey(node1.getNodeID()));
    Assert.assertEquals("Fail_Message",
      ((RMAppImpl)rmApp).getLogAggregationFailureMessagesForNM(nodeId2));
  }

  @Test (timeout = 10000)
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    rmApp = (RMAppImpl)createRMApp(conf);
    Assert.assertEquals(LogAggregationStatus.NOT_START,
      rmApp.getLogAggregationStatusForAppReport());

    NodeId nodeId1 = NodeId.newInstance("localhost", 1111);
    NodeId nodeId2 = NodeId.newInstance("localhost", 2222);
    rmApp.aggregateLogReport(nodeId1, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.NOT_START, ""));
    rmApp.aggregateLogReport(nodeId2, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.NOT_START, ""));
    rmApp.aggregateLogReport(nodeId3, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.NOT_START, ""));
    rmApp.aggregateLogReport(nodeId4, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.NOT_START, ""));
    Assert.assertEquals(LogAggregationStatus.NOT_START,
      rmApp.getLogAggregationStatusForAppReport());

    rmApp.aggregateLogReport(nodeId1, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.NOT_START, ""));
    rmApp.aggregateLogReport(nodeId2, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.RUNNING, ""));
    rmApp.aggregateLogReport(nodeId3, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.SUCCEEDED, ""));
    rmApp.aggregateLogReport(nodeId4, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.SUCCEEDED, ""));
    Assert.assertEquals(LogAggregationStatus.RUNNING,
      rmApp.getLogAggregationStatusForAppReport());

    rmApp.aggregateLogReport(nodeId1, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.SUCCEEDED, ""));
    rmApp.aggregateLogReport(nodeId2, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.TIME_OUT, ""));
    rmApp.aggregateLogReport(nodeId3, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.SUCCEEDED, ""));
    rmApp.aggregateLogReport(nodeId4, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.SUCCEEDED, ""));
    Assert.assertEquals(LogAggregationStatus.TIME_OUT,
      rmApp.getLogAggregationStatusForAppReport());

    rmApp.aggregateLogReport(nodeId1, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.SUCCEEDED, ""));
    rmApp.aggregateLogReport(nodeId2, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.SUCCEEDED, ""));
    rmApp.aggregateLogReport(nodeId3, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.SUCCEEDED, ""));
    rmApp.aggregateLogReport(nodeId4, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.SUCCEEDED, ""));
    Assert.assertEquals(LogAggregationStatus.SUCCEEDED,
      rmApp.getLogAggregationStatusForAppReport());

    rmApp = (RMAppImpl)createRMApp(conf);
    rmApp.aggregateLogReport(nodeId1, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.NOT_START, ""));
    rmApp.aggregateLogReport(nodeId2, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.RUNNING, ""));
    rmApp.aggregateLogReport(nodeId3, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.NOT_START, ""));
    rmApp.aggregateLogReport(nodeId4, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.NOT_START, ""));
    Assert.assertEquals(LogAggregationStatus.RUNNING,
      rmApp.getLogAggregationStatusForAppReport());

    rmApp.aggregateLogReport(nodeId1, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.NOT_START, ""));
    rmApp.aggregateLogReport(nodeId2, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.RUNNING, ""));
    rmApp.aggregateLogReport(nodeId3, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.NOT_START, ""));
    rmApp.aggregateLogReport(nodeId4, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.RUNNING_WITH_FAILURE,
      ""));
    Assert.assertEquals(LogAggregationStatus.RUNNING_WITH_FAILURE,
      rmApp.getLogAggregationStatusForAppReport());

    rmApp.aggregateLogReport(nodeId1, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.NOT_START, ""));
    rmApp.aggregateLogReport(nodeId2, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.RUNNING, ""));
    rmApp.aggregateLogReport(nodeId3, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.NOT_START, ""));
    rmApp.aggregateLogReport(nodeId4, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.RUNNING, ""));
    Assert.assertEquals(LogAggregationStatus.RUNNING_WITH_FAILURE,
      rmApp.getLogAggregationStatusForAppReport());

    rmApp.handle(new RMAppEvent(rmApp.getApplicationId(), RMAppEventType.KILL));
    Assert.assertTrue(RMAppImpl.isAppInFinalState(rmApp));
    rmApp.aggregateLogReport(nodeId1, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.SUCCEEDED, ""));
    rmApp.aggregateLogReport(nodeId2, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.TIME_OUT, ""));
    rmApp.aggregateLogReport(nodeId3, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.FAILED, ""));
    rmApp.aggregateLogReport(nodeId4, LogAggregationReport.newInstance(
      rmApp.getApplicationId(), LogAggregationStatus.FAILED, ""));
    Assert.assertEquals(LogAggregationStatus.FAILED,
      rmApp.getLogAggregationStatusForAppReport());


