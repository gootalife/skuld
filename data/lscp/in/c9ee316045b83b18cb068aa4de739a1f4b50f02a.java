hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppBlock.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.AppBlock;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

import java.util.Collection;
import java.util.Set;


  private static final Log LOG = LogFactory.getLog(RMAppBlock.class);
  private final ResourceManager rm;
  private final Configuration conf;


  @Inject
  RMAppBlock(ViewContext ctx, Configuration conf, ResourceManager rm) {
    super(rm.getClientRMService(), ctx, conf);
    this.conf = conf;
    this.rm = rm;
  }

            .th(".started", "Started").th(".node", "Node").th(".logs", "Logs")
            .th(".blacklistednodes", "Blacklisted Nodes")._()._().tbody();

    RMApp rmApp = this.rm.getRMContext().getRMApps().get(this.appID);
    if (rmApp == null) {
      return;
    }
    StringBuilder attemptsTableData = new StringBuilder("[\n");
    for (final ApplicationAttemptReport appAttemptReport : attempts) {
      RMAppAttempt rmAppAttempt =
          rmApp.getRMAppAttempt(appAttemptReport.getApplicationAttemptId());
      if (rmAppAttempt == null) {
        continue;
      }
      AppAttemptInfo attemptInfo =
          new AppAttemptInfo(this.rm, rmAppAttempt, rmApp.getUser());
      String blacklistedNodesCount = "N/A";
      Set<String> nodes =
          RMAppAttemptBlock.getBlacklistedNodes(rm,
            rmAppAttempt.getAppAttemptId());
      if(nodes != null) {
        blacklistedNodesCount = String.valueOf(nodes.size());
      }
      String nodeLink = attemptInfo.getNodeHttpAddress();
      if (nodeLink != null) {
        nodeLink = WebAppUtils.getHttpSchemePrefix(conf) + nodeLink;
      }
      String logsLink = attemptInfo.getLogsLink();
      attemptsTableData
          .append("[\"<a href='")
          .append(url("appattempt", rmAppAttempt.getAppAttemptId().toString()))
          .append("'>")
          .append(String.valueOf(rmAppAttempt.getAppAttemptId()))
          .append("</a>\",\"")
          .append(attemptInfo.getStartTime())
          .append("\",\"<a ")
          .append(nodeLink == null ? "#" : "href='" + nodeLink)
          .append("'>")

