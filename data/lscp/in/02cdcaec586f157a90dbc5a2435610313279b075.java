hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/TaskPage.java

package org.apache.hadoop.mapreduce.v2.app.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;

      for (TaskAttempt attempt : getTaskAttempts()) {
        TaskAttemptInfo ta = new TaskAttemptInfo(attempt, true);
        String progress = StringUtils.formatPercent(ta.getProgress() / 100, 2);

        String nodeHttpAddr = ta.getNode();
        String diag = ta.getNote() == null ? "" : ta.getNote();

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/TasksBlock.java
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_STATE;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_TYPE;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR_VALUE;

import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;

      TaskInfo info = new TaskInfo(task);
      String tid = info.getId();
      String pct = StringUtils.formatPercent(info.getProgress() / 100, 2);
      tasksTableData.append("[\"<a href='").append(url("task", tid))
      .append("'>").append(tid).append("</a>\",\"")

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/dao/JobInfo.java
package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRApps.TaskAttemptStateUI;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.util.Times;

@XmlRootElement(name = "job")
    this.mapsTotal = job.getTotalMaps();
    this.mapsCompleted = job.getCompletedMaps();
    this.mapProgress = report.getMapProgress() * 100;
    this.mapProgressPercent =
        StringUtils.formatPercent(report.getMapProgress(), 2);
    this.reducesTotal = job.getTotalReduces();
    this.reducesCompleted = job.getCompletedReduces();
    this.reduceProgress = report.getReduceProgress() * 100;
    this.reduceProgressPercent =
        StringUtils.formatPercent(report.getReduceProgress(), 2);

    this.acls = new ArrayList<ConfEntryInfo>();
    if (hasAccess) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/StringHelper.java
    }
    sb.append(part);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/AppsBlock.java
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
        continue;
      }
      AppInfo app = new AppInfo(appReport);
      String percent = StringUtils.format("%.1f", app.getProgress());
      appsTableData
        .append("[\"<a href='")
        .append(url("app", app.getAppId()))

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/resource/ResourceWeights.java
      }
      ResourceType resourceType = ResourceType.values()[i];
      sb.append(StringUtils.toLowerCase(resourceType.name()));
      sb.append(StringUtils.format(" weight=%.1f", getWeight(resourceType)));
    }
    sb.append(">");
    return sb.toString();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/CapacitySchedulerPage.java
          _("Configured Capacity:", percent(lqinfo.getCapacity() / 100)).
          _("Configured Max Capacity:", percent(lqinfo.getMaxCapacity() / 100)).
          _("Configured Minimum User Limit Percent:", Integer.toString(lqinfo.getUserLimit()) + "%").
          _("Configured User Limit Factor:", StringUtils.format(
              "%.1f", lqinfo.getUserLimitFactor())).
          _("Accessible Node Labels:", StringUtils.join(",", lqinfo.getNodeLabels())).
          _("Ordering Policy: ", lqinfo.getOrderingPolicyInfo()).
          _("Preemption:", lqinfo.getPreemptionDisabled() ? "disabled" : "enabled");
  }

  static String percent(float f) {
    return StringUtils.formatPercent(f, 1);
  }

  static String width(float f) {
    return StringUtils.format("width:%.1f%%", f * 100);
  }

  static String left(float f) {
    return StringUtils.format("left:%.1f%%", f * 100);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/DefaultSchedulerPage.java

import static org.apache.hadoop.yarn.util.StringHelper.join;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FifoSchedulerInfo;
  }

  static String percent(float f) {
    return StringUtils.formatPercent(f, 1);
  }

  static String width(float f) {
    return StringUtils.format("width:%.1f%%", f * 100);
  }

  static String left(float f) {
    return StringUtils.format("left:%.1f%%", f * 100);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/FairSchedulerAppsBlock.java

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
        continue;
      }
      AppInfo appInfo = new AppInfo(rm, app, true, WebAppUtils.getHttpSchemePrefix(conf));
      String percent = StringUtils.format("%.1f", appInfo.getProgress());
      ApplicationAttemptId attemptId = app.getCurrentAppAttempt().getAppAttemptId();
      int fairShare = fsinfo.getAppFairShare(attemptId);
      if (fairShare == FairSchedulerInfo.INVALID_FAIR_SHARE) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/FairSchedulerPage.java

import java.util.Collection;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
  }

  static String percent(float f) {
    return StringUtils.formatPercent(f, 1);
  }

  static String width(float f) {
    return StringUtils.format("width:%.1f%%", f * 100);
  }

  static String left(float f) {
    return StringUtils.format("left:%.1f%%", f * 100);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppsBlock.java
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
      if (nodes != null) {
        blacklistedNodesCount = String.valueOf(nodes.size());
      }
      String percent = StringUtils.format("%.1f", app.getProgress());
      appsTableData
        .append("[\"<a href='")
        .append(url("app", app.getAppId()))

