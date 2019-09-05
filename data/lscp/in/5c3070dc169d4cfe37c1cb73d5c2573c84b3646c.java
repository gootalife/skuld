hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/TaskPage.java
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import java.util.EnumSet;
import java.util.Collection;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
  static class AttemptsBlock extends HtmlBlock {
    final App app;
    final boolean enableUIActions;

    @Inject
    AttemptsBlock(App ctx, Configuration conf) {
        return;
      }

      JobId jobId = app.getJob().getID();
      if (enableUIActions) {

        StringBuilder script = new StringBuilder();
        script
            .append("function confirmAction(appID, jobID, taskID, attID) {\n")
            .append("  var b = confirm(\"Are you sure?\");\n")
            .append("  if (b == true) {\n")
            .append("    var current = '/proxy/' + appID")
            .append("      + '/mapreduce/task/' + taskID;\n")
            .append("    var stateURL = '/proxy/' + appID")
            .append("      + '/ws/v1/mapreduce/jobs/' + jobID")
            .append("      + '/tasks/' + taskID")
            .append("      + '/attempts/' + attID + '/state';\n")
            .append("    $.ajax({\n")
            .append("      type: 'PUT',\n")
            .append("      url: stateURL,\n")
            .append("      contentType: 'application/json',\n")
            .append("      data: '{\"state\":\"KILLED\"}',\n")
            .append("      dataType: 'json'\n")
            .append("    }).done(function(data) {\n")
            .append("         setTimeout(function() {\n")
            .append("           location.href = current;\n")
            .append("         }, 1000);\n")
            .append("    }).fail(function(data) {\n")
            .append("         console.log(data);\n")
            .append("    });\n")
            .append("  }\n")
            .append("}\n");

        html.script().$type("text/javascript")._(script.toString())._();
      }
        .append(StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(
          diag)));
        if (enableUIActions) {
          attemptsTableData.append("\",\"");
          if (EnumSet.of(
                  TaskAttemptState.SUCCEEDED,
                  TaskAttemptState.FAILED,
                  TaskAttemptState.KILLED).contains(attempt.getState())) {
            attemptsTableData.append("N/A");
          } else {
            attemptsTableData
              .append("<a href=javascript:void(0) onclick=confirmAction('")
              .append(jobId.getAppId()).append("','")
              .append(jobId).append("','")
              .append(attempt.getID().getTaskId()).append("','")
              .append(ta.getId())
              .append("');>Kill</a>");
          }
          attemptsTableData.append("\"],\n");
        }
      }

