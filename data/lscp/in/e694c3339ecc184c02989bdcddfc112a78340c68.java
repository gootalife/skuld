hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/AttemptsPage.java
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
public class AttemptsPage extends TaskPage {
  static class FewAttemptsBlock extends TaskPage.AttemptsBlock {
    @Inject
    FewAttemptsBlock(App ctx, Configuration conf) {
      super(ctx, conf);
    }

    @Override

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/TaskPage.java
import java.util.Collection;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.THEAD;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

  static class AttemptsBlock extends HtmlBlock {
    final App app;
    final boolean enableUIActions;
    private String stateURLFormat;

    @Inject
    AttemptsBlock(App ctx, Configuration conf) {
      app = ctx;
      this.enableUIActions =
          conf.getBoolean(MRConfig.MASTER_WEBAPP_UI_ACTIONS_ENABLED,
              MRConfig.DEFAULT_MASTER_WEBAPP_UI_ACTIONS_ENABLED);
    }

    @Override
          h2($(TITLE));
        return;
      }

      if (enableUIActions) {
        String appID = app.getJob().getID().getAppId().toString();
        String jobID = app.getJob().getID().toString();
        String taskID = app.getTask().getID().toString();
        stateURLFormat =
            String.format("/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks/%s/"
                + "attempts", appID, jobID, taskID) + "/%s/state";

        String current =
            String.format("/proxy/%s/mapreduce/task/%s", appID, taskID);

        StringBuilder script = new StringBuilder();
        script.append("function confirmAction(stateURL) {")
            .append(" b = confirm(\"Are you sure?\");")
            .append(" if (b == true) {")
            .append(" $.ajax({")
            .append(" type: 'PUT',")
            .append(" url: stateURL,")
            .append(" contentType: 'application/json',")
            .append(" data: '{\"state\":\"KILLED\"}',")
            .append(" dataType: 'json'")
            .append(" }).done(function(data){")
            .append(" setTimeout(function(){")
            .append(" location.href = '").append(current).append("';")
            .append(" }, 1000);")
            .append(" }).fail(function(data){")
            .append(" console.log(data);")
            .append(" });")
            .append(" }")
            .append("}");

        html.script().$type("text/javascript")._(script.toString())._();
      }

      TR<THEAD<TABLE<Hamlet>>> tr = html.table("#attempts").thead().tr();
      tr.th(".id", "Attempt").
      th(".progress", "Progress").
      th(".state", "State").
      th(".status", "Status").
      th(".tsh", "Started").
      th(".tsh", "Finished").
      th(".tsh", "Elapsed").
      th(".note", "Note");
      if (enableUIActions) {
        tr.th(".actions", "Actions");
      }

      TBODY<TABLE<Hamlet>> tbody = tr._()._().tbody();
      StringBuilder attemptsTableData = new StringBuilder("[\n");
        .append(ta.getFinishTime()).append("\",\"")
        .append(ta.getElapsedTime()).append("\",\"")
        .append(StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(
          diag)));
        if (enableUIActions) {
          attemptsTableData.append("\",\"")
          .append("<a href=javascript:void(0) onclick=confirmAction('")
          .append(String.format(stateURLFormat, ta.getId()))
          .append("');>Kill</a>")
          .append("\"],\n");
        } else {
          attemptsTableData.append("\"],\n");
        }
      }
      if(attemptsTableData.charAt(attemptsTableData.length() - 2) == ',') {

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/MRConfig.java
  @Unstable
  public static final boolean DEFAULT_MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM =
      false;

  String MASTER_WEBAPP_UI_ACTIONS_ENABLED =
      "mapreduce.webapp.ui-actions.enabled";
  boolean DEFAULT_MASTER_WEBAPP_UI_ACTIONS_ENABLED = true;
}
  

