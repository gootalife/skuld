hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/AppBlock.java
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

      html.script().$type("text/javascript")._(script.toString())._();
    }

    String schedulerPath = WebAppUtils.getResolvedRMWebAppURLWithScheme(conf) +
        "/cluster/scheduler?openQueues=" + app.getQueue();

    ResponseInfo overviewTable = info("Application Overview")
      ._("User:", schedulerPath, app.getUser())
      ._("Name:", app.getName())
      ._("Application Type:", app.getType())
      ._("Application Tags:",
        "YarnApplicationState:",
        app.getAppState() == null ? UNAVAILABLE : clarifyAppState(app
          .getAppState()))
      ._("Queue:", schedulerPath, app.getQueue())
      ._("FinalStatus Reported by AM:",
        clairfyAppFinalStatus(app.getFinalAppStatus()))
      ._("Started:", Times.format(app.getStartedTime()))

