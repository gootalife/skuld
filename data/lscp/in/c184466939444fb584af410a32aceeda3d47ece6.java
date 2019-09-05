hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/AHSController.java
  public void logs() {
    render(AHSLogsPage.class);
  }

  public void errorsAndWarnings() {
    render(AHSErrorsAndWarningsPage.class);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/AHSErrorsAndWarningsPage.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/AHSErrorsAndWarningsPage.java

package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import org.apache.hadoop.yarn.server.webapp.ErrorsAndWarningsBlock;
import org.apache.hadoop.yarn.webapp.SubView;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

public class AHSErrorsAndWarningsPage extends AHSView {

  @Override
  protected Class<? extends SubView> content() {
    return ErrorsAndWarningsBlock.class;
  }

  @Override
  protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    String title = "Errors and Warnings in the Application History Server";
    setTitle(title);
    String tableId = "messages";
    set(DATATABLES_ID, tableId);
    set(initID(DATATABLES, tableId), tablesInit());
    setTableStyles(html, tableId, ".message {width:50em}",
        ".count {width:8em}", ".lasttime {width:16em}");
  }

  private String tablesInit() {
    StringBuilder b = tableInit().append(", aoColumnDefs: [");
    b.append("{'sType': 'string', 'aTargets': [ 0 ]}");
    b.append(", {'sType': 'string', 'bSearchable': true, 'aTargets': [ 1 ]}");
    b.append(", {'sType': 'numeric', 'bSearchable': false, 'aTargets': [ 2 ]}");
    b.append(", {'sType': 'date', 'aTargets': [ 3 ] }]");
    b.append(", aaSorting: [[3, 'desc']]}");
    return b.toString();
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/AHSWebApp.java
    route(
      pajoin("/logs", NM_NODENAME, CONTAINER_ID, ENTITY_STRING, APP_OWNER,
        CONTAINER_LOG_TYPE), AHSController.class, "logs");
    route("/errors-and-warnings", AHSController.class, "errorsAndWarnings");
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/NavBlock.java

package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Log4jWarningErrorMetricsAppender;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

public class NavBlock extends HtmlBlock {

  @Override
  public void render(Block html) {
    boolean addErrorsAndWarningsLink = false;
    Log log = LogFactory.getLog(NavBlock.class);
    if (log instanceof Log4JLogger) {
      Log4jWarningErrorMetricsAppender appender =
          Log4jWarningErrorMetricsAppender.findAppender();
      if (appender != null) {
        addErrorsAndWarningsLink = true;
      }
    }
    Hamlet.DIV<Hamlet> nav = html.
        div("#nav").
            h3("Application History").
                ul().
                            _().
                        _().
                    _().
                _();

    Hamlet.UL<Hamlet.DIV<Hamlet>> tools = nav.h3("Tools").ul();
    tools.li().a("/conf", "Configuration")._()
        .li().a("/logs", "Local logs")._()
        .li().a("/stacks", "Server stacks")._()
        .li().a("/jmx?qry=Hadoop:*", "Server metrics")._();

    if (addErrorsAndWarningsLink) {
      tools.li().a(url("errors-and-warnings"), "Errors/Warnings")._();
    }
    tools._()._();
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/ErrorsAndWarningsBlock.java
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.security.AdminACLsManager;
import org.apache.hadoop.yarn.util.Log4jWarningErrorMetricsAppender;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
public class ErrorsAndWarningsBlock extends HtmlBlock {

  long cutoffPeriodSeconds;
  final private AdminACLsManager adminAclsManager;

  @Inject
  ErrorsAndWarningsBlock(ViewContext ctx, Configuration conf) {
    super(ctx);
    cutoffPeriodSeconds = Time.now() / 1000;
    } catch (NumberFormatException ne) {
      cutoffPeriodSeconds = Time.now() / 1000;
    }
    adminAclsManager = new AdminACLsManager(conf);
  }

  @Override
  protected void render(Block html) {
    Log log = LogFactory.getLog(ErrorsAndWarningsBlock.class);

    boolean isAdmin = false;
    UserGroupInformation callerUGI = this.getCallerUGI();

    if (adminAclsManager.areACLsEnabled()) {
      if (callerUGI != null && adminAclsManager.isAdmin(callerUGI)) {
        isAdmin = true;
      }
    } else {
      isAdmin = true;
    }

    if (!isAdmin) {
      html.div().p()._("This page is for admins only.")._()._();
      return;
    }

    if (log instanceof Log4JLogger) {
      html._(ErrorMetrics.class);
      html._(WarningMetrics.class);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/NavBlock.java

  @Override public void render(Block html) {
    boolean addErrorsAndWarningsLink = false;
    Log log = LogFactory.getLog(NavBlock.class);
    if (log instanceof Log4JLogger) {
      Log4jWarningErrorMetricsAppender appender =
          Log4jWarningErrorMetricsAppender.findAppender();

