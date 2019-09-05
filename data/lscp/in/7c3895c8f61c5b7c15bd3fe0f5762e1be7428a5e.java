hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/webapp/ContainerLogsPage.java
      if (redirectUrl.equals("false")) {
        set(TITLE, join("Failed redirect for ", $(CONTAINER_ID)));
      }
    }
    

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/webapp/NMController.java

import static org.apache.hadoop.yarn.util.StringHelper.join;

import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.YarnWebParams;


public class NMController extends Controller implements YarnWebParams {
  
  @Inject
  public NMController(RequestContext requestContext) {
    super(requestContext);
  }

  @Override
  }

  public void logs() {
    render(ContainerLogsPage.class);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/webapp/NMWebAppFilter.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/webapp/NMWebAppFilter.java

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.io.IOException;
import java.io.PrintWriter;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.webapp.Controller.RequestContext;
import com.google.inject.Injector;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

@Singleton
public class NMWebAppFilter extends GuiceContainer{

  private Injector injector;
  private Context nmContext;

  private static final long serialVersionUID = 1L;

  @Inject
  public NMWebAppFilter(Injector injector, Context nmContext) {
    super(injector);
    this.injector = injector;
    this.nmContext = nmContext;
  }

  @Override
  public void doFilter(HttpServletRequest request,
      HttpServletResponse response, FilterChain chain) throws IOException,
      ServletException {
    String uri = HtmlQuoting.quoteHtmlChars(request.getRequestURI());
    String redirectPath = containerLogPageRedirectPath(uri);
    if (redirectPath != null) {
      String redirectMsg =
          "Redirecting to log server" + " : " + redirectPath;
      PrintWriter out = response.getWriter();
      out.println(redirectMsg);
      response.setHeader("Location", redirectPath);
      response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
      return;
    }
    super.doFilter(request, response, chain);
  }

  private String containerLogPageRedirectPath(String uri) {
    String redirectPath = null;
    if (!uri.contains("/ws/v1/node") && uri.contains("/containerlogs")) {
      String[] parts = uri.split("/");
      String containerIdStr = parts[3];
      String appOwner = parts[4];
      if (containerIdStr != null && !containerIdStr.isEmpty()) {
        ContainerId containerId = null;
        try {
          containerId = ContainerId.fromString(containerIdStr);
        } catch (IllegalArgumentException ex) {
          return redirectPath;
        }
        ApplicationId appId =
            containerId.getApplicationAttemptId().getApplicationId();
        Application app = nmContext.getApplications().get(appId);
        Configuration nmConf = nmContext.getLocalDirsHandler().getConfig();
        if (app == null
            && nmConf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
              YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
          String logServerUrl =
              nmConf.get(YarnConfiguration.YARN_LOG_SERVER_URL);
          if (logServerUrl != null && !logServerUrl.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append(logServerUrl);
            sb.append("/");
            sb.append(nmContext.getNodeId().toString());
            sb.append("/");
            sb.append(containerIdStr);
            sb.append("/");
            sb.append(containerIdStr);
            sb.append("/");
            sb.append(appOwner);
            redirectPath = sb.toString();
          } else {
            injector.getInstance(RequestContext.class).set(
              ContainerLogsPage.REDIRECT_URL, "false");
          }
        }
      }
    }
    return redirectPath;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/webapp/WebServer.java

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

public class WebServer extends AbstractService {

  private static final Log LOG = LogFactory.getLog(WebServer.class);
      route("/errors-and-warnings", NMController.class, "errorsAndWarnings");
    }

    @Override
    protected Class<? extends GuiceContainer> getWebAppFilterClass() {
      return NMWebAppFilter.class;
    }
  }
}

