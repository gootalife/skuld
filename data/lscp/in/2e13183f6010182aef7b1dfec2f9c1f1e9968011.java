hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/server/security/ApplicationACLsManager.java
    }
    return false;
  }

  public final boolean isAdmin(final UserGroupInformation calledUGI) {
    return this.adminAclsManager.isAdmin(calledUGI);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/CapacitySchedulerPage.java
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.SubView;
  static class QueuesBlock extends HtmlBlock {
    final CapacityScheduler cs;
    final CSQInfo csqinfo;
    private final ResourceManager rm;

    @Inject QueuesBlock(ResourceManager rm, CSQInfo info) {
      cs = (CapacityScheduler) rm.getResourceScheduler();
      csqinfo = info;
      this.rm = rm;
    }

    @Override
    public void render(Block html) {
      html._(MetricsOverviewTable.class);

      UserGroupInformation callerUGI = this.getCallerUGI();
      boolean isAdmin = false;
      ApplicationACLsManager aclsManager = rm.getApplicationACLsManager();
      if (aclsManager.areACLsEnabled()) {
        if (callerUGI != null && aclsManager.isAdmin(callerUGI)) {
          isAdmin = true;
        }
      } else {
        isAdmin = true;
      }

      if (isAdmin) {
        html.div()
          .button()
          .$style(
              "border-style: solid; border-color: #000000; border-width: 1px;"
                  + " cursor: hand; cursor: pointer; border-radius: 4px")
          .$onclick("confirmAction()").b("Dump scheduler logs")._().select()
          .$id("time").option().$value("60")._("1 min")._().option()
          .$value("300")._("5 min")._().option().$value("600")._("10 min")._()
          ._()._();

        StringBuilder script = new StringBuilder();
        script
          .append("function confirmAction() {")
          .append(" b = confirm(\"Are you sure you wish to generate"
              + " scheduler logs?\");")
          .append(" if (b == true) {")
          .append(" var timePeriod = $(\"#time\").val();")
          .append(" $.ajax({")
          .append(" alert(\"Scheduler log is being generated.\");")
          .append(" }, 1000);")
          .append(" }).fail(function(data){")
          .append(
              " alert(\"Scheduler log generation failed. Please check the"
                  + " ResourceManager log for more informtion.\");")
          .append(" console.log(data);").append(" });").append(" }")
          .append("}");

        html.script().$type("text/javascript")._(script.toString())._();
      }

      UL<DIV<DIV<Hamlet>>> ul = html.
        div("#cs-wrapper.ui-widget").

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMWebServices.java
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.StatisticsItemInfo;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.AdHocLogDumper;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

  @POST
  @Path("/scheduler/logs")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public String dumpSchedulerLogs(@FormParam("time") String time,
      @Context HttpServletRequest hsr) throws IOException {
    init();
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    ApplicationACLsManager aclsManager = rm.getApplicationACLsManager();
    if (aclsManager.areACLsEnabled()) {
      if (callerUGI == null || !aclsManager.isAdmin(callerUGI)) {
        String msg = "Only admins can carry out this operation.";
        throw new ForbiddenException(msg);
      }
    }
    ResourceScheduler rs = rm.getResourceScheduler();
    int period = Integer.parseInt(time);
    if (period <= 0) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServices.java
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.StringReader;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
        null, null, null, null, null, null, null, emptySet, emptySet);
    assertTrue(appsInfo.getApps().isEmpty());
  }

  @Test
  public void testDumpingSchedulerLogs() throws Exception {

    ResourceManager mockRM = mock(ResourceManager.class);
    Configuration conf = new YarnConfiguration();
    HttpServletRequest mockHsr = mock(HttpServletRequest.class);
    ApplicationACLsManager aclsManager = new ApplicationACLsManager(conf);
    when(mockRM.getApplicationACLsManager()).thenReturn(aclsManager);
    RMWebServices webSvc =
        new RMWebServices(mockRM, conf, mock(HttpServletResponse.class));

    webSvc.dumpSchedulerLogs("1", mockHsr);
    Thread.sleep(1000);
    checkSchedulerLogFileAndCleanup();

    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.setStrings(YarnConfiguration.YARN_ADMIN_ACL, "admin");
    aclsManager = new ApplicationACLsManager(conf);
    when(mockRM.getApplicationACLsManager()).thenReturn(aclsManager);
    webSvc = new RMWebServices(mockRM, conf, mock(HttpServletResponse.class));
    boolean exceptionThrown = false;
    try {
      webSvc.dumpSchedulerLogs("1", mockHsr);
      fail("Dumping logs should fail");
    } catch (ForbiddenException ae) {
      exceptionThrown = true;
    }
    assertTrue("ForbiddenException expected", exceptionThrown);
    exceptionThrown = false;
    when(mockHsr.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return "testuser";
      }
    });
    try {
      webSvc.dumpSchedulerLogs("1", mockHsr);
      fail("Dumping logs should fail");
    } catch (ForbiddenException ae) {
      exceptionThrown = true;
    }
    assertTrue("ForbiddenException expected", exceptionThrown);

    when(mockHsr.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return "admin";
      }
    });
    webSvc.dumpSchedulerLogs("1", mockHsr);
    Thread.sleep(1000);
    checkSchedulerLogFileAndCleanup();
  }

  private void checkSchedulerLogFileAndCleanup() {
    String targetFile;
    ResourceScheduler scheduler = rm.getResourceScheduler();
    if (scheduler instanceof FairScheduler) {
      targetFile = "yarn-fair-scheduler-debug.log";
    } else if (scheduler instanceof CapacityScheduler) {
      targetFile = "yarn-capacity-scheduler-debug.log";
    } else {
      targetFile = "yarn-scheduler-debug.log";
    }
    File logFile = new File(System.getProperty("yarn.log.dir"), targetFile);
    assertTrue("scheduler log file doesn't exist", logFile.exists());
    FileUtils.deleteQuietly(logFile);
  }
}

