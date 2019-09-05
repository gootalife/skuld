hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/AdHocLogDumper.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/AdHocLogDumper.java

package org.apache.hadoop.yarn.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.log4j.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AdHocLogDumper {

  private static final Log LOG = LogFactory.getLog(AdHocLogDumper.class);

  private String name;
  private String targetFilename;
  private Map<String, Priority> appenderLevels;
  private Level currentLogLevel;
  public static final String AD_HOC_DUMPER_APPENDER = "ad-hoc-dumper-appender";
  private static boolean logFlag = false;
  private static final Object lock = new Object();

  public AdHocLogDumper(String name, String targetFilename) {
    this.name = name;
    this.targetFilename = targetFilename;
    appenderLevels = new HashMap<>();
  }

  public void dumpLogs(String level, int timePeriod)
      throws YarnRuntimeException, IOException {
    synchronized (lock) {
      if (logFlag) {
        LOG.info("Attempt to dump logs when appender is already running");
        throw new YarnRuntimeException("Appender is already dumping logs");
      }
      Level targetLevel = Level.toLevel(level);
      Log log = LogFactory.getLog(name);
      appenderLevels.clear();
      if (log instanceof Log4JLogger) {
        Logger packageLogger = ((Log4JLogger) log).getLogger();
        currentLogLevel = packageLogger.getLevel();
        Level currentEffectiveLevel = packageLogger.getEffectiveLevel();

        Layout layout = new PatternLayout("%d{ISO8601} %p %c: %m%n");
        FileAppender fApp;
        File file =
            new File(System.getProperty("yarn.log.dir"), targetFilename);
        try {
          fApp = new FileAppender(layout, file.getAbsolutePath(), false);
        } catch (IOException ie) {
          LOG
            .warn(
              "Error creating file, can't dump logs to "
                  + file.getAbsolutePath(), ie);
          throw ie;
        }
        fApp.setName(AdHocLogDumper.AD_HOC_DUMPER_APPENDER);
        fApp.setThreshold(targetLevel);

        for (Enumeration appenders = Logger.getRootLogger().getAllAppenders(); appenders
          .hasMoreElements();) {
          Object obj = appenders.nextElement();
          if (obj instanceof AppenderSkeleton) {
            AppenderSkeleton appender = (AppenderSkeleton) obj;
            appenderLevels.put(appender.getName(), appender.getThreshold());
            appender.setThreshold(currentEffectiveLevel);
          }
        }

        packageLogger.addAppender(fApp);
        LOG.info("Dumping adhoc logs for " + name + " to "
            + file.getAbsolutePath() + " for " + timePeriod + " milliseconds");
        packageLogger.setLevel(targetLevel);
        logFlag = true;

        TimerTask restoreLogLevel = new RestoreLogLevel();
        Timer restoreLogLevelTimer = new Timer();
        restoreLogLevelTimer.schedule(restoreLogLevel, timePeriod);
      }
    }
  }

  class RestoreLogLevel extends TimerTask {
    @Override
    public void run() {
      Log log = LogFactory.getLog(name);
      if (log instanceof Log4JLogger) {
        Logger logger = ((Log4JLogger) log).getLogger();
        logger.removeAppender(AD_HOC_DUMPER_APPENDER);
        logger.setLevel(currentLogLevel);
        for (Enumeration appenders = Logger.getRootLogger().getAllAppenders(); appenders
          .hasMoreElements();) {
          Object obj = appenders.nextElement();
          if (obj instanceof AppenderSkeleton) {
            AppenderSkeleton appender = (AppenderSkeleton) obj;
            appender.setThreshold(appenderLevels.get(appender.getName()));
          }
        }
        logFlag = false;
        LOG.info("Done dumping adhoc logs for " + name);
      }
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/util/TestAdHocLogDumper.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/util/TestAdHocLogDumper.java

package org.apache.hadoop.yarn.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class TestAdHocLogDumper {

  private static final Log LOG = LogFactory.getLog(TestAdHocLogDumper.class);

  @Test
  public void testDumpingSchedulerLogs() throws Exception {

    Map<Appender, Priority> levels = new HashMap<>();
    String logHierarchy = TestAdHocLogDumper.class.getName();
    String logFilename = "test.log";
    Log log = LogFactory.getLog(logHierarchy);
    if (log instanceof Log4JLogger) {
      for (Enumeration appenders = Logger.getRootLogger().getAllAppenders(); appenders
        .hasMoreElements();) {
        Object obj = appenders.nextElement();
        if (obj instanceof AppenderSkeleton) {
          AppenderSkeleton appender = (AppenderSkeleton) obj;
          levels.put(appender, appender.getThreshold());
        }
      }
    }

    AdHocLogDumper dumper = new AdHocLogDumper(logHierarchy, logFilename);
    dumper.dumpLogs("DEBUG", 1000);
    LOG.debug("test message 1");
    LOG.info("test message 2");
    File logFile = new File(logFilename);
    Assert.assertTrue(logFile.exists());
    Thread.sleep(2000);
    long lastWrite = logFile.lastModified();
    Assert.assertTrue(lastWrite < Time.now());
    Assert.assertTrue(logFile.length() != 0);

    if (log instanceof Log4JLogger) {
      for (Enumeration appenders = Logger.getRootLogger().getAllAppenders(); appenders
        .hasMoreElements();) {
        Object obj = appenders.nextElement();
        if (obj instanceof AppenderSkeleton) {
          AppenderSkeleton appender = (AppenderSkeleton) obj;
          Assert.assertEquals(levels.get(appender), appender.getThreshold());
        }
      }
    }
    boolean del = logFile.delete();
    if(!del) {
      LOG.info("Couldn't clean up after test");
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/CapacitySchedulerPage.java
    @Override
    public void render(Block html) {
      html._(MetricsOverviewTable.class);
      html.div()
          .button()
          .$onclick("confirmAction()").b("Dump scheduler logs")._()
          .select().$id("time")
            .option().$value("60")._("1 min")._()
            .option().$value("300")._("5 min")._()
            .option().$value("600")._("10 min")._()
          ._()._();

      StringBuilder script = new StringBuilder();
      script.append("function confirmAction() {")
          .append(" b = confirm(\"Are you sure you wish to generate scheduler logs?\");")
          .append(" if (b == true) {")
          .append(" var timePeriod = $(\"#time\").val();")
          .append(" $.ajax({")
          .append(" type: 'POST',")
          .append(" url: '/ws/v1/cluster/scheduler/logs',")
          .append(" contentType: 'text/plain',")
          .append(" data: 'time=' + timePeriod,")
          .append(" dataType: 'text'")
          .append(" }).done(function(data){")
          .append(" setTimeout(function(){")
          .append(" alert(\"Scheduler log is being generated.\");")
          .append(" }, 1000);")
          .append(" }).fail(function(data){")
          .append(" alert(\"Scheduler log generation failed. Please check the ResourceManager log for more informtion.\");")
          .append(" console.log(data);")
          .append(" });")
          .append(" }")
          .append("}");

      html.script().$type("text/javascript")._(script.toString())._();

      UL<DIV<DIV<Hamlet>>> ul = html.
        div("#cs-wrapper.ui-widget").
          div(".ui-widget-header.ui-corner-top").

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMWebServices.java
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.AdHocLogDumper;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
    return new SchedulerTypeInfo(sinfo);
  }

  @POST
  @Path("/scheduler/logs")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public String dumpSchedulerLogs(@FormParam("time") String time) throws IOException {
    init();
    ResourceScheduler rs = rm.getResourceScheduler();
    int period = Integer.parseInt(time);
    if (period <= 0) {
      throw new BadRequestException("Period must be greater than 0");
    }
    final String logHierarchy =
        "org.apache.hadoop.yarn.server.resourcemanager.scheduler";
    String logfile = "yarn-scheduler-debug.log";
    if (rs instanceof CapacityScheduler) {
      logfile = "yarn-capacity-scheduler-debug.log";
    } else if (rs instanceof FairScheduler) {
      logfile = "yarn-fair-scheduler-debug.log";
    }
    AdHocLogDumper dumper = new AdHocLogDumper(logHierarchy, logfile);
    dumper.dumpLogs("DEBUG", period * 1000);
    return "Capacity scheduler logs are being created.";
  }


