hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/main/java/org/apache/hadoop/mapred/NotRunningJob.java
    return ApplicationReport.newInstance(unknownAppId, unknownAttemptId,
      "N/A", "N/A", "N/A", "N/A", 0, null, YarnApplicationState.NEW, "N/A",
      "N/A", 0, 0, FinalApplicationStatus.UNDEFINED, null, "N/A", 0.0f,
      YarnConfiguration.DEFAULT_APPLICATION_TYPE, null);
  }

  NotRunningJob(ApplicationReport applicationReport, JobState jobState) {

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapred/TestClientServiceDelegate.java
    return ApplicationReport.newInstance(appId, attemptId, "user", "queue",
      "appname", "host", 124, null, YarnApplicationState.FINISHED,
      "diagnostics", "url", 0, 0, FinalApplicationStatus.SUCCEEDED, null,
      "N/A", 0.0f, YarnConfiguration.DEFAULT_APPLICATION_TYPE, null);
  }

  private ApplicationReport getRunningApplicationReport(String host, int port) {
    return ApplicationReport.newInstance(appId, attemptId, "user", "queue",
      "appname", host, port, null, YarnApplicationState.RUNNING, "diagnostics",
      "url", 0, 0, FinalApplicationStatus.UNDEFINED, null, "N/A", 0.0f,
      YarnConfiguration.DEFAULT_APPLICATION_TYPE, null);
  }

  private ResourceMgrDelegate getRMDelegate() throws IOException {

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapred/TestYARNRunner.java
            ApplicationReport.newInstance(appId, null, "tmp", "tmp", "tmp",
                "tmp", 0, null, YarnApplicationState.FINISHED, "tmp", "tmp",
                0l, 0l, FinalApplicationStatus.SUCCEEDED, null, null, 0f,
                "tmp", null));
    yarnRunner.killJob(jobId);
    verify(clientDelegate).killJob(jobId);
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/ApplicationReport.java
      YarnApplicationState state, String diagnostics, String url,
      long startTime, long finishTime, FinalApplicationStatus finalStatus,
      ApplicationResourceUsageReport appResources, String origTrackingUrl,
      float progress, String applicationType, Token amRmToken) {
    ApplicationReport report = Records.newRecord(ApplicationReport.class);
    report.setApplicationId(applicationId);
    report.setCurrentApplicationAttemptId(applicationAttemptId);
    report.setProgress(progress);
    report.setApplicationType(applicationType);
    report.setAMRMToken(amRmToken);
    return report;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/ProtocolHATestBase.java
              "fakeQueue", "fakeApplicationName", "localhost", 0, null,
              YarnApplicationState.FINISHED, "fake an application report", "",
              1000l, 1200l, FinalApplicationStatus.FAILED, null, "", 50f,
              "fakeApplicationType", null);
      return report;
    }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/api/impl/TestAHSClient.java
            "queue", "appname", "host", 124, null,
            YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0,
            FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN",
            null);
      List<ApplicationReport> applicationReports =
          new ArrayList<ApplicationReport>();
      applicationReports.add(newApplicationReport);
            "queue2", "appname2", "host2", 125, null,
            YarnApplicationState.FINISHED, "diagnostics2", "url2", 2, 2,
            FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.63789f,
            "NON-YARN", null);
      applicationReports.add(newApplicationReport2);

      ApplicationId applicationId3 = ApplicationId.newInstance(1234, 7);
            "queue3", "appname3", "host3", 126, null,
            YarnApplicationState.RUNNING, "diagnostics3", "url3", 3, 3,
            FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.73789f,
            "MAPREDUCE", null);
      applicationReports.add(newApplicationReport3);

      ApplicationId applicationId4 = ApplicationId.newInstance(1234, 8);
            "queue4", "appname4", "host4", 127, null,
            YarnApplicationState.FAILED, "diagnostics4", "url4", 4, 4,
            FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.83789f,
            "NON-MAPREDUCE", null);
      applicationReports.add(newApplicationReport4);
      reports = applicationReports;
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/api/impl/TestYarnClient.java
          applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
          "user", "queue", "appname", "host", 124, null,
          YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0,
          FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
      List<ApplicationReport> applicationReports = new ArrayList<ApplicationReport>();
      applicationReports.add(newApplicationReport);
      List<ApplicationAttemptReport> appAttempts = new ArrayList<ApplicationAttemptReport>();
          "user2", "queue2", "appname2", "host2", 125, null,
          YarnApplicationState.FINISHED, "diagnostics2", "url2", 2, 2,
          FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.63789f, "NON-YARN", 
        null);
      applicationReports.add(newApplicationReport2);

      ApplicationId applicationId3 = ApplicationId.newInstance(1234, 7);
          "user3", "queue3", "appname3", "host3", 126, null,
          YarnApplicationState.RUNNING, "diagnostics3", "url3", 3, 3,
          FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.73789f, "MAPREDUCE",
        null);
      applicationReports.add(newApplicationReport3);

      ApplicationId applicationId4 = ApplicationId.newInstance(1234, 8);
              "user4", "queue4", "appname4", "host4", 127, null,
              YarnApplicationState.FAILED, "diagnostics4", "url4", 4, 4,
              FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.83789f,
              "NON-MAPREDUCE", null);
      applicationReports.add(newApplicationReport4);
      return applicationReports;
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/cli/TestYarnCLI.java
          "user", "queue", "appname", "host", 124, null,
          YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0,
          FinalApplicationStatus.SUCCEEDED, usageReport, "N/A", 0.53789f, "YARN",
          null);
      newApplicationReport.setLogAggregationStatus(LogAggregationStatus.SUCCEEDED);
      when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
          newApplicationReport);
        applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
    List<ApplicationReport> applicationReports = new ArrayList<ApplicationReport>();
    applicationReports.add(newApplicationReport);

        "user2", "queue2", "appname2", "host2", 125, null,
        YarnApplicationState.FINISHED, "diagnostics2", "url2", 2, 2,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.63789f, "NON-YARN", 
      null);
    applicationReports.add(newApplicationReport2);

    ApplicationId applicationId3 = ApplicationId.newInstance(1234, 7);
        "user3", "queue3", "appname3", "host3", 126, null,
        YarnApplicationState.RUNNING, "diagnostics3", "url3", 3, 3,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.73789f, "MAPREDUCE", 
        null);
    applicationReports.add(newApplicationReport3);

    ApplicationId applicationId4 = ApplicationId.newInstance(1234, 8);
        "user4", "queue4", "appname4", "host4", 127, null,
        YarnApplicationState.FAILED, "diagnostics4", "url4", 4, 4,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.83789f, "NON-MAPREDUCE",
        null);
    applicationReports.add(newApplicationReport4);

    ApplicationId applicationId5 = ApplicationId.newInstance(1234, 9);
        "user5", "queue5", "appname5", "host5", 128, null,
        YarnApplicationState.ACCEPTED, "diagnostics5", "url5", 5, 5,
        FinalApplicationStatus.KILLED, null, "N/A", 0.93789f, "HIVE",
        null);
    applicationReports.add(newApplicationReport5);

    ApplicationId applicationId6 = ApplicationId.newInstance(1234, 10);
        "user6", "queue6", "appname6", "host6", 129, null,
        YarnApplicationState.SUBMITTED, "diagnostics6", "url6", 6, 6,
        FinalApplicationStatus.KILLED, null, "N/A", 0.99789f, "PIG",
        null);
    applicationReports.add(newApplicationReport6);

        applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
    when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
        newApplicationReport2);
    int result = cli.run(new String[] { "application","-kill", applicationId.toString() });
        applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
    when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
        newApplicationReport);
    result = cli.run(new String[] { "application","-kill", applicationId.toString() });
        applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
    when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
        newApplicationReport2);
    int result = cli.run(new String[] { "application", "-movetoqueue",
        applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
    when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
        newApplicationReport);
    result = cli.run(new String[] { "application", "-movetoqueue",

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/api/TestApplicatonReport.java
        ApplicationReport.newInstance(appId, appAttemptId, "user", "queue",
          "appname", "host", 124, null, YarnApplicationState.FINISHED,
          "diagnostics", "url", 0, 0, FinalApplicationStatus.SUCCEEDED, null,
          "N/A", 0.53789f, YarnConfiguration.DEFAULT_APPLICATION_TYPE, null);
    return appReport;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/ApplicationHistoryManagerImpl.java
      appHistory.getYarnApplicationState(), appHistory.getDiagnosticsInfo(),
      trackingUrl, appHistory.getStartTime(), appHistory.getFinishTime(),
      appHistory.getFinalApplicationStatus(), null, "", 100,
      appHistory.getApplicationType(), null);
  }

  private ApplicationAttemptHistoryData getLastAttempt(ApplicationId appId)

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/ApplicationHistoryManagerOnTimelineStore.java
package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
    FinalApplicationStatus finalStatus = FinalApplicationStatus.UNDEFINED;
    YarnApplicationState state = YarnApplicationState.ACCEPTED;
    ApplicationResourceUsageReport appResources = null;
    Map<ApplicationAccessType, String> appViewACLs =
        new HashMap<ApplicationAccessType, String>();
    Map<String, Object> entityInfo = entity.getOtherInfo();
            ConverterUtils.toApplicationId(entity.getEntityId()),
            latestApplicationAttemptId, user, queue, name, null, -1, null, state,
            diagnosticsInfo, null, createdTime, finishedTime, finalStatus, null,
            null, progress, type, null), appViewACLs);
      }
      if (entityInfo.containsKey(ApplicationMetricsConstants.QUEUE_ENTITY_INFO)) {
        queue =
        appResources=ApplicationResourceUsageReport
            .newInstance(0, 0, null, null, null, memorySeconds, vcoreSeconds);
      }
    }
    List<TimelineEvent> events = entity.getEvents();
    if (events != null) {
        ConverterUtils.toApplicationId(entity.getEntityId()),
        latestApplicationAttemptId, user, queue, name, null, -1, null, state,
        diagnosticsInfo, null, createdTime, finishedTime, finalStatus, appResources,
        null, progress, type, null), appViewACLs);
  }

  private static ApplicationAttemptReport convertToApplicationAttemptReport(

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/applicationhistoryservice/TestApplicationHistoryManagerOnTimelineStore.java
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
      Assert.assertEquals(Integer.MAX_VALUE + 3L
          + +app.getApplicationId().getId(), app.getFinishTime());
      Assert.assertTrue(Math.abs(app.getProgress() - 1.0F) < 0.0001);
      if ((i ==  1 && callerUGI != null &&
      entityInfo.put(ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO,
          "user2");
    }
    entity.setOtherInfo(entityInfo);
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.CREATED_EVENT_TYPE);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/metrics/ApplicationMetricsConstants.java
  public static final String LATEST_APP_ATTEMPT_EVENT_INFO =
      "YARN_APPLICATION_LATEST_APP_ATTEMPT";

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/metrics/ApplicationCreatedEvent.java

package org.apache.hadoop.yarn.server.resourcemanager.metrics;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public class ApplicationCreatedEvent extends
  private String user;
  private String queue;
  private long submittedTime;

  public ApplicationCreatedEvent(ApplicationId appId,
      String name,
      String user,
      String queue,
      long submittedTime,
      long createdTime) {
    super(SystemMetricsEventType.APP_CREATED, createdTime);
    this.appId = appId;
    this.name = name;
    this.user = user;
    this.queue = queue;
    this.submittedTime = submittedTime;
  }

  @Override
    return submittedTime;
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/metrics/SystemMetricsPublisher.java
              app.getUser(),
              app.getQueue(),
              app.getSubmitTime(),
              createdTime));
    }
  }

        event.getQueue());
    entityInfo.put(ApplicationMetricsConstants.SUBMITTED_TIME_ENTITY_INFO,
        event.getSubmittedTime());
    entity.setOtherInfo(entityInfo);
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/applicationsmanager/MockAsm.java
            getName(), null, 0, null, null, getDiagnostics().toString(), 
            getTrackingUrl(), getStartTime(), getFinishTime(), 
            getFinalApplicationStatus(), usageReport , null, getProgress(),
            type, null);
        return report;
      }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/metrics/TestSystemMetricsPublisher.java
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.EnumSet;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
      Assert.assertEquals(app.getSubmitTime(),
          entity.getOtherInfo().get(
              ApplicationMetricsConstants.SUBMITTED_TIME_ENTITY_INFO));
      if (i == 1) {
        Assert.assertEquals("uers1,user2",
            entity.getOtherInfo().get(
        FinalApplicationStatus.UNDEFINED);
    when(app.getRMAppMetrics()).thenReturn(
        new RMAppMetrics(null, 0, 0, Integer.MAX_VALUE, Long.MAX_VALUE));
    return app;
  }

    return container;
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebApp.java
              app.getStartTime(), app.getFinishTime(),
              app.getFinalApplicationStatus(),
              (ApplicationResourceUsageReport) null, app.getTrackingUrl(),
              app.getProgress(), app.getApplicationType(), (Token) null);
      appReports.add(appReport);
    }
    GetApplicationsResponse response = mock(GetApplicationsResponse.class);

