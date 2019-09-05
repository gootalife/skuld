hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/ApplicationReport.java
    return report;
  }

  @Private
  @Unstable
  public static ApplicationReport newInstance(ApplicationId applicationId,
      ApplicationAttemptId applicationAttemptId, String user, String queue,
      String name, String host, int rpcPort, Token clientToAMToken,
      YarnApplicationState state, String diagnostics, String url,
      long startTime, long finishTime, FinalApplicationStatus finalStatus,
      ApplicationResourceUsageReport appResources, String origTrackingUrl,
      float progress, String applicationType, Token amRmToken,
      Set<String> tags) {
    ApplicationReport report =
        newInstance(applicationId, applicationAttemptId, user, queue, name,
          host, rpcPort, clientToAMToken, state, diagnostics, url, startTime,
          finishTime, finalStatus, appResources, origTrackingUrl, progress,
          applicationType, amRmToken);
    report.setApplicationTags(tags);
    return report;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/ApplicationHistoryManagerOnTimelineStore.java
package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
    FinalApplicationStatus finalStatus = FinalApplicationStatus.UNDEFINED;
    YarnApplicationState state = YarnApplicationState.ACCEPTED;
    ApplicationResourceUsageReport appResources = null;
    Set<String> appTags = null;
    Map<ApplicationAccessType, String> appViewACLs =
        new HashMap<ApplicationAccessType, String>();
    Map<String, Object> entityInfo = entity.getOtherInfo();
            ConverterUtils.toApplicationId(entity.getEntityId()),
            latestApplicationAttemptId, user, queue, name, null, -1, null, state,
            diagnosticsInfo, null, createdTime, finishedTime, finalStatus, null,
            null, progress, type, null, appTags), appViewACLs);
      }
      if (entityInfo.containsKey(ApplicationMetricsConstants.QUEUE_ENTITY_INFO)) {
        queue =
        appResources=ApplicationResourceUsageReport
            .newInstance(0, 0, null, null, null, memorySeconds, vcoreSeconds);
      }
      if (entityInfo.containsKey(ApplicationMetricsConstants.APP_TAGS_INFO)) {
        appTags = new HashSet<String>();
        Object obj = entityInfo.get(ApplicationMetricsConstants.APP_TAGS_INFO);
        if (obj != null && obj instanceof Collection<?>) {
          for(Object o : (Collection<?>)obj) {
            if (o != null) {
              appTags.add(o.toString());
            }
          }
        }
      }
    }
    List<TimelineEvent> events = entity.getEvents();
    if (events != null) {
        ConverterUtils.toApplicationId(entity.getEntityId()),
        latestApplicationAttemptId, user, queue, name, null, -1, null, state,
        diagnosticsInfo, null, createdTime, finishedTime, finalStatus, appResources,
        null, progress, type, null, appTags), appViewACLs);
  }

  private static ApplicationAttemptReport convertToApplicationAttemptReport(

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/applicationhistoryservice/TestApplicationHistoryManagerOnTimelineStore.java
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
      Assert.assertEquals(Integer.MAX_VALUE + 3L
          + +app.getApplicationId().getId(), app.getFinishTime());
      Assert.assertTrue(Math.abs(app.getProgress() - 1.0F) < 0.0001);
      Assert.assertEquals(2, app.getApplicationTags().size());
      Assert.assertTrue(app.getApplicationTags().contains("Test_APP_TAGS_1"));
      Assert.assertTrue(app.getApplicationTags().contains("Test_APP_TAGS_2"));
      if ((i ==  1 && callerUGI != null &&
      entityInfo.put(ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO,
          "user2");
    }
    Set<String> appTags = new HashSet<String>();
    appTags.add("Test_APP_TAGS_1");
    appTags.add("Test_APP_TAGS_2");
    entityInfo.put(ApplicationMetricsConstants.APP_TAGS_INFO, appTags);
    entity.setOtherInfo(entityInfo);
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.CREATED_EVENT_TYPE);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/metrics/ApplicationMetricsConstants.java
  public static final String LATEST_APP_ATTEMPT_EVENT_INFO =
      "YARN_APPLICATION_LATEST_APP_ATTEMPT";

  public static final String APP_TAGS_INFO = "YARN_APPLICATION_TAGS";
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/metrics/ApplicationCreatedEvent.java

package org.apache.hadoop.yarn.server.resourcemanager.metrics;

import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public class ApplicationCreatedEvent extends
  private String user;
  private String queue;
  private long submittedTime;
  private Set<String> appTags;

  public ApplicationCreatedEvent(ApplicationId appId,
      String name,
      String user,
      String queue,
      long submittedTime,
      long createdTime,
      Set<String> appTags) {
    super(SystemMetricsEventType.APP_CREATED, createdTime);
    this.appId = appId;
    this.name = name;
    this.user = user;
    this.queue = queue;
    this.submittedTime = submittedTime;
    this.appTags = appTags;
  }

  @Override
    return submittedTime;
  }

  public Set<String> getAppTags() {
    return appTags;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/metrics/SystemMetricsPublisher.java
              app.getUser(),
              app.getQueue(),
              app.getSubmitTime(),
              createdTime, app.getApplicationTags()));
    }
  }

        event.getQueue());
    entityInfo.put(ApplicationMetricsConstants.SUBMITTED_TIME_ENTITY_INFO,
        event.getSubmittedTime());
    entityInfo.put(ApplicationMetricsConstants.APP_TAGS_INFO,
        event.getAppTags());
    entity.setOtherInfo(entityInfo);
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/metrics/TestSystemMetricsPublisher.java
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
      Assert.assertEquals(app.getSubmitTime(),
          entity.getOtherInfo().get(
              ApplicationMetricsConstants.SUBMITTED_TIME_ENTITY_INFO));
      Assert.assertTrue(verifyAppTags(app.getApplicationTags(),
          entity.getOtherInfo()));
      if (i == 1) {
        Assert.assertEquals("uers1,user2",
            entity.getOtherInfo().get(
        FinalApplicationStatus.UNDEFINED);
    when(app.getRMAppMetrics()).thenReturn(
        new RMAppMetrics(null, 0, 0, Integer.MAX_VALUE, Long.MAX_VALUE));
    Set<String> appTags = new HashSet<String>();
    appTags.add("test");
    appTags.add("tags");
    when(app.getApplicationTags()).thenReturn(appTags);
    return app;
  }

    return container;
  }

  private static boolean verifyAppTags(Set<String> appTags,
      Map<String, Object> entityInfo) {
    if (!entityInfo.containsKey(ApplicationMetricsConstants.APP_TAGS_INFO)) {
      return false;
    }
    Object obj = entityInfo.get(ApplicationMetricsConstants.APP_TAGS_INFO);
    if (obj instanceof Collection<?>) {
      Collection<?> collection = (Collection<?>) obj;
      if (collection.size() != appTags.size()) {
        return false;
      }
      for (String appTag : appTags) {
        boolean match = false;
        for (Object o : collection) {
          if (o.toString().equals(appTag)) {
            match = true;
            break;
          }
        }
        if (!match) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}

