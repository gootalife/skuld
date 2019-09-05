hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/ApplicationHistoryManagerOnTimelineStore.java
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
public class ApplicationHistoryManagerOnTimelineStore extends AbstractService
    implements
    ApplicationHistoryManager {
  private static final Log LOG = LogFactory
      .getLog(ApplicationHistoryManagerOnTimelineStore.class);

  @VisibleForTesting
  static final String UNAVAILABLE = "N/A";
        new LinkedHashMap<ApplicationId, ApplicationReport>();
    if (entities != null && entities.getEntities() != null) {
      for (TimelineEntity entity : entities.getEntities()) {
        try {
          ApplicationReportExt app =
              generateApplicationReport(entity, ApplicationReportField.ALL);
          apps.put(app.appReport.getApplicationId(), app.appReport);
        } catch (Exception e) {
          LOG.error("Error on generating application report for " +
              entity.getEntityId(), e);
        }
      }
    }
    return apps;
  @Override
  public ApplicationAttemptReport getApplicationAttempt(
      ApplicationAttemptId appAttemptId) throws YarnException, IOException {
    return getApplicationAttempt(appAttemptId, true);
  }

  private ApplicationAttemptReport getApplicationAttempt(
      ApplicationAttemptId appAttemptId, boolean checkACLs)
      throws YarnException, IOException {
    if (checkACLs) {
      ApplicationReportExt app = getApplication(
          appAttemptId.getApplicationId(),
          ApplicationReportField.USER_AND_ACLS);
      checkAccess(app);
    }
    TimelineEntity entity = timelineDataManager.getEntity(
        AppAttemptMetricsConstants.ENTITY_TYPE,
        appAttemptId.toString(), EnumSet.allOf(Field.class),
  @Override
  public ContainerReport getAMContainer(ApplicationAttemptId appAttemptId)
      throws YarnException, IOException {
    ApplicationAttemptReport appAttempt =
        getApplicationAttempt(appAttemptId, false);
    return getContainer(appAttempt.getAMContainerId());
  }

    try {
      checkAccess(app);
      if (app.appReport.getCurrentApplicationAttemptId() != null) {
        ApplicationAttemptReport appAttempt = getApplicationAttempt(
            app.appReport.getCurrentApplicationAttemptId(), false);
        app.appReport.setHost(appAttempt.getHost());
        app.appReport.setRpcPort(appAttempt.getRpcPort());
        app.appReport.setTrackingUrl(appAttempt.getTrackingUrl());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/applicationhistoryservice/TestApplicationHistoryManagerOnTimelineStore.java
    store = createStore(SCALE);
    TimelineEntities entities = new TimelineEntities();
    entities.addEntity(createApplicationTimelineEntity(
        ApplicationId.newInstance(0, SCALE + 1), true, true, false));
    entities.addEntity(createApplicationTimelineEntity(
        ApplicationId.newInstance(0, SCALE + 2), true, false, true));
    store.put(entities);
  }

      TimelineEntities entities = new TimelineEntities();
      ApplicationId appId = ApplicationId.newInstance(0, i);
      if (i == 2) {
        entities.addEntity(createApplicationTimelineEntity(
            appId, true, false, false));
      } else {
        entities.addEntity(createApplicationTimelineEntity(
            appId, false, false, false));
      }
      store.put(entities);
      for (int j = 1; j <= scale; ++j) {
        historyManager.getAllApplications().values();
    Assert.assertNotNull(apps);
    Assert.assertEquals(SCALE + 1, apps.size());
    ApplicationId ignoredAppId = ApplicationId.newInstance(0, SCALE + 2);
    for (ApplicationReport app : apps) {
      Assert.assertNotEquals(ignoredAppId, app.getApplicationId());
    }
  }

  @Test
  }

  private static TimelineEntity createApplicationTimelineEntity(
      ApplicationId appId, boolean emptyACLs, boolean noAttemptId,
      boolean wrongAppId) {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityType(ApplicationMetricsConstants.ENTITY_TYPE);
    if (wrongAppId) {
      entity.setEntityId("wrong_app_id");
    } else {
      entity.setEntityId(appId.toString());
    }
    entity.setDomainId(TimelineDataManager.DEFAULT_DOMAIN_ID);
    entity.addPrimaryFilter(
        TimelineStore.SystemFilter.ENTITY_OWNER.toString(), "yarn");
        FinalApplicationStatus.UNDEFINED.toString());
    eventInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO,
        YarnApplicationState.FINISHED.toString());
    if (!noAttemptId) {
      eventInfo.put(ApplicationMetricsConstants.LATEST_APP_ATTEMPT_EVENT_INFO,
          ApplicationAttemptId.newInstance(appId, 1));
    }

