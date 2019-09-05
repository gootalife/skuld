hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/webapp/YarnWebParams.java
  String ENTITY_STRING = "entity.string";
  String APP_OWNER = "app.owner";
  String APP_STATE = "app.state";
  String APP_START_TIME_BEGIN = "app.started-time.begin";
  String APP_START_TIME_END = "app.started-time.end";
  String APPS_NUM = "apps.num";
  String QUEUE_NAME = "queue.name";
  String NODE_STATE = "node.state";

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/ApplicationHistoryClientService.java
  public GetApplicationsResponse
      getApplications(GetApplicationsRequest request) throws YarnException,
          IOException {
    long startedBegin =
        request.getStartRange() == null ? 0L : request.getStartRange()
          .getMinimumLong();
    long startedEnd =
        request.getStartRange() == null ? Long.MAX_VALUE : request
          .getStartRange().getMaximumLong();
    GetApplicationsResponse response =
        GetApplicationsResponse.newInstance(new ArrayList<ApplicationReport>(
          history.getApplications(request.getLimit(), startedBegin, startedEnd)
            .values()));
    return response;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/ApplicationHistoryManager.java
      IOException;

  @Public
  @Unstable
  Map<ApplicationId, ApplicationReport> getApplications(long appsNum,
      long appStartedTimeBegin, long appStartedTimeEnd) throws YarnException,
      IOException;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/ApplicationHistoryManagerImpl.java
  }

  @Override
  public Map<ApplicationId, ApplicationReport> getApplications(long appsNum,
      long appStartedTimeBegin, long appStartedTimeEnd) throws IOException {
    Map<ApplicationId, ApplicationHistoryData> histData =
        historyStore.getAllApplications();
    HashMap<ApplicationId, ApplicationReport> applicationsReport =

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/ApplicationHistoryManagerOnTimelineStore.java
  }

  @Override
  public Map<ApplicationId, ApplicationReport> getApplications(long appsNum,
      long appStartedTimeBegin, long appStartedTimeEnd) throws YarnException,
      IOException {
    TimelineEntities entities =
        timelineDataManager.getEntities(
          ApplicationMetricsConstants.ENTITY_TYPE, null, null,
          appStartedTimeBegin, appStartedTimeEnd, null, null,
          appsNum == Long.MAX_VALUE ? this.maxLoadedApplications : appsNum,
          EnumSet.allOf(Field.class), UserGroupInformation.getLoginUser());
    Map<ApplicationId, ApplicationReport> apps =
        new LinkedHashMap<ApplicationId, ApplicationReport>();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/applicationhistoryservice/TestApplicationHistoryManagerOnTimelineStore.java
  @Test
  public void testGetApplications() throws Exception {
    Collection<ApplicationReport> apps =
        historyManager.getApplications(Long.MAX_VALUE, 0L, Long.MAX_VALUE)
          .values();
    Assert.assertNotNull(apps);
    Assert.assertEquals(SCALE + 1, apps.size());
    ApplicationId ignoredAppId = ApplicationId.newInstance(0, SCALE + 2);
    for (ApplicationReport app : apps) {
      Assert.assertNotEquals(ignoredAppId, app.getApplicationId());
    }

    apps =
        historyManager.getApplications(Long.MAX_VALUE, 2147483653L,
          Long.MAX_VALUE).values();
    Assert.assertNotNull(apps);
    Assert.assertEquals(2, apps.size());
  }

  @Test

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/AppsBlock.java

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_STATE;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_START_TIME_BEGIN;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_START_TIME_END;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APPS_NUM;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR_VALUE;
import java.util.EnumSet;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
      long appsNum = Long.parseLong(appsNumStr);
      request.setLimit(appsNum);
    }

    String appStartedTimeBegainStr = $(APP_START_TIME_BEGIN);
    long appStartedTimeBegain = 0;
    if (appStartedTimeBegainStr != null && !appStartedTimeBegainStr.isEmpty()) {
      appStartedTimeBegain = Long.parseLong(appStartedTimeBegainStr);
      if (appStartedTimeBegain < 0) {
        throw new BadRequestException(
          "app.started-time.begin must be greater than 0");
      }
    }
    String appStartedTimeEndStr = $(APP_START_TIME_END);
    long appStartedTimeEnd = Long.MAX_VALUE;
    if (appStartedTimeEndStr != null && !appStartedTimeEndStr.isEmpty()) {
      appStartedTimeEnd = Long.parseLong(appStartedTimeEndStr);
      if (appStartedTimeEnd < 0) {
        throw new BadRequestException(
          "app.started-time.end must be greater than 0");
      }
    }
    if (appStartedTimeBegain > appStartedTimeEnd) {
      throw new BadRequestException(
        "app.started-time.end must be greater than app.started-time.begin");
    }
    request.setStartRange(
        new LongRange(appStartedTimeBegain, appStartedTimeEnd));

    if (callerUGI == null) {
      appReports = appBaseProt.getApplications(request).getApplicationList();
    } else {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/WebServices.java
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;

import org.apache.commons.lang.math.LongRange;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.util.StringUtils;
      String startedEnd, String finishBegin, String finishEnd,
      Set<String> applicationTypes) {
    UserGroupInformation callerUGI = getUser(req);
    boolean checkEnd = false;
    boolean checkAppTypes = false;
    boolean checkAppStates = false;
    }

    if (startedBegin != null && !startedBegin.isEmpty()) {
      sBegin = Long.parseLong(startedBegin);
      if (sBegin < 0) {
        throw new BadRequestException("startedTimeBegin must be greater than 0");
      }
    }
    if (startedEnd != null && !startedEnd.isEmpty()) {
      sEnd = Long.parseLong(startedEnd);
      if (sEnd < 0) {
        throw new BadRequestException("startedTimeEnd must be greater than 0");
    final GetApplicationsRequest request =
        GetApplicationsRequest.newInstance();
    request.setLimit(countNum);
    request.setStartRange(new LongRange(sBegin, sEnd));
    try {
      if (callerUGI == null) {
        continue;
      }

      if (checkEnd
          && (appReport.getFinishTime() < fBegin || appReport.getFinishTime() > fEnd)) {
        continue;

