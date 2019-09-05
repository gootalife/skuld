hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
  public static final String DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS =
      "0.0.0.0:" + DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_PORT;

  public static final String APPLICATION_HISTORY_PREFIX_MAX_APPS =
      APPLICATION_HISTORY_PREFIX + "max-applications";
  public static final long DEFAULT_APPLICATION_HISTORY_PREFIX_MAX_APPS = 10000;

  public static final String TIMELINE_SERVICE_STORE =
      TIMELINE_SERVICE_PREFIX + "store-class";

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/webapp/YarnWebParams.java
  String ENTITY_STRING = "entity.string";
  String APP_OWNER = "app.owner";
  String APP_STATE = "app.state";
  String APPS_NUM = "apps.num";
  String QUEUE_NAME = "queue.name";
  String NODE_STATE = "node.state";
  String NODE_LABEL = "node.label";

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/ApplicationHistoryClientService.java
          IOException {
    GetApplicationsResponse response =
        GetApplicationsResponse.newInstance(new ArrayList<ApplicationReport>(
          history.getApplications(request.getLimit()).values()));
    return response;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/ApplicationHistoryManager.java
      IOException;

  @Public
  @Unstable
  Map<ApplicationId, ApplicationReport>
      getApplications(long appsNum) throws YarnException,
          IOException;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/ApplicationHistoryManagerImpl.java
  }

  @Override
  public Map<ApplicationId, ApplicationReport> getApplications(long appsNum)
      throws IOException {
    Map<ApplicationId, ApplicationHistoryData> histData =
        historyStore.getAllApplications();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/ApplicationHistoryManagerOnTimelineStore.java
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
  private TimelineDataManager timelineDataManager;
  private ApplicationACLsManager aclsManager;
  private String serverHttpAddress;
  private long maxLoadedApplications;

  public ApplicationHistoryManagerOnTimelineStore(
      TimelineDataManager timelineDataManager,
  protected void serviceInit(Configuration conf) throws Exception {
    serverHttpAddress = WebAppUtils.getHttpSchemePrefix(conf) +
        WebAppUtils.getAHSWebAppURLWithoutScheme(conf);
    maxLoadedApplications =
        conf.getLong(YarnConfiguration.APPLICATION_HISTORY_PREFIX_MAX_APPS,
          YarnConfiguration.DEFAULT_APPLICATION_HISTORY_PREFIX_MAX_APPS);
    super.serviceInit(conf);
  }

  }

  @Override
  public Map<ApplicationId, ApplicationReport> getApplications(long appsNum)
      throws YarnException, IOException {
    TimelineEntities entities = timelineDataManager.getEntities(
        ApplicationMetricsConstants.ENTITY_TYPE, null, null, null, null, null,
        null, appsNum == Long.MAX_VALUE ? this.maxLoadedApplications : appsNum,
        EnumSet.allOf(Field.class), UserGroupInformation.getLoginUser());
    Map<ApplicationId, ApplicationReport> apps =
        new LinkedHashMap<ApplicationId, ApplicationReport>();
    if (entities != null && entities.getEntities() != null) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/applicationhistoryservice/TestApplicationHistoryClientService.java
public class TestApplicationHistoryClientService {

  private static ApplicationHistoryClientService clientService;
  private static TimelineDataManager dataManager;

  @BeforeClass
  public static void setup() throws Exception {
    TimelineStore store =
        TestApplicationHistoryManagerOnTimelineStore.createStore(2);
    TimelineACLsManager aclsManager = new TimelineACLsManager(conf);
    dataManager =
        new TimelineDataManager(store, aclsManager);
    ApplicationACLsManager appAclsManager = new ApplicationACLsManager(conf);
    ApplicationHistoryManagerOnTimelineStore historyManager =
        clientService.getApplications(request);
    List<ApplicationReport> appReport = response.getApplicationList();
    Assert.assertNotNull(appReport);
    Assert.assertEquals(appId, appReport.get(1).getApplicationId());
    Assert.assertEquals(appId1, appReport.get(0).getApplicationId());

    Configuration conf = new YarnConfiguration();
    conf.setLong(YarnConfiguration.APPLICATION_HISTORY_PREFIX_MAX_APPS, 1);
    ApplicationHistoryManagerOnTimelineStore historyManager2 =
        new ApplicationHistoryManagerOnTimelineStore(dataManager,
          new ApplicationACLsManager(conf));
    historyManager2.init(conf);
    historyManager2.start();
    @SuppressWarnings("resource")
    ApplicationHistoryClientService clientService2 =
        new ApplicationHistoryClientService(historyManager2);
    response = clientService2.getApplications(request);
    appReport = response.getApplicationList();
    Assert.assertNotNull(appReport);
    Assert.assertTrue(appReport.size() == 1);
    Assert.assertEquals(appId1, appReport.get(0).getApplicationId());
  }

  @Test

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/applicationhistoryservice/TestApplicationHistoryManagerOnTimelineStore.java
      Assert.assertEquals("test app type", app.getApplicationType());
      Assert.assertEquals("user1", app.getUser());
      Assert.assertEquals("test queue", app.getQueue());
      Assert.assertEquals(Integer.MAX_VALUE + 2L
          + app.getApplicationId().getId(), app.getStartTime());
      Assert.assertEquals(Integer.MAX_VALUE + 3L
          + +app.getApplicationId().getId(), app.getFinishTime());
      Assert.assertTrue(Math.abs(app.getProgress() - 1.0F) < 0.0001);
  @Test
  public void testGetApplications() throws Exception {
    Collection<ApplicationReport> apps =
        historyManager.getApplications(Long.MAX_VALUE).values();
    Assert.assertNotNull(apps);
    Assert.assertEquals(SCALE + 1, apps.size());
    ApplicationId ignoredAppId = ApplicationId.newInstance(0, SCALE + 2);
    entity.setOtherInfo(entityInfo);
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    tEvent.setTimestamp(Integer.MAX_VALUE + 2L + appId.getId());
    entity.addEvent(tEvent);
    tEvent = new TimelineEvent();
    tEvent.setEventType(
        ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(Integer.MAX_VALUE + 3L + appId.getId());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO,
        "test diagnostics info");

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/AppsBlock.java

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_STATE;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APPS_NUM;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR_VALUE;

    callerUGI = getCallerUGI();
    final GetApplicationsRequest request =
        GetApplicationsRequest.newInstance(reqAppStates);
    String appsNumStr = $(APPS_NUM);
    if (appsNumStr != null && !appsNumStr.isEmpty()) {
      long appsNum = Long.parseLong(appsNumStr);
      request.setLimit(appsNum);
    }
    if (callerUGI == null) {
      appReports = appBaseProt.getApplications(request).getApplicationList();
    } else {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/WebServices.java
      String startedEnd, String finishBegin, String finishEnd,
      Set<String> applicationTypes) {
    UserGroupInformation callerUGI = getUser(req);
    boolean checkStart = false;
    boolean checkEnd = false;
    boolean checkAppTypes = false;
    boolean checkAppStates = false;
    long countNum = Long.MAX_VALUE;

    long sBegin = 0;
    long fEnd = Long.MAX_VALUE;

    if (count != null && !count.isEmpty()) {
      countNum = Long.parseLong(count);
      if (countNum <= 0) {
        throw new BadRequestException("limit value must be greater then 0");

    AppsInfo allApps = new AppsInfo();
    Collection<ApplicationReport> appReports = null;
    final GetApplicationsRequest request =
        GetApplicationsRequest.newInstance();
    request.setLimit(countNum);
    try {
      if (callerUGI == null) {
        appReports = appBaseProt.getApplications(request).getApplicationList();
      } else {
        appReports = callerUGI.doAs(
            new PrivilegedExceptionAction<Collection<ApplicationReport>> () {
          @Override
          public Collection<ApplicationReport> run() throws Exception {
            return appBaseProt.getApplications(request).getApplicationList();
          }
        });
      }
    }
    for (ApplicationReport appReport : appReports) {

      if (checkAppStates &&
          !appStates.contains(StringUtils.toLowerCase(
              appReport.getYarnApplicationState().toString()))) {
      AppInfo app = new AppInfo(appReport);

      allApps.add(app);
    }
    return allApps;
  }

