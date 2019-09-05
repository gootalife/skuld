hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/timeline/webapp/TimelineWebServices.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/applicationhistoryservice/TestApplicationHistoryClientService.java
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;

  private static ApplicationHistoryClientService clientService;
  private static TimelineDataManager dataManager;
  private final static int MAX_APPS = 2;

  @BeforeClass
  public static void setup() throws Exception {
    Configuration conf = new YarnConfiguration();
    TimelineStore store =
        TestApplicationHistoryManagerOnTimelineStore.createStore(MAX_APPS);
    TimelineACLsManager aclsManager = new TimelineACLsManager(conf);
    dataManager =
        new TimelineDataManager(store, aclsManager);
    clientService = new ApplicationHistoryClientService(historyManager);
  }

  @Test
  public void testApplicationNotFound() throws IOException, YarnException {
    ApplicationId appId = null;
    appId = ApplicationId.newInstance(0, MAX_APPS + 1);
    GetApplicationReportRequest request =
        GetApplicationReportRequest.newInstance(appId);
    try {
      @SuppressWarnings("unused")
      GetApplicationReportResponse response =
          clientService.getApplicationReport(request);
      Assert.fail("Exception should have been thrown before we reach here.");
    } catch (ApplicationNotFoundException e) {
      Assert.assertTrue(e.getMessage().contains(
          "doesn't exist in the timeline store"));
    } catch (Exception e) {
      Assert.fail("Undesired exception caught");
    }
  }

  @Test
  public void testApplicationAttemptNotFound() throws IOException, YarnException {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, MAX_APPS + 1);
    GetApplicationAttemptReportRequest request =
        GetApplicationAttemptReportRequest.newInstance(appAttemptId);
    try {
      @SuppressWarnings("unused")
      GetApplicationAttemptReportResponse response =
          clientService.getApplicationAttemptReport(request);
      Assert.fail("Exception should have been thrown before we reach here.");
    } catch (ApplicationAttemptNotFoundException e) {
      System.out.println(e.getMessage());
      Assert.assertTrue(e.getMessage().contains(
          "doesn't exist in the timeline store"));
    } catch (Exception e) {
      Assert.fail("Undesired exception caught");
    }
  }

  @Test
  public void testContainerNotFound() throws IOException, YarnException {
   ApplicationId appId = ApplicationId.newInstance(0, 1);
   ApplicationAttemptId appAttemptId =
       ApplicationAttemptId.newInstance(appId, 1);
   ContainerId containerId = ContainerId.newContainerId(appAttemptId,
       MAX_APPS + 1);
   GetContainerReportRequest request =
       GetContainerReportRequest.newInstance(containerId);
   try {
   @SuppressWarnings("unused")
   GetContainerReportResponse response =
       clientService.getContainerReport(request);
   } catch (ContainerNotFoundException e) {
     Assert.assertTrue(e.getMessage().contains(
         "doesn't exist in the timeline store"));
   }  catch (Exception e) {
      Assert.fail("Undesired exception caught");
   }
 }

  @Test
  public void testApplicationReport() throws IOException, YarnException {
    ApplicationId appId = null;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/WebServices.java
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;

  private static void rewrapAndThrowException(Exception e) {
    if (e instanceof UndeclaredThrowableException) {
      rewrapAndThrowThrowable(e.getCause());
    } else {
      rewrapAndThrowThrowable(e);
    }
  }

  private static void rewrapAndThrowThrowable(Throwable t) {
    if (t instanceof AuthorizationException) {
      throw new ForbiddenException(t);
    } if (t instanceof ApplicationNotFoundException ||
        t instanceof ApplicationAttemptNotFoundException ||
        t instanceof ContainerNotFoundException) {
      throw new NotFoundException(t);
    } else {
      throw new WebApplicationException(t);
    }
  }


