hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/timeline/TimelineAbout.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/timeline/TimelineAbout.java

package org.apache.hadoop.yarn.api.records.timeline;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "about")
@XmlAccessorType(XmlAccessType.NONE)
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TimelineAbout {

  private String about;
  private String timelineServiceVersion;
  private String timelineServiceBuildVersion;
  private String timelineServiceVersionBuiltOn;
  private String hadoopVersion;
  private String hadoopBuildVersion;
  private String hadoopVersionBuiltOn;

  public TimelineAbout() {
  }

  public TimelineAbout(String about) {
    this.about = about;
  }

  @XmlElement(name = "About")
  public String getAbout() {
    return about;
  }

  public void setAbout(String about) {
    this.about = about;
  }

  @XmlElement(name = "timeline-service-version")
  public String getTimelineServiceVersion() {
    return timelineServiceVersion;
  }

  public void setTimelineServiceVersion(String timelineServiceVersion) {
    this.timelineServiceVersion = timelineServiceVersion;
  }

  @XmlElement(name = "timeline-service-build-version")
  public String getTimelineServiceBuildVersion() {
    return timelineServiceBuildVersion;
  }

  public void setTimelineServiceBuildVersion(
      String timelineServiceBuildVersion) {
    this.timelineServiceBuildVersion = timelineServiceBuildVersion;
  }

  @XmlElement(name = "timeline-service-version-built-on")
  public String getTimelineServiceVersionBuiltOn() {
    return timelineServiceVersionBuiltOn;
  }

  public void setTimelineServiceVersionBuiltOn(
      String timelineServiceVersionBuiltOn) {
    this.timelineServiceVersionBuiltOn = timelineServiceVersionBuiltOn;
  }

  @XmlElement(name = "hadoop-version")
  public String getHadoopVersion() {
    return hadoopVersion;
  }

  public void setHadoopVersion(String hadoopVersion) {
    this.hadoopVersion = hadoopVersion;
  }

  @XmlElement(name = "hadoop-build-version")
  public String getHadoopBuildVersion() {
    return hadoopBuildVersion;
  }

  public void setHadoopBuildVersion(String hadoopBuildVersion) {
    this.hadoopBuildVersion = hadoopBuildVersion;
  }

  @XmlElement(name = "hadoop-version-built-on")
  public String getHadoopVersionBuiltOn() {
    return hadoopVersionBuiltOn;
  }

  public void setHadoopVersionBuiltOn(String hadoopVersionBuiltOn) {
    this.hadoopVersionBuiltOn = hadoopVersionBuiltOn;
  }
}


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/timeline/TimelineUtils.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
    }
  }

  public static TimelineAbout createTimelineAbout(String about) {
    TimelineAbout tsInfo = new TimelineAbout(about);
    tsInfo.setHadoopBuildVersion(VersionInfo.getBuildVersion());
    tsInfo.setHadoopVersion(VersionInfo.getVersion());
    tsInfo.setHadoopVersionBuiltOn(VersionInfo.getDate());
    tsInfo.setTimelineServiceBuildVersion(YarnVersionInfo.getBuildVersion());
    tsInfo.setTimelineServiceVersion(YarnVersionInfo.getVersion());
    tsInfo.setTimelineServiceVersionBuiltOn(YarnVersionInfo.getDate());
    return tsInfo;
  }

  public static InetSocketAddress getTimelineTokenServiceAddress(
      Configuration conf) {
    InetSocketAddress timelineServiceAddr = null;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/AHSController.java
    setTitle("Application History");
  }

  public void about() {
    render(AboutPage.class);
  }

  public void app() {
    render(AppPage.class);
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/AHSWebApp.java
    bind(ApplicationBaseProtocol.class).toInstance(historyClientService);
    bind(TimelineDataManager.class).toInstance(timelineDataManager);
    route("/", AHSController.class);
    route("/about", AHSController.class, "about");
    route(pajoin("/apps", APP_STATE), AHSController.class);
    route(pajoin("/app", APPLICATION_ID), AHSController.class, "app");
    route(pajoin("/appattempt", APPLICATION_ATTEMPT_ID), AHSController.class,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/AHSWebServices.java
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.server.webapp.WebServices;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;

import com.google.inject.Inject;
    super(appBaseProt);
  }

  @GET
  @Path("/about")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TimelineAbout about(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res) {
    init(res);
    return TimelineUtils.createTimelineAbout("Generic History Service API");
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppsInfo get(@Context HttpServletRequest req,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/AboutBlock.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/AboutBlock.java

package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import com.google.inject.Inject;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

public class AboutBlock extends HtmlBlock {
  @Inject
  AboutBlock(View.ViewContext ctx) {
    super(ctx);
  }

  @Override
  protected void render(Block html) {
    TimelineAbout tsInfo = TimelineUtils.createTimelineAbout(
        "Timeline Server - Generic History Service UI");
    info("Timeline Server Overview").
        _("Timeline Server Version:", tsInfo.getTimelineServiceBuildVersion() +
            " on " + tsInfo.getTimelineServiceVersionBuiltOn()).
        _("Hadoop Version:", tsInfo.getHadoopBuildVersion() +
            " on " + tsInfo.getHadoopVersionBuiltOn());
    html._(InfoBlock.class);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/AboutPage.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/AboutPage.java

package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;


import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

import static org.apache.hadoop.yarn.util.StringHelper.join;

public class AboutPage extends AHSView {
  @Override protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    set(TITLE, "Timeline Server - Generic History Service");
  }

  @Override protected Class<? extends SubView> content() {
    return AboutBlock.class;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/NavBlock.java
        div("#nav").
            h3("Application History").
                ul().
                    li().a(url("about"), "About").
                    _().
                    li().a(url("apps"), "Applications").
                        ul().
                            li().a(url("apps",

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/timeline/webapp/TimelineWebServices.java
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
    this.timelineDataManager = timelineDataManager;
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON /* , MediaType.APPLICATION_XML */})
  public TimelineAbout about(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res) {
    init(res);
    return TimelineUtils.createTimelineAbout("Timeline API");
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/TestAHSWebApp.java
    WebAppTests.flushOutput(injector);
  }

  @Test
  public void testAboutPage() throws Exception {
    Injector injector =
        WebAppTests.createMockInjector(ApplicationBaseProtocol.class,
            mockApplicationHistoryClientService(0, 0, 0));
    AboutPage aboutPageInstance = injector.getInstance(AboutPage.class);

    aboutPageInstance.render();
    WebAppTests.flushOutput(injector);

    aboutPageInstance.render();
    WebAppTests.flushOutput(injector);
  }

  @Test
  public void testAppPage() throws Exception {
    Injector injector =

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/TestAHSWebServices.java
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
    }
  }

  @Test
  public void testAbout() throws Exception {
    WebResource r = resource();
    ClientResponse response = r
        .path("ws").path("v1").path("applicationhistory").path("about")
        .queryParam("user.name", USERS[round])
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    TimelineAbout actualAbout = response.getEntity(TimelineAbout.class);
    TimelineAbout expectedAbout =
        TimelineUtils.createTimelineAbout("Generic History Service API");
    Assert.assertNotNull(
        "Timeline service about response is null", actualAbout);
    Assert.assertEquals(expectedAbout.getAbout(), actualAbout.getAbout());
    Assert.assertEquals(expectedAbout.getTimelineServiceVersion(),
        actualAbout.getTimelineServiceVersion());
    Assert.assertEquals(expectedAbout.getTimelineServiceBuildVersion(),
        actualAbout.getTimelineServiceBuildVersion());
    Assert.assertEquals(expectedAbout.getTimelineServiceVersionBuiltOn(),
        actualAbout.getTimelineServiceVersionBuiltOn());
    Assert.assertEquals(expectedAbout.getHadoopVersion(),
        actualAbout.getHadoopVersion());
    Assert.assertEquals(expectedAbout.getHadoopBuildVersion(),
        actualAbout.getHadoopBuildVersion());
    Assert.assertEquals(expectedAbout.getHadoopVersionBuiltOn(),
        actualAbout.getHadoopVersionBuiltOn());
  }

  @Test
  public void testAppsQuery() throws Exception {
    WebResource r = resource();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/timeline/webapp/TestTimelineWebServices.java
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.server.timeline.security.TimelineAuthenticationFilter;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    TimelineAbout actualAbout = response.getEntity(TimelineAbout.class);
    TimelineAbout expectedAbout =
        TimelineUtils.createTimelineAbout("Timeline API");
    Assert.assertNotNull(
        "Timeline service about response is null", actualAbout);
    Assert.assertEquals(expectedAbout.getAbout(), actualAbout.getAbout());
    Assert.assertEquals(expectedAbout.getTimelineServiceVersion(),
        actualAbout.getTimelineServiceVersion());
    Assert.assertEquals(expectedAbout.getTimelineServiceBuildVersion(),
        actualAbout.getTimelineServiceBuildVersion());
    Assert.assertEquals(expectedAbout.getTimelineServiceVersionBuiltOn(),
        actualAbout.getTimelineServiceVersionBuiltOn());
    Assert.assertEquals(expectedAbout.getHadoopVersion(),
        actualAbout.getHadoopVersion());
    Assert.assertEquals(expectedAbout.getHadoopBuildVersion(),
        actualAbout.getHadoopBuildVersion());
    Assert.assertEquals(expectedAbout.getHadoopVersionBuiltOn(),
        actualAbout.getHadoopVersionBuiltOn());
  }

  private static void verifyEntities(TimelineEntities entities) {

