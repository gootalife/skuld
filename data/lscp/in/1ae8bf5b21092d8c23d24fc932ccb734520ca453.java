hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/client/MRClientService.java
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
    }
  }

  public KillTaskAttemptResponse forceKillTaskAttempt(
      KillTaskAttemptRequest request) throws YarnException, IOException {
    return protocolHandler.killTaskAttempt(request);
  }

  public WebApp getWebApp() {
    return webApp;
  }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/AMWebServices.java
package org.apache.hadoop.mapreduce.v2.app.webapp;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.KillTaskAttemptRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.MRClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskAttemptCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskAttemptState;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ReduceTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TasksInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

@Path("/ws/v1/mapreduce")
public class AMWebServices {
  private final AppContext appCtx;
  private final App app;
  private final MRClientService service;

  private @Context HttpServletResponse response;
  
  public AMWebServices(final App app, final AppContext context) {
    this.appCtx = context;
    this.app = app;
    this.service = new MRClientService(context);
  }

  Boolean hasAccess(Job job, HttpServletRequest request) {
    }
  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/state")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobTaskAttemptState getJobTaskAttemptState(
      @Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @PathParam("taskid") String tid,
      @PathParam("attemptid") String attId)
          throws IOException, InterruptedException {
    init();
    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    Task task = getTaskFromTaskIdString(tid, job);
    TaskAttempt ta = getTaskAttemptFromTaskAttemptString(attId, task);
    return new JobTaskAttemptState(ta.getState().toString());
  }

  @PUT
  @Path("/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/state")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response updateJobTaskAttemptState(JobTaskAttemptState targetState,
      @Context HttpServletRequest hsr, @PathParam("jobid") String jid,
      @PathParam("taskid") String tid, @PathParam("attemptid") String attId)
          throws IOException, InterruptedException {
    init();
    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);

    String remoteUser = hsr.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }

    Task task = getTaskFromTaskIdString(tid, job);
    TaskAttempt ta = getTaskAttemptFromTaskAttemptString(attId, task);
    if (!ta.getState().toString().equals(targetState.getState())) {
      if (targetState.getState().equals(TaskAttemptState.KILLED.toString())) {
        return killJobTaskAttempt(ta, callerUGI, hsr);
      }
      throw new BadRequestException("Only '"
          + TaskAttemptState.KILLED.toString()
          + "' is allowed as a target state.");
    }

    JobTaskAttemptState ret = new JobTaskAttemptState();
    ret.setState(ta.getState().toString());

    return Response.status(Status.OK).entity(ret).build();
  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/counters")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    TaskAttempt ta = getTaskAttemptFromTaskAttemptString(attId, task);
    return new JobTaskAttemptCounterInfo(ta);
  }

  protected Response killJobTaskAttempt(TaskAttempt ta,
      UserGroupInformation callerUGI, HttpServletRequest hsr)
          throws IOException, InterruptedException {
    Preconditions.checkNotNull(ta, "ta cannot be null");

    String userName = callerUGI.getUserName();
    final TaskAttemptId attemptId = ta.getID();
    try {
      callerUGI
          .doAs(new PrivilegedExceptionAction<KillTaskAttemptResponse>() {
            @Override
            public KillTaskAttemptResponse run()
                throws IOException, YarnException {
              KillTaskAttemptRequest req =  new KillTaskAttemptRequestPBImpl();
              req.setTaskAttemptId(attemptId);
              return service.forceKillTaskAttempt(req);
            }
          });
    } catch (UndeclaredThrowableException ue) {
      if (ue.getCause() instanceof YarnException) {
        YarnException ye = (YarnException) ue.getCause();
        if (ye.getCause() instanceof AccessControlException) {
          String taId = attemptId.toString();
          String msg =
              "Unauthorized attempt to kill task attempt " + taId
                  + " by remote user " + userName;
          return Response.status(Status.FORBIDDEN).entity(msg).build();
        } else {
          throw ue;
        }
      } else {
        throw ue;
      }
    }

    JobTaskAttemptState ret = new JobTaskAttemptState();
    ret.setState(TaskAttemptState.KILLED.toString());

    return Response.status(Status.OK).entity(ret).build();
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/JAXBContextResolver.java

package org.apache.hadoop.mapreduce.v2.app.webapp;

import java.util.HashMap;
import java.util.Map;

import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.CounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskAttemptState;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskAttemptCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskCounterInfo;
@Provider
public class JAXBContextResolver implements ContextResolver<JAXBContext> {

  private final Map<Class, JAXBContext> typesContextMap;

  private final Class[] cTypes = {AMAttemptInfo.class, AMAttemptsInfo.class,
    TaskAttemptInfo.class, TaskInfo.class, TasksInfo.class,
    TaskAttemptsInfo.class, ConfEntryInfo.class, RemoteExceptionData.class};

  private final Class[] rootUnwrappedTypes = {JobTaskAttemptState.class};

  public JAXBContextResolver() throws Exception {
    JAXBContext context;
    JAXBContext unWrappedRootContext;

    this.typesContextMap = new HashMap<Class, JAXBContext>();
    context =
        new JSONJAXBContext(JSONConfiguration.natural().rootUnwrapping(false)
            .build(), cTypes);
    unWrappedRootContext =
        new JSONJAXBContext(JSONConfiguration.natural().rootUnwrapping(true)
            .build(), rootUnwrappedTypes);
    for (Class type : cTypes) {
      typesContextMap.put(type, context);
    }
    for (Class type : rootUnwrappedTypes) {
      typesContextMap.put(type, unWrappedRootContext);
    }
  }

  @Override
  public JAXBContext getContext(Class<?> objectType) {
    return typesContextMap.get(objectType);
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/dao/JobTaskAttemptState.java
++ b/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/dao/JobTaskAttemptState.java

package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "jobTaskAttemptState")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobTaskAttemptState {

  private String state;

  public JobTaskAttemptState() {
  }

  public JobTaskAttemptState(String state) {
    this.state = state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getState() {
    return this.state;
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/MockAppContext.java
  @SuppressWarnings("rawtypes")
  @Override
  public EventHandler getEventHandler() {
    return new MockEventHandler();
  }

  @Override

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/MockEventHandler.java
++ b/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/MockEventHandler.java

package org.apache.hadoop.mapreduce.v2.app;

import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.yarn.event.EventHandler;

public class MockEventHandler implements EventHandler<TaskAttemptEvent> {
  @Override
  public void handle(TaskAttemptEvent event) {
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/webapp/TestAMWebServicesAttempt.java
++ b/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/webapp/TestAMWebServicesAttempt.java

package org.apache.hadoop.mapreduce.v2.app.webapp;

import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MockAppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;

public class TestAMWebServicesAttempt extends JerseyTest {

  private static Configuration conf = new Configuration();
  private static AppContext appContext;
  private String webserviceUserName = "testuser";

  private Injector injector = Guice.createInjector(new ServletModule() {
    @Override
    protected void configureServlets() {
      appContext = new MockAppContext(0, 1, 2, 1);
      bind(JAXBContextResolver.class);
      bind(AMWebServices.class);
      bind(GenericExceptionHandler.class);
      bind(AppContext.class).toInstance(appContext);
      bind(Configuration.class).toInstance(conf);

      serve("/*").with(GuiceContainer.class);
      filter("/*").through(TestRMCustomAuthFilter.class);
    }
  });

  @Singleton
  public static class TestRMCustomAuthFilter extends AuthenticationFilter {
    @Override
    protected Properties getConfiguration(String configPrefix,
        FilterConfig filterConfig) throws ServletException {
      Properties props = new Properties();
      Enumeration<?> names = filterConfig.getInitParameterNames();
      while (names.hasMoreElements()) {
        String name = (String) names.nextElement();
        if (name.startsWith(configPrefix)) {
          String value = filterConfig.getInitParameter(name);
          props.put(name.substring(configPrefix.length()), value);
        }
      }
      props.put(AuthenticationFilter.AUTH_TYPE, "simple");
      props.put(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "false");
      return props;
    }
  }


  public class GuiceServletConfig extends GuiceServletContextListener {
    @Override
    protected Injector getInjector() {
      return injector;
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public TestAMWebServicesAttempt() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.mapreduce.v2.app.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testGetTaskAttemptIdState() throws Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();

    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      for (Task task : jobsMap.get(id).getTasks().values()) {
        String tid = MRApps.toString(task.getID());

        for (TaskAttempt att : task.getAttempts().values()) {
          TaskAttemptId attemptid = att.getID();
          String attid = MRApps.toString(attemptid);

          ClientResponse response = r.path("ws").path("v1").path("mapreduce")
              .path("jobs").path(jobId).path("tasks").path(tid)
              .path("attempts").path(attid).path("state")
              .queryParam("user.name", webserviceUserName)
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
          assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
          JSONObject json = response.getEntity(JSONObject.class);
          assertEquals("incorrect number of elements", 1, json.length());
          assertEquals(att.getState().toString(), json.get("state"));
        }
      }
    }
  }

  @Test
  public void testGetTaskAttemptIdXMLState() throws Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        for (TaskAttempt att : task.getAttempts().values()) {
          TaskAttemptId attemptid = att.getID();
          String attid = MRApps.toString(attemptid);

          ClientResponse response = r.path("ws").path("v1").path("mapreduce")
              .path("jobs").path(jobId).path("tasks").path(tid)
              .path("attempts").path(attid).path("state")
              .queryParam("user.name", webserviceUserName)
              .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);

          assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
          String xml = response.getEntity(String.class);
          DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
          DocumentBuilder db = dbf.newDocumentBuilder();
          InputSource is = new InputSource();
          is.setCharacterStream(new StringReader(xml));
          Document dom = db.parse(is);
          NodeList nodes = dom.getElementsByTagName("jobTaskAttemptState");
          assertEquals(1, nodes.getLength());
          String state = WebServicesTestUtils.getXmlString(
              (Element) nodes.item(0), "state");
          assertEquals(att.getState().toString(), state);
        }
      }
    }
  }

  @Test
  public void testPutTaskAttemptIdState() throws Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();

    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      for (Task task : jobsMap.get(id).getTasks().values()) {
        String tid = MRApps.toString(task.getID());

        for (TaskAttempt att : task.getAttempts().values()) {
          TaskAttemptId attemptid = att.getID();
          String attid = MRApps.toString(attemptid);

          ClientResponse response = r.path("ws").path("v1").path("mapreduce")
              .path("jobs").path(jobId).path("tasks").path(tid)
              .path("attempts").path(attid).path("state")
              .queryParam("user.name", webserviceUserName)
              .accept(MediaType.APPLICATION_JSON)
              .type(MediaType.APPLICATION_JSON)
              .put(ClientResponse.class, "{\"state\":\"KILLED\"}");
          assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
          JSONObject json = response.getEntity(JSONObject.class);
          assertEquals("incorrect number of elements", 1, json.length());
          assertEquals(TaskAttemptState.KILLED.toString(), json.get("state"));
        }
      }
    }
  }

  @Test
  public void testPutTaskAttemptIdXMLState() throws Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();

    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      for (Task task : jobsMap.get(id).getTasks().values()) {
        String tid = MRApps.toString(task.getID());

        for (TaskAttempt att : task.getAttempts().values()) {
          TaskAttemptId attemptid = att.getID();
          String attid = MRApps.toString(attemptid);

          ClientResponse response = r.path("ws").path("v1").path("mapreduce")
              .path("jobs").path(jobId).path("tasks").path(tid)
              .path("attempts").path(attid).path("state")
              .queryParam("user.name", webserviceUserName)
              .accept(MediaType.APPLICATION_XML_TYPE)
              .type(MediaType.APPLICATION_XML_TYPE)
              .put(ClientResponse.class,
                  "<jobTaskAttemptState><state>KILLED" +
                      "</state></jobTaskAttemptState>");
          assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
          String xml = response.getEntity(String.class);
          DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
          DocumentBuilder db = dbf.newDocumentBuilder();
          InputSource is = new InputSource();
          is.setCharacterStream(new StringReader(xml));
          Document dom = db.parse(is);
          NodeList nodes = dom.getElementsByTagName("jobTaskAttemptState");
          assertEquals(1, nodes.getLength());
          String state = WebServicesTestUtils.getXmlString(
              (Element) nodes.item(0), "state");
          assertEquals(TaskAttemptState.KILLED.toString(), state);
        }
      }
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-web-proxy/src/main/java/org/apache/hadoop/yarn/server/webproxy/WebAppProxyServlet.java

package org.apache.hadoop.yarn.server.webproxy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
        "Accept",
        "Accept-Encoding",
        "Accept-Language",
        "Accept-Charset",
        "Content-Type"));

  public static final String PROXY_USER_COOKIE_NAME = "proxy-user";

  private final String rmAppPageUrlBase;
  private transient YarnConfiguration conf;

  private enum HTTP { GET, POST, HEAD, PUT, DELETE };

  private static class _ implements Hamlet._ {
  }
  private static void proxyLink(final HttpServletRequest req,
      final HttpServletResponse resp, final URI link, final Cookie c,
      final String proxyHost, final HTTP method) throws IOException {
    DefaultHttpClient client = new DefaultHttpClient();
    client
        .getParams()
    }
    client.getParams()
        .setParameter(ConnRoutePNames.LOCAL_ADDRESS, localAddress);

    HttpRequestBase base = null;
    if (method.equals(HTTP.GET)) {
      base = new HttpGet(link);
    } else if (method.equals(HTTP.PUT)) {
      base = new HttpPut(link);

      StringBuilder sb = new StringBuilder();
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(req.getInputStream(), "UTF-8"));
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }

      ((HttpPut) base).setEntity(new StringEntity(sb.toString()));
    } else {
      resp.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
      return;
    }

    @SuppressWarnings("unchecked")
    Enumeration<String> names = req.getHeaderNames();
    while(names.hasMoreElements()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("REQ HEADER: {} : {}", name, value);
        }
        base.setHeader(name, value);
      }
    }

    String user = req.getRemoteUser();
    if (user != null && !user.isEmpty()) {
      base.setHeader("Cookie",
          PROXY_USER_COOKIE_NAME + "=" + URLEncoder.encode(user, "ASCII"));
    }
    OutputStream out = resp.getOutputStream();
    try {
      HttpResponse httpResp = client.execute(base);
      resp.setStatus(httpResp.getStatusLine().getStatusCode());
      for (Header header : httpResp.getAllHeaders()) {
        resp.setHeader(header.getName(), header.getValue());
        IOUtils.copyBytes(in, out, 4096, true);
      }
    } finally {
      base.releaseConnection();
    }
  }
  
  
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    methodAction(req, resp, HTTP.GET);
  }

  @Override
  protected final void doPut(final HttpServletRequest req,
      final HttpServletResponse resp) throws ServletException, IOException {
    methodAction(req, resp, HTTP.PUT);
  }

  private void methodAction(final HttpServletRequest req,
      final HttpServletResponse resp,
      final HTTP method) throws ServletException, IOException {
    try {
      String userApprovedParamS = 
        req.getParameter(ProxyUriUtils.PROXY_APPROVAL_PARAM);
      if (userWasWarned && userApproved) {
        c = makeCheckCookie(id, true);
      }
      proxyLink(req, resp, toFetch, c, getProxyHost(), method);

    } catch(URISyntaxException | YarnException e) {
      throw new IOException(e); 

