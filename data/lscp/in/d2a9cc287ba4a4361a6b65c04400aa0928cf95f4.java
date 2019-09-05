hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/token/delegation/web/DelegationTokenAuthenticationFilter.java
        if (doAsUser != null) {
          ugi = UserGroupInformation.createProxyUser(doAsUser, ugi);
          try {
            ProxyUsers.authorize(ugi, request.getRemoteAddr());
          } catch (AuthorizationException ex) {
            HttpExceptionUtils.createServletExceptionResponse(response,
                HttpServletResponse.SC_FORBIDDEN, ex);

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/token/delegation/web/DelegationTokenAuthenticationHandler.java
            requestUgi = UserGroupInformation.createProxyUser(
                doAsUser, requestUgi);
            try {
              ProxyUsers.authorize(requestUgi, request.getRemoteAddr());
            } catch (AuthorizationException ex) {
              HttpExceptionUtils.createServletExceptionResponse(response,
                  HttpServletResponse.SC_FORBIDDEN, ex);

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/token/delegation/web/TestWebDelegationToken.java
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.AbstractConnector;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
      org.apache.hadoop.conf.Configuration conf =
          new org.apache.hadoop.conf.Configuration(false);
      conf.set("proxyuser.client.users", OK_USER);
      conf.set("proxyuser.client.hosts", "127.0.0.1");
      return conf;
    }
  }
    Context context = new Context();
    context.setContextPath("/foo");
    jetty.setHandler(context);
    ((AbstractConnector)jetty.getConnectors()[0]).setResolveNames(true);
    context.addFilter(new FilterHolder(KDTAFilter.class), "/*", 0);
    context.addServlet(new ServletHolder(UserServlet.class), "/bar");
    try {
    }
  }

  public static class IpAddressBasedPseudoDTAFilter extends PseudoDTAFilter {
    @Override
    protected org.apache.hadoop.conf.Configuration getProxyuserConfiguration
            (FilterConfig filterConfig) throws ServletException {
      org.apache.hadoop.conf.Configuration configuration = super
              .getProxyuserConfiguration(filterConfig);
      configuration.set("proxyuser.foo.hosts", "127.0.0.1");
      return configuration;
    }
  }

  @Test
  public void testIpaddressCheck() throws Exception {
    final Server jetty = createJettyServer();
    ((AbstractConnector)jetty.getConnectors()[0]).setResolveNames(true);
    Context context = new Context();
    context.setContextPath("/foo");
    jetty.setHandler(context);

    context.addFilter(new FilterHolder(IpAddressBasedPseudoDTAFilter.class), "/*", 0);
    context.addServlet(new ServletHolder(UGIServlet.class), "/bar");

    try {
      jetty.start();
      final URL url = new URL(getJettyURL() + "/foo/bar");

      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(FOO_USER);
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          DelegationTokenAuthenticatedURL.Token token =
                  new DelegationTokenAuthenticatedURL.Token();
          DelegationTokenAuthenticatedURL aUrl =
                  new DelegationTokenAuthenticatedURL();

          HttpURLConnection conn = aUrl.openConnection(url, token, OK_USER);
          Assert.assertEquals(HttpURLConnection.HTTP_OK,
                  conn.getResponseCode());
          List<String> ret = IOUtils.readLines(conn.getInputStream());
          Assert.assertEquals(1, ret.size());
          Assert.assertEquals("realugi=" + FOO_USER +":remoteuser=" + OK_USER +
                  ":ugi=" + OK_USER, ret.get(0));

          return null;
        }
      });
    } finally {
      jetty.stop();
    }
  }

}

