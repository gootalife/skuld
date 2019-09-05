hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-web-proxy/src/main/java/org/apache/hadoop/yarn/server/webproxy/WebAppProxyServlet.java
        "Accept-Encoding",
        "Accept-Language",
        "Accept-Charset",
        "Content-Type",
        "Origin",
        "Access-Control-Request-Method",
        "Access-Control-Request-Headers"));

  public static final String PROXY_USER_COOKIE_NAME = "proxy-user";


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-web-proxy/src/test/java/org/apache/hadoop/yarn/server/webproxy/TestWebAppProxyServlet.java
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;


  private static Server server;
  private static int originalPort = 0;
  private static int numberOfHeaders = 0;
  private static final String UNKNOWN_HEADER = "Unknown-Header";
  private static boolean hasUnknownHeader = false;


    originalPort = server.getConnectors()[0].getLocalPort();
    LOG.info("Running embedded servlet container at: http://localhost:"
        + originalPort);
    System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
  }

  @SuppressWarnings("serial")
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      int numHeaders = 0;
      hasUnknownHeader = false;
      @SuppressWarnings("unchecked")
      Enumeration<String> names = req.getHeaderNames();
      while(names.hasMoreElements()) {
        String headerName = names.nextElement();
        if (headerName.equals(UNKNOWN_HEADER)) {
          hasUnknownHeader = true;
        }
        ++numHeaders;
      }
      numberOfHeaders = numHeaders;
      resp.setStatus(HttpServletResponse.SC_OK);
    }

    }
  }

  @Test(timeout=5000)
  public void testWebAppProxyPassThroughHeaders() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(YarnConfiguration.PROXY_ADDRESS, "localhost:9091");
    configuration.setInt("hadoop.http.max.threads", 5);
    WebAppProxyServerForTest proxy = new WebAppProxyServerForTest();
    proxy.init(configuration);
    proxy.start();

    int proxyPort = proxy.proxy.proxyServer.getConnectorAddress(0).getPort();

    try {
      URL url = new URL("http://localhost:" + proxyPort + "/proxy/application_00_1");
      HttpURLConnection proxyConn = (HttpURLConnection) url.openConnection();
      proxyConn.addRequestProperty("Origin", "http://www.someurl.com");
      proxyConn.addRequestProperty("Access-Control-Request-Method", "GET");
      proxyConn.addRequestProperty(
          "Access-Control-Request-Headers", "Authorization");
      proxyConn.addRequestProperty(UNKNOWN_HEADER, "unknown");
      assertEquals(proxyConn.getRequestProperties().size(), 4);
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, proxyConn.getResponseCode());
      assertEquals(numberOfHeaders, 8);
      assertFalse(hasUnknownHeader);
    } finally {
      proxy.close();
    }
  }



