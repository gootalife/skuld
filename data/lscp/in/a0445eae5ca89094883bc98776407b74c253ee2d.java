hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-web-proxy/src/main/java/org/apache/hadoop/yarn/server/webproxy/WebAppProxyServlet.java
      final String remoteUser = req.getRemoteUser();
      final String pathInfo = req.getPathInfo();

      String[] parts = null;
      if (pathInfo != null) {
        parts = pathInfo.split("/", 3);
      }
      if(parts == null || parts.length < 2) {
        LOG.warn("{} gave an invalid proxy path {}", remoteUser,  pathInfo);
        notFound(resp, "Your path appears to be formatted incorrectly.");
        return;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-web-proxy/src/test/java/org/apache/hadoop/yarn/server/webproxy/TestWebAppProxyServlet.java

    try {
      URL emptyUrl = new URL("http://localhost:" + proxyPort + "/proxy");
      HttpURLConnection emptyProxyConn = (HttpURLConnection) emptyUrl
          .openConnection();
      emptyProxyConn.connect();;
      assertEquals(HttpURLConnection.HTTP_NOT_FOUND, emptyProxyConn.getResponseCode());

      URL wrongUrl = new URL("http://localhost:" + proxyPort + "/proxy/app");
      HttpURLConnection proxyConn = (HttpURLConnection) wrongUrl

