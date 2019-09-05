hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/client/api/impl/TimelineClientImpl.java
  public long renewDelegationToken(
      final Token<TimelineDelegationTokenIdentifier> timelineDT)
          throws IOException, YarnException {
    final boolean isTokenServiceAddrEmpty =
        timelineDT.getService().toString().isEmpty();
    final String scheme = isTokenServiceAddrEmpty ? null
        : (YarnConfiguration.useHttps(this.getConfig()) ? "https" : "http");
    final InetSocketAddress address = isTokenServiceAddrEmpty ? null
        : SecurityUtil.getTokenServiceAddr(timelineDT);
    PrivilegedExceptionAction<Long> renewDTAction =
        new PrivilegedExceptionAction<Long>() {

            DelegationTokenAuthenticatedURL authUrl =
                new DelegationTokenAuthenticatedURL(authenticator,
                    connConfigurator);
            final URI serviceURI = isTokenServiceAddrEmpty ? resURI
                : new URI(scheme, null, address.getHostName(),
                address.getPort(), RESOURCE_URI_STR, null, null);
            return authUrl
                .renewDelegationToken(serviceURI.toURL(), token, doAsUser);
  public void cancelDelegationToken(
      final Token<TimelineDelegationTokenIdentifier> timelineDT)
          throws IOException, YarnException {
    final boolean isTokenServiceAddrEmpty =
        timelineDT.getService().toString().isEmpty();
    final String scheme = isTokenServiceAddrEmpty ? null
        : (YarnConfiguration.useHttps(this.getConfig()) ? "https" : "http");
    final InetSocketAddress address = isTokenServiceAddrEmpty ? null
        : SecurityUtil.getTokenServiceAddr(timelineDT);
    PrivilegedExceptionAction<Void> cancelDTAction =
        new PrivilegedExceptionAction<Void>() {

            DelegationTokenAuthenticatedURL authUrl =
                new DelegationTokenAuthenticatedURL(authenticator,
                    connConfigurator);
            final URI serviceURI = isTokenServiceAddrEmpty ? resURI
                : new URI(scheme, null, address.getHostName(),
                address.getPort(), RESOURCE_URI_STR, null, null);
            authUrl.cancelDelegationToken(serviceURI.toURL(), token, doAsUser);
            return null;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/timeline/security/TestTimelineAuthenticationFilter.java
    Assert.assertEquals(new Text(HTTP_USER), tDT.getOwner());

    Assert.assertFalse(token.getService().toString().isEmpty());
    long renewTime1 = httpUserClient.renewDelegationToken(token);
    Thread.sleep(100);
    token.setService(new Text());
    Assert.assertTrue(token.getService().toString().isEmpty());
    long renewTime2 = httpUserClient.renewDelegationToken(token);
    Assert.assertTrue(renewTime1 < renewTime2);

    Assert.assertTrue(token.getService().toString().isEmpty());
    httpUserClient.cancelDelegationToken(token);
    try {
    Assert.assertTrue(renewTime1 < renewTime2);

    Assert.assertFalse(tokenToRenew.getService().toString().isEmpty());
    fooUserClient.cancelDelegationToken(tokenToRenew);


