hadoop-common-project/hadoop-auth/src/test/java/org/apache/hadoop/security/authentication/util/TestCertificateUtil.java

  @Test
  public void testCorruptPEM() throws Exception {
    String pem = "MIICOjCCAaOgAwIBAgIJANXi/oWxvJNzMA0GCSqGSIb3DQEBBQUAMF8xCzAJBgNVBAYTAlVTMQ0w"
        + "CwYDVQQIEwRUZXN0MQ0wCwYDVQQHEwRUZXN0MQ8wDQYDVQQKEwZIYWRvb3AxDTALBgNVBAsTBFRl"
        + "c3QxEjAQBgNVBAMTCWxvY2FsaG9zdDAeFw0xNTAxMDIyMTE5MjRaFw0xNjAxMDIyMTE5MjRaMF8x"
        + "CzAJBgNVBAYTAlVTMQ0wCwYDVQQIEwRUZXN0MQ0wCwYDVQQHEwRUZXN0MQ8wDQYDVQQKEwZIYWRv"
        + "7OPuuaHb25J8isiOyA3RiWuJGQlXTdkCAwEAATANBgkqhkiG9w0BAQUFAAOBgQAdRUyCUqE9sdim"
        + "Fbll9BuZDKV16WXeWGq+kTd7ETe7l0fqXjq5EnrifOai0L/pXwVvS2jrFkKQRlRxRGUNaeEBZ2Wy"
        + "9aTyR+HGHCfvwoCegc9rAVw/DLaRriSO/jnEXzYK6XLVKH+hx5UXrJ7Oyc7JjZUc3g9kCWORThCX"
        + "Mzc1xA++";
    try {
      CertificateUtil.parseRSAPublicKey(pem);
      fail("Should not have thrown ServletException");

