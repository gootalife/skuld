hadoop-common-project/hadoop-auth/src/main/java/org/apache/hadoop/security/authentication/server/PseudoAuthenticationHandler.java
  }

  private String getUserName(HttpServletRequest request) {
    String queryString = request.getQueryString();
    if(queryString == null || queryString.length() == 0) {
      return null;
    }
    List<NameValuePair> list = URLEncodedUtils.parse(queryString, UTF8_CHARSET);
    if (list != null) {
      for (NameValuePair nv : list) {
        if (PseudoAuthenticator.USER_NAME.equals(nv.getName())) {

