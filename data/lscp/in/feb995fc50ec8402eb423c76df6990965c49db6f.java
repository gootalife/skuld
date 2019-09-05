hadoop-common-project/hadoop-auth/src/main/java/org/apache/hadoop/security/authentication/server/AuthenticationFilter.java
      errCode = HttpServletResponse.SC_FORBIDDEN;
      authenticationEx = ex;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Authentication exception: " + ex.getMessage(), ex);
      } else {
        LOG.warn("Authentication exception: " + ex.getMessage());
      }
    }
    if (unauthorizedResponse) {
      if (!httpResponse.isCommitted()) {

