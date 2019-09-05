hadoop-common-project/hadoop-auth/src/main/java/org/apache/hadoop/security/authentication/server/AuthenticationFilter.java
  private long validity;
  private String cookieDomain;
  private String cookiePath;
  private boolean isInitializedByTomcat;

        secretProvider = constructSecretProvider(
            filterConfig.getServletContext(),
            config, false);
        isInitializedByTomcat = true;
      } catch (Exception ex) {
        throw new ServletException(ex);
      }
      authHandler.destroy();
      authHandler = null;
    }
    if (secretProvider != null && isInitializedByTomcat) {
      secretProvider.destroy();
      secretProvider = null;
    }
  }


