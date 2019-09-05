hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/http/HttpServer2.java
  protected final List<String> filterNames = new ArrayList<>();
  static final String STATE_DESCRIPTION_ALIVE = " - alive";
  static final String STATE_DESCRIPTION_NOT_LIVE = " - not live";
  private final SignerSecretProvider secretProvider;

    this.adminsAcl = b.adminsAcl;
    this.webAppContext = createWebAppContext(b.name, b.conf, adminsAcl, appDir);
    try {
      this.secretProvider =
          constructSecretProvider(b, webAppContext.getServletContext());
      this.webAppContext.getServletContext().setAttribute
          (AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE,
    }

    try {
      secretProvider.destroy();
      webAppContext.clearAttributes();
      webAppContext.stop();

