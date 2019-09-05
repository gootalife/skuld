hadoop-common-project/hadoop-auth/src/main/java/org/apache/hadoop/security/authentication/server/AuthenticationFilter.java
  protected void doFilter(FilterChain filterChain, HttpServletRequest request,
      HttpServletResponse response) throws IOException, ServletException {

hadoop-common-project/hadoop-auth/src/main/java/org/apache/hadoop/security/authentication/util/CertificateUtil.java
  public static RSAPublicKey parseRSAPublicKey(String pem) throws ServletException {
    String fullPem = PEM_HEADER + pem + PEM_FOOTER;

hadoop-common-project/hadoop-auth/src/main/java/org/apache/hadoop/security/authentication/util/RolloverSignerSecretProvider.java
  @Override
  public void init(Properties config, ServletContext servletContext,

hadoop-common-project/hadoop-auth/src/main/java/org/apache/hadoop/security/authentication/util/SignerSecretProvider.java
  public abstract void init(Properties config, ServletContext servletContext,
          long tokenValidity) throws Exception;

hadoop-common-project/hadoop-auth/src/main/java/org/apache/hadoop/security/authentication/util/ZKSignerSecretProvider.java
  protected CuratorFramework createCuratorClient(Properties config)
          throws Exception {

hadoop-common-project/hadoop-kms/src/main/java/org/apache/hadoop/crypto/key/kms/server/KeyAuthorizationKeyProvider.java
  public KeyAuthorizationKeyProvider(KeyProviderCryptoExtension keyProvider,
      KeyACLs acls) {

