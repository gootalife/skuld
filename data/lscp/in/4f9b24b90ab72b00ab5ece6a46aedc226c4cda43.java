hadoop-common-project/hadoop-auth/src/main/java/org/apache/hadoop/security/authentication/server/JWTRedirectAuthenticationHandler.java
  public void setPublicKey(RSAPublicKey pk) {
    publicKey = pk;

    HttpServletRequest req = (HttpServletRequest) request;
    serializedJWT = getJWTFromCookie(req);
    if (serializedJWT == null) {
      String loginURL = constructLoginURL(request);
      LOG.info("sending redirect to: " + loginURL);
      ((HttpServletResponse) response).sendRedirect(loginURL);
    } else {
        LOG.debug("Issuing AuthenticationToken for user.");
        token = new AuthenticationToken(userName, userName, getType());
      } else {
        String loginURL = constructLoginURL(request);
        LOG.info("token validation failed - sending redirect to: " + loginURL);
        ((HttpServletResponse) response).sendRedirect(loginURL);
      }
  protected String getJWTFromCookie(HttpServletRequest req) {
  protected String constructLoginURL(HttpServletRequest request) {
    String delimiter = "?";
    if (authenticationProviderUrl.contains("?")) {
      delimiter = "&";
  protected boolean validateToken(SignedJWT jwtToken) {
    boolean sigValid = validateSignature(jwtToken);
  protected boolean validateSignature(SignedJWT jwtToken) {
    boolean valid = false;
  protected boolean validateExpiration(SignedJWT jwtToken) {
    boolean valid = false;

