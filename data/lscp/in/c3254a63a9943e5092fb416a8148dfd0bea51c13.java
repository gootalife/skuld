hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/ResourceLocalizationService.java
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
        if (LOG.isDebugEnabled()) {
          for (Token<? extends TokenIdentifier> tk : credentials
              .getAllTokens()) {
            LOG.debug(tk + " : " + buildTokenFingerprint(tk));
          }
        }
        if (UserGroupInformation.isSecurityEnabled()) {

  }

  @VisibleForTesting
  static String buildTokenFingerprint(Token<? extends TokenIdentifier> tk)
      throws IOException {
    char[] digest = DigestUtils.sha256Hex(tk.encodeToUrlString()).toCharArray();
    StringBuilder fingerprint = new StringBuilder();
    for (int i = 0; i < 10; ++i) {
      if (i > 0) {
        fingerprint.append(' ');
      }
      fingerprint.append(digest[2 * i]);
      fingerprint.append(digest[2 * i + 1]);
    }
    return fingerprint.toString();
  }

  static class CacheCleanup extends Thread {

    private final Dispatcher dispatcher;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/TestResourceLocalizationService.java
  }

  private static Container getMockContainer(ApplicationId appId, int id,
      String user) throws IOException {
    Container c = mock(Container.class);
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(appId, 1);
    when(c.getUser()).thenReturn(user);
    when(c.getContainerId()).thenReturn(cId);
    Credentials creds = new Credentials();
    Token<? extends TokenIdentifier> tk = getToken(id);
    String fingerprint = ResourceLocalizationService.buildTokenFingerprint(tk);
    assertNotNull(fingerprint);
    assertTrue(
        "Expected token fingerprint of 10 hex bytes delimited by space.",
        fingerprint.matches("^(([0-9a-f]){2} ){9}([0-9a-f]){2}$"));
    creds.addToken(new Text("tok" + id), tk);
    when(c.getCredentials()).thenReturn(creds);
    when(c.toString()).thenReturn(cId.toString());
    return c;

