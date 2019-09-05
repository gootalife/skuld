hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/TestRMFailover.java
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestRMFailover extends ClientBaseWithFixes {
    assertEquals(404, response.getResponseCode());
  }

  @Test
  public void testRMWebAppRedirect() throws YarnException,
      InterruptedException, IOException {
    getAdminService(0).transitionToActive(req);
    String rm1Url = "http://0.0.0.0:18088";
    String rm2Url = "http://0.0.0.0:28088";
    String redirectURL = getRedirectURL(rm2Url);
    assertEquals(redirectURL,rm1Url+"/");

    redirectURL = getRedirectURL(rm2Url + "/metrics");
    assertEquals(redirectURL,rm1Url + "/metrics");

    redirectURL = getRedirectURL(rm2Url + "/jmx");
    assertEquals(redirectURL,rm1Url + "/jmx");

    redirectURL = getRedirectURL(rm2Url + "/cluster/cluster");
    assertNull(redirectURL);

    redirectURL = getRedirectURL(rm2Url + "/conf");
    assertNull(redirectURL);

    redirectURL = getRedirectURL(rm2Url + "/stacks");
    assertNull(redirectURL);

    redirectURL = getRedirectURL(rm2Url + "/logLevel");
    assertNull(redirectURL);

    redirectURL = getRedirectURL(rm2Url + "/static");
    assertNull(redirectURL);

    redirectURL = getRedirectURL(rm2Url + "/logs");
    assertNull(redirectURL);

    redirectURL = getRedirectURL(rm2Url + "/ws/v1/cluster/info");
    assertNull(redirectURL);

    redirectURL = getRedirectURL(rm2Url + "/ws/v1/cluster/apps");
    assertEquals(redirectURL, rm1Url + "/ws/v1/cluster/apps");

    redirectURL = getRedirectURL(rm2Url + "/proxy/" + fakeAppId);
    assertNull(redirectURL);
  }

  static String getRedirectURL(String url) {
    String redirectUrl = null;
    try {
      HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
      conn.setInstanceFollowRedirects(false);
      if(conn.getResponseCode() == HttpServletResponse.SC_TEMPORARY_REDIRECT)
        redirectUrl = conn.getHeaderField("Location");
    } catch (Exception e) {
    }
    return redirectUrl;
  }

}

