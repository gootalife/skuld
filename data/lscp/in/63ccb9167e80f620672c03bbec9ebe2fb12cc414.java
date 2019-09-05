hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/TestRMFailover.java
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestRMFailover extends ClientBaseWithFixes {
    assertEquals(404, response.getResponseCode());
  }

  @Ignore
  @Test
  public void testRMWebAppRedirect() throws YarnException,
      InterruptedException, IOException {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMWebAppFilter.java

      if (redirectPath != null && !redirectPath.isEmpty()) {
        String redirectMsg =
            "This is standby RM. The redirect url is: " + redirectPath;
        PrintWriter out = response.getWriter();
        out.println(redirectMsg);
        response.setHeader("Location", redirectPath);
        response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
        return;
      }
    }

