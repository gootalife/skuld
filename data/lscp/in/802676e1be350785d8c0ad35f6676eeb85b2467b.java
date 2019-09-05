hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/TestRMFailover.java
    header = getHeader("Refresh", rm2Url + "/ws/v1/cluster/apps");
    assertTrue(header.contains("; url=" + rm1Url));

    header = getHeader("Refresh", rm2Url + "/proxy/" + fakeAppId);
    assertEquals(null, header);

  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMWebAppFilter.java
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;

import com.google.common.collect.Sets;
import com.google.inject.Injector;
  private boolean shouldRedirect(RMWebApp rmWebApp, String uri) {
    return !uri.equals("/" + rmWebApp.wsName() + "/v1/cluster/info")
        && !uri.equals("/" + rmWebApp.name() + "/cluster")
        && !uri.startsWith(ProxyUriUtils.PROXY_BASE)
        && !NON_REDIRECTED_URIS.contains(uri);
  }
}

