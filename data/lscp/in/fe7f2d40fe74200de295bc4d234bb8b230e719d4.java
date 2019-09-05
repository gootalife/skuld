hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/RMHAUtils.java
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.yarn.client.RMHAServiceTarget;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

@Private

  public static List<String> getRMHAWebappAddresses(
      final YarnConfiguration conf) {
    String prefix;
    String defaultPort;
    if (YarnConfiguration.useHttps(conf)) {
      prefix = YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS;
      defaultPort = ":" + YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT;
    } else {
      prefix =YarnConfiguration.RM_WEBAPP_ADDRESS;
      defaultPort = ":" + YarnConfiguration.DEFAULT_RM_WEBAPP_PORT;
    }
    Collection<String> rmIds =
        conf.getStringCollection(YarnConfiguration.RM_HA_IDS);
    List<String> addrs = new ArrayList<String>();
    for (String id : rmIds) {
      String addr = conf.get(HAUtil.addSuffix(prefix, id));
      if (addr == null) {
        String hostname =
            conf.get(HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME, id));
        if (hostname != null) {
          addr = hostname + defaultPort;
        }
      }
      if (addr != null) {
        addrs.add(addr);
      }
    }
    return addrs;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-web-proxy/src/test/java/org/apache/hadoop/yarn/server/webproxy/amfilter/TestAmFilterInitializer.java
    assertEquals(WebAppUtils.getResolvedRMWebAppURLWithoutScheme(conf),
        proxyHosts.get(0));

    conf = new Configuration(false);
    conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS,
        "${yarn.resourcemanager.hostname}:8088"); // default in yarn-default.xml
    conf.set(YarnConfiguration.RM_HOSTNAME, "host1");
    proxyHosts = WebAppUtils.getProxyHostsAndPortsForAmFilter(conf);
    assertEquals(1, proxyHosts.size());
    assertEquals("host1:8088", proxyHosts.get(0));

    conf = new Configuration(false);
    conf.set(YarnConfiguration.PROXY_ADDRESS, "host1:1000");
    Collections.sort(proxyHosts);
    assertEquals("host5:5000", proxyHosts.get(0));
    assertEquals("host6:6000", proxyHosts.get(1));

    conf = new Configuration(false);
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2,rm3");
    conf.set(YarnConfiguration.RM_HOSTNAME + ".rm1", "host2");
    conf.set(YarnConfiguration.RM_HOSTNAME + ".rm2", "host3");
    conf.set(YarnConfiguration.RM_HOSTNAME + ".rm3", "host4");
    conf.set(YarnConfiguration.RM_HOSTNAME + ".rm4", "dummy");
    proxyHosts = WebAppUtils.getProxyHostsAndPortsForAmFilter(conf);
    assertEquals(3, proxyHosts.size());
    Collections.sort(proxyHosts);
    assertEquals("host2:" + YarnConfiguration.DEFAULT_RM_WEBAPP_PORT,
        proxyHosts.get(0));
    assertEquals("host3:" + YarnConfiguration.DEFAULT_RM_WEBAPP_PORT,
        proxyHosts.get(1));
    assertEquals("host4:" + YarnConfiguration.DEFAULT_RM_WEBAPP_PORT,
        proxyHosts.get(2));

    conf = new Configuration(false);
    conf.set(YarnConfiguration.YARN_HTTP_POLICY_KEY,
        HttpConfig.Policy.HTTPS_ONLY.toString());
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2,rm3");
    conf.set(YarnConfiguration.RM_HOSTNAME + ".rm1", "host2");
    conf.set(YarnConfiguration.RM_HOSTNAME + ".rm2", "host3");
    conf.set(YarnConfiguration.RM_HOSTNAME + ".rm3", "host4");
    conf.set(YarnConfiguration.RM_HOSTNAME + ".rm4", "dummy");
    proxyHosts = WebAppUtils.getProxyHostsAndPortsForAmFilter(conf);
    assertEquals(3, proxyHosts.size());
    Collections.sort(proxyHosts);
    assertEquals("host2:" + YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT,
        proxyHosts.get(0));
    assertEquals("host3:" + YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT,
        proxyHosts.get(1));
    assertEquals("host4:" + YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT,
        proxyHosts.get(2));
  }

  class MockAmFilterInitializer extends AmFilterInitializer {

