hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/webapp/util/TestWebAppUtils.java

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.http.HttpServer2.Builder;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Assert;
import org.junit.Test;

public class TestWebAppUtils {
  private static final String RM1_NODE_ID = "rm1";
  private static final String RM2_NODE_ID = "rm2";

  private static String dummyHostNames[] = {"host1", "host2", "host3"};
  private static final String anyIpAddress = "1.2.3.4";
  private static Map<String, String> savedStaticResolution = new HashMap<>();

  @BeforeClass
  public static void initializeDummyHostnameResolution() throws Exception {
    String previousIpAddress;
    for (String hostName : dummyHostNames) {
      if (null != (previousIpAddress = NetUtils.getStaticResolution(hostName))) {
        savedStaticResolution.put(hostName, previousIpAddress);
      }
      NetUtils.addStaticResolution(hostName, anyIpAddress);
    }
  }

  @AfterClass
  public static void restoreDummyHostnameResolution() throws Exception {
    for (Map.Entry<String, String> hostnameToIpEntry : savedStaticResolution.entrySet()) {
      NetUtils.addStaticResolution(hostnameToIpEntry.getKey(), hostnameToIpEntry.getValue());
    }
  }

  @Test
  public void TestRMWebAppURLRemoteAndLocal() throws UnknownHostException {
    Configuration configuration = new Configuration();
    final String rmAddress = "host1:8088";
    configuration.set(YarnConfiguration.RM_WEBAPP_ADDRESS, rmAddress);
    final String rm1Address = "host2:8088";
    final String rm2Address = "host3:8088";
    configuration.set(YarnConfiguration.RM_WEBAPP_ADDRESS + "." + RM1_NODE_ID, rm1Address);
    configuration.set(YarnConfiguration.RM_WEBAPP_ADDRESS + "." + RM2_NODE_ID, rm2Address);
    configuration.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    configuration.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);

    String rmRemoteUrl = WebAppUtils.getResolvedRemoteRMWebAppURLWithoutScheme(configuration);
    Assert.assertEquals("ResolvedRemoteRMWebAppUrl should resolve to the first HA RM address", rm1Address, rmRemoteUrl);

    String rmLocalUrl = WebAppUtils.getResolvedRMWebAppURLWithoutScheme(configuration);
    Assert.assertEquals("ResolvedRMWebAppUrl should resolve to the default RM webapp address", rmAddress, rmLocalUrl);
  }

  @Test
  public void testGetPassword() throws Exception {

