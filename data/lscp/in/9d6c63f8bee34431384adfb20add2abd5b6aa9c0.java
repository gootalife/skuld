hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-tests/src/test/java/org/apache/hadoop/yarn/server/TestContainerManagerSecurity.java
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(Parameterized.class)
public class TestContainerManagerSecurity extends KerberosSecurityTestcase {
    testRootDir.mkdirs();
    httpSpnegoKeytabFile.deleteOnExit();
    getKdc().createPrincipal(httpSpnegoKeytabFile, httpSpnegoPrincipal);

    yarnCluster =
        new MiniYARNCluster(TestContainerManagerSecurity.class.getName(), 1, 1,
            1);
    yarnCluster.init(conf);
    yarnCluster.start();
  }
 
  @After
  public void tearDown() {
    if (yarnCluster != null) {
      yarnCluster.stop();
      yarnCluster = null;
    }
    testRootDir.delete();
  }

  
  @Test (timeout = 120000)
  public void testContainerManager() throws Exception {
      
      testNMTokens(conf);
      testContainerToken(conf);
      
      testContainerTokenWithEpoch(conf);

  }

  private void testNMTokens(Configuration conf) throws Exception {

