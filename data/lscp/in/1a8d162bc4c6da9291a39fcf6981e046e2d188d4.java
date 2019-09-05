hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMRestart.java
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
    List<ApplicationReport> appList2 = response2.getApplicationList();
    Assert.assertTrue(3 == appList2.size());

    verify(rm2.getRMAppManager(), timeout(1000).times(3)).
        logApplicationSummary(isA(ApplicationId.class));
  }

  private MockAM launchAM(RMApp app, MockRM rm, MockNM nm)

