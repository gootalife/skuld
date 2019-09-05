hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/cli/RMAdminCLI.java
    }
  }

  @Override
  protected Collection<String> getTargetIds(String targetNodeToActivate) {
    return HAUtil.getRMHAIds(getConf());
  }

  @Override
  protected String getUsageString() {
    return "Usage: rmadmin";

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/cli/TestRMAdminCLI.java
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2");
    rmAdminCLIWithHAEnabled = new RMAdminCLI(conf) {

      @Override
    assertEquals(0, rmAdminCLIWithHAEnabled.run(args));
    verify(haadmin).transitionToActive(
        any(HAServiceProtocol.StateChangeRequestInfo.class));
    verify(haadmin, times(1)).getServiceStatus();
  }

  @Test(timeout = 500)

