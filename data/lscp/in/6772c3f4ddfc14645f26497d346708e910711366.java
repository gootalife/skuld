hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
  public static final String RECOVERY_ENABLED = RM_PREFIX + "recovery.enabled";
  public static final boolean DEFAULT_RM_RECOVERY_ENABLED = false;

  public static final String YARN_FAIL_FAST = YARN_PREFIX + "fail-fast";
  public static final boolean DEFAULT_YARN_FAIL_FAST = true;

  public static final String RM_FAIL_FAST = RM_PREFIX + "fail-fast";

  @Private
  public static final String RM_WORK_PRESERVING_RECOVERY_ENABLED = RM_PREFIX
      + "work-preserving-recovery.enabled";
            YARN_HTTP_POLICY_DEFAULT));
  }

  public static boolean shouldRMFailFast(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.RM_FAIL_FAST,
        conf.getBoolean(YarnConfiguration.YARN_FAIL_FAST,
            YarnConfiguration.DEFAULT_YARN_FAIL_FAST));
  }

  @Private
  public static String getClusterId(Configuration conf) {
    String clusterId = conf.get(YarnConfiguration.RM_CLUSTER_ID);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/RMStateStore.java
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
  protected void notifyStoreOperationFailed(Exception failureCause) {
    LOG.error("State store operation failed ", failureCause);
    if (failureCause instanceof StoreFencedException) {
      updateFencedState();
      Thread standByTransitionThread =
      standByTransitionThread.setName("StandByTransitionThread Handler");
      standByTransitionThread.start();
    } else {
      if (YarnConfiguration.shouldRMFailFast(getConfig())) {
        rmDispatcher.getEventHandler().handle(
            new RMFatalEvent(RMFatalEventType.STATE_STORE_OP_FAILED,
                failureCause));
      }
    }
  }
 

