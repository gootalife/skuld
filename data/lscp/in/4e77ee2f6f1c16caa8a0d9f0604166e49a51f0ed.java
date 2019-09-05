hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/NodeHealthScriptRunner.java
  @Override
  protected void serviceStart() throws Exception {
    nodeHealthScriptScheduler = new Timer("NodeHealthMonitor-Timer", true);
  @Override
  protected void serviceStop() {
    if (nodeHealthScriptScheduler != null) {
      nodeHealthScriptScheduler.cancel();
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeHealthCheckerService.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.NodeHealthScriptRunner;


  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (nodeHealthScriptRunner != null) {
      addService(nodeHealthScriptRunner);
    }
    addService(dirsHandler);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeManager.java
    String nodeHealthScript = 
        conf.get(YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_PATH);
    if(!NodeHealthScriptRunner.shouldRun(nodeHealthScript)) {
      LOG.info("Node Manager health check script is not available "
          + "or doesn't have execute permission, so not "
          + "starting the node health script runner.");
      return null;
    }
    long nmCheckintervalTime = conf.getLong(

