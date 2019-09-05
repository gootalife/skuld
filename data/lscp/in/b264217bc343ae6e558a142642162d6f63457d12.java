hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeManager.java
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final Log LOG = LogFactory.getLog(NodeManager.class);
  private static long nmStartupTime = System.currentTimeMillis();
  protected final NodeManagerMetrics metrics = NodeManagerMetrics.create();
  private ApplicationACLsManager aclsManager;
  private NodeHealthCheckerService nodeHealthChecker;
    super(NodeManager.class.getName());
  }

  public static long getNMStartupTime() {
    return nmStartupTime;
  }

  protected NodeStatusUpdater createNodeStatusUpdater(Context context,
      Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
    return new NodeStatusUpdaterImpl(context, dispatcher, healthChecker,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/webapp/NodePage.java
              info.getLastNodeUpdateTime()))
          ._("NodeHealthReport",
              info.getHealthReport())
          ._("NodeManager started on", new Date(
              info.getNMStartupTime()))
          ._("NodeManager Version:", info.getNMBuildVersion() +
              " on " + info.getNMVersionBuiltOn())
          ._("Hadoop Version:", info.getHadoopBuildVersion() +

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/webapp/dao/NodeInfo.java

import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

  protected String hadoopVersionBuiltOn;
  protected String id;
  protected String nodeHostName;
  protected long nmStartupTime;

  public NodeInfo() {
  } // JAXB needs this
    this.hadoopVersion = VersionInfo.getVersion();
    this.hadoopBuildVersion = VersionInfo.getBuildVersion();
    this.hadoopVersionBuiltOn = VersionInfo.getDate();
    this.nmStartupTime = NodeManager.getNMStartupTime();
  }

  public String getNodeId() {
    return this.pmemCheckEnabled;
  }

  public long getNMStartupTime() {
    return nmStartupTime;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/webapp/TestNMWebServices.java
  public void verifyNodeInfo(JSONObject json) throws JSONException, Exception {
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject info = json.getJSONObject("nodeInfo");
    assertEquals("incorrect number of elements", 17, info.length());
    verifyNodeInfoGeneric(info.getString("id"), info.getString("healthReport"),
        info.getLong("totalVmemAllocatedContainersMB"),
        info.getLong("totalPmemAllocatedContainersMB"),

