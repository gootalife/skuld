hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/nodelabels/CommonNodeLabelsManager.java
    isDistributedNodeLabelConfiguration  =
        YarnConfiguration.isDistributedNodeLabelConfiguration(conf);
    
    labelCollections.put(NO_LABEL, new RMNodeLabel(NO_LABEL));
  }


  @Override
  protected void serviceStart() throws Exception {
    if (nodeLabelsEnabled) {
      initNodeLabelStore(getConfig());
    }
    
    initDispatcher(getConfig());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/nodelabels/TestFileSystemNodeLabelsStore.java

    mgr = new MockNodeLabelManager();
    mgr.init(conf);
    mgr.start();

    Assert.assertEquals(3, mgr.getClusterNodeLabelNames().size());
    mgr.stop();
    mgr = new MockNodeLabelManager();
    mgr.init(conf);
    mgr.start();

    Assert.assertEquals(3, mgr.getClusterNodeLabelNames().size());
    cf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);
    mgr.init(cf);
    mgr.start();

    Assert.assertEquals(3, mgr.getClusterNodeLabels().size());

    mgr = new MockNodeLabelManager();
    mgr.init(conf);
    mgr.start();

    Assert.assertEquals(3, mgr.getClusterNodeLabelNames().size());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/MockRM.java
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
  static final Logger LOG = Logger.getLogger(MockRM.class);
  static final String ENABLE_WEBAPP = "mockrm.webapp.enabled";
  
  final private boolean useNullRMNodeLabelsManager;

  public MockRM() {
    this(new YarnConfiguration());
  }
  }
  
  public MockRM(Configuration conf, RMStateStore store) {
    this(conf, store, true);
  }
  
  public MockRM(Configuration conf, RMStateStore store,
      boolean useNullRMNodeLabelsManager) {
    super();
    this.useNullRMNodeLabelsManager = useNullRMNodeLabelsManager;
    init(conf instanceof YarnConfiguration ? conf : new YarnConfiguration(conf));
    if(store != null) {
      setRMStateStore(store);
  }
  
  @Override
  protected RMNodeLabelsManager createNodeLabelManager()
      throws InstantiationException, IllegalAccessException {
    if (useNullRMNodeLabelsManager) {
      RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
      mgr.init(getConfig());
      return mgr;
    } else {
      return super.createNodeLabelManager();
    }
  }

  public void waitForState(ApplicationId appId, RMAppState finalState)

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/RMHATestBase.java
  }

  protected void startRMs() throws IOException {
    rm1 = new MockRM(confForRM1, null, false);
    rm2 = new MockRM(confForRM2, null, false);
    startRMs(rm1, confForRM1, rm2, confForRM2);

  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMHAForNodeLabels.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMHAForNodeLabels.java

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class TestRMHAForNodeLabels extends RMHATestBase {
  public static final Log LOG = LogFactory
      .getLog(TestSubmitApplicationWithRMHA.class);

  @Before
  @Override
  public void setup() throws Exception {
    super.setup();
    
    File tempDir = File.createTempFile("nlb", ".tmp");
    tempDir.delete();
    tempDir.mkdirs();
    tempDir.deleteOnExit();
    
    confForRM1.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    confForRM1.set(YarnConfiguration.FS_NODE_LABELS_STORE_ROOT_DIR,
        tempDir.getAbsolutePath());
    
    confForRM2.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    confForRM2.set(YarnConfiguration.FS_NODE_LABELS_STORE_ROOT_DIR,
        tempDir.getAbsolutePath());
  }
  
  @Test
  public void testRMHARecoverNodeLabels() throws Exception {
    startRMs();
    
    rm1.getRMContext()
        .getNodeLabelManager()
        .addToCluserNodeLabels(
            Arrays.asList(NodeLabel.newInstance("a"),
                NodeLabel.newInstance("b"), NodeLabel.newInstance("c")));
   
    Map<NodeId, Set<String>> nodeToLabels = new HashMap<>();
    nodeToLabels.put(NodeId.newInstance("host1", 0), ImmutableSet.of("a"));
    nodeToLabels.put(NodeId.newInstance("host2", 0), ImmutableSet.of("b"));
    
    rm1.getRMContext().getNodeLabelManager().replaceLabelsOnNode(nodeToLabels);

    explicitFailover();

    Assert
        .assertTrue(rm2.getRMContext().getNodeLabelManager()
            .getClusterNodeLabelNames()
            .containsAll(ImmutableSet.of("a", "b", "c")));
    Assert.assertTrue(rm2.getRMContext().getNodeLabelManager()
        .getNodeLabels().get(NodeId.newInstance("host1", 0)).contains("a"));
    Assert.assertTrue(rm2.getRMContext().getNodeLabelManager()
        .getNodeLabels().get(NodeId.newInstance("host2", 0)).contains("b"));
  }
}

