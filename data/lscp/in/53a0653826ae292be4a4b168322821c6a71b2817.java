hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/nodelabels/RMNodeLabelsManager.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
      throws IOException {
    try {
      writeLock.lock();
      if (getServiceState() == Service.STATE.STARTED) {
        checkRemoveFromClusterNodeLabelsOfQueue(labelsToRemove);
      }
      Map<String, Host> before = cloneNodeMap();


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/nodelabels/TestRMNodeLabelsManager.java

package org.apache.hadoop.yarn.server.resourcemanager.nodelabels;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.nodelabels.NodeLabelTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
  private final Resource LARGE_NODE = Resource.newInstance(1000, 0);
  
  NullRMNodeLabelsManager mgr = null;
  RMNodeLabelsManager lmgr = null;
  boolean checkQueueCall = false;
  @Before
  public void before() {
    mgr = new NullRMNodeLabelsManager();
    checkNodeLabelInfo(infos, "z", 0, 0);
  }

  @Test(timeout = 60000)
  public void testcheckRemoveFromClusterNodeLabelsOfQueue() throws Exception {
    class TestRMLabelManger extends RMNodeLabelsManager {
      @Override
      protected void checkRemoveFromClusterNodeLabelsOfQueue(
          Collection<String> labelsToRemove) throws IOException {
        checkQueueCall = true;
      }

    }
    lmgr = new TestRMLabelManger();
    Configuration conf = new Configuration();
    File tempDir = File.createTempFile("nlb", ".tmp");
    tempDir.delete();
    tempDir.mkdirs();
    tempDir.deleteOnExit();
    conf.set(YarnConfiguration.FS_NODE_LABELS_STORE_ROOT_DIR,
        tempDir.getAbsolutePath());
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    MockRM rm = new MockRM(conf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return lmgr;
      }
    };
    lmgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("a"));
    lmgr.removeFromClusterNodeLabels(Arrays.asList(new String[] { "a" }));
    rm.getRMContext().setNodeLabelManager(lmgr);
    rm.start();
    lmgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("a"));
    Assert.assertEquals(false, checkQueueCall);
    lmgr.removeFromClusterNodeLabels(Arrays.asList(new String[] { "a" }));
    Assert.assertEquals(true, checkQueueCall);
    lmgr.stop();
    lmgr.close();
    rm.stop();
  }

  @Test(timeout = 5000)
  public void testLabelsToNodesOnNodeActiveDeactive() throws Exception {

