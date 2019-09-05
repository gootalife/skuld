hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/jobhistory/TestJobHistoryEventHandler.java
    long currentTime = System.currentTimeMillis();
    try {
      yarnCluster = new MiniYARNCluster(
            TestJobHistoryEventHandler.class.getSimpleName(), 1, 1, 1, 1);
      yarnCluster.init(conf);
      yarnCluster.start();
      jheh.start();

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapred/TestMRTimelineEventHandling.java

public class TestMRTimelineEventHandling {

  @Test
  public void testTimelineServiceStartInMiniCluster() throws Exception {
    Configuration conf = new YarnConfiguration();

    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA, true);
    MiniMRYarnCluster cluster = null;
    try {
      cluster = new MiniMRYarnCluster(
          TestJobHistoryEventHandler.class.getSimpleName(), 1);
      cluster.init(conf);
      cluster.start();

      Assert.assertNull("Timeline Service should not have been started",
          cluster.getApplicationHistoryServer());
    }
    finally {
      if(cluster != null) {
        cluster.stop();
      }
    }
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA, false);
    cluster = null;
    try {
      cluster = new MiniMRYarnCluster(
          TestJobHistoryEventHandler.class.getSimpleName(), 1);
      cluster.init(conf);
      cluster.start();

      Assert.assertNull("Timeline Service should not have been started",
          cluster.getApplicationHistoryServer());
    }
    finally {
      if(cluster != null) {
        cluster.stop();
      }
    }
  }

  @Test
  public void testMRTimelineEventHandling() throws Exception {
    Configuration conf = new YarnConfiguration();
    MiniMRYarnCluster cluster = null;
    try {
      cluster = new MiniMRYarnCluster(
          TestJobHistoryEventHandler.class.getSimpleName(), 1);
      cluster.init(conf);
      cluster.start();
      TimelineStore ts = cluster.getApplicationHistoryServer()
    MiniMRYarnCluster cluster = null;
    try {
      cluster = new MiniMRYarnCluster(
          TestJobHistoryEventHandler.class.getSimpleName(), 1);
      cluster.init(conf);
      cluster.start();
      TimelineStore ts = cluster.getApplicationHistoryServer()
    cluster = null;
    try {
      cluster = new MiniMRYarnCluster(
          TestJobHistoryEventHandler.class.getSimpleName(), 1);
      cluster.init(conf);
      cluster.start();
      TimelineStore ts = cluster.getApplicationHistoryServer()

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-applications-distributedshell/src/test/java/org/apache/hadoop/yarn/applications/distributedshell/TestDistributedShell.java
    if (yarnCluster == null) {
      yarnCluster =
          new MiniYARNCluster(TestDistributedShell.class.getSimpleName(), 1,
              numNodeManager, 1, 1);
      yarnCluster.init(conf);
      
      yarnCluster.start();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-tests/src/test/java/org/apache/hadoop/yarn/server/MiniYARNCluster.java
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.MemoryApplicationHistoryStore;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
      addService(new NodeManagerWrapper(index));
    }

    if(conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED) || enableAHS) {
        addService(new ApplicationHistoryServerWrapper());
    }
    

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-tests/src/test/java/org/apache/hadoop/yarn/server/TestMiniYarnCluster.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-tests/src/test/java/org/apache/hadoop/yarn/server/TestMiniYarnCluster.java

package org.apache.hadoop.yarn.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class TestMiniYarnCluster {
  @Test
  public void testTimelineServiceStartInMiniCluster() throws Exception {
    Configuration conf = new YarnConfiguration();
    int numNodeManagers = 1;
    int numLocalDirs = 1;
    int numLogDirs = 1;
    boolean enableAHS;

    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false);
    enableAHS = false;
    MiniYARNCluster cluster = null;
    try {
      cluster = new MiniYARNCluster(TestMiniYarnCluster.class.getSimpleName(),
          numNodeManagers, numLocalDirs, numLogDirs, numLogDirs, enableAHS);
      cluster.init(conf);
      cluster.start();

      Assert.assertNull("Timeline Service should not have been started",
          cluster.getApplicationHistoryServer());
    }
    finally {
      if(cluster != null) {
        cluster.stop();
      }
    }

    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    enableAHS = false;
    cluster = null;
    try {
      cluster = new MiniYARNCluster(TestMiniYarnCluster.class.getSimpleName(),
          numNodeManagers, numLocalDirs, numLogDirs, numLogDirs, enableAHS);
      cluster.init(conf);
      cluster.start();

      int wait = 0;
      while(cluster.getApplicationHistoryServer() == null && wait < 20) {
        Thread.sleep(500);
        wait++;
      }
      Assert.assertNotNull("Timeline Service should have been started",
          cluster.getApplicationHistoryServer());
    }
    finally {
      if(cluster != null) {
        cluster.stop();
      }
    }
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false);
    enableAHS = true;
    cluster = null;
    try {
      cluster = new MiniYARNCluster(TestMiniYarnCluster.class.getSimpleName(),
          numNodeManagers, numLocalDirs, numLogDirs, numLogDirs, enableAHS);
      cluster.init(conf);
      cluster.start();

      int wait = 0;
      while(cluster.getApplicationHistoryServer() == null && wait < 20) {
        Thread.sleep(500);
        wait++;
      }
      Assert.assertNotNull("Timeline Service should have been started",
          cluster.getApplicationHistoryServer());
    }
    finally {
      if(cluster != null) {
        cluster.stop();
      }
    }
  }
}

