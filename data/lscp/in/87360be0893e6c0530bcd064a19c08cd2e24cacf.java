hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/ParentQueue.java
      		" for children of queue " + queueName);
    }
    for (String nodeLabel : queueCapacities.getExistingNodeLabels()) {
      float capacityByLabel = queueCapacities.getCapacity(nodeLabel);
      float sum = 0;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestQueueParsing.java

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
    
    capacityScheduler.reinitialize(csConf, rmContext);
  }

  @Test(timeout = 60000, expected = java.lang.IllegalArgumentException.class)
  public void testRMStartWrongNodeCapacity() throws Exception {
    YarnConfiguration config = new YarnConfiguration();
    nodeLabelManager = new NullRMNodeLabelsManager();
    nodeLabelManager.init(config);
    config.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);

    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] { "a" });
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "y", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "z", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 100);
    conf.setAccessibleNodeLabels(A, ImmutableSet.of("x", "y", "z"));
    conf.setCapacityByLabel(A, "x", 100);
    conf.setCapacityByLabel(A, "y", 100);
    conf.setCapacityByLabel(A, "z", 70);
    MockRM rm = null;
    try {
      rm = new MockRM(conf) {
        @Override
        public RMNodeLabelsManager createNodeLabelManager() {
          return nodeLabelManager;
        }
      };
    } finally {
      IOUtils.closeStream(rm);
    }
  }
}

