hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/FairSchedulerQueueInfo.java
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;


import java.util.ArrayList;
import java.util.Collection;

import javax.xml.bind.annotation.XmlAccessType;
  }

  public Collection<FairSchedulerQueueInfo> getChildQueues() {
    return childQueues != null ? childQueues.getQueueInfoList() :
        new ArrayList<FairSchedulerQueueInfo>();
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/TestFairSchedulerQueueInfo.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/TestFairSchedulerQueueInfo.java

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueueManager;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFairSchedulerQueueInfo {

  @Test
  public void testEmptyChildQueues() throws Exception {
    FairSchedulerConfiguration conf = new FairSchedulerConfiguration();
    FairScheduler scheduler = mock(FairScheduler.class);
    AllocationConfiguration allocConf = new AllocationConfiguration(conf);
    when(scheduler.getAllocationConfiguration()).thenReturn(allocConf);
    when(scheduler.getConf()).thenReturn(conf);
    when(scheduler.getClusterResource()).thenReturn(Resource.newInstance(1, 1));
    SystemClock clock = new SystemClock();
    when(scheduler.getClock()).thenReturn(clock);
    QueueManager queueManager = new QueueManager(scheduler);
    queueManager.initialize(conf);

    FSQueue testQueue = queueManager.getLeafQueue("test", true);
    FairSchedulerQueueInfo queueInfo =
        new FairSchedulerQueueInfo(testQueue, scheduler);
    Collection<FairSchedulerQueueInfo> childQueues =
        queueInfo.getChildQueues();
    Assert.assertNotNull(childQueues);
    Assert.assertEquals("Child QueueInfo was not empty", 0, childQueues.size());
  }
}

