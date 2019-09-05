hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/AbstractYarnScheduler.java
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.SettableFuture;


    new Timer().schedule(new TimerTask() {
      @Override
      public void run() {
        clearPendingContainerCache();
        LOG.info("Release request cache is cleaned up");
      }
    }, nmExpireInterval);
  }

  @VisibleForTesting
  public void clearPendingContainerCache() {
    for (SchedulerApplication<T> app : applications.values()) {
      T attempt = app.getCurrentAppAttempt();
      if (attempt != null) {
        synchronized (attempt) {
          for (ContainerId containerId : attempt.getPendingRelease()) {
            RMAuditLogger.logFailure(app.getUser(),
                AuditConstants.RELEASE_CONTAINER,
                "Unauthorized access or invalid container", "Scheduler",
                "Trying to release container not owned by app "
                    + "or with invalid id.", attempt.getApplicationId(),
                containerId);
          }
          attempt.getPendingRelease().clear();
        }
      }
    }
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/TestAbstractYarnScheduler.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ParameterizedSchedulerTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class TestAbstractYarnScheduler extends ParameterizedSchedulerTestBase {

    }
  }

  @SuppressWarnings({ "rawtypes" })
  @Test(timeout = 10000)
  public void testReleasedContainerIfAppAttemptisNull() throws Exception {
    YarnConfiguration conf=getConf();
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    MockRM rm1 = new MockRM(conf, memStore);
    try {
      rm1.start();
      MockNM nm1 =
          new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
      nm1.registerNode();

      AbstractYarnScheduler scheduler =
          (AbstractYarnScheduler) rm1.getResourceScheduler();
      RMApp mockAPp =
          new MockRMApp(125, System.currentTimeMillis(), RMAppState.NEW);
      SchedulerApplication<FiCaSchedulerApp> application =
          new SchedulerApplication<FiCaSchedulerApp>(null, mockAPp.getUser());

      RMApp app = rm1.submitApp(200);
      MockAM am1 = MockRM.launchAndRegisterAM(app, rm1, nm1);
      final ContainerId runningContainer =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
      am1.allocate(null, Arrays.asList(runningContainer));

      Map schedulerApplications = scheduler.getSchedulerApplications();
      SchedulerApplication schedulerApp =
          (SchedulerApplication) scheduler.getSchedulerApplications().get(
              app.getApplicationId());
      schedulerApplications.put(mockAPp.getApplicationId(), application);

      scheduler.clearPendingContainerCache();

      Assert.assertEquals("Pending containers are not released "
          + "when one of the application attempt is null !", schedulerApp
          .getCurrentAppAttempt().getPendingRelease().size(), 0);
    } finally {
      if (rm1 != null) {
        rm1.stop();
      }
    }
  }

  private void verifyMaximumResourceCapability(
      Resource expectedMaximumResource, AbstractYarnScheduler scheduler) {


