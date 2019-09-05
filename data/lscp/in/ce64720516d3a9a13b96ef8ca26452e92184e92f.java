hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ApplicationMasterService.java
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStatusupdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

    return hasApplicationMasterRegistered;
  }

  protected final static List<Container> EMPTY_CONTAINER_LIST =
      new ArrayList<Container>();
  protected static final Allocation EMPTY_ALLOCATION = new Allocation(
      EMPTY_CONTAINER_LIST, Resources.createResource(0), null, null, null);

  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnException, IOException {
      }

      Allocation allocation;
      RMAppAttemptState state =
          app.getRMAppAttempt(appAttemptId).getAppAttemptState();
      if (state.equals(RMAppAttemptState.FINAL_SAVING) ||
          state.equals(RMAppAttemptState.FINISHING) ||
          app.isAppFinalStateStored()) {
        LOG.warn(appAttemptId + " is in " + state +
                 " state, ignore container allocate request.");
        allocation = EMPTY_ALLOCATION;
      } else {
        allocation =
          this.rScheduler.allocate(appAttemptId, ask, release,
              blacklistAdditions, blacklistRemovals);
      }

      if (!blacklistAdditions.isEmpty() || !blacklistRemovals.isEmpty()) {
        LOG.info("blacklist are updated in Scheduler." +

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestApplicationMasterService.java
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidContainerReleaseException;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
      rm.stop();
    }
  }

  @Test(timeout=1200000)
  public void  testAllocateAfterUnregister() throws Exception {
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher rmDispatcher = (DrainDispatcher) rm.getRMContext()
            .getDispatcher();
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);

    RMApp app1 = rm.submitApp(2048);

    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    FinishApplicationMasterRequest req =
        FinishApplicationMasterRequest.newInstance(
           FinalApplicationStatus.KILLED, "", "");
    am1.unregisterAppAttempt(req, false);
    am1.addRequests(new String[] { "127.0.0.1" }, GB, 1, 1);
    AllocateResponse alloc1Response = am1.schedule();

    nm1.nodeHeartbeat(true);
    rmDispatcher.await();
    alloc1Response = am1.schedule();
    Assert.assertEquals(0, alloc1Response.getAllocatedContainers().size());
  }

  private static class MyResourceManager extends MockRM {

    public MyResourceManager(YarnConfiguration conf) {
      super(conf);
    }
    @Override
    protected Dispatcher createDispatcher() {
      return new DrainDispatcher();
    }
  }
}

