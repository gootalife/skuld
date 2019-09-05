hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmcontainer/RMContainerImpl.java
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerAllocatedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerRescheduledEvent;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
    .addTransition(RMContainerState.ALLOCATED, RMContainerState.EXPIRED,
        RMContainerEventType.EXPIRE, new FinishedTransition())
    .addTransition(RMContainerState.ALLOCATED, RMContainerState.KILLED,
        RMContainerEventType.KILL, new ContainerRescheduledTransition())

    .addTransition(RMContainerState.ACQUIRED, RMContainerState.RUNNING,
    }
  }

  private static final class ContainerRescheduledTransition extends
      FinishedTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {
      container.eventHandler.handle(new ContainerRescheduledEvent(container));
      super.transition(container, event);
    }
  }

  private static class FinishedTransition extends BaseTransition {

    @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacityScheduler.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerRescheduledEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeLabelsUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
      killContainer(containerToBeKilled);
    }
    break;
    case CONTAINER_RESCHEDULED:
    {
      ContainerRescheduledEvent containerRescheduledEvent =
          (ContainerRescheduledEvent) event;
      RMContainer container = containerRescheduledEvent.getContainer();
      recoverResourceRequestForContainer(container);
    }
    break;
    default:
      LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("KILL_CONTAINER: container" + cont.toString());
    }
    completedContainer(cont, SchedulerUtils.createPreemptedContainerStatus(
      cont.getContainerId(), SchedulerUtils.PREEMPTED_CONTAINER),
      RMContainerEventType.KILL);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/event/ContainerRescheduledEvent.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/event/ContainerRescheduledEvent.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

public class ContainerRescheduledEvent extends SchedulerEvent {

  private RMContainer container;

  public ContainerRescheduledEvent(RMContainer container) {
    super(SchedulerEventType.CONTAINER_RESCHEDULED);
    this.container = container;
  }

  public RMContainer getContainer() {
    return container;
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/event/SchedulerEventType.java
  CONTAINER_EXPIRED,

  CONTAINER_RESCHEDULED,

  DROP_RESERVATION,
  PREEMPT_CONTAINER,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FairScheduler.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerRescheduledEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
          SchedulerUtils.createPreemptedContainerStatus(
            container.getContainerId(), SchedulerUtils.PREEMPTED_CONTAINER);

        completedContainer(container, status, RMContainerEventType.KILL);
              SchedulerUtils.EXPIRED_CONTAINER),
          RMContainerEventType.EXPIRE);
      break;
    case CONTAINER_RESCHEDULED:
      if (!(event instanceof ContainerRescheduledEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      ContainerRescheduledEvent containerRescheduledEvent =
          (ContainerRescheduledEvent) event;
      RMContainer container = containerRescheduledEvent.getContainer();
      recoverResourceRequestForContainer(container);
      break;
    default:
      LOG.error("Unknown event arrived at FairScheduler: " + event.toString());
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fifo/FifoScheduler.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerRescheduledEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
          RMContainerEventType.EXPIRE);
    }
    break;
    case CONTAINER_RESCHEDULED:
    {
      ContainerRescheduledEvent containerRescheduledEvent =
          (ContainerRescheduledEvent) event;
      RMContainer container = containerRescheduledEvent.getContainer();
      recoverResourceRequestForContainer(container);
    }
    break;
    default:
      LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/applicationsmanager/TestAMRestart.java
  private void waitForContainersToFinish(int expectedNum, RMAppAttempt attempt)
      throws InterruptedException {
    int count = 0;
    while (attempt.getJustFinishedContainers().size() < expectedNum
        && count < 500) {
      Thread.sleep(100);
      count++;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/TestAbstractYarnScheduler.java
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
    }
  }

  @Test(timeout = 60000)
  public void testResourceRequestRestoreWhenRMContainerIsAtAllocated()
      throws Exception {
    configureScheduler();
    YarnConfiguration conf = getConf();
    MockRM rm1 = new MockRM(conf);
    try {
      rm1.start();
      RMApp app1 =
          rm1.submitApp(200, "name", "user",
              new HashMap<ApplicationAccessType, String>(), false, "default",
              -1, null, "Test", false, true);
      MockNM nm1 =
          new MockNM("127.0.0.1:1234", 10240, rm1.getResourceTrackerService());
      nm1.registerNode();

      MockNM nm2 =
          new MockNM("127.0.0.1:2351", 10240, rm1.getResourceTrackerService());
      nm2.registerNode();

      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

      int NUM_CONTAINERS = 1;
      am1.allocate("127.0.0.1", 1024, NUM_CONTAINERS,
          new ArrayList<ContainerId>());
      nm1.nodeHeartbeat(true);

      List<Container> containers =
          am1.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers();
      while (containers.size() != NUM_CONTAINERS) {
        nm1.nodeHeartbeat(true);
        containers.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers());
        Thread.sleep(200);
      }

      nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2,
          ContainerState.RUNNING);
      ContainerId containerId2 =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
      rm1.waitForState(nm1, containerId2, RMContainerState.RUNNING);

      am1.allocate("127.0.0.1", 1024, NUM_CONTAINERS,
          new ArrayList<ContainerId>());
      nm2.nodeHeartbeat(true);
      ContainerId containerId3 =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), 3);
      rm1.waitForContainerAllocated(nm2, containerId3);
      rm1.waitForState(nm2, containerId3, RMContainerState.ALLOCATED);

      nm2.registerNode();

      rm1.waitForState(nm2, containerId3, RMContainerState.KILLED);

      containers =
          am1.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers();
      while (containers.size() != NUM_CONTAINERS) {
        nm2.nodeHeartbeat(true);
        containers.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers());
        Thread.sleep(200);
      }

      nm2.nodeHeartbeat(am1.getApplicationAttemptId(), 4,
          ContainerState.RUNNING);
      ContainerId containerId4 =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), 4);
      rm1.waitForState(nm2, containerId4, RMContainerState.RUNNING);
    } finally {
      rm1.stop();
    }
  }

  private void verifyMaximumResourceCapability(
      Resource expectedMaximumResource, AbstractYarnScheduler scheduler) {


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/TestFairScheduler.java
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerRescheduledEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
    scheduler.warnOrKillContainer(rmContainer);

    scheduler.handle(new ContainerRescheduledEvent(rmContainer));

    List<ResourceRequest> requests = rmContainer.getResourceRequests();
    Assert.assertEquals(3, requests.size());

