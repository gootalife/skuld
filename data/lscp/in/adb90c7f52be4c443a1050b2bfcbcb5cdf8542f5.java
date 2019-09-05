hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-applications-distributedshell/src/main/java/org/apache/hadoop/yarn/applications/distributedshell/ApplicationMaster.java
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
  private final String linux_bash_command = "bash";
  private final String windows_command = "cmd /c";

  @VisibleForTesting
  protected final Set<ContainerId> launchedContainers =
      Collections.newSetFromMap(new ConcurrentHashMap<ContainerId, Boolean>());

        response.getContainersFromPreviousAttempts();
    LOG.info(appAttemptID + " received " + previousAMRunningContainers.size()
      + " previous attempts' running containers on AM registration.");
    for(Container container: previousAMRunningContainers) {
      launchedContainers.add(container.getId());
    }
    numAllocatedContainers.addAndGet(previousAMRunningContainers.size());


    int numTotalContainersToRequest =
        numTotalContainers - previousAMRunningContainers.size();
    return success;
  }

  @VisibleForTesting
  class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    @SuppressWarnings("unchecked")
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {

        assert (containerStatus.getState() == ContainerState.COMPLETE);
        if (!launchedContainers.contains(containerStatus.getContainerId())) {
          LOG.info("Ignoring completed status of "
              + containerStatus.getContainerId()
              + "; unknown container(probably launched by previous attempt)");
          continue;
        }

        int exitStatus = containerStatus.getExitStatus();

        Thread launchThread = createLaunchContainerThread(allocatedContainer);

        launchThreads.add(launchThread);
        launchedContainers.add(allocatedContainer.getId());
        launchThread.start();
      }
    }
          + appAttemptId.toString(), e);
    }
  }

  RMCallbackHandler getRMCallbackHandler() {
    return new RMCallbackHandler();
  }

  @VisibleForTesting
  void setAmRMClient(AMRMClientAsync client) {
    this.amRMClient = client;
  }

  @VisibleForTesting
  int getNumCompletedContainers() {
    return numCompletedContainers.get();
  }

  @VisibleForTesting
  boolean getDone() {
    return done;
  }

  @VisibleForTesting
  Thread createLaunchContainerThread(Container allocatedContainer) {
    LaunchContainerRunnable runnableLaunchContainer =
        new LaunchContainerRunnable(allocatedContainer, containerListener);
    return new Thread(runnableLaunchContainer);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-applications-distributedshell/src/test/java/org/apache/hadoop/yarn/applications/distributedshell/TestDSAppMaster.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.impl.TimelineClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestDSAppMaster {

  static class TestAppMaster extends ApplicationMaster {
    private int threadsLaunched = 0;

    @Override
    protected Thread createLaunchContainerThread(Container allocatedContainer) {
      threadsLaunched++;
      launchedContainers.add(allocatedContainer.getId());
      return new Thread();
    }

    void setNumTotalContainers(int numTotalContainers) {
      this.numTotalContainers = numTotalContainers;
    }

    int getAllocatedContainers() {
      return this.numAllocatedContainers.get();
    }

    @Override
    void startTimelineClient(final Configuration conf) throws YarnException,
        IOException, InterruptedException {
      timelineClient = null;
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDSAppMasterAllocateHandler() throws Exception {

    TestAppMaster master = new TestAppMaster();
    int targetContainers = 2;
    AMRMClientAsync mockClient = Mockito.mock(AMRMClientAsync.class);
    master.setAmRMClient(mockClient);
    master.setNumTotalContainers(targetContainers);
    Mockito.doNothing().when(mockClient)
        .addContainerRequest(Matchers.any(AMRMClient.ContainerRequest.class));

    ApplicationMaster.RMCallbackHandler handler = master.getRMCallbackHandler();

    List<Container> containers = new ArrayList<>(1);
    ContainerId id1 = BuilderUtils.newContainerId(1, 1, 1, 1);
    containers.add(generateContainer(id1));

    master.numRequestedContainers.set(targetContainers);

    handler.onContainersAllocated(containers);
    Assert.assertEquals("Wrong container allocation count", 1,
        master.getAllocatedContainers());
    Mockito.verifyZeroInteractions(mockClient);
    Assert.assertEquals("Incorrect number of threads launched", 1,
        master.threadsLaunched);

    containers.clear();
    ContainerId id2 = BuilderUtils.newContainerId(1, 1, 1, 2);
    containers.add(generateContainer(id2));
    ContainerId id3 = BuilderUtils.newContainerId(1, 1, 1, 3);
    containers.add(generateContainer(id3));
    ContainerId id4 = BuilderUtils.newContainerId(1, 1, 1, 4);
    containers.add(generateContainer(id4));
    handler.onContainersAllocated(containers);
    Assert.assertEquals("Wrong final container allocation count", 4,
        master.getAllocatedContainers());

    Assert.assertEquals("Incorrect number of threads launched", 4,
        master.threadsLaunched);

    List<ContainerStatus> status = new ArrayList<>();
    status.add(generateContainerStatus(id1, ContainerExitStatus.SUCCESS));
    status.add(generateContainerStatus(id2, ContainerExitStatus.SUCCESS));
    status.add(generateContainerStatus(id3, ContainerExitStatus.ABORTED));
    status.add(generateContainerStatus(id4, ContainerExitStatus.ABORTED));
    handler.onContainersCompleted(status);

    Assert.assertEquals("Unexpected number of completed containers",
        targetContainers, master.getNumCompletedContainers());
    Assert.assertTrue("Master didn't finish containers as expected",
        master.getDone());

    status = new ArrayList<>();
    ContainerId id5 = BuilderUtils.newContainerId(1, 1, 1, 5);
    status.add(generateContainerStatus(id5, ContainerExitStatus.ABORTED));
    Assert.assertEquals("Unexpected number of completed containers",
        targetContainers, master.getNumCompletedContainers());
    Assert.assertTrue("Master didn't finish containers as expected",
        master.getDone());
    status.add(generateContainerStatus(id5, ContainerExitStatus.SUCCESS));
    Assert.assertEquals("Unexpected number of completed containers",
        targetContainers, master.getNumCompletedContainers());
    Assert.assertTrue("Master didn't finish containers as expected",
        master.getDone());
  }

  private Container generateContainer(ContainerId cid) {
    return Container.newInstance(cid, NodeId.newInstance("host", 5000),
      "host:80", Resource.newInstance(1024, 1), Priority.newInstance(0), null);
  }

  private ContainerStatus
      generateContainerStatus(ContainerId id, int exitStatus) {
    return ContainerStatus.newInstance(id, ContainerState.COMPLETE, "",
      exitStatus);
  }

  @Test
  public void testTimelineClientInDSAppMaster() throws Exception {
    ApplicationMaster appMaster = new ApplicationMaster();

