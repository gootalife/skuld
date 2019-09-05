hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-applications-distributedshell/src/main/java/org/apache/hadoop/yarn/applications/distributedshell/ApplicationMaster.java
    DS_APP_ATTEMPT, DS_CONTAINER
  }

  private static final String YARN_SHELL_ID = "YARN_SHELL_ID";

  private Configuration conf;

  private final String linux_bash_command = "bash";
  private final String windows_command = "cmd /c";

  private int yarnShellIdCounter = 1;

  @VisibleForTesting
  protected final Set<ContainerId> launchedContainers =
      Collections.newSetFromMap(new ConcurrentHashMap<ContainerId, Boolean>());
          + allocatedContainers.size());
      numAllocatedContainers.addAndGet(allocatedContainers.size());
      for (Container allocatedContainer : allocatedContainers) {
        String yarnShellId = Integer.toString(yarnShellIdCounter);
        yarnShellIdCounter++;
        LOG.info("Launching shell command on a new container."
            + ", containerId=" + allocatedContainer.getId()
            + ", yarnShellId=" + yarnShellId
            + ", containerNode=" + allocatedContainer.getNodeId().getHost()
            + ":" + allocatedContainer.getNodeId().getPort()
            + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()

        Thread launchThread = createLaunchContainerThread(allocatedContainer,
            yarnShellId);

  private class LaunchContainerRunnable implements Runnable {

    private Container container;
    private String shellId;

    NMCallbackHandler containerListener;

    public LaunchContainerRunnable(Container lcontainer,
        NMCallbackHandler containerListener, String shellId) {
      this.container = lcontainer;
      this.containerListener = containerListener;
      this.shellId = shellId;
    }

    @Override
    public void run() {
      LOG.info("Setting up container launch container for containerid="
          + container.getId() + " with shellid=" + shellId);

      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
      Map<String, String> myShellEnv = new HashMap<String, String>(shellEnv);
      myShellEnv.put(YARN_SHELL_ID, shellId);
      ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
        localResources, myShellEnv, commands, null, allTokens.duplicate(),
          null);
      containerListener.addContainer(container.getId(), container);
      nmClientAsync.startContainerAsync(container, ctx);
    }
  }

  @VisibleForTesting
  Thread createLaunchContainerThread(Container allocatedContainer,
      String shellId) {
    LaunchContainerRunnable runnableLaunchContainer =
        new LaunchContainerRunnable(allocatedContainer, containerListener,
            shellId);
    return new Thread(runnableLaunchContainer);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-applications-distributedshell/src/test/java/org/apache/hadoop/yarn/applications/distributedshell/TestDSAppMaster.java

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


  static class TestAppMaster extends ApplicationMaster {
    private int threadsLaunched = 0;
    public List<String> yarnShellIds = new ArrayList<String>();

    @Override
    protected Thread createLaunchContainerThread(Container allocatedContainer,
        String shellId) {
      threadsLaunched++;
      launchedContainers.add(allocatedContainer.getId());
      yarnShellIds.add(shellId);
      return new Thread();
    }

    Mockito.verifyZeroInteractions(mockClient);
    Assert.assertEquals("Incorrect number of threads launched", 1,
        master.threadsLaunched);
    Assert.assertEquals("Incorrect YARN Shell IDs",
        Arrays.asList("1"), master.yarnShellIds);

    containers.clear();
    Assert.assertEquals("Incorrect number of threads launched", 4,
        master.threadsLaunched);

    Assert.assertEquals("Incorrect YARN Shell IDs",
        Arrays.asList("1", "2", "3", "4"), master.yarnShellIds);

    List<ContainerStatus> status = new ArrayList<>();
    status.add(generateContainerStatus(id1, ContainerExitStatus.SUCCESS));

