hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestDockerContainerExecutor.java
public class TestDockerContainerExecutor {

    dockerUrl = System.getProperty("docker-service-url");
    LOG.info("dockerUrl: " + dockerUrl);
    if (!Strings.isNullOrEmpty(dockerUrl)) {
      dockerUrl = " -H " + dockerUrl;
    } else if(isDockerDaemonRunningLocally()) {
      dockerUrl = "";
    } else {
      return;
    }
    dockerExec = "docker " + dockerUrl;
    conf.set(
      YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, yarnImage);
    return exec != null;
  }

  private boolean isDockerDaemonRunningLocally() {
    boolean dockerDaemonRunningLocally = true;
      try {
        shellExec("docker info");
      } catch (Exception e) {
        LOG.info("docker daemon is not running on local machine.");
        dockerDaemonRunningLocally = false;
      }
      return dockerDaemonRunningLocally;
  }

  @Test(timeout=1000000)
  public void testLaunchContainer() throws IOException {
    if (!shouldRun()) {
      LOG.warn("Docker not installed, aborting test.");

