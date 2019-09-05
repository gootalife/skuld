hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
  public static final String APPLICATION_HISTORY_STORE =
      APPLICATION_HISTORY_PREFIX + "store-class";

  @Private
  public static final String
      APPLICATION_HISTORY_SAVE_NON_AM_CONTAINER_META_INFO =
        APPLICATION_HISTORY_PREFIX + "save-non-am-container-meta-info";
  @Private
  public static final boolean
            DEFAULT_APPLICATION_HISTORY_SAVE_NON_AM_CONTAINER_META_INFO = true;

  @Private
  public static final String FS_APPLICATION_HISTORY_STORE_URI =

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/AppBlock.java
            @Override
            public ContainerReport run() throws Exception {
              ContainerReport report = null;
              if (request.getContainerId() != null) {
                  try {
                    report = appBaseProt.getContainerReport(request)
                        .getContainerReport();
                  } catch (ContainerNotFoundException ex) {
                    LOG.warn(ex.getMessage());
                  }
              }
              return report;
            }
          });

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmcontainer/RMContainerImpl.java
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
        .currentTimeMillis(), "");
  }

  private boolean saveNonAMContainerMetaInfo;

  public RMContainerImpl(Container container,
      ApplicationAttemptId appAttemptId, NodeId nodeId, String user,
      RMContext rmContext, String nodeLabelExpression) {
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    saveNonAMContainerMetaInfo = rmContext.getYarnConfiguration().getBoolean(
       YarnConfiguration.APPLICATION_HISTORY_SAVE_NON_AM_CONTAINER_META_INFO,
       YarnConfiguration
                 .DEFAULT_APPLICATION_HISTORY_SAVE_NON_AM_CONTAINER_META_INFO);

    rmContext.getRMApplicationHistoryWriter().containerStarted(this);

    if (saveNonAMContainerMetaInfo) {
      rmContext.getSystemMetricsPublisher().containerCreated(
          this, this.creationTime);
    }
  }

  @Override
  public ContainerId getContainerId() {
    } finally {
      writeLock.unlock();
    }

    if (!saveNonAMContainerMetaInfo && this.isAMContainer) {
      rmContext.getSystemMetricsPublisher().containerCreated(
          this, this.creationTime);
    }
  }
  
  @Override

      container.rmContext.getRMApplicationHistoryWriter().containerFinished(
        container);

      boolean saveNonAMContainerMetaInfo =
          container.rmContext.getYarnConfiguration().getBoolean(
              YarnConfiguration
                .APPLICATION_HISTORY_SAVE_NON_AM_CONTAINER_META_INFO,
              YarnConfiguration
                .DEFAULT_APPLICATION_HISTORY_SAVE_NON_AM_CONTAINER_META_INFO);

      if (saveNonAMContainerMetaInfo || container.isAMContainer()) {
        container.rmContext.getSystemMetricsPublisher().containerFinished(
            container, container.finishTime);
      }

    }

    private static void updateAttemptMetrics(RMContainerImpl container) {
      Resource resource = container.getContainer().getResource();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestClientRMService.java
    when(rmContext.getRMApplicationHistoryWriter()).thenReturn(writer);
    SystemMetricsPublisher publisher = mock(SystemMetricsPublisher.class);
    when(rmContext.getSystemMetricsPublisher()).thenReturn(publisher);
    when(rmContext.getYarnConfiguration()).thenReturn(new YarnConfiguration());
    ConcurrentHashMap<ApplicationId, RMApp> apps = getRMApps(rmContext,
        yarnScheduler);
    when(rmContext.getRMApps()).thenReturn(apps);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/rmcontainer/TestRMContainerImpl.java
package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

    Assert.assertNull(scheduler.getRMContainer(containerId2)
        .getResourceRequests());
  }

  @Test (timeout = 180000)
  public void testStoreAllContainerMetrics() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    MockRM rm1 = new MockRM(conf);

    SystemMetricsPublisher publisher = mock(SystemMetricsPublisher.class);
    rm1.getRMContext().setSystemMetricsPublisher(publisher);

    rm1.start();
    MockNM nm1 = rm1.registerNode("unknownhost:1234", 8000);
    RMApp app1 = rm1.submitApp(1024);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.RUNNING);

    am1.allocate("127.0.0.1", 1024, 1, new ArrayList<ContainerId>());
    ContainerId containerId2 = ContainerId.newContainerId(
        am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.ALLOCATED);
    am1.allocate(new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>())
        .getAllocatedContainers();
    rm1.waitForState(nm1, containerId2, RMContainerState.ACQUIRED);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2, ContainerState.COMPLETE);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm1.waitForState(nm1, containerId2, RMContainerState.COMPLETED);
    rm1.stop();

    verify(publisher, times(2)).containerCreated(any(RMContainer.class), anyLong());
    verify(publisher, times(2)).containerFinished(any(RMContainer.class), anyLong());
  }

  @Test (timeout = 180000)
  public void testStoreOnlyAMContainerMetrics() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    conf.setBoolean(
        YarnConfiguration.APPLICATION_HISTORY_SAVE_NON_AM_CONTAINER_META_INFO,
        false);
    MockRM rm1 = new MockRM(conf);

    SystemMetricsPublisher publisher = mock(SystemMetricsPublisher.class);
    rm1.getRMContext().setSystemMetricsPublisher(publisher);

    rm1.start();
    MockNM nm1 = rm1.registerNode("unknownhost:1234", 8000);
    RMApp app1 = rm1.submitApp(1024);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.RUNNING);

    am1.allocate("127.0.0.1", 1024, 1, new ArrayList<ContainerId>());
    ContainerId containerId2 = ContainerId.newContainerId(
        am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.ALLOCATED);
    am1.allocate(new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>())
        .getAllocatedContainers();
    rm1.waitForState(nm1, containerId2, RMContainerState.ACQUIRED);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2, ContainerState.COMPLETE);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm1.waitForState(nm1, containerId2, RMContainerState.COMPLETED);
    rm1.stop();

    verify(publisher, times(1)).containerCreated(any(RMContainer.class), anyLong());
    verify(publisher, times(1)).containerFinished(any(RMContainer.class), anyLong());
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestChildQueueOrder.java
    when(rmContext.getDispatcher()).thenReturn(drainDispatcher);
    when(rmContext.getRMApplicationHistoryWriter()).thenReturn(writer);
    when(rmContext.getSystemMetricsPublisher()).thenReturn(publisher);
    when(rmContext.getYarnConfiguration()).thenReturn(new YarnConfiguration());
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        app_0.getApplicationId(), 1);
    ContainerId containerId = BuilderUtils.newContainerId(appAttemptId, 1);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestLeafQueue.java
    cs.start();

    when(spyRMContext.getScheduler()).thenReturn(cs);
    when(spyRMContext.getYarnConfiguration())
        .thenReturn(new YarnConfiguration());
    when(cs.getNumClusterNodes()).thenReturn(3);
  }
  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestReservations.java

    spyRMContext = spy(rmContext);
    when(spyRMContext.getScheduler()).thenReturn(cs);
    when(spyRMContext.getYarnConfiguration())
        .thenReturn(new YarnConfiguration());

    cs.setRMContext(spyRMContext);
    cs.init(csConf);
    when(rmContext.getDispatcher()).thenReturn(drainDispatcher);
    when(rmContext.getRMApplicationHistoryWriter()).thenReturn(writer);
    when(rmContext.getSystemMetricsPublisher()).thenReturn(publisher);
    when(rmContext.getYarnConfiguration()).thenReturn(new YarnConfiguration());
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        app_0.getApplicationId(), 1);
    ContainerId containerId = BuilderUtils.newContainerId(appAttemptId, 1);
    when(rmContext.getDispatcher()).thenReturn(drainDispatcher);
    when(rmContext.getRMApplicationHistoryWriter()).thenReturn(writer);
    when(rmContext.getSystemMetricsPublisher()).thenReturn(publisher);
    when(rmContext.getYarnConfiguration()).thenReturn(new YarnConfiguration());
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        app_0.getApplicationId(), 1);
    ContainerId containerId = BuilderUtils.newContainerId(appAttemptId, 1);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fifo/TestFifoScheduler.java
        scheduler);
    ((RMContextImpl) rmContext).setSystemMetricsPublisher(
        mock(SystemMetricsPublisher.class));
    ((RMContextImpl) rmContext).setYarnConfiguration(new YarnConfiguration());

    scheduler.setRMContext(rmContext);
    scheduler.init(conf);
        scheduler);
    ((RMContextImpl) rmContext).setSystemMetricsPublisher(
        mock(SystemMetricsPublisher.class));
    ((RMContextImpl) rmContext).setYarnConfiguration(new YarnConfiguration());
    NullRMNodeLabelsManager nlm = new NullRMNodeLabelsManager();
    nlm.init(new Configuration());
    rmContext.setNodeLabelManager(nlm);

