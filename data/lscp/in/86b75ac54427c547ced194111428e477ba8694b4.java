hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmnode/RMNodeImpl.java
        if (rmNode.getHttpPort() == newNode.getHttpPort()) {
          rmNode.getLastNodeHeartBeatResponse().setResponseId(0);
          if (!rmNode.getTotalCapability().equals(
              newNode.getTotalCapability())) {
            rmNode.totalCapability = newNode.getTotalCapability();
          }
          if (rmNode.getState().equals(NodeState.RUNNING)) {
            rmNode.context.getDispatcher().getEventHandler().handle(
                new NodeAddedSchedulerEvent(rmNode));
          }
        } else {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/resourcetracker/TestNMReconnect.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.ConfigurationProviderFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.NMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

      RecordFactoryProvider.getRecordFactory(null);

  private List<RMNodeEvent> rmNodeEvents = new ArrayList<RMNodeEvent>();
  private Dispatcher dispatcher;
  private RMContextImpl context;

  private class TestRMNodeEventDispatcher implements
      EventHandler<RMNodeEvent> {
  public void setUp() {
    Configuration conf = new Configuration();
    dispatcher = new InlineDispatcher();

    dispatcher.register(RMNodeEventType.class,
        new TestRMNodeEventDispatcher());

    context = new RMContextImpl(dispatcher, null,
        null, null, null, null, null, null, null, null);
    dispatcher.register(SchedulerEventType.class,
        new InlineDispatcher.EmptyEventHandler());
    resourceTrackerService.start();
  }

  @After
  public void tearDown() {
    resourceTrackerService.stop();
  }

  @Test
  public void testReconnect() throws Exception {
    String hostname1 = "localhost1";
    Assert.assertEquals(RMNodeEventType.RECONNECTED,
        rmNodeEvents.get(0).getType());
  }

  @Test
  public void testCompareRMNodeAfterReconnect() throws Exception {
    Configuration yarnConf = new YarnConfiguration();
    CapacityScheduler scheduler = new CapacityScheduler();
    scheduler.setConf(yarnConf);
    ConfigurationProvider configurationProvider =
        ConfigurationProviderFactory.getConfigurationProvider(yarnConf);
    configurationProvider.init(yarnConf);
    context.setConfigurationProvider(configurationProvider);
    RMNodeLabelsManager nlm = new RMNodeLabelsManager();
    nlm.init(yarnConf);
    nlm.start();
    context.setNodeLabelManager(nlm);
    scheduler.setRMContext(context);
    scheduler.init(yarnConf);
    scheduler.start();
    dispatcher.register(SchedulerEventType.class, scheduler);

    String hostname1 = "localhost1";
    Resource capability = BuilderUtils.newResource(4096, 4);

    RegisterNodeManagerRequest request1 = recordFactory
        .newRecordInstance(RegisterNodeManagerRequest.class);
    NodeId nodeId1 = NodeId.newInstance(hostname1, 0);
    request1.setNodeId(nodeId1);
    request1.setHttpPort(0);
    request1.setResource(capability);
    resourceTrackerService.registerNodeManager(request1);
    Assert.assertNotNull(context.getRMNodes().get(nodeId1));
    Assert.assertTrue(scheduler.getSchedulerNode(nodeId1).getRMNode() ==
        context.getRMNodes().get(nodeId1));
    Assert.assertEquals(context.getRMNodes().get(nodeId1).
        getTotalCapability(), capability);
    Resource capability1 = BuilderUtils.newResource(2048, 2);
    request1.setResource(capability1);
    resourceTrackerService.registerNodeManager(request1);
    Assert.assertNotNull(context.getRMNodes().get(nodeId1));
    Assert.assertTrue(scheduler.getSchedulerNode(nodeId1).getRMNode() ==
        context.getRMNodes().get(nodeId1));
    Assert.assertEquals(context.getRMNodes().get(nodeId1).
        getTotalCapability(), capability1);
    nlm.stop();
    scheduler.stop();
  }
}

