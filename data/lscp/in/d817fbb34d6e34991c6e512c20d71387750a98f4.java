hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ApplicationMasterService.java
      try {
        RMServerUtils.normalizeAndValidateRequests(ask,
            rScheduler.getMaximumResourceCapability(), app.getQueue(),
            rScheduler, rmContext);
      } catch (InvalidResourceRequestException e) {
        LOG.warn("Invalid resource ask by application " + appAttemptId, e);
        throw e;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/RMAppManager.java
      try {
        SchedulerUtils.normalizeAndValidateRequest(amReq,
            scheduler.getMaximumResourceCapability(),
            submissionContext.getQueue(), scheduler, isRecovery, rmContext);
      } catch (InvalidResourceRequestException e) {
        LOG.warn("RM app submission failed in validating AM resource request"
            + " for application " + submissionContext.getApplicationId(), e);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/RMServerUtils.java
  public static void normalizeAndValidateRequests(List<ResourceRequest> ask,
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      RMContext rmContext)
      throws InvalidResourceRequestException {
    for (ResourceRequest resReq : ask) {
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maximumResource,
          queueName, scheduler, rmContext);
    }
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/SchedulerUtils.java
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

  public static void normalizeAndValidateRequest(ResourceRequest resReq,
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      boolean isRecovery, RMContext rmContext)
      throws InvalidResourceRequestException {

    QueueInfo queueInfo = null;
    }
    SchedulerUtils.normalizeNodeLabelExpressionInRequest(resReq, queueInfo);
    if (!isRecovery) {
      validateResourceRequest(resReq, maximumResource, queueInfo, rmContext);
    }
  }

  public static void normalizeAndvalidateRequest(ResourceRequest resReq,
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      RMContext rmContext)
      throws InvalidResourceRequestException {
    normalizeAndValidateRequest(resReq, maximumResource, queueName, scheduler,
        false, rmContext);
  }

  private static void validateResourceRequest(ResourceRequest resReq,
      Resource maximumResource, QueueInfo queueInfo, RMContext rmContext)
      throws InvalidResourceRequestException {
    if (resReq.getCapability().getMemory() < 0 ||
        resReq.getCapability().getMemory() > maximumResource.getMemory()) {
    
    if (labelExp != null && !labelExp.trim().isEmpty() && queueInfo != null) {
      if (!checkQueueLabelExpression(queueInfo.getAccessibleNodeLabels(),
          labelExp, rmContext)) {
        throw new InvalidResourceRequestException("Invalid resource request"
            + ", queue="
            + queueInfo.getQueueName()
    }
  }

  public static boolean checkQueueLabelExpression(Set<String> queueLabels,
      String labelExpression, RMContext rmContext) {
    if (labelExpression == null) {
      return true;
    }
    for (String str : labelExpression.split("&&")) {
      str = str.trim();
      if (!str.trim().isEmpty()) {
        if (queueLabels == null) {
          return false; 
        } else {
          if (!queueLabels.contains(str)
              && !queueLabels.contains(RMNodeLabelsManager.ANY)) {
            return false;
          }
        }
        
        if (null != rmContext) {
          RMNodeLabelsManager nlm = rmContext.getNodeLabelManager();
          if (nlm != null && !nlm.containsNodeLabel(str)) {
            return false;
          }
        }
      }
    }
    return true;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/AbstractCSQueue.java
  protected void setupConfigurableCapacities() {
    CSQueueUtils.loadUpdateAndCheckCapacities(
        getQueuePath(),
        csContext.getConfiguration(), 
        queueCapacities,
        parent == null ? null : parent.getQueueCapacities());
  }
  
  @Override
    if (this.accessibleLabels == null && parent != null) {
      this.accessibleLabels = parent.getAccessibleNodeLabels();
    }
    
    if (this.defaultLabelExpression == null && parent != null

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CSQueueUtils.java
  public static void loadUpdateAndCheckCapacities(String queuePath,
      CapacitySchedulerConfiguration csConf,
      QueueCapacities queueCapacities, QueueCapacities parentQueueCapacities) {
    loadCapacitiesByLabelsFromConf(queuePath,
        queueCapacities, csConf);

    updateAbsoluteCapacitiesByNodeLabels(queueCapacities, parentQueueCapacities);
    capacitiesSanityCheck(queuePath, queueCapacities);
  }
  
  private static void loadCapacitiesByLabelsFromConf(String queuePath,
      QueueCapacities queueCapacities, CapacitySchedulerConfiguration csConf) {
    queueCapacities.clearConfigurableFields();
    Set<String> configuredNodelabels =
        csConf.getConfiguredNodeLabels(queuePath);

    for (String label : configuredNodelabels) {
      if (label.equals(CommonNodeLabelsManager.NO_LABEL)) {
        queueCapacities.setCapacity(CommonNodeLabelsManager.NO_LABEL,
            csConf.getNonLabeledQueueCapacity(queuePath) / 100);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacitySchedulerConfiguration.java
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.SchedulableEntity;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.ImmutableSet;

public class CapacitySchedulerConfiguration extends ReservationSchedulerConfiguration {
                   defaultVal);
    return preemptionDisabled;
  }

  public Set<String> getConfiguredNodeLabels(String queuePath) {
    Set<String> configuredNodeLabels = new HashSet<String>();
    Entry<String, String> e = null;
    
    Iterator<Entry<String, String>> iter = iterator();
    while (iter.hasNext()) {
      e = iter.next();
      String key = e.getKey();

      if (key.startsWith(getQueuePrefix(queuePath) + ACCESSIBLE_NODE_LABELS
          + DOT)) {
        int labelStartIdx =
            key.indexOf(ACCESSIBLE_NODE_LABELS)
                + ACCESSIBLE_NODE_LABELS.length() + 1;
        int labelEndIndx = key.indexOf('.', labelStartIdx);
        String labelName = key.substring(labelStartIdx, labelEndIndx);
        configuredNodeLabels.add(labelName);
      }
    }
    
    configuredNodeLabels.add(RMNodeLabelsManager.NO_LABEL);
    
    return configuredNodeLabels;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/LeafQueue.java
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
    maxAMResourcePerQueuePercent =
        conf.getMaximumApplicationMasterResourcePerQueuePercent(getQueuePath());

    if (!SchedulerUtils.checkQueueLabelExpression(
        this.accessibleLabels, this.defaultLabelExpression, null)) {
      throw new IOException("Invalid default label expression of "
          + " queue="
          + getQueueName()

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/TestSchedulerUtils.java
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.TestAMAuthorization.MockRMWithAMS;
import org.apache.hadoop.yarn.server.resourcemanager.TestAMAuthorization.MyContainerManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class TestSchedulerUtils {

  private static final Log LOG = LogFactory.getLog(TestSchedulerUtils.class);
  
  private RMContext rmContext = getMockRMContext();
  
  @Test (timeout = 30000)
  public void testNormalizeRequest() {
    ResourceCalculator resourceCalculator = new DefaultResourceCalculator();
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays.asList("x", "y"));
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
          ImmutableSet.of(NodeLabel.newInstance("x"),
              NodeLabel.newInstance("y")));
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);

      resReq.setNodeLabelExpression("y");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      
      resReq.setNodeLabelExpression("");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      
      resReq.setNodeLabelExpression(" ");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
    } catch (InvalidResourceRequestException e) {
      e.printStackTrace();
      fail("Should be valid when request labels is a subset of queue labels");
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
          Arrays.asList("x", "y"));
    }
    
    try {
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays.asList("x", "y"));
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    }
    
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays.asList("x", "y"));
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
          ImmutableSet.of(NodeLabel.newInstance("x"),
              NodeLabel.newInstance("y")));
      
      Resource resource = Resources.createResource(
          0,
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("z");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
          Arrays.asList("x", "y"));
    }
    
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays.asList("x", "y"));
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
          ImmutableSet.of(NodeLabel.newInstance("x"),
              NodeLabel.newInstance("y")));
      
      Resource resource = Resources.createResource(
          0,
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x && y");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
          Arrays.asList("x", "y"));
    }
    
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      
      resReq.setNodeLabelExpression("");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      
      resReq.setNodeLabelExpression("  ");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
    } catch (InvalidResourceRequestException e) {
      e.printStackTrace();
      fail("Should be valid when request labels is empty");
      queueAccessibleNodeLabels.clear();
      
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
          ImmutableSet.of(NodeLabel.newInstance("x")));
      
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
          Arrays.asList("x"));
    }
    
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.add(RMNodeLabelsManager.ANY);
      
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
          ImmutableSet.of(NodeLabel.newInstance("x"),
              NodeLabel.newInstance("y"), NodeLabel.newInstance("z")));
      
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      
      resReq.setNodeLabelExpression("y");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      
      resReq.setNodeLabelExpression("z");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
    } catch (InvalidResourceRequestException e) {
      e.printStackTrace();
      fail("Should be valid when queue can access any labels");
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
          Arrays.asList("x", "y", "z"));
    }
    
    try {
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.add(RMNodeLabelsManager.ANY);
      
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    }
    
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays.asList("x", "y"));
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
          ImmutableSet.of(NodeLabel.newInstance("x"),
              NodeLabel.newInstance("y")));
      
      Resource resource = Resources.createResource(
          0,
          mock(Priority.class), "rack", resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
          Arrays.asList("x", "y"));
    }
    
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays
          .asList(CommonNodeLabelsManager.ANY));
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
          ImmutableSet.of(NodeLabel.newInstance("x")));
      
      Resource resource = Resources.createResource(
          0,
          mock(Priority.class), "rack", resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
          Arrays.asList("x"));
    }
  }

          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler, rmContext);
    } catch (InvalidResourceRequestException e) {
      fail("Zero memory should be accepted");
    }
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler, rmContext);
    } catch (InvalidResourceRequestException e) {
      fail("Zero vcores should be accepted");
    }
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler, rmContext);
    } catch (InvalidResourceRequestException e) {
      fail("Max memory should be accepted");
    }
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler, rmContext);
    } catch (InvalidResourceRequestException e) {
      fail("Max vcores should not be accepted");
    }
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler, rmContext);
      fail("Negative memory should not be accepted");
    } catch (InvalidResourceRequestException e) {
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler, rmContext);
      fail("Negative vcores should not be accepted");
    } catch (InvalidResourceRequestException e) {
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler, rmContext);
      fail("More than max memory should not be accepted");
    } catch (InvalidResourceRequestException e) {
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler, rmContext);
      fail("More than max vcores should not be accepted");
    } catch (InvalidResourceRequestException e) {
    Assert.assertNull(applications.get(appId));
    return app;
  }
  
  private static RMContext getMockRMContext() {
    RMContext rmContext = mock(RMContext.class);
    RMNodeLabelsManager nlm = new NullRMNodeLabelsManager();
    nlm.init(new Configuration(false));
    when(rmContext.getNodeLabelManager()).thenReturn(nlm);
    return rmContext;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestQueueParsing.java
    ServiceOperations.stopQuietly(capacityScheduler);
  }
  
  @Test
  public void testQueueParsingWhenLabelsNotExistedInNodeLabelManager()
      throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    ServiceOperations.stopQuietly(nodeLabelsManager);
  }
  
  @Test
  public void testQueueParsingWhenLabelsInheritedNotExistedInNodeLabelManager()
      throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    ServiceOperations.stopQuietly(nodeLabelsManager);
  }
  
  @Test
  public void testSingleLevelQueueParsingWhenLabelsNotExistedInNodeLabelManager()
      throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    ServiceOperations.stopQuietly(nodeLabelsManager);
  }
  
  @Test
  public void testQueueParsingWhenLabelsNotExist() throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
    Assert.assertEquals(0.10 * 0.4, a2.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.15, a2.getAbsoluteMaximumCapacity(), DELTA);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testQueueParsingFailWhenSumOfChildrenNonLabeledCapacityNot100Percent()
      throws IOException {
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet
        .of("red", "blue"));
    
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfiguration(csConf);
    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + ".c.c2", 5);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    ServiceOperations.stopQuietly(capacityScheduler);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testQueueParsingFailWhenSumOfChildrenLabeledCapacityNot100Percent()
      throws IOException {
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet
        .of("red", "blue"));
    
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithLabels(csConf);
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT + ".b.b3",
        "red", 24);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    ServiceOperations.stopQuietly(capacityScheduler);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueueParsingWithSumOfChildLabelCapacityNot100PercentWithWildCard()
      throws IOException {
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet
        .of("red", "blue"));
    
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithLabels(csConf);
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT + ".b.b3",
        "red", 24);
    csConf.setAccessibleNodeLabels(CapacitySchedulerConfiguration.ROOT,
        ImmutableSet.of(RMNodeLabelsManager.ANY));
    csConf.setAccessibleNodeLabels(CapacitySchedulerConfiguration.ROOT + ".b",
        ImmutableSet.of(RMNodeLabelsManager.ANY));

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    ServiceOperations.stopQuietly(capacityScheduler);
  }
}

