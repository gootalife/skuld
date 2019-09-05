hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/QueueMetrics.java
  static final MetricsInfo RECORD_INFO = info("QueueMetrics",
      "Metrics for the resource scheduler");
  protected static final MetricsInfo QUEUE_INFO = info("Queue", "Metrics by queue");
  protected static final MetricsInfo USER_INFO =
      info("User", "Metrics by user");
  static final Splitter Q_SPLITTER =
      Splitter.on('.').omitEmptyStrings().trimResults();

  protected final MetricsRegistry registry;
  protected final String queueName;
  protected final QueueMetrics parent;
  protected final MetricsSystem metricsSystem;
  protected final Map<String, QueueMetrics> users;
  protected final Configuration conf;

  protected QueueMetrics(MetricsSystem ms, String queueName, Queue parent, 
	       boolean enableUserMetrics, Configuration conf) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/AbstractCSQueue.java
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
  final Resource minimumAllocation;
  Resource maximumAllocation;
  QueueState state;
  final CSQueueMetrics metrics;
  protected final PrivilegedEntity queueEntity;

  final ResourceCalculator resourceCalculator;
    this.resourceCalculator = cs.getResourceCalculator();
    
    this.metrics =
        old != null ? (CSQueueMetrics) old.getMetrics() : CSQueueMetrics
            .forQueue(getQueuePath(), parent, cs.getConfiguration()
                .getEnableUserMetrics(), cs.getConf());

    this.csContext = cs;
    this.minimumAllocation = csContext.getMinimumResourceCapability();
  }
  
  @Override
  public CSQueueMetrics getMetrics() {
    return metrics;
  }
  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CSQueueMetrics.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CSQueueMetrics.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;

@Metrics(context = "yarn")
public class CSQueueMetrics extends QueueMetrics {

  @Metric("AM memory limit in MB")
  MutableGaugeInt AMResourceLimitMB;
  @Metric("AM CPU limit in virtual cores")
  MutableGaugeInt AMResourceLimitVCores;
  @Metric("Used AM memory limit in MB")
  MutableGaugeInt usedAMResourceMB;
  @Metric("Used AM CPU limit in virtual cores")
  MutableGaugeInt usedAMResourceVCores;

  CSQueueMetrics(MetricsSystem ms, String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {
    super(ms, queueName, parent, enableUserMetrics, conf);
  }

  public int getAMResourceLimitMB() {
    return AMResourceLimitMB.value();
  }

  public int getAMResourceLimitVCores() {
    return AMResourceLimitVCores.value();
  }

  public int getUsedAMResourceMB() {
    return usedAMResourceMB.value();
  }

  public int getUsedAMResourceVCores() {
    return usedAMResourceVCores.value();
  }

  public void setAMResouceLimit(Resource res) {
    AMResourceLimitMB.set(res.getMemory());
    AMResourceLimitVCores.set(res.getVirtualCores());
  }

  public void setAMResouceLimitForUser(String user, Resource res) {
    CSQueueMetrics userMetrics = (CSQueueMetrics) getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.setAMResouceLimit(res);
    }
  }

  public void incAMUsed(String user, Resource res) {
    usedAMResourceMB.incr(res.getMemory());
    usedAMResourceVCores.incr(res.getVirtualCores());
    CSQueueMetrics userMetrics = (CSQueueMetrics) getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.incAMUsed(user, res);
    }
  }

  public void decAMUsed(String user, Resource res) {
    usedAMResourceMB.decr(res.getMemory());
    usedAMResourceVCores.decr(res.getVirtualCores());
    CSQueueMetrics userMetrics = (CSQueueMetrics) getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.decAMUsed(user, res);
    }
  }

  public synchronized static CSQueueMetrics forQueue(String queueName,
      Queue parent, boolean enableUserMetrics, Configuration conf) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    QueueMetrics metrics = queueMetrics.get(queueName);
    if (metrics == null) {
      metrics =
          new CSQueueMetrics(ms, queueName, parent, enableUserMetrics, conf)
              .tag(QUEUE_INFO, queueName);

      if (ms != null) {
        metrics =
            ms.register(sourceName(queueName).toString(), "Metrics for queue: "
                + queueName, metrics);
      }
      queueMetrics.put(queueName, metrics);
    }

    return (CSQueueMetrics) metrics;
  }

  @Override
  public synchronized QueueMetrics getUserMetrics(String userName) {
    if (users == null) {
      return null;
    }
    CSQueueMetrics metrics = (CSQueueMetrics) users.get(userName);
    if (metrics == null) {
      metrics = new CSQueueMetrics(metricsSystem, queueName, null, false, conf);
      users.put(userName, metrics);
      metricsSystem.register(
          sourceName(queueName).append(",user=").append(userName).toString(),
          "Metrics for user '" + userName + "' in queue '" + queueName + "'",
          ((CSQueueMetrics) metrics.tag(QUEUE_INFO, queueName)).tag(USER_INFO,
              userName));
    }
    return metrics;
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/LeafQueue.java
     }
     Resource queueCap = Resources.max(resourceCalculator, lastClusterResource,
       absoluteCapacityResource, queueCurrentLimit);
    Resource amResouceLimit =
        Resources.multiplyAndNormalizeUp(resourceCalculator, queueCap,
            maxAMResourcePerQueuePercent, minimumAllocation);

    metrics.setAMResouceLimit(amResouceLimit);
    return amResouceLimit;
  }
  
  public synchronized Resource getUserAMResourceLimit() {
      orderingPolicy.addSchedulableEntity(application);
      queueUsage.incAMUsed(application.getAMResource());
      user.getResourceUsage().incAMUsed(application.getAMResource());
      metrics.incAMUsed(application.getUser(), application.getAMResource());
      metrics.setAMResouceLimitForUser(application.getUser(), userAMLimit);
      i.remove();
      LOG.info("Application " + application.getApplicationId() +
          " from user: " + application.getUser() + 
    } else {
      queueUsage.decAMUsed(application.getAMResource());
      user.getResourceUsage().decAMUsed(application.getAMResource());
      metrics.decAMUsed(application.getUser(), application.getAMResource());
    }
    applicationAttemptMap.remove(application.getApplicationAttemptId());


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestApplicationLimits.java
    		" UserAMResourceLimit=" + 
    		queue.getUserAMResourceLimit());
    
    Resource amResourceLimit = Resource.newInstance(160 * GB, 1);
    assertEquals(queue.getAMResourceLimit(), amResourceLimit);
    assertEquals(queue.getAMResourceLimit(), amResourceLimit);
    assertEquals(queue.getUserAMResourceLimit(), 
      Resource.newInstance(80*GB, 1));
    
    assertEquals(queue.getMetrics().getAMResourceLimitMB(),
        amResourceLimit.getMemory());
    assertEquals(queue.getMetrics().getAMResourceLimitVCores(),
        amResourceLimit.getVirtualCores());

    assertEquals(
        (int)(clusterResource.getMemory() * queue.getAbsoluteCapacity()),
        queue.getMetrics().getAvailableMB()

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestLeafQueue.java
        .getMockApplicationAttemptId(0, 2);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, null,
        spyRMContext);
    app_1.setAMResource(Resource.newInstance(100, 1));
    a.submitApplicationAttempt(app_1, user_0); // same user

    assertEquals(1, a.getMetrics().getAppsSubmitted());
    assertEquals(1, a.getMetrics().getAppsPending());
    assertEquals(1, a.getUser(user_0).getActiveApplications());
    assertEquals(app_1.getAMResource().getMemory(), a.getMetrics()
        .getUsedAMResourceMB());
    assertEquals(app_1.getAMResource().getVirtualCores(), a.getMetrics()
        .getUsedAMResourceVCores());
    
    event = new AppAttemptRemovedSchedulerEvent(appAttemptId_0,
        RMAppAttemptState.FINISHED, false);

