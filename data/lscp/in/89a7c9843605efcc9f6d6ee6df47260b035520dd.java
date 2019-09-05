hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/ResourceUsage.java

  private enum ResourceType {
    USED(0), PENDING(1), AMUSED(2), RESERVED(3), CACHED_USED(4),
      CACHED_PENDING(5);

    private int idx;

    return _get(label, ResourceType.USED);
  }
  
  public Resource getCachedUsed(String label) {
    return _get(label, ResourceType.CACHED_USED);
  }
  
  public Resource getCachedPending(String label) {
    return _get(label, ResourceType.CACHED_PENDING);
  }

  public void incUsed(String label, Resource res) {
    _inc(label, ResourceType.USED, res);
  }
    _set(label, ResourceType.USED, res);
  }
  
  public void setCachedUsed(String label, Resource res) {
    _set(label, ResourceType.CACHED_USED, res);
  }
  
  public void setCachedPending(String label, Resource res) {
    _set(label, ResourceType.CACHED_PENDING, res);
  }

    }
  }
  
  private Resource _getAll(ResourceType type) {
    try {
      readLock.lock();
      Resource allOfType = Resources.createResource(0);
      for (Map.Entry<String, UsageByLabel> usageEntry : usages.entrySet()) {
        Resources.addTo(allOfType, usageEntry.getValue().resArr[type.idx]);
      }
      return allOfType;
    } finally {
      readLock.unlock();
    }
  }
  
  public Resource getAllPending() {
    return _getAll(ResourceType.PENDING);
  }
  
  public Resource getAllUsed() {
    return _getAll(ResourceType.USED);
  }

  private UsageByLabel getAndAddIfMissing(String label) {
    if (label == null) {
      label = RMNodeLabelsManager.NO_LABEL;
    }
  }

  public Resource getCachedDemand(String label) {
    try {
      readLock.lock();
      Resource demand = Resources.createResource(0);
      Resources.addTo(demand, getCachedUsed(label));
      Resources.addTo(demand, getCachedPending(label));
      return demand;
    } finally {
      readLock.unlock();
    }
  }
  
  @Override
  public String toString() {
    try {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/AbstractComparatorOrderingPolicy.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/AbstractComparatorOrderingPolicy.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import com.google.common.annotations.VisibleForTesting;


public abstract class AbstractComparatorOrderingPolicy<S extends SchedulableEntity> implements OrderingPolicy<S> {
  
  private static final Log LOG = LogFactory.getLog(OrderingPolicy.class);
                                            
  protected TreeSet<S> schedulableEntities;
  protected Comparator<SchedulableEntity> comparator;
  
  public AbstractComparatorOrderingPolicy() { }
  
  @Override
  public Collection<S> getSchedulableEntities() {
    return schedulableEntities;
  }
  
  @Override
  public Iterator<S> getAssignmentIterator() {
    return schedulableEntities.iterator();
  }
  
  @Override
  public Iterator<S> getPreemptionIterator() {
    return schedulableEntities.descendingIterator();
  }
  
  public static void updateSchedulingResourceUsage(ResourceUsage ru) {
    ru.setCachedUsed(CommonNodeLabelsManager.ANY, ru.getAllUsed());
    ru.setCachedPending(CommonNodeLabelsManager.ANY, ru.getAllPending());
  }
  
  protected void reorderSchedulableEntity(S schedulableEntity) {
    schedulableEntities.remove(schedulableEntity);
    updateSchedulingResourceUsage(
      schedulableEntity.getSchedulingResourceUsage());
    schedulableEntities.add(schedulableEntity);
  }
  
  public void setComparator(Comparator<SchedulableEntity> comparator) {
    this.comparator = comparator;
    TreeSet<S> schedulableEntities = new TreeSet<S>(comparator);
    if (this.schedulableEntities != null) {
      schedulableEntities.addAll(this.schedulableEntities);
    }
    this.schedulableEntities = schedulableEntities;
  }
  
  @VisibleForTesting
  public Comparator<SchedulableEntity> getComparator() {
    return comparator; 
  }
  
  @Override
  public void addSchedulableEntity(S s) {
    schedulableEntities.add(s); 
  }
  
  @Override
  public boolean removeSchedulableEntity(S s) {
    return schedulableEntities.remove(s); 
  }
  
  @Override
  public void addAllSchedulableEntities(Collection<S> sc) {
    schedulableEntities.addAll(sc);
  }
  
  @Override
  public int getNumSchedulableEntities() {
    return schedulableEntities.size(); 
  }
  
  @Override
  public abstract void configure(String conf);
  
  @Override
  public abstract void containerAllocated(S schedulableEntity, 
    RMContainer r);
  
  @Override
  public abstract void containerReleased(S schedulableEntity, 
    RMContainer r);
  
  @Override
  public abstract String getStatusMessage();
  
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/FifoComparator.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/FifoComparator.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.*;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;

public class FifoComparator 
    implements Comparator<SchedulableEntity> {
      
    @Override
    public int compare(SchedulableEntity r1, SchedulableEntity r2) {
      int res = r1.compareInputOrderTo(r2);
      return res;
    }
}


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/FifoOrderingPolicy.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/FifoOrderingPolicy.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.*;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;

public class FifoOrderingPolicy<S extends SchedulableEntity> extends AbstractComparatorOrderingPolicy<S> {
  
  public FifoOrderingPolicy() {
    setComparator(new FifoComparator());
  }
  
  @Override
  public void configure(String conf) {
    
  }
  
  @Override
  public void containerAllocated(S schedulableEntity, 
    RMContainer r) {
    }

  @Override
  public void containerReleased(S schedulableEntity, 
    RMContainer r) {
    }
  
  @Override
  public String getStatusMessage() {
    return "FifoOrderingPolicy";
  }
  
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/OrderingPolicy.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/OrderingPolicy.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;


public interface OrderingPolicy<S extends SchedulableEntity> {
  
  public Collection<S> getSchedulableEntities();
  
  public Iterator<S> getAssignmentIterator();
  
  public Iterator<S> getPreemptionIterator();
  
  public void addSchedulableEntity(S s);
  
  public boolean removeSchedulableEntity(S s);
  
  public void addAllSchedulableEntities(Collection<S> sc);
  
  public int getNumSchedulableEntities();
  
  public void configure(String conf);
  
  public void containerAllocated(S schedulableEntity, 
    RMContainer r);
  
  public void containerReleased(S schedulableEntity, 
    RMContainer r);
  
  public String getStatusMessage();
  
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/SchedulableEntity.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/SchedulableEntity.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.*;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;


public interface SchedulableEntity {
  
  public String getId();
  
  public int compareInputOrderTo(SchedulableEntity other);
  
  public ResourceUsage getSchedulingResourceUsage();
  
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/MockSchedulableEntity.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/MockSchedulableEntity.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.*;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;


public class MockSchedulableEntity implements SchedulableEntity {
  
  private String id;
  private long serial = 0;
  
  public MockSchedulableEntity() { }
  
  public void setId(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }
  
  public void setSerial(long serial) {
    this.serial = serial;
  }
  
  public long getSerial() {
    return serial; 
  }
  
  public void setUsed(Resource value) {
    schedulingResourceUsage.setUsed(CommonNodeLabelsManager.ANY, value);
  }
  
  public void setPending(Resource value) {
    schedulingResourceUsage.setPending(CommonNodeLabelsManager.ANY, value);
  }
  
  private ResourceUsage schedulingResourceUsage = new ResourceUsage();
  
  @Override
  public ResourceUsage getSchedulingResourceUsage() {
    return schedulingResourceUsage;
  }
  
  @Override
  public int compareInputOrderTo(SchedulableEntity other) {
    if (other instanceof MockSchedulableEntity) {
      MockSchedulableEntity r2 = (MockSchedulableEntity) other;
      int res = (int) Math.signum(getSerial() - r2.getSerial());
      return res;
    }
    return 1;//let other types go before this, if any
  }
  
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/TestFifoOrderingPolicy.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/policy/TestFifoOrderingPolicy.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.*;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;

public class TestFifoOrderingPolicy {
  
  @Test
  public void testFifoOrderingPolicy() {
    FifoOrderingPolicy<MockSchedulableEntity> policy = 
      new FifoOrderingPolicy<MockSchedulableEntity>();
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();
    
    Assert.assertEquals(policy.getComparator().compare(r1, r2), 0);
    
    r1.setSerial(1);
    Assert.assertEquals(policy.getComparator().compare(r1, r2), 1);
    
    r2.setSerial(2);
    Assert.assertEquals(policy.getComparator().compare(r1, r2), -1);
  }
  
  @Test
  public void testIterators() {
    OrderingPolicy<MockSchedulableEntity> schedOrder =
     new FifoOrderingPolicy<MockSchedulableEntity>();
    
    MockSchedulableEntity msp1 = new MockSchedulableEntity();
    MockSchedulableEntity msp2 = new MockSchedulableEntity();
    MockSchedulableEntity msp3 = new MockSchedulableEntity();
    
    msp1.setSerial(3);
    msp2.setSerial(2);
    msp3.setSerial(1);
    
    schedOrder.addSchedulableEntity(msp1);
    schedOrder.addSchedulableEntity(msp2);
    schedOrder.addSchedulableEntity(msp3);
    
    checkSerials(schedOrder.getAssignmentIterator(), new long[]{1, 2, 3});
    
    checkSerials(schedOrder.getPreemptionIterator(), new long[]{3, 2, 1});
  }
  
  public void checkSerials(Iterator<MockSchedulableEntity> si, 
      long[] serials) {
    for (int i = 0;i < serials.length;i++) {
      Assert.assertEquals(si.next().getSerial(), 
        serials[i]);
    }
  }
  
}

