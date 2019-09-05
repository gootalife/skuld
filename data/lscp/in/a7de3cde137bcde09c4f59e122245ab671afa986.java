hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/capacity/ProportionalCapacityPreemptionPolicy.java
          ret.untouchableExtra = Resource.newInstance(0, 0);
        } else {
          ret.untouchableExtra =
                Resources.subtract(extra, childrensPreemptable);
        }
        ret.preemptableExtra = Resources.min(
            rc, partitionResource, childrensPreemptable, extra);
      }
    }
    addTempQueuePartition(ret);
    }
  }

  @VisibleForTesting
  public Map<String, Map<String, TempQueuePerPartition>> getQueuePartitions() {
    return queueToPartitions;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/capacity/TestProportionalCapacityPreemptionPolicy.java
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.TempQueuePerPartition;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
    verify(mDisp, times(5)).handle(argThat(new IsPreemptionRequestFor(appA)));
  }

  @Test
  public void testHierarchicalLarge3Levels() {
    int[][] qData = new int[][] {
      { 400, 200,  60, 140, 100, 40,  100,  70,  30, 100,  10,  90 },  // abs
      { 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400 },  // maxCap
      { 400, 210,  60, 150, 100, 50,  100,  50,  50,  90,  10,   80 },  // used
      {  10,   0,   0,   0,  0,   0,   0,   0,   0,   0,   0,  10 },  // pending
      {   0,   0,   0,   0,  0,   0,   0,   0,   0,   0,   0,   0 },  // reserved
      {   7,   3,   1,   2,   1,   1,  2,   1,   1,   2,   1,   1 },  // apps
      {  -1,  -1,   1,   -1,  1,   1,  -1,   1,   1,  -1,   1,   1 },  // req granularity
      {   3,   2,   0,   2,   0,   0,   2,   0,   0,   2,   0,   0 },  // subqueues
    };
    ProportionalCapacityPreemptionPolicy policy = buildPolicy(qData);
    policy.editSchedule();
    verify(mDisp, times(9)).handle(argThat(new IsPreemptionRequestFor(appC)));
    assertEquals(10, policy.getQueuePartitions().get("queueE").get("").preemptableExtra.getMemory());
    TempQueuePerPartition tempQueueAPartition = policy.getQueuePartitions().get("queueA").get("");
    assertEquals(0, tempQueueAPartition.untouchableExtra.getMemory());
    int extraForQueueA = tempQueueAPartition.current.getMemory()- tempQueueAPartition.guaranteed.getMemory();
    assertEquals(extraForQueueA,tempQueueAPartition.preemptableExtra.getMemory());
  }

  static class IsPreemptionRequestFor
      extends ArgumentMatcher<ContainerPreemptEvent> {
    private final ApplicationAttemptId appAttId;

