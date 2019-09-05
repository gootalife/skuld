hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/MockAM.java

  public List<Container> allocateAndWaitForContainers(int nContainer,
      int memory, MockNM nm) throws Exception {
    return allocateAndWaitForContainers("ANY", nContainer, memory, nm);
  }

  public List<Container> allocateAndWaitForContainers(String host,
      int nContainer, int memory, MockNM nm) throws Exception {
    allocate(host, memory, nContainer, null);
    nm.nodeHeartbeat(true);
    List<Container> conts = allocate(new ArrayList<ResourceRequest>(), null)
        .getAllocatedContainers();
    while (conts.size() < nContainer) {
      nm.nodeHeartbeat(true);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestApplicationPriority.java
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.junit.Test;

public class TestApplicationPriority {
  private final int GB = 1024;

  private YarnConfiguration conf;
    MockAM am1 = MockRM.launchAM(app1, rm, nm1);
    am1.registerAppAttempt();

    List<Container> allocated1 = am1.allocateAndWaitForContainers("127.0.0.1",
        7, 2 * GB, nm1);

    Assert.assertEquals(7, allocated1.size());
    Assert.assertEquals(2 * GB, allocated1.get(0).getResource().getMemory());

    RMApp app2 = rm.submitApp(1 * GB, appPriority2);

    MockAM am2 = MockRM.launchAM(app2, rm, nm1);
    am2.registerAppAttempt();

    FiCaSchedulerApp schedulerAppAttempt = cs.getSchedulerApplications()
        .get(app1.getApplicationId()).getCurrentAppAttempt();

    int counter = 0;
    for (Container c : allocated1) {
      if (++counter > 2) {
    Assert.assertEquals(12 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(4 * GB, report_nm1.getAvailableResource().getMemory());

    am1.allocate("127.0.0.1", 2 * GB, 10, new ArrayList<ContainerId>());

    List<Container> allocated2 = am2.allocateAndWaitForContainers("127.0.0.1",
        2, 2 * GB, nm1);

    Assert.assertEquals(2, allocated2.size());

    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    MockAM am1 = MockRM.launchAM(app1, rm, nm1);
    am1.registerAppAttempt();

    List<Container> allocated1 = am1.allocateAndWaitForContainers("127.0.0.1",
        7, 1 * GB, nm1);

    Assert.assertEquals(7, allocated1.size());
    Assert.assertEquals(1 * GB, allocated1.get(0).getResource().getMemory());

    rm.killApp(app1.getApplicationId());

    MockAM am3 = MockRM.launchAM(app3, rm, nm1);
    am3.registerAppAttempt();


