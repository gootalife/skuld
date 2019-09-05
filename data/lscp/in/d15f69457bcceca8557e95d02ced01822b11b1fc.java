hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/TestResourceTrackerOnHA.java
    failoverThread = createAndStartFailoverThread();
    NodeStatus status =
        NodeStatus.newInstance(NodeId.newInstance("localhost", 0), 0, null,
            null, null, null);
    NodeHeartbeatRequest request2 =
        NodeHeartbeatRequest.newInstance(status, null, null,null);
    resourceTracker.nodeHeartbeat(request2);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/records/NodeStatus.java

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Records;

public abstract class NodeStatus {

  public static NodeStatus newInstance(NodeId nodeId, int responseId,
      List<ContainerStatus> containerStatuses,
      List<ApplicationId> keepAliveApplications,
      NodeHealthStatus nodeHealthStatus,
      ResourceUtilization containersUtilization) {
    NodeStatus nodeStatus = Records.newRecord(NodeStatus.class);
    nodeStatus.setResponseId(responseId);
    nodeStatus.setNodeId(nodeId);
    nodeStatus.setContainersStatuses(containerStatuses);
    nodeStatus.setKeepAliveApplications(keepAliveApplications);
    nodeStatus.setNodeHealthStatus(nodeHealthStatus);
    nodeStatus.setContainersUtilization(containersUtilization);
    return nodeStatus;
  }


  public abstract void setNodeId(NodeId nodeId);
  public abstract void setResponseId(int responseId);

  @Public
  @Stable
  public abstract ResourceUtilization getContainersUtilization();

  @Private
  @Unstable
  public abstract void setContainersUtilization(
      ResourceUtilization containersUtilization);
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/records/ResourceUtilization.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/records/ResourceUtilization.java

package org.apache.hadoop.yarn.server.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.util.Records;

@Private
@Evolving
public abstract class ResourceUtilization implements
    Comparable<ResourceUtilization> {
  public static ResourceUtilization newInstance(int pmem, int vmem, float cpu) {
    ResourceUtilization utilization =
        Records.newRecord(ResourceUtilization.class);
    utilization.setPhysicalMemory(pmem);
    utilization.setVirtualMemory(vmem);
    utilization.setCPU(cpu);
    return utilization;
  }

  public abstract int getVirtualMemory();

  public abstract void setVirtualMemory(int vmem);

  public abstract int getPhysicalMemory();

  public abstract void setPhysicalMemory(int pmem);

  public abstract float getCPU();

  public abstract void setCPU(float cpu);

  @Override
  public int hashCode() {
    final int prime = 263167;
    int result = 3571;
    result = prime * result + getVirtualMemory();
    result = prime * result + getPhysicalMemory();
    result = 31 * result + Float.valueOf(getCPU()).hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof ResourceUtilization)) {
      return false;
    }
    ResourceUtilization other = (ResourceUtilization) obj;
    if (getVirtualMemory() != other.getVirtualMemory()
        || getPhysicalMemory() != other.getPhysicalMemory()
        || getCPU() != other.getCPU()) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "<pmem:" + getPhysicalMemory() + ", vmem:" + getVirtualMemory()
        + ", vCores:" + getCPU() + ">";
  }

  public void addTo(int pmem, int vmem, float cpu) {
    this.setPhysicalMemory(this.getPhysicalMemory() + pmem);
    this.setVirtualMemory(this.getVirtualMemory() + vmem);
    this.setCPU(this.getCPU() + cpu);
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/records/impl/pb/NodeStatusPBImpl.java
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeHealthStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.ResourceUtilizationProto;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.ResourceUtilization;

public class NodeStatusPBImpl extends NodeStatus {
  NodeStatusProto proto = NodeStatusProto.getDefaultInstance();
    this.nodeHealthStatus = healthStatus;
  }

  @Override
  public ResourceUtilization getContainersUtilization() {
    NodeStatusProtoOrBuilder p =
        this.viaProto ? this.proto : this.builder;
    if (!p.hasContainersUtilization()) {
      return null;
    }
    return convertFromProtoFormat(p.getContainersUtilization());
  }

  @Override
  public void setContainersUtilization(
      ResourceUtilization containersUtilization) {
    maybeInitBuilder();
    if (containersUtilization == null) {
      this.builder.clearContainersUtilization();
      return;
    }
    this.builder
        .setContainersUtilization(convertToProtoFormat(containersUtilization));
  }

  private NodeIdProto convertToProtoFormat(NodeId nodeId) {
    return ((NodeIdPBImpl)nodeId).getProto();
  }
  private ApplicationIdProto convertToProtoFormat(ApplicationId c) {
    return ((ApplicationIdPBImpl)c).getProto();
  }

  private ResourceUtilizationProto convertToProtoFormat(ResourceUtilization r) {
    return ((ResourceUtilizationPBImpl) r).getProto();
  }

  private ResourceUtilizationPBImpl convertFromProtoFormat(
      ResourceUtilizationProto p) {
    return new ResourceUtilizationPBImpl(p);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/records/impl/pb/ResourceUtilizationPBImpl.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/records/impl/pb/ResourceUtilizationPBImpl.java

package org.apache.hadoop.yarn.server.api.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.ResourceUtilizationProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.ResourceUtilizationProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.ResourceUtilization;

@Private
@Unstable
public class ResourceUtilizationPBImpl extends ResourceUtilization {
  private ResourceUtilizationProto proto = ResourceUtilizationProto
      .getDefaultInstance();
  private ResourceUtilizationProto.Builder builder = null;
  private boolean viaProto = false;

  public ResourceUtilizationPBImpl() {
    builder = ResourceUtilizationProto.newBuilder();
  }

  public ResourceUtilizationPBImpl(ResourceUtilizationProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ResourceUtilizationProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceUtilizationProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int getPhysicalMemory() {
    ResourceUtilizationProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getPmem());
  }

  @Override
  public void setPhysicalMemory(int pmem) {
    maybeInitBuilder();
    builder.setPmem(pmem);
  }

  @Override
  public int getVirtualMemory() {
    ResourceUtilizationProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getVmem());
  }

  @Override
  public void setVirtualMemory(int vmem) {
    maybeInitBuilder();
    builder.setPmem(vmem);
  }

  @Override
  public float getCPU() {
    ResourceUtilizationProtoOrBuilder p = viaProto ? proto : builder;
    return p.getCpu();
  }

  @Override
  public void setCPU(float cpu) {
    maybeInitBuilder();
    builder.setCpu(cpu);
  }

  @Override
  public int compareTo(ResourceUtilization other) {
    int diff = this.getPhysicalMemory() - other.getPhysicalMemory();
    if (diff == 0) {
      diff = this.getVirtualMemory() - other.getVirtualMemory();
      if (diff == 0) {
        diff = Float.compare(this.getCPU(), other.getCPU());
      }
    }
    return diff;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/records/package-info.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/api/records/package-info.java
package org.apache.hadoop.yarn.server.api.records;
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeStatusUpdaterImpl.java
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeLabelsProvider;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

          + ", " + nodeHealthStatus.getHealthReport());
    }
    List<ContainerStatus> containersStatuses = getContainerStatuses();
    ResourceUtilization containersUtilization = getContainersUtilization();
    NodeStatus nodeStatus =
        NodeStatus.newInstance(nodeId, responseId, containersStatuses,
          createKeepAliveApplicationList(), nodeHealthStatus,
          containersUtilization);

    return nodeStatus;
  }

  private ResourceUtilization getContainersUtilization() {
    ContainerManagerImpl containerManager =
        (ContainerManagerImpl) this.context.getContainerManager();
    ContainersMonitor containersMonitor =
        containerManager.getContainersMonitor();
    return containersMonitor.getContainersUtilization();
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/monitor/ContainersMonitor.java
package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.server.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;

public interface ContainersMonitor extends Service,
    EventHandler<ContainersMonitorEvent>, ResourceView {
  public ResourceUtilization getContainersUtilization();
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/monitor/ContainersMonitorImpl.java
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
  private static final long UNKNOWN_MEMORY_LIMIT = -1L;
  private int nodeCpuPercentageForYARN;

  private ResourceUtilization containersUtilization;

  public ContainersMonitorImpl(ContainerExecutor exec,
      AsyncDispatcher dispatcher, Context context) {
    super("containers-monitor");
    this.containersToBeAdded = new HashMap<ContainerId, ProcessTreeInfo>();
    this.containersToBeRemoved = new ArrayList<ContainerId>();
    this.monitoringThread = new MonitoringThread();

    this.containersUtilization = ResourceUtilization.newInstance(0, 0, 0.0f);
  }

  @Override
          containersToBeRemoved.clear();
        }

        ResourceUtilization trackedContainersUtilization  =
            ResourceUtilization.newInstance(0, 0, 0.0f);

        long vmemUsageByAllContainers = 0;
                      currentPmemUsage, pmemLimit));
            }

            trackedContainersUtilization.addTo(
                (int) (currentPmemUsage >> 20),
                (int) (currentVmemUsage >> 20),
                milliVcoresUsed / 1000.0f);

            if (containerMetricsEnabled) {
              ContainerMetrics.forContainer(
              + cpuUsagePercentPerCoreByAllContainers);
        }

        setContainersUtilization(trackedContainersUtilization);

        try {
          Thread.sleep(monitoringInterval);
        } catch (InterruptedException e) {
    return this.vmemCheckEnabled;
  }

  @Override
  public ResourceUtilization getContainersUtilization() {
    return this.containersUtilization;
  }

  public void setContainersUtilization(ResourceUtilization utilization) {
    this.containersUtilization = utilization;
  }

  @Override
  public void handle(ContainersMonitorEvent monitoringEvent) {


