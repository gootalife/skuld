hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/AbstractCSQueue.java
        queueUsage, nodePartition, cluster, schedulingMode);
  }
  
  public boolean accessibleToPartition(String nodePartition) {
    if (accessibleLabels != null
        && accessibleLabels.contains(RMNodeLabelsManager.ANY)) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/CapacitySchedulerPage.java

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerLeafQueueInfo;
  static class CSQInfo {
    CapacitySchedulerInfo csinfo;
    CapacitySchedulerQueueInfo qinfo;
    String label;
  }

  static class LeafQueueInfoBlock extends HtmlBlock {
    final CapacitySchedulerLeafQueueInfo lqinfo;
    private String nodeLabel;

    @Inject LeafQueueInfoBlock(ViewContext ctx, CSQInfo info) {
      super(ctx);
      lqinfo = (CapacitySchedulerLeafQueueInfo) info.qinfo;
      nodeLabel = info.label;
    }

    @Override
    protected void render(Block html) {
      if (nodeLabel == null) {
        renderLeafQueueInfoWithoutParition(html);
      } else {
        renderLeafQueueInfoWithPartition(html);
      }
    }

    private void renderLeafQueueInfoWithPartition(Block html) {
      nodeLabel = nodeLabel.length() == 0 ? "<DEFAULT_PARTITION>" : nodeLabel;
      ResponseInfo ri =
          info("\'" + lqinfo.getQueuePath().substring(5)
              + "\' Queue Status for Partition \'" + nodeLabel + "\'");
      renderQueueCapacityInfo(ri);
      html._(InfoBlock.class);
      ri.clear();

      ri =
          info("\'" + lqinfo.getQueuePath().substring(5) + "\' Queue Status")
              ._("Queue State:", lqinfo.getQueueState());
      renderCommonLeafQueueInfo(ri);

      html._(InfoBlock.class);
      ri.clear();
    }

    private void renderLeafQueueInfoWithoutParition(Block html) {
      ResponseInfo ri =
          info("\'" + lqinfo.getQueuePath().substring(5) + "\' Queue Status")
              ._("Queue State:", lqinfo.getQueueState());
      renderQueueCapacityInfo(ri);
      renderCommonLeafQueueInfo(ri);
      html._(InfoBlock.class);
      ri.clear();
    }

    private void renderQueueCapacityInfo(ResponseInfo ri) {
      ri.
      _("Used Capacity:", percent(lqinfo.getUsedCapacity() / 100)).
      _("Configured Capacity:", percent(lqinfo.getCapacity() / 100)).
      _("Configured Max Capacity:", percent(lqinfo.getMaxCapacity() / 100)).
      _("Absolute Used Capacity:", percent(lqinfo.getAbsoluteUsedCapacity() / 100)).
      _("Absolute Configured Capacity:", percent(lqinfo.getAbsoluteCapacity() / 100)).
      _("Absolute Configured Max Capacity:", percent(lqinfo.getAbsoluteMaxCapacity() / 100)).
      _("Used Resources:", lqinfo.getResourcesUsed().toString());
    }

    private void renderCommonLeafQueueInfo(ResponseInfo ri) {
      ri.
      _("Num Schedulable Applications:", Integer.toString(lqinfo.getNumActiveApplications())).
      _("Num Non-Schedulable Applications:", Integer.toString(lqinfo.getNumPendingApplications())).
      _("Num Containers:", Integer.toString(lqinfo.getNumContainers())).
      _("Max Application Master Resources:", lqinfo.getAMResourceLimit().toString()).
      _("Used Application Master Resources:", lqinfo.getUsedAMResource().toString()).
      _("Max Application Master Resources Per User:", lqinfo.getUserAMResourceLimit().toString()).
      _("Configured Minimum User Limit Percent:", Integer.toString(lqinfo.getUserLimit()) + "%").
      _("Configured User Limit Factor:", StringUtils.format(
          "%.1f", lqinfo.getUserLimitFactor())).
      _("Accessible Node Labels:", StringUtils.join(",", lqinfo.getNodeLabels())).
      _("Ordering Policy: ", lqinfo.getOrderingPolicyInfo()).
      _("Preemption:", lqinfo.getPreemptionDisabled() ? "disabled" : "enabled");
    }
  }

              span().$style(join(width(absUsedCap/absMaxCap),
                ";font-size:1px;left:0%;", absUsedCap > absCap ? Q_OVER : Q_UNDER)).
                _('.')._().
              span(".q", "Queue: "+info.getQueuePath().substring(5))._().
            span().$class("qstats").$style(left(Q_STATS_POS)).
              _(join(percent(used), " used"))._();

    final CapacityScheduler cs;
    final CSQInfo csqinfo;
    private final ResourceManager rm;
    private List<RMNodeLabel> nodeLabelsInfo;

    @Inject QueuesBlock(ResourceManager rm, CSQInfo info) {
      cs = (CapacityScheduler) rm.getResourceScheduler();
      csqinfo = info;
      this.rm = rm;
      RMNodeLabelsManager nodeLabelManager =
          rm.getRMContext().getNodeLabelManager();
      nodeLabelsInfo = nodeLabelManager.pullRMNodeLabelsInfo();
    }

    @Override
              span().$style(Q_END)._("100% ")._().
              span(".q", "default")._()._();
      } else {
        ul.
          li().$style("margin-bottom: 1em").
            span().$style("font-weight: bold")._("Legend:")._().
              _("Used (over capacity)")._().
            span().$class("qlegend ui-corner-all ui-state-default").
              _("Max Capacity")._().
          _();

        float used = 0;
        if (null == nodeLabelsInfo
            || (nodeLabelsInfo.size() == 1 && nodeLabelsInfo.get(0)
                .getLabelName().isEmpty())) {
          CSQueue root = cs.getRootQueue();
          CapacitySchedulerInfo sinfo =
              new CapacitySchedulerInfo(root, cs, new RMNodeLabel(
                  RMNodeLabelsManager.NO_LABEL));
          csqinfo.csinfo = sinfo;
          csqinfo.qinfo = null;

          used = sinfo.getUsedCapacity() / 100;
          ul.li().
            a(_Q).$style(width(Q_MAX_WIDTH)).
              span().$style(join(width(used), ";left:0%;",
                  used > 1 ? Q_OVER : Q_UNDER))._(".")._().
            span().$class("qstats").$style(left(Q_STATS_POS)).
              _(join(percent(used), " used"))._().
            _(QueueBlock.class)._();
        } else {
          for (RMNodeLabel label : nodeLabelsInfo) {
            CSQueue root = cs.getRootQueue();
            CapacitySchedulerInfo sinfo =
                new CapacitySchedulerInfo(root, cs, label);
            csqinfo.csinfo = sinfo;
            csqinfo.qinfo = null;
            csqinfo.label = label.getLabelName();
            String nodeLabel =
                csqinfo.label.length() == 0 ? "<DEFAULT_PARTITION>"
                    : csqinfo.label;
            QueueCapacities queueCapacities = root.getQueueCapacities();
            used = queueCapacities.getUsedCapacity(label.getLabelName());
            String partitionUiTag =
                "Partition: " + nodeLabel + " " + label.getResource();
            ul.li().
            a(_Q).$style(width(Q_MAX_WIDTH)).
              span().$style(join(width(used), ";left:0%;",
                  used > 1 ? Q_OVER : Q_UNDER))._(".")._().
              span(".q", partitionUiTag)._().
            span().$class("qstats").$style(left(Q_STATS_POS)).
              _(join(percent(used), " used"))._();

            UL<Hamlet> underLabel = html.ul("#pq");
            underLabel.li().
            a(_Q).$style(width(Q_MAX_WIDTH)).
              span().$style(join(width(used), ";left:0%;",
                  used > 1 ? Q_OVER : Q_UNDER))._(".")._().
              span(".q", "Queue: root")._().
            span().$class("qstats").$style(left(Q_STATS_POS)).
              _(join(percent(used), " used"))._().
            _(QueueBlock.class)._()._();
          }
        }
      }
      ul._()._().
      script().$type("text/javascript").

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMWebServices.java
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
    if (rs instanceof CapacityScheduler) {
      CapacityScheduler cs = (CapacityScheduler) rs;
      CSQueue root = cs.getRootQueue();
      sinfo =
          new CapacitySchedulerInfo(root, cs, new RMNodeLabel(
              RMNodeLabelsManager.NO_LABEL));
    } else if (rs instanceof FairScheduler) {
      FairScheduler fs = (FairScheduler) rs;
      sinfo = new FairSchedulerInfo(fs);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/CapacitySchedulerInfo.java
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;

@XmlRootElement(name = "capacityScheduler")
@XmlType(name = "capacityScheduler")
  public CapacitySchedulerInfo() {
  } // JAXB needs this

  public CapacitySchedulerInfo(CSQueue parent, CapacityScheduler cs,
      RMNodeLabel nodeLabel) {
    String label = nodeLabel.getLabelName();
    QueueCapacities parentQueueCapacities = parent.getQueueCapacities();
    this.queueName = parent.getQueueName();
    this.usedCapacity = parentQueueCapacities.getUsedCapacity(label) * 100;
    this.capacity = parentQueueCapacities.getCapacity(label) * 100;
    float max = parentQueueCapacities.getMaximumCapacity(label);
    if (max < EPSILON || max > 1f)
      max = 1f;
    this.maxCapacity = max * 100;

    queues = getQueues(parent, nodeLabel);
    health = new CapacitySchedulerHealthInfo(cs);
  }

    return this.queues;
  }

  protected CapacitySchedulerQueueInfoList getQueues(CSQueue parent,
      RMNodeLabel nodeLabel) {
    CSQueue parentQueue = parent;
    CapacitySchedulerQueueInfoList queuesInfo =
        new CapacitySchedulerQueueInfoList();
    for (CSQueue queue : parentQueue.getChildQueues()) {
      if (nodeLabel.getIsExclusive()
          && !((AbstractCSQueue) queue).accessibleToPartition(nodeLabel
              .getLabelName())) {
        continue;
      }
      CapacitySchedulerQueueInfo info;
      if (queue instanceof LeafQueue) {
        info =
            new CapacitySchedulerLeafQueueInfo((LeafQueue) queue,
                nodeLabel.getLabelName());
      } else {
        info = new CapacitySchedulerQueueInfo(queue, nodeLabel.getLabelName());
        info.queues = getQueues(queue, nodeLabel);
      }
      queuesInfo.addToQueueInfoList(info);
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/CapacitySchedulerLeafQueueInfo.java
  CapacitySchedulerLeafQueueInfo() {
  };

  CapacitySchedulerLeafQueueInfo(LeafQueue q, String nodeLabel) {
    super(q, nodeLabel);
    numActiveApplications = q.getNumActiveApplications();
    numPendingApplications = q.getNumPendingApplications();
    numContainers = q.getNumContainers();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/CapacitySchedulerQueueInfo.java
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlanQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
  CapacitySchedulerQueueInfo() {
  };

  CapacitySchedulerQueueInfo(CSQueue q, String nodeLabel) {
    QueueCapacities qCapacities = q.getQueueCapacities();
    ResourceUsage queueResourceUsage = q.getQueueResourceUsage();

    queuePath = q.getQueuePath();
    capacity = qCapacities.getCapacity(nodeLabel) * 100;
    usedCapacity = qCapacities.getUsedCapacity(nodeLabel) * 100;

    maxCapacity = qCapacities.getMaximumCapacity(nodeLabel);
    if (maxCapacity < EPSILON || maxCapacity > 1f)
      maxCapacity = 1f;
    maxCapacity *= 100;

    absoluteCapacity =
        cap(qCapacities.getAbsoluteCapacity(nodeLabel), 0f, 1f) * 100;
    absoluteMaxCapacity =
        cap(qCapacities.getAbsoluteMaximumCapacity(nodeLabel), 0f, 1f) * 100;
    absoluteUsedCapacity =
        cap(qCapacities.getAbsoluteUsedCapacity(nodeLabel), 0f, 1f) * 100;
    numApplications = q.getNumApplications();
    queueName = q.getQueueName();
    state = q.getState();
    resourcesUsed = new ResourceInfo(queueResourceUsage.getUsed(nodeLabel));
    if (q instanceof PlanQueue && !((PlanQueue) q).showReservationsAsQueues()) {
      hideReservationQueues = true;
    }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestCapacityScheduler.java
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.AdminService;
import org.apache.hadoop.yarn.server.resourcemanager.Application;
    CapacityScheduler cs =
        (CapacityScheduler) resourceManager.getResourceScheduler();
    CSQueue origRootQ = cs.getRootQueue();
    CapacitySchedulerInfo oldInfo =
        new CapacitySchedulerInfo(origRootQ, cs, new RMNodeLabel(
            RMNodeLabelsManager.NO_LABEL));
    int origNumAppsA = getNumAppsInQueue("a", origRootQ.getChildQueues());
    int origNumAppsRoot = origRootQ.getNumApplications();

    CSQueue newRootQ = cs.getRootQueue();
    int newNumAppsA = getNumAppsInQueue("a", newRootQ.getChildQueues());
    int newNumAppsRoot = newRootQ.getNumApplications();
    CapacitySchedulerInfo newInfo =
        new CapacitySchedulerInfo(newRootQ, cs, new RMNodeLabel(
            RMNodeLabelsManager.NO_LABEL));
    CapacitySchedulerLeafQueueInfo origOldA1 =
        (CapacitySchedulerLeafQueueInfo) getQueueInfo("a1", oldInfo.getQueues());
    CapacitySchedulerLeafQueueInfo origNewA1 =

