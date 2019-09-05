hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/RMServerUtils.java
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      RMContext rmContext)
      throws InvalidResourceRequestException {
    QueueInfo queueInfo = null;
    try {
      queueInfo = scheduler.getQueueInfo(queueName, false, false);
    } catch (IOException e) {
    }

    for (ResourceRequest resReq : ask) {
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maximumResource,
          queueName, scheduler, rmContext, queueInfo);
    }
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/SchedulerUtils.java
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      boolean isRecovery, RMContext rmContext)
      throws InvalidResourceRequestException {
    normalizeAndValidateRequest(resReq, maximumResource, queueName, scheduler,
        isRecovery, rmContext, null);
  }

  public static void normalizeAndValidateRequest(ResourceRequest resReq,
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      boolean isRecovery, RMContext rmContext, QueueInfo queueInfo)
      throws InvalidResourceRequestException {
    if (null == queueInfo) {
      try {
        queueInfo = scheduler.getQueueInfo(queueName, false, false);
      } catch (IOException e) {
      }
    }
    SchedulerUtils.normalizeNodeLabelExpressionInRequest(resReq, queueInfo);
    if (!isRecovery) {
      validateResourceRequest(resReq, maximumResource, queueInfo, rmContext);
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      RMContext rmContext)
      throws InvalidResourceRequestException {
    normalizeAndvalidateRequest(resReq, maximumResource, queueName, scheduler,
        rmContext, null);
  }

  public static void normalizeAndvalidateRequest(ResourceRequest resReq,
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      RMContext rmContext, QueueInfo queueInfo)
      throws InvalidResourceRequestException {
    normalizeAndValidateRequest(resReq, maximumResource, queueName, scheduler,
        false, rmContext, queueInfo);
  }


