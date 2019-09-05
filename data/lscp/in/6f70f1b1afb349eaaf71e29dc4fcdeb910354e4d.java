hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ApplicationMasterService.java

      if (app.getApplicationSubmissionContext()
          .getKeepContainersAcrossApplicationAttempts()) {
        List<Container> transferredContainers = ((AbstractYarnScheduler) rScheduler)
            .getTransferredContainers(applicationAttemptId);
        if (!transferredContainers.isEmpty()) {
          response.setContainersFromPreviousAttempts(transferredContainers);
                nmTokens.add(token);
              }
            } catch (IllegalArgumentException e) {
              if (e.getCause() instanceof UnknownHostException) {
                throw (UnknownHostException) e.getCause();
              + transferredContainers.size() + " containers from previous"
              + " attempts and " + nmTokens.size() + " NM tokens.");
        }
      }

      response.setSchedulerResourceTypes(rScheduler
        .getSchedulingResourceTypes());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/AbstractYarnScheduler.java
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;


@SuppressWarnings("unchecked")
@Private
@Unstable
public abstract class AbstractYarnScheduler
    <T extends SchedulerApplicationAttempt, N extends SchedulerNode>
    extends AbstractService implements ResourceScheduler {
  private long configuredMaximumAllocationWaitTime;

  protected RMContext rmContext;
  
  protected ConcurrentMap<ApplicationId, SchedulerApplication<T>> applications;
  protected int nmExpireInterval;

  protected final static List<Container> EMPTY_CONTAINER_LIST =
    super.serviceInit(conf);
  }

  public List<Container> getTransferredContainers(
      ApplicationAttemptId currentAttempt) {
    ApplicationId appId = currentAttempt.getApplicationId();
    SchedulerApplication<T> app = applications.get(appId);
    if (appImpl.getApplicationSubmissionContext().getUnmanagedAM()) {
      return containerList;
    }
    if (app == null) {
      return containerList;
    }
    Collection<RMContainer> liveContainers =
        app.getCurrentAppAttempt().getLiveContainers();
    ContainerId amContainerId =

