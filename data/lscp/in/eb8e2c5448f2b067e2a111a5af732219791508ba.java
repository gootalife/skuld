hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/LeafQueue.java
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
    for (Iterator<FiCaSchedulerApp> i=pendingApplications.iterator(); 
         i.hasNext(); ) {
      FiCaSchedulerApp application = i.next();
      ApplicationId applicationId = application.getApplicationId();
      Resource amIfStarted = 
        Resources.add(application.getAMResource(), queueUsage.getAMUsed());
            " single application in queue, it is likely set too low." +
            " skipping enforcement to allow at least one application to start"); 
        } else {
          LOG.info("Not activating application " + applicationId
              + " as  amIfStarted: " + amIfStarted + " exceeds amLimit: "
              + amLimit);
          continue;
        }
      }
            " single application in queue for user, it is likely set too low." +
            " skipping enforcement to allow at least one application to start"); 
        } else {
          LOG.info("Not activating application " + applicationId
              + " for user: " + user + " as userAmIfStarted: "
              + userAmIfStarted + " exceeds userAmLimit: " + userAMLimit);
          continue;
        }
      }
      metrics.incAMUsed(application.getUser(), application.getAMResource());
      metrics.setAMResouceLimitForUser(application.getUser(), userAMLimit);
      i.remove();
      LOG.info("Application " + applicationId + " from user: "
          + application.getUser() + " activated in queue: " + getQueueName());
    }
  }
  

