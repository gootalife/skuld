hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-applications-distributedshell/src/main/java/org/apache/hadoop/yarn/applications/distributedshell/ApplicationMaster.java
  private AMRMClientAsync amRMClient;

  @VisibleForTesting
  UserGroupInformation appSubmitterUgi;

  private NMClientAsync nmClientAsync;
  private List<Thread> launchThreads = new ArrayList<Thread>();

  @VisibleForTesting
  TimelineClient timelineClient;

  private final String linux_bash_command = "bash";
  private final String windows_command = "cmd /c";
    }
    requestPriority = Integer.parseInt(cliParser
        .getOptionValue("priority", "0"));
    return true;
  }

  @SuppressWarnings({ "unchecked" })
  public void run() throws YarnException, IOException, InterruptedException {
    LOG.info("Starting ApplicationMaster");

        UserGroupInformation.createRemoteUser(appSubmitterUserName);
    appSubmitterUgi.addCredentials(credentials);


    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    nmClientAsync.init(conf);
    nmClientAsync.start();

    startTimelineClient(conf);
    if(timelineClient != null) {
      publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
          DSEvent.DS_APP_ATTEMPT_START, domainId, appSubmitterUgi);
    }

      amRMClient.addContainerRequest(containerAsk);
    }
    numRequestedContainers.set(numTotalContainers);
  }

  @VisibleForTesting
  void startTimelineClient(final Configuration conf)
      throws YarnException, IOException, InterruptedException {
    try {
      appSubmitterUgi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
              YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
            timelineClient = TimelineClient.createTimelineClient();
            timelineClient.init(conf);
            timelineClient.start();
          } else {
            timelineClient = null;
            LOG.warn("Timeline service is not enabled");
          }
          return null;
        }
      });
    } catch (UndeclaredThrowableException e) {
      throw new YarnException(e.getCause());
    }
  }

      } catch (InterruptedException ex) {}
    }

    if(timelineClient != null) {
      publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
          DSEvent.DS_APP_ATTEMPT_END, domainId, appSubmitterUgi);
    }

    event.addEventInfo("State", container.getState().name());
    event.addEventInfo("Exit Status", container.getExitStatus());
    entity.addEvent(event);
    try {
      timelineClient.putEntities(entity);
    } catch (YarnException | IOException e) {
      LOG.error("Container end event could not be published for "
          + container.getContainerId().toString(), e);
    }
  }

    event.setEventType(appEvent.toString());
    event.setTimestamp(System.currentTimeMillis());
    entity.addEvent(event);
    try {
      timelineClient.putEntities(entity);
    } catch (YarnException | IOException e) {
      LOG.error("App Attempt "
          + (appEvent.equals(DSEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
          + " event could not be published for "
          + appAttemptId.toString(), e);
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-applications-distributedshell/src/test/java/org/apache/hadoop/yarn/applications/distributedshell/TestDSAppMaster.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-applications-distributedshell/src/test/java/org/apache/hadoop/yarn/applications/distributedshell/TestDSAppMaster.java

package org.apache.hadoop.yarn.applications.distributedshell;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.client.api.impl.TimelineClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class TestDSAppMaster {

  @Test
  public void testTimelineClientInDSAppMaster() throws Exception {
    ApplicationMaster appMaster = new ApplicationMaster();
    appMaster.appSubmitterUgi =
        UserGroupInformation.createUserForTesting("foo", new String[]{"bar"});
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    appMaster.startTimelineClient(conf);
    Assert.assertEquals(appMaster.appSubmitterUgi,
        ((TimelineClientImpl)appMaster.timelineClient).getUgi());
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-applications-distributedshell/src/test/java/org/apache/hadoop/yarn/applications/distributedshell/TestDSFailedAppMaster.java
  private static final Log LOG = LogFactory.getLog(TestDSFailedAppMaster.class);

  @Override
  public void run() throws YarnException, IOException, InterruptedException {
    super.run();


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/client/api/TimelineClient.java
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
@Unstable
public abstract class TimelineClient extends AbstractService {

  @Public
  public static TimelineClient createTimelineClient() {
    TimelineClient client = new TimelineClientImpl();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/client/api/impl/TimelineClientImpl.java
    new HelpFormatter().printHelp("TimelineClient", opts);
  }

  @VisibleForTesting
  @Private
  public UserGroupInformation getUgi() {
    return authUgi;
  }
}

