hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/container/ContainerImpl.java
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationCleanupEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache.SharedCacheUploadEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache.SharedCacheUploadEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerContainerFinishedEvent;
        return ContainerState.LOCALIZING;
      }

      container.dispatcher.getEventHandler().handle(
          new ContainerLocalizationEvent(LocalizationEventType.
              CONTAINER_RESOURCES_LOCALIZED, container));

      container.sendLaunchEvent();
      container.metrics.endInitingContainer();

                SharedCacheUploadEventType.UPLOAD));
      }

      return ContainerState.LOCALIZED;
    }
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/ResourceLocalizationService.java
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResourceFailedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationCleanupEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
    case INIT_CONTAINER_RESOURCES:
      handleInitContainerResources((ContainerLocalizationRequestEvent) event);
      break;
    case CONTAINER_RESOURCES_LOCALIZED:
      handleContainerResourcesLocalized((ContainerLocalizationEvent) event);
      break;
    case CACHE_CLEANUP:
      handleCacheCleanup(event);
      break;
    }
  }

  private void handleContainerResourcesLocalized(
      ContainerLocalizationEvent event) {
    Container c = event.getContainer();
    String locId = ConverterUtils.toString(c.getContainerId());
    localizerTracker.endContainerLocalization(locId);
  }

  private void handleCacheCleanup(LocalizationEvent event) {
    ResourceRetentionSet retain =
      new ResourceRetentionSet(delService, cacheTargetSize);
          response.setLocalizerAction(LocalizerAction.DIE);
          return response;
        }
        return localizer.processHeartbeat(status.getResources());
      }
    }
    
        localizer.interrupt();
      }
    }

    public void endContainerLocalization(String locId) {
      LocalizerRunner localizer;
      synchronized (privLocalizers) {
        localizer = privLocalizers.get(locId);
        if (null == localizer) {
          return; // ignore
        }
      }
      localizer.endContainerLocalization();
    }
  }
  

    final Map<LocalResourceRequest,LocalizerResourceRequestEvent> scheduled;
    final List<LocalizerResourceRequestEvent> pending;
    private AtomicBoolean killContainerLocalizer = new AtomicBoolean(false);

    private final RecordFactory recordFactory =
      pending.add(request);
    }

    public void endContainerLocalization() {
      killContainerLocalizer.set(true);
    }

      }
    }

    LocalizerHeartbeatResponse processHeartbeat(
        List<LocalResourceStatus> remoteResourceStatuses) {
      LocalizerHeartbeatResponse response =
        recordFactory.newRecordInstance(LocalizerHeartbeatResponse.class);
      ApplicationId applicationId =
          context.getContainerId().getApplicationAttemptId().getApplicationId();

      boolean fetchFailed = false;
      for (LocalResourceStatus stat : remoteResourceStatuses) {
        LocalResource rsrc = stat.getResource();
          case FETCH_FAILURE:
            final String diagnostics = stat.getException().toString();
            LOG.warn(req + " failed: " + diagnostics);
            fetchFailed = true;
            getLocalResourcesTracker(req.getVisibility(), user, applicationId)
              .handle(new ResourceFailedLocalizationEvent(
                  req, diagnostics));
            break;
          default:
            LOG.info("Unknown status: " + stat.getStatus());
            fetchFailed = true;
            getLocalResourcesTracker(req.getVisibility(), user, applicationId)
              .handle(new ResourceFailedLocalizationEvent(
                  req, stat.getException().getMessage()));
            break;
        }
      }
      if (fetchFailed || killContainerLocalizer.get()) {
        response.setLocalizerAction(LocalizerAction.DIE);
        return response;
      }

        } catch (URISyntaxException e) {
        }
      }

      response.setLocalizerAction(LocalizerAction.LIVE);
      response.setResourceSpecs(rsrcs);
      return response;
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/event/LocalizationEventType.java
  CACHE_CLEANUP,
  CLEANUP_CONTAINER_RESOURCES,
  DESTROY_APPLICATION_RESOURCES,
  CONTAINER_RESOURCES_LOCALIZED,
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/TestResourceLocalizationService.java
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService.PublicLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationCleanupEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
        .thenReturn(Collections.<LocalResourceStatus>emptyList())
        .thenReturn(Collections.singletonList(rsrc1success))
        .thenReturn(Collections.singletonList(rsrc2pending))
        .thenReturn(rsrcs4)
        .thenReturn(Collections.<LocalResourceStatus>emptyList());

      String localPath = Path.SEPARATOR + ContainerLocalizer.USERCACHE +
          Path.SEPARATOR + "user0" + Path.SEPARATOR +
      assertTrue(localizedPath.getFile().endsWith(
          localPath + Path.SEPARATOR + "1" + Path.SEPARATOR + "12"));

      response = spyService.heartbeat(stat);
      assertEquals(LocalizerAction.LIVE, response.getLocalizerAction());

      spyService.handle(new ContainerLocalizationEvent(
          LocalizationEventType.CONTAINER_RESOURCES_LOCALIZED, c));

      response = spyService.heartbeat(stat);
      assertEquals(LocalizerAction.DIE, response.getLocalizerAction());


