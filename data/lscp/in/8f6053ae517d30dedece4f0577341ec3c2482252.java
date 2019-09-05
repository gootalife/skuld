hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/ResourceLocalizationService.java
          case FETCH_FAILURE:
            final String diagnostics = stat.getException().toString();
            LOG.warn(req + " failed: " + diagnostics);
            action = LocalizerAction.DIE;
            getLocalResourcesTracker(req.getVisibility(), user, applicationId)
              .handle(new ResourceFailedLocalizationEvent(
                  req, diagnostics));

