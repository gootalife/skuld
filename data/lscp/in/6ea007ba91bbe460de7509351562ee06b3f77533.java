hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/ResourceLocalizationService.java
        try {
          req = new LocalResourceRequest(rsrc);
        } catch (URISyntaxException e) {
          LOG.error(
              "Got exception in parsing URL of LocalResource:"
                  + rsrc.getResource(), e);
        }
        LocalizerResourceRequestEvent assoc = scheduled.get(req);
        if (assoc == null) {
          LOG.error("Inorrect path for PRIVATE localization."
              + next.getResource().getFile(), e);
        } catch (URISyntaxException e) {
          LOG.error(
              "Got exception in parsing URL of LocalResource:"
                  + next.getResource(), e);
        }
      }


