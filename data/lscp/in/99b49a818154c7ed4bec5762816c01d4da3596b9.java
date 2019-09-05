hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/ResourceLocalizationService.java
    for (LocalizedResourceProto proto : state.getLocalizedResources()) {
      LocalResource rsrc = new LocalResourcePBImpl(proto.getResource());
      LocalResourceRequest req = new LocalResourceRequest(rsrc);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Recovering localized resource " + req + " at "
            + proto.getLocalPath());
      }
      tracker.handle(new ResourceRecoveredEvent(req,
          new Path(proto.getLocalPath()), proto.getSize()));
    }

