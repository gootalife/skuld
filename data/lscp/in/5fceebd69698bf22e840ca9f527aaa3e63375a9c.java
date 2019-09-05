hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeStatusUpdaterImpl.java
        + durationToTrackStoppedContainers);
    }
    super.serviceInit(conf);
    LOG.info("Initialized nodemanager with :" +
        " physical-memory=" + memoryMb + " virtual-memory=" + virtualMemoryMb +
        " virtual-cores=" + virtualCores);


    this.nodeId = this.context.getNodeId();
    LOG.info("Node ID assigned is : " + this.nodeId);
    this.httpPort = this.context.getHttpPort();
    this.nodeManagerVersionId = YarnVersionInfo.getVersion();
    try {

