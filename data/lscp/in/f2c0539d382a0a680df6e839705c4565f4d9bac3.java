hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/net/NetworkTopology.java
    int newDepth = NodeBase.locationToDepth(node.getNetworkLocation()) + 1;
    netlock.writeLock().lock();
    try {
      if( node instanceof InnerNode ) {
        throw new IllegalArgumentException(
          "Not allow to add an inner node: "+NodeBase.getPath(node));
      }
      if ((depthOfAllLeaves != -1) && (depthOfAllLeaves != newDepth)) {
        LOG.error("Error: can't add leaf node " + NodeBase.getPath(node) +
            " at depth " + newDepth + " to topology:\n" + this.toString());
        throw new InvalidTopologyException("Failed to add " + NodeBase.getPath(node) +
            ": You cannot have a rack and a non-rack node at the same " +
            "level of the network topology.");

