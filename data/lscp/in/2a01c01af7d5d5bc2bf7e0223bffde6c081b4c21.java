hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmnode/RMNodeImpl.java
        rmNode.httpPort = newNode.getHttpPort();
        rmNode.httpAddress = newNode.getHttpAddress();
        boolean isCapabilityChanged = false;
        if (!rmNode.getTotalCapability().equals(
            newNode.getTotalCapability())) {
          rmNode.totalCapability = newNode.getTotalCapability();
          isCapabilityChanged = true;
        }

