hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java
        LOG.warn("Exception shutting down DataNode", e);
      }
    }
    if (httpServer != null) {
      try {
        httpServer.close();
      } catch (Exception e) {
        LOG.warn("Exception shutting down DataNode HttpServer", e);
      }
    }

    if (pauseMonitor != null) {
      pauseMonitor.stop();

