hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockReceiver.java
      
      IOException cause = DatanodeUtil.getCauseIfDiskError(ioe);
      DataNode.LOG.warn("IOException in BlockReceiver constructor"
          + (cause == null ? "" : ". Cause is "), cause);
      
      if (cause != null) { // possible disk error
        ioe = cause;

