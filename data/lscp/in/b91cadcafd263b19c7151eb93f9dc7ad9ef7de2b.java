hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/webhdfs/ExceptionHandler.java
    Exception e = cause instanceof Exception ? (Exception) cause : new Exception(cause);

    if (LOG.isTraceEnabled()) {
      LOG.trace("GOT EXCEPTION", e);
    }


