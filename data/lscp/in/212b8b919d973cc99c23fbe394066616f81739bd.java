hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/ha/StandbyState.java
        (op == OperationCategory.READ && context.allowStaleReads())) {
      return;
    }
    String faq = ". Visit https://s.apache.org/sbnn-error";
    String msg = "Operation category " + op + " is not supported in state "
        + context.getState() + faq;
    throw new StandbyException(msg);
  }


