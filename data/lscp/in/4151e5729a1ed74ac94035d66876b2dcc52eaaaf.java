hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ha/HAAdmin.java
            "supported with auto-failover enabled.");
        return -1;
      }
      try {
        return gracefulFailoverThroughZKFCs(toNode);
      } catch (UnsupportedOperationException e){
        errOut.println("Failover command is not supported with " +
            "auto-failover enabled: " + e.getLocalizedMessage());
        return -1;
      }
    }
    
    FailoverController fc = new FailoverController(getConf(),

