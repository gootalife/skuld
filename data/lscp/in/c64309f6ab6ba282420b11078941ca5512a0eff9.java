hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ha/ZKFailoverController.java
        }
      });
    } catch (RuntimeException rte) {
      LOG.fatal("The failover controller encounters runtime error: " + rte);
      throw (Exception)rte.getCause();
    }
  }

