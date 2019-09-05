hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMEmbeddedElector.java
                ServiceFailedException {
              try {
                callbackCalled.set(true);
                TestRMEmbeddedElector.LOG.info("Callback called. Sleeping now");
                Thread.sleep(delayMs);
                TestRMEmbeddedElector.LOG.info("Sleep done");
              } catch (InterruptedException e) {
                e.printStackTrace();
              }

