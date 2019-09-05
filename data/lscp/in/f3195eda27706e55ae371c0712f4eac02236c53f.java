hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapred/LocalContainerLauncher.java
        try {
          event = eventQueue.take();
        } catch (InterruptedException e) {  // mostly via T_KILL? JOB_KILL?
          LOG.warn("Returning, interrupted : " + e);
          break;
        }


