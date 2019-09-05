hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSInputStream.java
        } else {
          connectFailedOnce = true;
          DFSClient.LOG.warn("Failed to connect to " + targetAddr + " for block "
            +targetBlock.getBlock()+ ", add to deadNodes and continue. " + ex, ex);
          addToDeadNodes(chosenNode);
        }

