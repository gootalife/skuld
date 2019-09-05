hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
  public int getPendingDataNodeMessageCount() {
    return pendingDNMessages.count();
  }

  private final long replicationRecheckInterval;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/UnderReplicatedBlocks.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.apache.hadoop.hdfs.server.namenode.NameNode;


  private int corruptReplOneBlocks = 0;

  UnderReplicatedBlocks() {
  }

  synchronized void clear() {
    for (int i = 0; i < LEVEL; i++) {
      priorityQueues.get(i).clear();
    }
    corruptReplOneBlocks = 0;
  }

    return size;
  }

  synchronized int getCorruptBlockSize() {
    return priorityQueues.get(QUEUE_WITH_CORRUPT_BLOCKS).size();
              + " has only {} replicas and need {} replicas so is added to" +
              " neededReplications at priority level {}", block, curReplicas,
          expectedReplicas, priLevel);

      return true;
    }
    return false;
      NameNode.blockStateChangeLog.debug(
        "BLOCK* NameSystem.UnderReplicationBlock.remove: Removing block {}" +
            " from priority queue {}", block, priLevel);
      return true;
    } else {
          NameNode.blockStateChangeLog.debug(
              "BLOCK* NameSystem.UnderReplicationBlock.remove: Removing block" +
                  " {} from priority queue {}", block, priLevel);
          return true;
        }
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
    return blockManager.getMissingReplOneBlocksCount();
  }
  
  @Metric({"ExpiredHeartbeats", "Number of expired heartbeats"})
  public int getExpiredHeartbeats() {
    return datanodeStatistics.getExpiredHeartbeats();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestUnderReplicatedBlocks.java
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.junit.Test;

import java.util.Iterator;

  }

}

