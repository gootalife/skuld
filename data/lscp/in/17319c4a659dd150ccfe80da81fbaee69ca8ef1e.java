hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
  public int getPendingDataNodeMessageCount() {
    return pendingDNMessages.count();
  }
  public long getTimeOfTheOldestBlockToBeReplicated() {
    return neededReplications.getTimeOfTheOldestBlockToBeReplicated();
  }

  private final long replicationRecheckInterval;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/UnderReplicatedBlocks.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.Time;


  private int corruptReplOneBlocks = 0;
  private final Map<BlockInfo, Long> timestampsMap =
      Collections.synchronizedMap(new LinkedHashMap<BlockInfo, Long>());

  UnderReplicatedBlocks() {
  }

  void clear() {
    for (int i = 0; i < LEVEL; i++) {
      priorityQueues.get(i).clear();
    }
    timestampsMap.clear();
  }

    return size;
  }

  long getTimeOfTheOldestBlockToBeReplicated() {
    synchronized (timestampsMap) {
      if (timestampsMap.isEmpty()) {
        return 0;
      }
      return timestampsMap.entrySet().iterator().next().getValue();
    }
  }

  synchronized int getCorruptBlockSize() {
    return priorityQueues.get(QUEUE_WITH_CORRUPT_BLOCKS).size();
              + " has only {} replicas and need {} replicas so is added to" +
              " neededReplications at priority level {}", block, curReplicas,
          expectedReplicas, priLevel);
      timestampsMap.put(block, Time.now());
      return true;
    }
    return false;
      NameNode.blockStateChangeLog.debug(
          "BLOCK* NameSystem.UnderReplicationBlock.remove: Removing block {}" +
              " from priority queue {}", block, priLevel);
      timestampsMap.remove(block);
      return true;
    } else {
          NameNode.blockStateChangeLog.debug(
              "BLOCK* NameSystem.UnderReplicationBlock.remove: Removing block" +
                  " {} from priority queue {}", block, priLevel);
          timestampsMap.remove(block);
          return true;
        }
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
    return blockManager.getMissingReplOneBlocksCount();
  }

  @Metric({"TimeOfTheOldestBlockToBeReplicated",
      "The timestamp of the oldest block to be replicated. If there are no" +
      "under-replicated or corrupt blocks, return 0."})
  public long getTimeOfTheOldestBlockToBeReplicated() {
    return blockManager.getTimeOfTheOldestBlockToBeReplicated();
  }

  @Metric({"ExpiredHeartbeats", "Number of expired heartbeats"})
  public int getExpiredHeartbeats() {
    return datanodeStatistics.getExpiredHeartbeats();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestUnderReplicatedBlocks.java
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.util.Time;
import org.junit.Test;

import java.util.Iterator;

  }

  @Test
  public void testGetTimeOfTheOldestBlockToBeReplicated() {
    UnderReplicatedBlocks blocks = new UnderReplicatedBlocks();
    BlockInfo block1 = new BlockInfoContiguous(new Block(1), (short) 1);
    BlockInfo block2 = new BlockInfoContiguous(new Block(2), (short) 1);

    assertEquals(blocks.getTimeOfTheOldestBlockToBeReplicated(), 0L);

    long time1 = Time.now();
    blocks.add(block1, 1, 0, 3);
    long time2 = Time.now();
    assertTrue(blocks.getTimeOfTheOldestBlockToBeReplicated() >= time1);
    assertTrue(blocks.getTimeOfTheOldestBlockToBeReplicated() <= time2);

    blocks.add(block2, 2, 0, 3);
    long time3 = Time.now();
    assertTrue(blocks.getTimeOfTheOldestBlockToBeReplicated() >= time1);
    assertTrue(blocks.getTimeOfTheOldestBlockToBeReplicated() <= time2);

    blocks.remove(block1, UnderReplicatedBlocks.QUEUE_HIGHEST_PRIORITY);
    assertTrue(blocks.getTimeOfTheOldestBlockToBeReplicated() >= time2);
    assertTrue(blocks.getTimeOfTheOldestBlockToBeReplicated() <= time3);

    blocks.remove(block2, UnderReplicatedBlocks.QUEUE_UNDER_REPLICATED);
    assertEquals(blocks.getTimeOfTheOldestBlockToBeReplicated(), 0L);

    time1 = Time.now();
    blocks.add(block2, 2, 0, 3);
    time2 = Time.now();
    assertTrue(blocks.getTimeOfTheOldestBlockToBeReplicated() >= time1);
    assertTrue(blocks.getTimeOfTheOldestBlockToBeReplicated() <= time2);

    blocks.add(block1, 1, 0, 3);
    assertTrue(blocks.getTimeOfTheOldestBlockToBeReplicated() >= time1);
    assertTrue(blocks.getTimeOfTheOldestBlockToBeReplicated() <= time2);

    blocks.remove(block1, UnderReplicatedBlocks.QUEUE_HIGHEST_PRIORITY);
    assertTrue(blocks.getTimeOfTheOldestBlockToBeReplicated() >= time1);
    assertTrue(blocks.getTimeOfTheOldestBlockToBeReplicated() <= time2);

    blocks.remove(block2, UnderReplicatedBlocks.QUEUE_UNDER_REPLICATED);
    assertEquals(blocks.getTimeOfTheOldestBlockToBeReplicated(), 0L);
  }
}

