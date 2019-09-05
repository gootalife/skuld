hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/CorruptReplicasMap.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.Server;

import com.google.common.annotations.VisibleForTesting;

    CORRUPTION_REPORTED  // client or datanode reported the corruption
  }

  private final Map<Block, Map<DatanodeDescriptor, Reason>> corruptReplicasMap =
    new HashMap<Block, Map<DatanodeDescriptor, Reason>>();

  @VisibleForTesting
  long[] getCorruptReplicaBlockIdsForTesting(int numExpectedBlocks,
                                   Long startingBlockId) {
    if (numExpectedBlocks < 0 || numExpectedBlocks > 100) {
      return null;
    }
    
    Iterator<Block> blockIt = 
        new TreeMap<>(corruptReplicasMap).keySet().iterator();
    

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestCorruptReplicaInfo.java
      
      assertEquals("Number of corrupt blocks must initially be 0", 0, crm.size());
      assertNull("Param n cannot be less than 0", crm.getCorruptReplicaBlockIdsForTesting(-1, null));
      assertNull("Param n cannot be greater than 100", crm.getCorruptReplicaBlockIdsForTesting(101, null));
      long[] l = crm.getCorruptReplicaBlockIdsForTesting(0, null);
      assertNotNull("n = 0 must return non-null", l);
      assertEquals("n = 0 must return an empty list", 0, l.length);

      
      assertTrue("First five block ids not returned correctly ",
                Arrays.equals(new long[]{0,1,2,3,4},
                              crm.getCorruptReplicaBlockIdsForTesting(5, null)));
                              
      LOG.info(crm.getCorruptReplicaBlockIdsForTesting(10, 7L));
      LOG.info(block_ids.subList(7, 18));

      assertTrue("10 blocks after 7 not returned correctly ",
                Arrays.equals(new long[]{8,9,10,11,12,13,14,15,16,17},
                              crm.getCorruptReplicaBlockIdsForTesting(10, 7L)));
      
  }
  

