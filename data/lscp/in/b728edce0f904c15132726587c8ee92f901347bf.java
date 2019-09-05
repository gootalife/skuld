hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  public static final long    DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT = 10 * 1000;
  public static final String  DFS_BLOCK_INVALIDATE_LIMIT_KEY = "dfs.block.invalidate.limit";
  public static final int     DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT = 1000;
  public static final String  DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED_KEY = "dfs.corruptfilesreturned.max";
  public static final int     DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED = 500;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoUnderConstruction.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

  protected Block truncateBlock;

        "BlockInfoUnderConstruction cannot be in COMPLETE state");
    this.blockUCState = state;
    setExpectedLocations(targets);
  }

    if (replicas.size() == 0) {
      NameNode.blockStateChangeLog.warn("BLOCK* " +
          "BlockInfoUnderConstruction.initLeaseRecovery: " +
          "No blocks found, lease removed.");
    }
    boolean allLiveReplicasTriedAsPrimary = true;
    for (int i = 0; i < replicas.size(); i++) {
      }
    }
    if (allLiveReplicasTriedAsPrimary) {
      for (int i = 0; i < replicas.size(); i++) {
        replicas.get(i).setChosenAsPrimary(false);
    replicas.add(new ReplicaUnderConstruction(block, storage, rState));
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.IOException;
  private BlockPlacementPolicy blockplacement;
  private final BlockStoragePolicySuite storagePolicySuite;

  private boolean checkNSRunning = true;

    this.namesystem = namesystem;
    datanodeManager = new DatanodeManager(this, namesystem, conf);
    heartbeatManager = datanodeManager.getHeartbeatManager();

    startupDelayBlockDeletionInMs = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_KEY,
  public BlockInfo forceCompleteBlock(final BlockCollection bc,
      final BlockInfoUnderConstruction block) throws IOException {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
            + "Removed empty last block and closed file.");
        return true;
      }
      long blockRecoveryId = nextGenerationStamp(blockIdManager.isLegacyBlock(uc));
      lease = reassignLease(lease, src, recoveryLeaseHolder, pendingFile);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestLeaseRecovery.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
      }
    }
  }
}

