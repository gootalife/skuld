hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  public static final long    DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT = 10 * 1000;
  public static final String  DFS_BLOCK_INVALIDATE_LIMIT_KEY = "dfs.block.invalidate.limit";
  public static final int     DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT = 1000;
  public static final String  DFS_BLOCK_UC_MAX_RECOVERY_ATTEMPTS = "dfs.block.uc.max.recovery.attempts";
  public static final int     DFS_BLOCK_UC_MAX_RECOVERY_ATTEMPTS_DEFAULT = 5;

  public static final String  DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED_KEY = "dfs.corruptfilesreturned.max";
  public static final int     DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED = 500;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoUnderConstruction.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

  protected Block truncateBlock;

  private int recoveryAttemptsBeforeMarkingBlockMissing;

        "BlockInfoUnderConstruction cannot be in COMPLETE state");
    this.blockUCState = state;
    setExpectedLocations(targets);
    this.recoveryAttemptsBeforeMarkingBlockMissing =
      BlockManager.getMaxBlockUCRecoveries();
  }

    if (replicas.size() == 0) {
      NameNode.blockStateChangeLog.warn("BLOCK* " +
          "BlockInfoUnderConstruction.initLeaseRecovery: " +
          "No replicas found.");
    }
    boolean allLiveReplicasTriedAsPrimary = true;
    for (int i = 0; i < replicas.size(); i++) {
      }
    }
    if (allLiveReplicasTriedAsPrimary) {
      recoveryAttemptsBeforeMarkingBlockMissing--;
      NameNode.blockStateChangeLog.info("Tried to recover " + this +" using all"
          + " replicas. Will try " + recoveryAttemptsBeforeMarkingBlockMissing
          + " more times");

      for (int i = 0; i < replicas.size(); i++) {
        replicas.get(i).setChosenAsPrimary(false);
    replicas.add(new ReplicaUnderConstruction(block, storage, rState));
  }

  public int getNumRecoveryAttemptsLeft() {
    return recoveryAttemptsBeforeMarkingBlockMissing;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.IOException;
  private BlockPlacementPolicy blockplacement;
  private final BlockStoragePolicySuite storagePolicySuite;

  private static int maxBlockUCRecoveries =
    DFSConfigKeys.DFS_BLOCK_UC_MAX_RECOVERY_ATTEMPTS_DEFAULT;
  public static int getMaxBlockUCRecoveries() { return maxBlockUCRecoveries; }

  private boolean checkNSRunning = true;

    this.namesystem = namesystem;
    datanodeManager = new DatanodeManager(this, namesystem, conf);
    heartbeatManager = datanodeManager.getHeartbeatManager();
    maxBlockUCRecoveries = conf.getInt(
      DFSConfigKeys.DFS_BLOCK_UC_MAX_RECOVERY_ATTEMPTS,
      DFSConfigKeys.DFS_BLOCK_UC_MAX_RECOVERY_ATTEMPTS_DEFAULT);

    startupDelayBlockDeletionInMs = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_KEY,
  public BlockInfo forceCompleteBlock(final BlockCollection bc,
      final BlockInfoUnderConstruction block) throws IOException {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
            + "Removed empty last block and closed file.");
        return true;
      }

      if(uc.getNumRecoveryAttemptsLeft() == 0) {
        blockManager.forceCompleteBlock(pendingFile, uc);
        finalizeINodeFileUnderConstruction(src, pendingFile,
            iip.getLatestSnapshotId());
        return true;
      }

      long blockRecoveryId = nextGenerationStamp(blockIdManager.isLegacyBlock(uc));
      lease = reassignLease(lease, src, recoveryLeaseHolder, pendingFile);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestLeaseRecovery.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
      }
    }
  }

  @Test
  public void testLeaseRecoveryWithMissingBlocks()
    throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.setLeasePeriod(LEASE_PERIOD, LEASE_PERIOD);
    cluster.waitActive();

    Path file = new Path("/testRecoveryFile");
    DistributedFileSystem dfs = cluster.getFileSystem();
    FSDataOutputStream out = dfs.create(file, (short) 1);

    long writtenBytes = 0;
    while (writtenBytes < 2 * 1024 * 1024) {
      out.writeLong(writtenBytes);
      writtenBytes += 8;
    }
    System.out.println("Written " + writtenBytes + " bytes");
    out.hsync();
    System.out.println("hsynced the data");

    DatanodeInfo dn =
      ((DFSOutputStream) out.getWrappedStream()).getPipeline()[0];
    DataNodeProperties dnStopped = cluster.stopDataNode(dn.getName());

    LeaseManager lm = NameNodeAdapter.getLeaseManager(cluster.getNamesystem());
    int i = 40;
    while(i-- > 0 && lm.countLease() != 0) {
      System.out.println("Still got " + lm.countLease() + " lease(s)");
      Thread.sleep(500);
    }
    assertTrue("The lease was not recovered", lm.countLease() == 0);
    System.out.println("Got " + lm.countLease() + " leases");

    FSDataInputStream in = dfs.open(file);
    try {
      in.readLong();
      assertTrue("Shouldn't have reached here", false);
    } catch(BlockMissingException bme) {
      System.out.println("Correctly got BlockMissingException because datanode"
        + " is still dead");
    }

    cluster.restartDataNode(dnStopped);
    System.out.println("Restart datanode");

    in = dfs.open(file);
    int readBytes = 0;
    while(in.available() != 0) {
      assertEquals("Didn't read the data we wrote", in.readLong(), readBytes);
      readBytes += 8;
    }
    assertEquals("Didn't get all the data", readBytes, writtenBytes);
    System.out.println("Read back all the " + readBytes + " bytes");
  }

}

