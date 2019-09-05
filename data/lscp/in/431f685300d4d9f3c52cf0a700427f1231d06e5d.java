hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
import org.apache.hadoop.hdfs.server.blockmanagement.PendingDataNodeMessages.ReportedBlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
        return;
      }

      CachedBlock cblock = namesystem.getCacheManager().getCachedBlocks()
          .get(new CachedBlock(block.getBlockId(), (short) 0, false));
      if (cblock != null) {
        boolean removed = false;
        removed |= node.getPendingCached().remove(cblock);
        removed |= node.getCached().remove(cblock);
        removed |= node.getPendingUncached().remove(cblock);
        if (removed) {
          blockLog.debug("BLOCK* removeStoredBlock: {} removed from caching "
              + "related lists on node {}", block, node);
        }
      }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BPServiceActor.java
        }
        processCommand(cmds == null ? null : cmds.toArray(new DatanodeCommand[cmds.size()]));

        if (!dn.areCacheReportsDisabledForTests()) {
          DatanodeCommand cmd = cacheReport();
          processCommand(new DatanodeCommand[]{ cmd });
        }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java
  ThreadGroup threadGroup = null;
  private DNConf dnConf;
  private volatile boolean heartbeatsDisabledForTests = false;
  private volatile boolean cacheReportsDisabledForTests = false;
  private DataStorage storage = null;

  private DatanodeHttpServer httpServer = null;

  
  @VisibleForTesting
  void setHeartbeatsDisabledForTests(
      boolean heartbeatsDisabledForTests) {
    this.heartbeatsDisabledForTests = heartbeatsDisabledForTests;
  }

  @VisibleForTesting
  boolean areHeartbeatsDisabledForTests() {
    return this.heartbeatsDisabledForTests;
  }

  @VisibleForTesting
  void setCacheReportsDisabledForTest(boolean disabled) {
    this.cacheReportsDisabledForTests = disabled;
  }

  @VisibleForTesting
  boolean areCacheReportsDisabledForTests() {
    return this.cacheReportsDisabledForTests;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/CacheManager.java
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
    if (cachedBlock == null) {
      return;
    }
    List<DatanodeDescriptor> cachedDNs = cachedBlock.getDatanodes(Type.CACHED);
    for (DatanodeDescriptor datanode : cachedDNs) {
      boolean found = false;
      for (DatanodeInfo loc : block.getLocations()) {
        if (loc.equals(datanode)) {
          block.addCachedLoc(loc);
          found = true;
          break;
        }
      }
      if (!found) {
        LOG.warn("Datanode {} is not a valid cache location for block {} "
            + "because that node does not have a backing replica!",
            datanode, block.getBlock().getBlockName());
      }
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
    this.dir = dir;
  }
  @Override
  public CacheManager getCacheManager() {
    return cacheManager;
  }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/Namesystem.java
@InterfaceAudience.Private
public interface Namesystem extends RwLock, SafeMode {
  boolean isRunning();

  void checkSuperuserPrivilege() throws AccessControlException;

  String getBlockPoolId();

  boolean isInStandbyState();

  boolean isGenStampInFuture(Block block);

  void adjustSafeModeBlockTotals(int deltaSafe, int deltaTotal);

  void checkOperation(OperationCategory read) throws StandbyException;

  boolean isInSnapshot(BlockInfoUnderConstruction blockUC);

  CacheManager getCacheManager();
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/DFSTestUtil.java
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.FsShell;
    }
  }

  public static void waitForReplication(final DistributedFileSystem dfs,
      final Path file, final short replication, int waitForMillis)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          FileStatus stat = dfs.getFileStatus(file);
          return replication == stat.getReplication();
        } catch (IOException e) {
          LOG.info("getFileStatus on path " + file + " failed!", e);
          return false;
        }
      }
    }, 100, waitForMillis);
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/DataNodeTestUtils.java
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
    dn.setHeartbeatsDisabledForTests(heartbeatsDisabledForTests);
  }

  public static void setCacheReportsDisabledForTests(MiniDFSCluster cluster,
      boolean disabled) {
    for (DataNode dn : cluster.getDataNodes()) {
      dn.setCacheReportsDisabledForTest(disabled);
    }
  }

  public static void triggerDeletionReport(DataNode dn) throws IOException {
    for (BPOfferService bpos : dn.getAllBpOs()) {
      bpos.triggerDeletionReportForTests();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestCacheDirectives.java
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList.Type;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator;
    Thread.sleep(1000);
    checkPendingCachedEmpty(cluster);
  }

  @Test(timeout=60000)
  public void testNoBackingReplica() throws Exception {
    final Path filename = new Path("/noback");
    final short replication = (short) 3;
    DFSTestUtil.createFile(dfs, filename, 1, replication, 0x0BAC);
    dfs.addCachePool(new CachePoolInfo("pool"));
    dfs.addCacheDirective(
        new CacheDirectiveInfo.Builder().setPool("pool").setPath(filename)
            .setReplication(replication).build());
    waitForCachedBlocks(namenode, 1, replication, "testNoBackingReplica:1");
    DataNodeTestUtils.setCacheReportsDisabledForTests(cluster, true);
    try {
      dfs.setReplication(filename, (short) 1);
      DFSTestUtil.waitForReplication(dfs, filename, (short) 1, 30000);
      waitForCachedBlocks(namenode, 1, (short) 1, "testNoBackingReplica:2");
    } finally {
      DataNodeTestUtils.setCacheReportsDisabledForTests(cluster, false);
    }
  }
}

