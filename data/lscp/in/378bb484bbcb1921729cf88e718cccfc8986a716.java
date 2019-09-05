hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  public static final int     DFS_BLOCKREPORT_INITIAL_DELAY_DEFAULT = 0;
  public static final String  DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY = "dfs.blockreport.split.threshold";
  public static final long    DFS_BLOCKREPORT_SPLIT_THRESHOLD_DEFAULT = 1000 * 1000;
  public static final String  DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES = "dfs.namenode.max.full.block.report.leases";
  public static final int     DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES_DEFAULT = 6;
  public static final String  DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS = "dfs.namenode.full.block.report.lease.length.ms";
  public static final long    DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT = 5L * 60L * 1000L;
  public static final String  DFS_CACHEREPORT_INTERVAL_MSEC_KEY = "dfs.cachereport.intervalMsec";
  public static final long    DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT = 10 * 1000;
  public static final String  DFS_BLOCK_INVALIDATE_LIMIT_KEY = "dfs.block.invalidate.limit";

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/DatanodeProtocolClientSideTranslatorPB.java
  public HeartbeatResponse sendHeartbeat(DatanodeRegistration registration,
      StorageReport[] reports, long cacheCapacity, long cacheUsed,
      int xmitsInProgress, int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary,
      boolean requestFullBlockReportLease) throws IOException {
    HeartbeatRequestProto.Builder builder = HeartbeatRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration))
        .setXmitsInProgress(xmitsInProgress).setXceiverCount(xceiverCount)
        .setFailedVolumes(failedVolumes)
        .setRequestFullBlockReportLease(requestFullBlockReportLease);
    builder.addAllReports(PBHelper.convertStorageReports(reports));
    if (cacheCapacity != 0) {
      builder.setCacheCapacity(cacheCapacity);
      rollingUpdateStatus = PBHelper.convert(resp.getRollingUpgradeStatus());
    }
    return new HeartbeatResponse(cmds, PBHelper.convert(resp.getHaStatus()),
        rollingUpdateStatus, resp.getFullBlockReportLeaseId());
  }

  @Override

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/DatanodeProtocolServerSideTranslatorPB.java
          report, request.getCacheCapacity(), request.getCacheUsed(),
          request.getXmitsInProgress(),
          request.getXceiverCount(), request.getFailedVolumes(),
          volumeFailureSummary, request.getRequestFullBlockReportLease());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
      builder.setRollingUpgradeStatus(PBHelper
          .convertRollingUpgradeStatus(rollingUpdateStatus));
    }
    builder.setFullBlockReportLeaseId(response.getFullBlockReportLeaseId());
    return builder.build();
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/PBHelper.java

  public static BlockReportContext convert(BlockReportContextProto proto) {
    return new BlockReportContext(proto.getTotalRpcs(),
        proto.getCurRpc(), proto.getId(), proto.getLeaseId());
  }

  public static BlockReportContextProto convert(BlockReportContext context) {
        setTotalRpcs(context.getTotalRpcs()).
        setCurRpc(context.getCurRpc()).
        setId(context.getReportId()).
        setLeaseId(context.getLeaseId()).
        build();
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
  private final AtomicLong excessBlocksCount = new AtomicLong(0L);
  private final AtomicLong postponedMisreplicatedBlocksCount = new AtomicLong(0L);
  private final long startupDelayBlockDeletionInMs;
  private final BlockReportLeaseManager blockReportLeaseManager;

  public long getPendingReplicationBlocksCount() {
    this.numBlocksPerIteration = conf.getInt(
        DFSConfigKeys.DFS_BLOCK_MISREPLICATION_PROCESSING_LIMIT,
        DFSConfigKeys.DFS_BLOCK_MISREPLICATION_PROCESSING_LIMIT_DEFAULT);
    this.blockReportLeaseManager = new BlockReportLeaseManager(conf);

    LOG.info("defaultReplication         = " + defaultReplication);
    LOG.info("maxReplication             = " + maxReplication);
    }
  }

  public long requestBlockReportLeaseId(DatanodeRegistration nodeReg) {
    assert namesystem.hasReadLock();
    DatanodeDescriptor node = null;
    try {
      node = datanodeManager.getDatanode(nodeReg);
    } catch (UnregisteredNodeException e) {
      LOG.warn("Unregistered datanode {}", nodeReg);
      return 0;
    }
    if (node == null) {
      LOG.warn("Failed to find datanode {}", nodeReg);
      return 0;
    }
    long leaseId = blockReportLeaseManager.requestLease(node);
    BlockManagerFaultInjector.getInstance().
        requestBlockReportLease(node, leaseId);
    return leaseId;
  }

            + " because namenode still in startup phase", nodeID);
        return !node.hasStaleStorages();
      }
      if (context != null) {
        if (!blockReportLeaseManager.checkLease(node, startTime,
              context.getLeaseId())) {
          return false;
        }
      }

      if (storageInfo.getBlockReportCount() == 0) {
        if (lastStorageInRpc) {
          int rpcsSeen = node.updateBlockReportContext(context);
          if (rpcsSeen >= context.getTotalRpcs()) {
            long leaseId = blockReportLeaseManager.removeLease(node);
            BlockManagerFaultInjector.getInstance().
                removeBlockReportLease(node, leaseId);
            List<DatanodeStorageInfo> zombies = node.removeZombieStorages();
            if (zombies.isEmpty()) {
              LOG.debug("processReport 0x{}: no zombie storages found.",
    clearQueues();
    blocksMap.clear();
  }

  public BlockReportLeaseManager getBlockReportLeaseManager() {
    return blockReportLeaseManager;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManagerFaultInjector.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManagerFaultInjector.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;

public class BlockManagerFaultInjector {
  @VisibleForTesting
  public static BlockManagerFaultInjector instance =
      new BlockManagerFaultInjector();

  @VisibleForTesting
  public static BlockManagerFaultInjector getInstance() {
    return instance;
  }

  @VisibleForTesting
  public void incomingBlockReportRpc(DatanodeID nodeID,
          BlockReportContext context) throws IOException {

  }

  @VisibleForTesting
  public void requestBlockReportLease(DatanodeDescriptor node, long leaseId) {
  }

  @VisibleForTesting
  public void removeBlockReportLease(DatanodeDescriptor node, long leaseId) {
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockReportLeaseManager.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockReportLeaseManager.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

class BlockReportLeaseManager {
  static final Logger LOG =
      LoggerFactory.getLogger(BlockReportLeaseManager.class);

  private static class NodeData {
    final String datanodeUuid;

    long leaseId;

    long leaseTimeMs;

    NodeData prev;

    NodeData next;

    static NodeData ListHead(String name) {
      NodeData node = new NodeData(name);
      node.next = node;
      node.prev = node;
      return node;
    }

    NodeData(String datanodeUuid) {
      this.datanodeUuid = datanodeUuid;
    }

    void removeSelf() {
      if (this.prev != null) {
        this.prev.next = this.next;
      }
      if (this.next != null) {
        this.next.prev = this.prev;
      }
      this.next = null;
      this.prev = null;
    }

    void addToEnd(NodeData node) {
      Preconditions.checkState(node.next == null);
      Preconditions.checkState(node.prev == null);
      node.prev = this.prev;
      node.next = this;
      this.prev.next = node;
      this.prev = node;
    }

    void addToBeginning(NodeData node) {
      Preconditions.checkState(node.next == null);
      Preconditions.checkState(node.prev == null);
      node.next = this.next;
      node.prev = this;
      this.next.prev = node;
      this.next = node;
    }
  }

  private final NodeData deferredHead = NodeData.ListHead("deferredHead");

  private final NodeData pendingHead = NodeData.ListHead("pendingHead");

  private final HashMap<String, NodeData> nodes = new HashMap<>();

  private int numPending = 0;

  private final int maxPending;

  private final long leaseExpiryMs;

  private long nextId = ThreadLocalRandom.current().nextLong();

  BlockReportLeaseManager(Configuration conf) {
    this(conf.getInt(
          DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES,
          DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES_DEFAULT),
        conf.getLong(
          DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
          DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT));
  }

  BlockReportLeaseManager(int maxPending, long leaseExpiryMs) {
    Preconditions.checkArgument(maxPending >= 1,
        "Cannot set the maximum number of block report leases to a " +
            "value less than 1.");
    this.maxPending = maxPending;
    Preconditions.checkArgument(leaseExpiryMs >= 1,
        "Cannot set full block report lease expiry period to a value " +
         "less than 1.");
    this.leaseExpiryMs = leaseExpiryMs;
  }

  private synchronized long getNextId() {
    long id;
    do {
      id = nextId++;
    } while (id == 0);
    return id;
  }

  public synchronized void register(DatanodeDescriptor dn) {
    registerNode(dn);
  }

  private synchronized NodeData registerNode(DatanodeDescriptor dn) {
    if (nodes.containsKey(dn.getDatanodeUuid())) {
      LOG.info("Can't register DN {} because it is already registered.",
          dn.getDatanodeUuid());
      return null;
    }
    NodeData node = new NodeData(dn.getDatanodeUuid());
    deferredHead.addToBeginning(node);
    nodes.put(dn.getDatanodeUuid(), node);
    LOG.info("Registered DN {} ({}).", dn.getDatanodeUuid(), dn.getXferAddr());
    return node;
  }

  private synchronized void remove(NodeData node) {
    if (node.leaseId != 0) {
      numPending--;
      node.leaseId = 0;
      node.leaseTimeMs = 0;
    }
    node.removeSelf();
  }

  public synchronized void unregister(DatanodeDescriptor dn) {
    NodeData node = nodes.remove(dn.getDatanodeUuid());
    if (node == null) {
      LOG.info("Can't unregister DN {} because it is not currently " +
          "registered.", dn.getDatanodeUuid());
      return;
    }
    remove(node);
  }

  public synchronized long requestLease(DatanodeDescriptor dn) {
    NodeData node = nodes.get(dn.getDatanodeUuid());
    if (node == null) {
      LOG.warn("DN {} ({}) requested a lease even though it wasn't yet " +
          "registered.  Registering now.", dn.getDatanodeUuid(),
          dn.getXferAddr());
      node = registerNode(dn);
    }
    if (node.leaseId != 0) {
      LOG.debug("Removing existing BR lease 0x{} for DN {} in order to " +
               "issue a new one.", Long.toHexString(node.leaseId),
               dn.getDatanodeUuid());
    }
    remove(node);
    long monotonicNowMs = Time.monotonicNow();
    pruneExpiredPending(monotonicNowMs);
    if (numPending >= maxPending) {
      if (LOG.isDebugEnabled()) {
        StringBuilder allLeases = new StringBuilder();
        String prefix = "";
        for (NodeData cur = pendingHead.next; cur != pendingHead;
             cur = cur.next) {
          allLeases.append(prefix).append(cur.datanodeUuid);
          prefix = ", ";
        }
        LOG.debug("Can't create a new BR lease for DN {}, because " +
              "numPending equals maxPending at {}.  Current leases: {}",
              dn.getDatanodeUuid(), numPending, allLeases.toString());
      }
      return 0;
    }
    numPending++;
    node.leaseId = getNextId();
    node.leaseTimeMs = monotonicNowMs;
    pendingHead.addToEnd(node);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created a new BR lease 0x{} for DN {}.  numPending = {}",
          Long.toHexString(node.leaseId), dn.getDatanodeUuid(), numPending);
    }
    return node.leaseId;
  }

  private synchronized boolean pruneIfExpired(long monotonicNowMs,
                                              NodeData node) {
    if (monotonicNowMs < node.leaseTimeMs + leaseExpiryMs) {
      return false;
    }
    LOG.info("Removing expired block report lease 0x{} for DN {}.",
        Long.toHexString(node.leaseId), node.datanodeUuid);
    Preconditions.checkState(node.leaseId != 0);
    remove(node);
    deferredHead.addToBeginning(node);
    return true;
  }

  private synchronized void pruneExpiredPending(long monotonicNowMs) {
    NodeData cur = pendingHead.next;
    while (cur != pendingHead) {
      NodeData next = cur.next;
      if (!pruneIfExpired(monotonicNowMs, cur)) {
        return;
      }
      cur = next;
    }
    LOG.trace("No entries remaining in the pending list.");
  }

  public synchronized boolean checkLease(DatanodeDescriptor dn,
                                         long monotonicNowMs, long id) {
    if (id == 0) {
      LOG.debug("Datanode {} is using BR lease id 0x0 to bypass " +
          "rate-limiting.", dn.getDatanodeUuid());
      return true;
    }
    NodeData node = nodes.get(dn.getDatanodeUuid());
    if (node == null) {
      LOG.info("BR lease 0x{} is not valid for unknown datanode {}",
          Long.toHexString(id), dn.getDatanodeUuid());
      return false;
    }
    if (node.leaseId == 0) {
      LOG.warn("BR lease 0x{} is not valid for DN {}, because the DN " +
               "is not in the pending set.",
               Long.toHexString(id), dn.getDatanodeUuid());
      return false;
    }
    if (pruneIfExpired(monotonicNowMs, node)) {
      LOG.warn("BR lease 0x{} is not valid for DN {}, because the lease " +
               "has expired.", Long.toHexString(id), dn.getDatanodeUuid());
      return false;
    }
    if (id != node.leaseId) {
      LOG.warn("BR lease 0x{} is not valid for DN {}.  Expected BR lease 0x{}.",
          Long.toHexString(id), dn.getDatanodeUuid(),
          Long.toHexString(node.leaseId));
      return false;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("BR lease 0x{} is valid for DN {}.",
          Long.toHexString(id), dn.getDatanodeUuid());
    }
    return true;
  }

  public synchronized long removeLease(DatanodeDescriptor dn) {
    NodeData node = nodes.get(dn.getDatanodeUuid());
    if (node == null) {
      LOG.info("Can't remove lease for unknown datanode {}",
               dn.getDatanodeUuid());
      return 0;
    }
    long id = node.leaseId;
    if (id == 0) {
      LOG.debug("DN {} has no lease to remove.", dn.getDatanodeUuid());
      return 0;
    }
    remove(node);
    deferredHead.addToEnd(node);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Removed BR lease 0x{} for DN {}.  numPending = {}",
                Long.toHexString(id), dn.getDatanodeUuid(), numPending);
    }
    return id;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DatanodeManager.java
    blockManager.removeBlocksAssociatedTo(nodeInfo);
    networktopology.remove(nodeInfo);
    decrementVersionCount(nodeInfo.getSoftwareVersion());
    blockManager.getBlockReportLeaseManager().unregister(nodeInfo);

    if (LOG.isDebugEnabled()) {
      LOG.debug("remove datanode " + nodeInfo);
    networktopology.add(node); // may throw InvalidTopologyException
    host2DatanodeMap.add(node);
    checkIfClusterIsNowMultiRack(node);
    blockManager.getBlockReportLeaseManager().register(node);

    if (LOG.isDebugEnabled()) {
      LOG.debug(getClass().getSimpleName() + ".addDatanode: "

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BPServiceActor.java
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Joiner;
import org.apache.commons.logging.Log;
  void triggerBlockReportForTests() {
    synchronized (pendingIncrementalBRperStorage) {
      scheduler.scheduleHeartbeat();
      long oldBlockReportTime = scheduler.nextBlockReportTime;
      scheduler.forceFullBlockReportNow();
      pendingIncrementalBRperStorage.notifyAll();
      while (oldBlockReportTime == scheduler.nextBlockReportTime) {
        try {
          pendingIncrementalBRperStorage.wait(100);
        } catch (InterruptedException e) {
  List<DatanodeCommand> blockReport(long fullBrLeaseId) throws IOException {
    final ArrayList<DatanodeCommand> cmds = new ArrayList<DatanodeCommand>();

        DatanodeCommand cmd = bpNamenode.blockReport(
            bpRegistration, bpos.getBlockPoolId(), reports,
              new BlockReportContext(1, 0, reportId, fullBrLeaseId));
        numRPCs = 1;
        numReportsSent = reports.length;
        if (cmd != null) {
          StorageBlockReport singleReport[] = { reports[r] };
          DatanodeCommand cmd = bpNamenode.blockReport(
              bpRegistration, bpos.getBlockPoolId(), singleReport,
              new BlockReportContext(reports.length, r, reportId,
                  fullBrLeaseId));
          numReportsSent++;
          numRPCs++;
          if (cmd != null) {
    return cmd;
  }
  
  HeartbeatResponse sendHeartBeat(boolean requestBlockReportLease)
      throws IOException {
    StorageReport[] reports =
        dn.getFSDataset().getStorageReports(bpos.getBlockPoolId());
    if (LOG.isDebugEnabled()) {
        dn.getXmitsInProgress(),
        dn.getXceiverCount(),
        numFailedVolumes,
        volumeFailureSummary,
        requestBlockReportLease);
  }
  
    LOG.info("For namenode " + nnAddr + " using"
        + " BLOCKREPORT_INTERVAL of " + dnConf.blockReportInterval + "msec"
        + " CACHEREPORT_INTERVAL of " + dnConf.cacheReportInterval + "msec"
        + " Initial delay: " + dnConf.initialBlockReportDelayMs + "msec"
        + "; heartBeatInterval=" + dnConf.heartBeatInterval);
    long fullBlockReportLeaseId = 0;

        final boolean sendHeartbeat = scheduler.isHeartbeatDue(startTime);
        HeartbeatResponse resp = null;
        if (sendHeartbeat) {
          boolean requestBlockReportLease = (fullBlockReportLeaseId == 0) &&
                  scheduler.isBlockReportDue(startTime);
          scheduler.scheduleNextHeartbeat();
          if (!dn.areHeartbeatsDisabledForTests()) {
            resp = sendHeartBeat(requestBlockReportLease);
            assert resp != null;
            if (resp.getFullBlockReportLeaseId() != 0) {
              if (fullBlockReportLeaseId != 0) {
                LOG.warn(nnAddr + " sent back a full block report lease " +
                        "ID of 0x" +
                        Long.toHexString(resp.getFullBlockReportLeaseId()) +
                        ", but we already have a lease ID of 0x" +
                        Long.toHexString(fullBlockReportLeaseId) + ". " +
                        "Overwriting old lease ID.");
              }
              fullBlockReportLeaseId = resp.getFullBlockReportLeaseId();
            }
            dn.getMetrics().addHeartbeat(scheduler.monotonicNow() - startTime);

          reportReceivedDeletedBlocks();
        }

        List<DatanodeCommand> cmds = null;
        boolean forceFullBr =
            scheduler.forceFullBlockReport.getAndSet(false);
        if (forceFullBr) {
          LOG.info("Forcing a full block report to " + nnAddr);
        }
        if ((fullBlockReportLeaseId != 0) || forceFullBr) {
          cmds = blockReport(fullBlockReportLeaseId);
          fullBlockReportLeaseId = 0;
        }
        processCommand(cmds == null ? null : cmds.toArray(new DatanodeCommand[cmds.size()]));

        DatanodeCommand cmd = cacheReport();
    bpos.registrationSucceeded(this, bpRegistration);

    scheduler.scheduleBlockReport(dnConf.initialBlockReportDelayMs);
  }


    } else {
      LOG.info(bpos.toString() + ": scheduling a full block report.");
      synchronized(pendingIncrementalBRperStorage) {
        scheduler.forceFullBlockReportNow();
        pendingIncrementalBRperStorage.notifyAll();
      }
    }
    @VisibleForTesting
    boolean resetBlockReportTime = true;

    private final AtomicBoolean forceFullBlockReport =
        new AtomicBoolean(false);

    private final long heartbeatIntervalMs;
    private final long blockReportIntervalMs;

      return (nextHeartbeatTime - startTime <= 0);
    }

    boolean isBlockReportDue(long curTime) {
      return nextBlockReportTime - curTime <= 0;
    }

    void forceFullBlockReportNow() {
      forceFullBlockReport.set(true);
      resetBlockReportTime = true;
    }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DNConf.java
  final long heartBeatInterval;
  final long blockReportInterval;
  final long blockReportSplitThreshold;
  final long initialBlockReportDelayMs;
  final long cacheReportInterval;
  final long dfsclientSlowIoWarningThresholdMs;
  final long datanodeSlowIoWarningThresholdMs;
          + "greater than or equal to" + "dfs.blockreport.intervalMsec."
          + " Setting initial delay to 0 msec:");
    }
    initialBlockReportDelayMs = initBRDelay;
    
    heartBeatInterval = conf.getLong(DFS_HEARTBEAT_INTERVAL_KEY,
        DFS_HEARTBEAT_INTERVAL_DEFAULT) * 1000L;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
  HeartbeatResponse handleHeartbeat(DatanodeRegistration nodeReg,
      StorageReport[] reports, long cacheCapacity, long cacheUsed,
      int xceiverCount, int xmitsInProgress, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary,
      boolean requestFullBlockReportLease) throws IOException {
    readLock();
    try {
      DatanodeCommand[] cmds = blockManager.getDatanodeManager().handleHeartbeat(
          nodeReg, reports, blockPoolId, cacheCapacity, cacheUsed,
          xceiverCount, maxTransfer, failedVolumes, volumeFailureSummary);
      long blockReportLeaseId = 0;
      if (requestFullBlockReportLease) {
        blockReportLeaseId =  blockManager.requestBlockReportLeaseId(nodeReg);
      }
      final NNHAStatusHeartbeat haState = new NNHAStatusHeartbeat(
          haContext.getState().getServiceState(),
          getFSImage().getLastAppliedOrWrittenTxId());

      return new HeartbeatResponse(cmds, haState, rollingUpgradeInfo,
          blockReportLeaseId);
    } finally {
      readUnlock();
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNodeRpcServer.java
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerFaultInjector;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
  public HeartbeatResponse sendHeartbeat(DatanodeRegistration nodeReg,
      StorageReport[] report, long dnCacheCapacity, long dnCacheUsed,
      int xmitsInProgress, int xceiverCount,
      int failedVolumes, VolumeFailureSummary volumeFailureSummary,
      boolean requestFullBlockReportLease) throws IOException {
    checkNNStartup();
    verifyRequest(nodeReg);
    return namesystem.handleHeartbeat(nodeReg, report,
        dnCacheCapacity, dnCacheUsed, xceiverCount, xmitsInProgress,
        failedVolumes, volumeFailureSummary, requestFullBlockReportLease);
  }

  @Override // DatanodeProtocol
          blocks, context, (r == reports.length - 1));
      metrics.incrStorageBlockReportOps();
    }
    BlockManagerFaultInjector.getInstance().
        incomingBlockReportRpc(nodeReg, context);

    if (nn.getFSImage().isUpgradeFinalized() &&
        !namesystem.isRollingUpgrade() &&

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/protocol/BlockReportContext.java
@InterfaceAudience.Private
public class BlockReportContext {
  private final int totalRpcs;

  private final int curRpc;

  private final long reportId;

  private final long leaseId;

  public BlockReportContext(int totalRpcs, int curRpc,
                            long reportId, long leaseId) {
    this.totalRpcs = totalRpcs;
    this.curRpc = curRpc;
    this.reportId = reportId;
    this.leaseId = leaseId;
  }

  public int getTotalRpcs() {
  public long getReportId() {
    return reportId;
  }

  public long getLeaseId() {
    return leaseId;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/protocol/DatanodeProtocol.java
  @Idempotent
                                       int xmitsInProgress,
                                       int xceiverCount,
                                       int failedVolumes,
                                       VolumeFailureSummary volumeFailureSummary,
                                       boolean requestFullBlockReportLease)
      throws IOException;


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/protocol/HeartbeatResponse.java

  private final RollingUpgradeStatus rollingUpdateStatus;

  private final long fullBlockReportLeaseId;
  
  public HeartbeatResponse(DatanodeCommand[] cmds,
      NNHAStatusHeartbeat haStatus, RollingUpgradeStatus rollingUpdateStatus,
      long fullBlockReportLeaseId) {
    commands = cmds;
    this.haStatus = haStatus;
    this.rollingUpdateStatus = rollingUpdateStatus;
    this.fullBlockReportLeaseId = fullBlockReportLeaseId;
  }
  
  public DatanodeCommand[] getCommands() {
  public RollingUpgradeStatus getRollingUpdateStatus() {
    return rollingUpdateStatus;
  }

  public long getFullBlockReportLeaseId() {
    return fullBlockReportLeaseId;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/protocol/RegisterCommand.java
import org.apache.hadoop.classification.InterfaceStability;


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/protocol/TestBlockListAsLongs.java
    request.set(null);
    nsInfo.setCapabilities(Capability.STORAGE_BLOCK_REPORT_BUFFERS.getMask());
    nn.blockReport(reg, "pool", sbr,
        new BlockReportContext(1, 0, System.nanoTime(), 0L));
    BlockReportRequestProto proto = request.get();
    assertNotNull(proto);
    assertTrue(proto.getReports(0).getBlocksList().isEmpty());
    request.set(null);
    nsInfo.setCapabilities(Capability.UNKNOWN.getMask());
    nn.blockReport(reg, "pool", sbr,
        new BlockReportContext(1, 0, System.nanoTime(), 0L));
    proto = request.get();
    assertNotNull(proto);
    assertFalse(proto.getReports(0).getBlocksList().isEmpty());

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockReportRateLimiting.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockReportRateLimiting.java

package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS;

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang.mutable.MutableObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TestBlockReportRateLimiting {
  static final Log LOG = LogFactory.getLog(TestBlockReportRateLimiting.class);

  private static void setFailure(AtomicReference<String> failure,
                                 String what) {
    failure.compareAndSet("", what);
    LOG.error("Test error: " + what);
  }

  @After
  public void restoreNormalBlockManagerFaultInjector() {
    BlockManagerFaultInjector.instance = new BlockManagerFaultInjector();
  }

  @BeforeClass
  public static void raiseBlockManagerLogLevels() {
    GenericTestUtils.setLogLevel(BlockManager.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(BlockReportLeaseManager.LOG, Level.ALL);
  }

  @Test(timeout=180000)
  public void testRateLimitingDuringDataNodeStartup() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES, 1);
    conf.setLong(DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
        20L * 60L * 1000L);

    final Semaphore fbrSem = new Semaphore(0);
    final HashSet<DatanodeID> expectedFbrDns = new HashSet<>();
    final HashSet<DatanodeID> fbrDns = new HashSet<>();
    final AtomicReference<String> failure = new AtomicReference<String>("");

    final BlockManagerFaultInjector injector = new BlockManagerFaultInjector() {
      private int numLeases = 0;

      @Override
      public void incomingBlockReportRpc(DatanodeID nodeID,
                    BlockReportContext context) throws IOException {
        LOG.info("Incoming full block report from " + nodeID +
            ".  Lease ID = 0x" + Long.toHexString(context.getLeaseId()));
        if (context.getLeaseId() == 0) {
          setFailure(failure, "Got unexpected rate-limiting-" +
              "bypassing full block report RPC from " + nodeID);
        }
        fbrSem.acquireUninterruptibly();
        synchronized (this) {
          fbrDns.add(nodeID);
          if (!expectedFbrDns.remove(nodeID)) {
            setFailure(failure, "Got unexpected full block report " +
                "RPC from " + nodeID + ".  expectedFbrDns = " +
                Joiner.on(", ").join(expectedFbrDns));
          }
          LOG.info("Proceeding with full block report from " +
              nodeID + ".  Lease ID = 0x" +
              Long.toHexString(context.getLeaseId()));
        }
      }

      @Override
      public void requestBlockReportLease(DatanodeDescriptor node,
                                          long leaseId) {
        if (leaseId == 0) {
          return;
        }
        synchronized (this) {
          numLeases++;
          expectedFbrDns.add(node);
          LOG.info("requestBlockReportLease(node=" + node +
              ", leaseId=0x" + Long.toHexString(leaseId) + ").  " +
              "expectedFbrDns = " +  Joiner.on(", ").join(expectedFbrDns));
          if (numLeases > 1) {
            setFailure(failure, "More than 1 lease was issued at once.");
          }
        }
      }

      @Override
      public void removeBlockReportLease(DatanodeDescriptor node, long leaseId) {
        LOG.info("removeBlockReportLease(node=" + node +
                 ", leaseId=0x" + Long.toHexString(leaseId) + ")");
        synchronized (this) {
          numLeases--;
        }
      }
    };
    BlockManagerFaultInjector.instance = injector;

    final int NUM_DATANODES = 5;
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES).build();
    cluster.waitActive();
    for (int n = 1; n <= NUM_DATANODES; n++) {
      LOG.info("Waiting for " + n + " datanode(s) to report in.");
      fbrSem.release();
      Uninterruptibles.sleepUninterruptibly(20, TimeUnit.MILLISECONDS);
      final int currentN = n;
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          synchronized (injector) {
            if (fbrDns.size() > currentN) {
              setFailure(failure, "Expected at most " + currentN +
                  " datanodes to have sent a block report, but actually " +
                  fbrDns.size() + " have.");
            }
            return (fbrDns.size() >= currentN);
          }
        }
      }, 25, 50000);
    }
    cluster.shutdown();
    Assert.assertEquals("", failure.get());
  }

  @Test(timeout=180000)
  public void testLeaseExpiration() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES, 1);
    conf.setLong(DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS, 100L);

    final Semaphore gotFbrSem = new Semaphore(0);
    final AtomicReference<String> failure = new AtomicReference<String>("");
    final AtomicReference<MiniDFSCluster> cluster =
        new AtomicReference<>(null);
    final BlockingQueue<Integer> datanodeToStop =
        new ArrayBlockingQueue<Integer>(1);
    final BlockManagerFaultInjector injector = new BlockManagerFaultInjector() {
      private String uuidToStop = "";

      @Override
      public void incomingBlockReportRpc(DatanodeID nodeID,
                BlockReportContext context) throws IOException {
        if (context.getLeaseId() == 0) {
          setFailure(failure, "Got unexpected rate-limiting-" +
              "bypassing full block report RPC from " + nodeID);
        }
        synchronized (this) {
          if (uuidToStop.equals(nodeID.getDatanodeUuid())) {
            throw new IOException("Injecting failure into block " +
                "report RPC for " + nodeID);
          }
        }
        gotFbrSem.release();
      }

      @Override
      public void requestBlockReportLease(DatanodeDescriptor node,
                                          long leaseId) {
        if (leaseId == 0) {
          return;
        }
        synchronized (this) {
          if (uuidToStop.isEmpty()) {
            MiniDFSCluster cl;
            do {
              cl = cluster.get();
            } while (cl == null);
            int datanodeIndexToStop = getDatanodeIndex(cl, node);
            uuidToStop = node.getDatanodeUuid();
            datanodeToStop.add(Integer.valueOf(datanodeIndexToStop));
          }
        }
      }

      private int getDatanodeIndex(MiniDFSCluster cl,
                                   DatanodeDescriptor node) {
        List<DataNode> datanodes = cl.getDataNodes();
        for (int i = 0; i < datanodes.size(); i++) {
          DataNode datanode = datanodes.get(i);
          if (datanode.getDatanodeUuid().equals(node.getDatanodeUuid())) {
            return i;
          }
        }
        throw new RuntimeException("Failed to find UUID " +
            node.getDatanodeUuid() + " in the list of datanodes.");
      }

      @Override
      public void removeBlockReportLease(DatanodeDescriptor node, long leaseId) {
      }
    };
    BlockManagerFaultInjector.instance = injector;
    cluster.set(new MiniDFSCluster.Builder(conf).numDataNodes(2).build());
    cluster.get().waitActive();
    int datanodeIndexToStop = datanodeToStop.take();
    cluster.get().stopDataNode(datanodeIndexToStop);
    gotFbrSem.acquire();
    cluster.get().shutdown();
    Assert.assertEquals("", failure.get());
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestDatanodeManager.java
  final int NUM_ITERATIONS = 500;

  private static DatanodeManager mockDatanodeManager(
      FSNamesystem fsn, Configuration conf) throws IOException {
    BlockManager bm = Mockito.mock(BlockManager.class);
    BlockReportLeaseManager blm = new BlockReportLeaseManager(conf);
    Mockito.when(bm.getBlockReportLeaseManager()).thenReturn(blm);
    DatanodeManager dm = new DatanodeManager(bm, fsn, conf);
    return dm;
  }

    FSNamesystem fsn = Mockito.mock(FSNamesystem.class);
    Mockito.when(fsn.hasWriteLock()).thenReturn(true);
    DatanodeManager dm = mockDatanodeManager(fsn, new Configuration());

    Random rng = new Random();
        TestDatanodeManager.MyResolver.class, DNSToSwitchMapping.class);
    
    DatanodeManager dm = mockDatanodeManager(fsn, conf);

    String storageID = "someStorageID-123";
      conf.set(DFSConfigKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
        resourcePath.toString());
    }
    DatanodeManager dm = mockDatanodeManager(fsn, conf);

    DatanodeInfo[] locs = new DatanodeInfo[5];

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestNameNodePrunesMissingStorages.java
      cluster.stopDataNode(0);
      cluster.getNameNodeRpc().sendHeartbeat(dnReg, prunedReports, 0L, 0L, 0, 0,
          0, null, true);

      assertThat(dnDescriptor.getStorageInfos().length, is(expectedStoragesAfterTest));

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestBPOfferService.java
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
          Mockito.anyInt(),
          Mockito.anyInt(),
          Mockito.anyInt(),
          Mockito.any(VolumeFailureSummary.class),
          Mockito.anyBoolean());
    mockHaStatuses[nnIdx] = new NNHAStatusHeartbeat(HAServiceState.STANDBY, 0);
    return mock;
  }
    public HeartbeatResponse answer(InvocationOnMock invocation) throws Throwable {
      heartbeatCounts[nnIdx]++;
      return new HeartbeatResponse(new DatanodeCommand[0],
          mockHaStatuses[nnIdx], null,
          ThreadLocalRandom.current().nextLong() | 1L);
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestBlockHasMultipleReplicasOnSameDN.java

    cluster.getNameNodeRpc().blockReport(dnReg, bpid, reports,
        new BlockReportContext(1, 0, System.nanoTime(), 0L));

    locatedBlocks = client.getLocatedBlocks(filename, 0, BLOCK_SIZE * NUM_BLOCKS);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestBlockRecovery.java
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.any(VolumeFailureSummary.class),
            Mockito.anyBoolean()))
        .thenReturn(new HeartbeatResponse(
            new DatanodeCommand[0],
            new NNHAStatusHeartbeat(HAServiceState.ACTIVE, 1),
            null, ThreadLocalRandom.current().nextLong() | 1L));

    dn = new DataNode(conf, locations, null) {
      @Override

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestBpServiceActorScheduler.java
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      assertTrue(scheduler.isHeartbeatDue(now));
      assertTrue(scheduler.isBlockReportDue(scheduler.monotonicNow()));
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDatanodeProtocolRetryPolicy.java
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.base.Supplier;
import org.apache.commons.logging.Log;
          heartbeatResponse = new HeartbeatResponse(
              new DatanodeCommand[]{RegisterCommand.REGISTER},
              new NNHAStatusHeartbeat(HAServiceState.ACTIVE, 1),
              null, ThreadLocalRandom.current().nextLong() | 1L);
        } else {
          LOG.info("mockito heartbeatResponse " + i);
          heartbeatResponse = new HeartbeatResponse(
              new DatanodeCommand[0],
              new NNHAStatusHeartbeat(HAServiceState.ACTIVE, 1),
              null, ThreadLocalRandom.current().nextLong() | 1L);
        }
        return heartbeatResponse;
      }
           Mockito.anyInt(),
           Mockito.anyInt(),
           Mockito.anyInt(),
           Mockito.any(VolumeFailureSummary.class),
           Mockito.anyBoolean());

    dn = new DataNode(conf, locations, null) {
      @Override

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestFsDatasetCache.java
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
      throws IOException {
    NNHAStatusHeartbeat ha = new NNHAStatusHeartbeat(HAServiceState.ACTIVE,
        fsImage.getLastAppliedOrWrittenTxId());
    HeartbeatResponse response =
        new HeartbeatResponse(cmds, ha, null,
            ThreadLocalRandom.current().nextLong() | 1L);
    doReturn(response).when(spyNN).sendHeartbeat(
        (DatanodeRegistration) any(),
        (StorageReport[]) any(), anyLong(), anyLong(),
        anyInt(), anyInt(), anyInt(), (VolumeFailureSummary) any(),
        anyBoolean());
  }

  private static DatanodeCommand[] cacheBlock(HdfsBlockLocation loc) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestNNHandlesBlockReportPerStorage.java
      LOG.info("Sending block report for storage " + report.getStorage().getStorageID());
      StorageBlockReport[] singletonReport = { report };
      cluster.getNameNodeRpc().blockReport(dnR, poolId, singletonReport,
          new BlockReportContext(reports.length, i, System.nanoTime(), 0L));
      i++;
    }
  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestNNHandlesCombinedBlockReport.java
                                  StorageBlockReport[] reports) throws IOException {
    LOG.info("Sending combined block reports for " + dnR);
    cluster.getNameNodeRpc().blockReport(dnR, poolId, reports,
        new BlockReportContext(1, 0, System.nanoTime(), 0L));
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestStorageReport.java
        any(DatanodeRegistration.class),
        captor.capture(),
        anyLong(), anyLong(), anyInt(), anyInt(), anyInt(),
        Mockito.any(VolumeFailureSummary.class), Mockito.anyBoolean());

    StorageReport[] reports = captor.getValue();


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/NNThroughputBenchmark.java
          new StorageBlockReport(storage, BlockListAsLongs.EMPTY)
      };
      dataNodeProto.blockReport(dnRegistration, bpid, reports,
              new BlockReportContext(1, 0, System.nanoTime(), 0L));
    }

      StorageReport[] rep = { new StorageReport(storage, false,
          DF_CAPACITY, DF_USED, DF_CAPACITY - DF_USED, DF_USED) };
      DatanodeCommand[] cmds = dataNodeProto.sendHeartbeat(dnRegistration, rep,
          0L, 0L, 0, 0, 0, null, true).getCommands();
      if(cmds != null) {
        for (DatanodeCommand cmd : cmds ) {
          if(LOG.isDebugEnabled()) {
      StorageReport[] rep = { new StorageReport(storage,
          false, DF_CAPACITY, DF_USED, DF_CAPACITY - DF_USED, DF_USED) };
      DatanodeCommand[] cmds = dataNodeProto.sendHeartbeat(dnRegistration,
          rep, 0L, 0L, 0, 0, 0, null, true).getCommands();
      if (cmds != null) {
        for (DatanodeCommand cmd : cmds) {
          if (cmd.getAction() == DatanodeProtocol.DNA_TRANSFER) {
      StorageBlockReport[] report = { new StorageBlockReport(
          dn.storage, dn.getBlockReportList()) };
      dataNodeProto.blockReport(dn.dnRegistration, bpid, report,
          new BlockReportContext(1, 0, System.nanoTime(), 0L));
      long end = Time.now();
      return end-start;
    }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/NameNodeAdapter.java
      DatanodeDescriptor dd, FSNamesystem namesystem) throws IOException {
    return namesystem.handleHeartbeat(nodeReg,
        BlockManagerTestUtil.getStorageReportsForDatanode(dd),
        dd.getCacheCapacity(), dd.getCacheRemaining(), 0, 0, 0, null, true);
  }

  public static boolean setReplication(final FSNamesystem ns,

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestDeadDatanode.java
        BlockListAsLongs.EMPTY) };
    try {
      dnp.blockReport(reg, poolId, report,
          new BlockReportContext(1, 0, System.nanoTime(), 0L));
      fail("Expected IOException is not thrown");
    } catch (IOException ex) {
    StorageReport[] rep = { new StorageReport(
        new DatanodeStorage(reg.getDatanodeUuid()),
        false, 0, 0, 0, 0) };
    DatanodeCommand[] cmd =
        dnp.sendHeartbeat(reg, rep, 0L, 0L, 0, 0, 0, null, true).getCommands();
    assertEquals(1, cmd.length);
    assertEquals(cmd[0].getAction(), RegisterCommand.REGISTER
        .getAction());

