hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirDeleteOp.java
  static long delete(
      FSDirectory fsd, INodesInPath iip, BlocksMapUpdateInfo collectedBlocks,
      List<INode> removedINodes, List<Long> removedUCFiles,
      long mtime) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: " + iip.getPath());
    }
        List<INodeDirectory> snapshottableDirs = new ArrayList<>();
        FSDirSnapshotOp.checkSnapshot(iip.getLastINode(), snapshottableDirs);
        filesRemoved = unprotectedDelete(fsd, iip, collectedBlocks,
                                         removedINodes, removedUCFiles, mtime);
        fsd.getFSNamesystem().removeSnapshottableDirs(snapshottableDirs);
      }
    } finally {
    FSNamesystem fsn = fsd.getFSNamesystem();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    List<INode> removedINodes = new ChunkedArrayList<>();
    List<Long> removedUCFiles = new ChunkedArrayList<>();

    final INodesInPath iip = fsd.getINodesInPath4Write(
        FSDirectory.normalizePath(src), false);
    List<INodeDirectory> snapshottableDirs = new ArrayList<>();
    FSDirSnapshotOp.checkSnapshot(iip.getLastINode(), snapshottableDirs);
    long filesRemoved = unprotectedDelete(
        fsd, iip, collectedBlocks, removedINodes, removedUCFiles, mtime);
    fsn.removeSnapshottableDirs(snapshottableDirs);

    if (filesRemoved >= 0) {
      fsn.removeLeasesAndINodes(removedUCFiles, removedINodes, false);
      fsn.removeBlocksAndUpdateSafemodeTotal(collectedBlocks);
    }
  }
    FSDirectory fsd = fsn.getFSDirectory();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    List<INode> removedINodes = new ChunkedArrayList<>();
    List<Long> removedUCFiles = new ChunkedArrayList<>();

    long mtime = now();
    long filesRemoved = delete(
        fsd, iip, collectedBlocks, removedINodes, removedUCFiles, mtime);
    if (filesRemoved < 0) {
      return null;
    }
    fsd.getEditLog().logDelete(src, mtime, logRetryCache);
    incrDeletedFileCount(filesRemoved);

    fsn.removeLeasesAndINodes(removedUCFiles, removedINodes, true);

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* Namesystem.delete: "
  private static long unprotectedDelete(
      FSDirectory fsd, INodesInPath iip, BlocksMapUpdateInfo collectedBlocks,
      List<INode> removedINodes, List<Long> removedUCFiles, long mtime) {
    assert fsd.hasWriteLock();

    if (!targetNode.isInLatestSnapshot(latestSnapshot)) {
      targetNode.destroyAndCollectBlocks(fsd.getBlockStoragePolicySuite(),
        collectedBlocks, removedINodes, removedUCFiles);
    } else {
      QuotaCounts counts = targetNode.cleanSubtree(
        fsd.getBlockStoragePolicySuite(), CURRENT_STATE_ID,
          latestSnapshot, collectedBlocks, removedINodes, removedUCFiles);
      removed = counts.getNameSpace();
      fsd.updateCountNoQuotaCheck(iip, iip.length() -1, counts.negation());
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirRenameOp.java
      srcParent.updateModificationTime(timestamp, srcIIP.getLatestSnapshotId());
      final INode dstParent = dstParentIIP.getLastINode();
      dstParent.updateModificationTime(timestamp, dstIIP.getLatestSnapshotId());
    }

    void restoreSource() throws QuotaExceededException {
        throws QuotaExceededException {
      Preconditions.checkState(oldDstChild != null);
      List<INode> removedINodes = new ChunkedArrayList<>();
      List<Long> removedUCFiles = new ChunkedArrayList<>();
      final boolean filesDeleted;
      if (!oldDstChild.isInLatestSnapshot(dstIIP.getLatestSnapshotId())) {
        oldDstChild.destroyAndCollectBlocks(bsps, collectedBlocks, removedINodes,
                                            removedUCFiles);
        filesDeleted = true;
      } else {
        filesDeleted = oldDstChild.cleanSubtree(
            bsps, Snapshot.CURRENT_STATE_ID,
            dstIIP.getLatestSnapshotId(), collectedBlocks,
            removedINodes, removedUCFiles).getNameSpace() >= 0;
      }
      fsd.getFSNamesystem().removeLeasesAndINodes(
          removedUCFiles, removedINodes, false);
      return filesDeleted;
    }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogLoader.java
import java.util.EnumSet;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
            addCloseOp.clientMachine,
            addCloseOp.storagePolicyId);
        iip = INodesInPath.replace(iip, iip.length() - 1, newFile);
        fsNamesys.leaseManager.addLease(addCloseOp.clientName, newFile.getId());

        if (toAddRetryCache) {
            "File is not under construction: " + path);
      }
      if (file.isUnderConstruction()) {
        fsNamesys.leaseManager.removeLeases(Lists.newArrayList(file.getId()));
        file.toCompleteFile(file.getModificationTime());
      }
      break;
          renameReservedPathsOnUpgrade(reassignLeaseOp.path, logVersion);
      INodeFile pendingFile = fsDir.getINode(path).asFile();
      Preconditions.checkState(pendingFile.isUnderConstruction());
      fsNamesys.reassignLeaseInternal(lease, reassignLeaseOp.newHolder,
              pendingFile);
      break;
    }
    case OP_START_LOG_SEGMENT:

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageFormat.java
        }

        if (!inSnapshot) {
          namesystem.leaseManager.addLease(uc.getClientName(), oldnode.getId());
        }
      }
    }
        saveFilesUnderConstruction(sourceNamesystem, out, snapshotUCMap);

        context.checkCancelled();
        sourceNamesystem.saveSecretManagerStateCompat(out, sdPath);
        counter.increment();
      }
    }

    void saveFilesUnderConstruction(FSNamesystem fsn, DataOutputStream out,
                                    Map<Long, INodeFile> snapshotUCMap) throws IOException {
      final LeaseManager leaseManager = fsn.getLeaseManager();
      final FSDirectory dir = fsn.getFSDirectory();
      synchronized (leaseManager) {
        Collection<Long> filesWithUC = leaseManager.getINodeIdWithLeases();
        for (Long id : filesWithUC) {
          snapshotUCMap.remove(id);
        }
        out.writeInt(filesWithUC.size() + snapshotUCMap.size()); // write the size

        for (Long id : filesWithUC) {
          INodeFile file = dir.getInode(id).asFile();
          String path = file.getFullPathName();
          FSImageSerialization.writeINodeUnderConstruction(
                  out, file, path);
        }

        for (Map.Entry<Long, INodeFile> entry : snapshotUCMap.entrySet()) {
          StringBuilder b = new StringBuilder();
          b.append(FSDirectory.DOT_RESERVED_PATH_PREFIX)
                  .append(Path.SEPARATOR).append(FSDirectory.DOT_INODES_STRING)
                  .append(Path.SEPARATOR).append(entry.getValue().getId());
          FSImageSerialization.writeINodeUnderConstruction(
                  out, entry.getValue(), b.toString());
        }
      }
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageFormatPBINode.java
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
        INodeFile file = dir.getInode(entry.getInodeId()).asFile();
        FileUnderConstructionFeature uc = file.getFileUnderConstructionFeature();
        Preconditions.checkState(uc != null); // file must be under-construction
        fsn.leaseManager.addLease(uc.getClientName(),
                entry.getInodeId());
      }
    }

    }

    void serializeFilesUCSection(OutputStream out) throws IOException {
      Collection<Long> filesWithUC = fsn.getLeaseManager()
              .getINodeIdWithLeases();
      for (Long id : filesWithUC) {
        INode inode = fsn.getFSDirectory().getInode(id);
        if (inode == null) {
          LOG.warn("Fail to find inode " + id + " when saving the leases.");
          continue;
        }
        INodeFile file = inode.asFile();
        if (!file.isUnderConstruction()) {
          LOG.warn("Fail to save the lease for inode id " + id
                       + " as the file is not under construction");
          continue;
        }
        String path = file.getFullPathName();
        FileUnderConstructionEntry.Builder b = FileUnderConstructionEntry
            .newBuilder().setInodeId(file.getId()).setFullPath(path);
        FileUnderConstructionEntry e = b.build();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
                               Block newBlock)
      throws IOException {
    INodeFile file = iip.getLastINode().asFile();
    file.recordModification(iip.getLatestSnapshotId());
    file.toUnderConstruction(leaseHolder, clientMachine);
    assert file.isUnderConstruction() : "inode should be under construction.";
    leaseManager.addLease(
        file.getFileUnderConstructionFeature().getClientName(), file.getId());
    boolean shouldRecoverNow = (newBlock == null);
    BlockInfoContiguous oldBlock = file.getLastBlock();
    boolean shouldCopyOnTruncate = shouldCopyOnTruncate(file, oldBlock);
      } else {
        if (overwrite) {
          toRemoveBlocks = new BlocksMapUpdateInfo();
          List<INode> toRemoveINodes = new ChunkedArrayList<>();
          List<Long> toRemoveUCFiles = new ChunkedArrayList<>();
          long ret = FSDirDeleteOp.delete(
              dir, iip, toRemoveBlocks, toRemoveINodes,
              toRemoveUCFiles, now());
          if (ret >= 0) {
            iip = INodesInPath.replace(iip, iip.length() - 1, null);
            FSDirDeleteOp.incrDeletedFileCount(ret);
            removeLeasesAndINodes(toRemoveUCFiles, toRemoveINodes, true);
          }
        } else {
        throw new IOException("Unable to add " + src +  " to namespace");
      }
      leaseManager.addLease(newNode.getFileUnderConstructionFeature()
          .getClientName(), newNode.getId());

      if (feInfo != null) {
    file.toUnderConstruction(leaseHolder, clientMachine);

    leaseManager.addLease(
        file.getFileUnderConstructionFeature().getClientName(), file.getId());

    LocatedBlock ret = null;
    if (!newBlock) {
      Lease lease = leaseManager.getLease(holder);

      if (!force && lease != null) {
        Lease leaseFile = leaseManager.getLease(file);
        if (leaseFile != null && leaseFile.equals(lease)) {
  
  void removeLeasesAndINodes(List<Long> removedUCFiles,
      List<INode> removedINodes,
      final boolean acquireINodeMapLock) {
    assert hasWriteLock();
    leaseManager.removeLeases(removedUCFiles);
    if (removedINodes != null) {
      if (acquireINodeMapLock) {
      return lease;
    logReassignLease(lease.getHolder(), src, newHolder);
    return reassignLeaseInternal(lease, newHolder, pendingFile);
  }
  
  Lease reassignLeaseInternal(Lease lease, String newHolder, INodeFile pendingFile) {
    assert hasWriteLock();
    pendingFile.getFileUnderConstructionFeature().setClientName(newHolder);
    return leaseManager.reassignLease(lease, pendingFile, newHolder);
  }

  private void commitOrCompleteLastBlock(final INodeFile fileINode,

    FileUnderConstructionFeature uc = pendingFile.getFileUnderConstructionFeature();
    Preconditions.checkArgument(uc != null);
    leaseManager.removeLease(uc.getClientName(), pendingFile);
    
    pendingFile.recordModification(latestSnapshot);

    persistBlocks(src, pendingFile, logRetryCache);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INode.java
  public abstract QuotaCounts cleanSubtree(
      final BlockStoragePolicySuite bsps, final int snapshotId,
      int priorSnapshotId, BlocksMapUpdateInfo collectedBlocks,
      List<INode> removedINodes, List<Long> removedUCFiles);
  
  public abstract void destroyAndCollectBlocks(
      BlockStoragePolicySuite bsps, BlocksMapUpdateInfo collectedBlocks,
      List<INode> removedINodes, List<Long> removedUCFiles);

  public final ContentSummary computeContentSummary(BlockStoragePolicySuite bsps) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeDirectory.java
  }

  public QuotaCounts cleanSubtreeRecursively(
      final BlockStoragePolicySuite bsps, final int snapshot, int prior,
      final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, List<Long> removedUCFiles,
      final Map<INode, INode> excludedNodes) {
    QuotaCounts counts = new QuotaCounts.Builder().build();
        continue;
      } else {
        QuotaCounts childCounts = child.cleanSubtree(bsps, snapshot, prior,
            collectedBlocks, removedINodes, removedUCFiles);
        counts.add(childCounts);
      }
    }
  }

  @Override
  public void destroyAndCollectBlocks(
      final BlockStoragePolicySuite bsps,
      final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, List<Long> removedUCFiles) {
    final DirectoryWithSnapshotFeature sf = getDirectoryWithSnapshotFeature();
    if (sf != null) {
      sf.clear(bsps, this, collectedBlocks, removedINodes, removedUCFiles);
    }
    for (INode child : getChildrenList(Snapshot.CURRENT_STATE_ID)) {
      child.destroyAndCollectBlocks(bsps, collectedBlocks, removedINodes,
                                    removedUCFiles);
    }
    if (getAclFeature() != null) {
      AclStorage.removeAclFeature(getAclFeature());
  }
  
  @Override
  public QuotaCounts cleanSubtree(
      final BlockStoragePolicySuite bsps, final int snapshotId, int priorSnapshotId,
      final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, List<Long> removedUCFiles) {
    DirectoryWithSnapshotFeature sf = getDirectoryWithSnapshotFeature();
    if (sf != null) {
      return sf.cleanDirectory(bsps, this, snapshotId, priorSnapshotId,
          collectedBlocks, removedINodes, removedUCFiles);
    }
    if (priorSnapshotId == Snapshot.NO_SNAPSHOT_ID
      QuotaCounts counts = new QuotaCounts.Builder().build();
      this.computeQuotaUsage(bsps, counts, true);
      destroyAndCollectBlocks(bsps, collectedBlocks, removedINodes,
                              removedUCFiles);
      return counts; 
    } else {
      QuotaCounts counts = cleanSubtreeRecursively(bsps, snapshotId, priorSnapshotId,
          collectedBlocks, removedINodes, removedUCFiles, null);
      if (isQuotaSet()) {
        getDirectoryWithQuotaFeature().addSpaceConsumed2Cache(counts.negation());
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeFile.java
  }

  @Override
  public QuotaCounts cleanSubtree(
      BlockStoragePolicySuite bsps, final int snapshot, int priorSnapshotId,
      final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, List<Long> removedUCFiles) {
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      return sf.cleanFile(bsps, this, snapshot, priorSnapshotId, collectedBlocks,
        computeQuotaUsage(bsps, counts, false);
        destroyAndCollectBlocks(bsps, collectedBlocks, removedINodes,
                                removedUCFiles);
      } else {
        FileUnderConstructionFeature uc = getFileUnderConstructionFeature();
        if (uc != null) {
          uc.cleanZeroSizeBlock(this, collectedBlocks);
          if (removedUCFiles != null) {
            removedUCFiles.add(getId());
          }
        }
      }
    }
  }

  @Override
  public void destroyAndCollectBlocks(
      BlockStoragePolicySuite bsps, BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, List<Long> removedUCFiles) {
    if (blocks != null && collectedBlocks != null) {
      for (BlockInfoContiguous blk : blocks) {
        collectedBlocks.addDeleteBlock(blk);
      sf.getDiffs().destroyAndCollectSnapshotBlocks(collectedBlocks);
      sf.clearDiffs();
    }
    if (isUnderConstruction() && removedUCFiles != null) {
      removedUCFiles.add(getId());
    }
  }

  @Override

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeMap.java
      }
      
      @Override
      public void destroyAndCollectBlocks(
          BlockStoragePolicySuite bsps, BlocksMapUpdateInfo collectedBlocks,
          List<INode> removedINodes, List<Long> removedUCFiles) {
      }

      }
      
      @Override
      public QuotaCounts cleanSubtree(
          BlockStoragePolicySuite bsps, int snapshotId, int priorSnapshotId,
          BlocksMapUpdateInfo collectedBlocks, List<INode> removedINodes,
          List<Long> removedUCFiles) {
          return null;
      }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeReference.java
  }

  @Override // used by WithCount
  public QuotaCounts cleanSubtree(
      BlockStoragePolicySuite bsps, int snapshot, int prior, BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, List<Long> removedUCFiles) {
    return referred.cleanSubtree(bsps, snapshot, prior, collectedBlocks,
        removedINodes, removedUCFiles);
  }

  @Override // used by WithCount
  public void destroyAndCollectBlocks(
      BlockStoragePolicySuite bsps, BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, List<Long> removedUCFiles) {
    if (removeReference(this) <= 0) {
      referred.destroyAndCollectBlocks(bsps, collectedBlocks, removedINodes,
                                       removedUCFiles);
    }
  }

    }
    
    @Override
    public QuotaCounts cleanSubtree(
        BlockStoragePolicySuite bsps, final int snapshot, int prior, final BlocksMapUpdateInfo collectedBlocks,
        final List<INode> removedINodes, List<Long> removedUCFiles) {
      Preconditions.checkArgument(snapshot != Snapshot.CURRENT_STATE_ID);
      }

      QuotaCounts counts = getReferredINode().cleanSubtree(bsps, snapshot, prior,
          collectedBlocks, removedINodes, removedUCFiles);
      INodeReference ref = getReferredINode().getParentReference();
      if (ref != null) {
        try {
    }
    
    @Override
    public void destroyAndCollectBlocks(
        BlockStoragePolicySuite bsps, BlocksMapUpdateInfo collectedBlocks,
        final List<INode> removedINodes, List<Long> removedUCFiles) {
      int snapshot = getSelfSnapshot();
      if (removeReference(this) <= 0) {
        getReferredINode().destroyAndCollectBlocks(bsps, collectedBlocks,
            removedINodes, removedUCFiles);
      } else {
        int prior = getPriorSnapshot(this);
        INode referred = getReferredINode().asReference().getReferredINode();
          }
          try {
            QuotaCounts counts = referred.cleanSubtree(bsps, snapshot, prior,
                collectedBlocks, removedINodes, removedUCFiles);
            INodeReference ref = getReferredINode().getParentReference();
            if (ref != null) {
              ref.addSpaceConsumed(counts.negation(), true);
    }
    
    @Override
    public QuotaCounts cleanSubtree(
        BlockStoragePolicySuite bsps, int snapshot, int prior,
        BlocksMapUpdateInfo collectedBlocks, List<INode> removedINodes,
        List<Long> removedUCFiles) {
      if (snapshot == Snapshot.CURRENT_STATE_ID
          && prior == Snapshot.NO_SNAPSHOT_ID) {
        QuotaCounts counts = new QuotaCounts.Builder().build();
        this.computeQuotaUsage(bsps, counts, true);
        destroyAndCollectBlocks(bsps, collectedBlocks, removedINodes,
                                removedUCFiles);
        return counts;
      } else {
          return new QuotaCounts.Builder().build();
        }
        return getReferredINode().cleanSubtree(bsps, snapshot, prior,
            collectedBlocks, removedINodes, removedUCFiles);
      }
    }
    
    @Override
    public void destroyAndCollectBlocks(
        BlockStoragePolicySuite bsps, BlocksMapUpdateInfo collectedBlocks,
        final List<INode> removedINodes, List<Long> removedUCFiles) {
      if (removeReference(this) <= 0) {
        getReferredINode().destroyAndCollectBlocks(bsps, collectedBlocks,
            removedINodes, removedUCFiles);
      } else {
          referred.cleanSubtree(bsps, snapshot, prior, collectedBlocks,
              removedINodes, removedUCFiles);
        } else if (referred.isDirectory()) {
          Preconditions.checkState(dir.isWithSnapshot());
          try {
            DirectoryWithSnapshotFeature.destroyDstSubtree(bsps, dir, snapshot,
                prior, collectedBlocks, removedINodes, removedUCFiles);
          } catch (QuotaExceededException e) {
            LOG.error("should not exceed quota while snapshot deletion", e);
          }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeSymlink.java
  }
  
  @Override
  public QuotaCounts cleanSubtree(
      BlockStoragePolicySuite bsps, final int snapshotId, int priorSnapshotId,
      final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, List<Long> removedUCFiles) {
    if (snapshotId == Snapshot.CURRENT_STATE_ID
        && priorSnapshotId == Snapshot.NO_SNAPSHOT_ID) {
      destroyAndCollectBlocks(bsps, collectedBlocks, removedINodes,
                              removedUCFiles);
    }
    return new QuotaCounts.Builder().nameSpace(1).build();
  }
  
  @Override
  public void destroyAndCollectBlocks(
      final BlockStoragePolicySuite bsps,
      final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, List<Long> removedUCFiles) {
    removedINodes.add(this);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/LeaseManager.java
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.util.Daemon;
  private final SortedMap<String, Lease> leases = new TreeMap<>();
  private final PriorityQueue<Lease> sortedLeases = new PriorityQueue<>(512,
      new Comparator<Lease>() {
        @Override
        public int compare(Lease o1, Lease o2) {
          return Long.signum(o1.getLastUpdate() - o2.getLastUpdate());
        }
  });
  private final HashMap<Long, Lease> leasesById = new HashMap<>();

  private Daemon lmthread;
  private volatile boolean shouldRunMonitor;
    return leases.get(holder);
  }

  synchronized long getNumUnderConstructionBlocks() {
    assert this.fsnamesystem.hasReadLock() : "The FSNamesystem read lock wasn't"
      + "acquired before counting under construction blocks";
    long numUCBlocks = 0;
    for (Long id : getINodeIdWithLeases()) {
      final INodeFile cons = fsnamesystem.getFSDirectory().getInode(id).asFile();
      Preconditions.checkState(cons.isUnderConstruction());
      BlockInfoContiguous[] blocks = cons.getBlocks();
      if(blocks == null) {
        continue;
      }
      for(BlockInfoContiguous b : blocks) {
        if(!b.isComplete())
          numUCBlocks++;
      }
    }
    LOG.info("Number of blocks under construction: " + numUCBlocks);
    return numUCBlocks;
  }

  Collection<Long> getINodeIdWithLeases() {return leasesById.keySet();}

  public synchronized Lease getLease(INodeFile src) {return leasesById.get(src.getId());}

  @VisibleForTesting
  public synchronized int countLease() {return sortedLeases.size();}

  synchronized Lease addLease(String holder, long inodeId) {
    Lease lease = getLease(holder);
    if (lease == null) {
      lease = new Lease(holder);
    } else {
      renewLease(lease);
    }
    leasesById.put(inodeId, lease);
    lease.files.add(inodeId);
    return lease;
  }

  private synchronized void removeLease(Lease lease, long inodeId) {
    leasesById.remove(inodeId);
    if (!lease.removeFile(inodeId)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("inode " + inodeId + " not found in lease.files (=" + lease
                + ")");
      }
    }

    if (!lease.hasFiles()) {
      leases.remove(lease.holder);
      if (!sortedLeases.remove(lease)) {
        LOG.error(lease + " not found in sortedLeases");
  synchronized void removeLease(String holder, INodeFile src) {
    Lease lease = getLease(holder);
    if (lease != null) {
      removeLease(lease, src.getId());
    } else {
      LOG.warn("Removing non-existent lease! holder=" + holder +
          " src=" + src.getFullPathName());
    }
  }

  synchronized void removeAllLeases() {
    sortedLeases.clear();
    leasesById.clear();
    leases.clear();
  }

  synchronized Lease reassignLease(Lease lease, INodeFile src,
                                   String newHolder) {
    assert newHolder != null : "new lease holder is null";
    if (lease != null) {
      removeLease(lease, src.getId());
    }
    return addLease(newHolder, src.getId());
  }

  class Lease {
    private final String holder;
    private long lastUpdate;
    private final HashSet<Long> files = new HashSet<>();
  
    private Lease(String holder) {
    }

    boolean hasFiles() {return !files.isEmpty();}

    boolean removeFile(long inodeId) {
      return files.remove(inodeId);
    }

    @Override
    public String toString() {
      return "[Lease.  Holder: " + holder
          + ", pending creates: " + files.size() + "]";
    }

    @Override
      return holder.hashCode();
    }
    
    private Collection<Long> getFiles() { return files; }

    String getHolder() {
      return holder;
    }

    @VisibleForTesting
    long getLastUpdate() {
      return lastUpdate;
    }
  }

  @VisibleForTesting
  synchronized void removeLeases(Collection<Long> inodes) {
    for (long inode : inodes) {
      Lease lease = leasesById.get(inode);
      if (lease != null) {
        removeLease(lease, inode);
      }
    }
  }

  public void setLeasePeriod(long softLimit, long hardLimit) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(name + " is interrupted", ie);
          }
        } catch(Throwable e) {
          LOG.warn("Unexpected throwable: ", e);
        }
      }
    }
  }

  synchronized boolean checkLeases() {
    boolean needSync = false;
    assert fsnamesystem.hasWriteLock();

    while(!sortedLeases.isEmpty() && sortedLeases.peek().expiredHardLimit()) {
      Lease leaseToCheck = sortedLeases.poll();
      LOG.info(leaseToCheck + " has expired hard limit");

      final List<Long> removing = new ArrayList<>();
      Collection<Long> files = leaseToCheck.getFiles();
      Long[] leaseINodeIds = files.toArray(new Long[files.size()]);
      FSDirectory fsd = fsnamesystem.getFSDirectory();
      String p = null;
      for(Long id : leaseINodeIds) {
        try {
          INodesInPath iip = INodesInPath.fromINode(fsd.getInode(id));
          p = iip.getPath();
          if (!p.startsWith("/")) {
            throw new IOException("Invalid path in the lease " + p);
          }
          boolean completed = fsnamesystem.internalReleaseLease(
              leaseToCheck, p, iip,
              HdfsServerConstants.NAMENODE_LEASE_HOLDER);
          if (LOG.isDebugEnabled()) {
            if (completed) {
              LOG.debug("Lease recovery for inode " + id + " is complete. " +
                            "File closed.");
            } else {
              LOG.debug("Started block recovery " + p + " lease " + leaseToCheck);
            }
        } catch (IOException e) {
          LOG.error("Cannot release the path " + p + " in the lease "
              + leaseToCheck, e);
          removing.add(id);
        }
      }

      for(Long id : removing) {
        removeLease(leaseToCheck, id);
      }
    }

    return needSync;
  }

    return getClass().getSimpleName() + "= {"
        + "\n leases=" + leases
        + "\n sortedLeases=" + sortedLeases
        + "\n leasesById=" + leasesById
        + "\n}";
  }

  @VisibleForTesting
  public void triggerMonitorCheckNow() {
    Preconditions.checkState(lmthread != null,
        "Lease monitor is not running");
    lmthread.interrupt();
  }

  @VisibleForTesting
  public void runLeaseChecks() {
    checkLeases();
  }

}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/AbstractINodeDiffList.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/DirectorySnapshottableFeature.java
      int prior = Snapshot.findLatestSnapshot(snapshotRoot, snapshot.getId());
      try {
        QuotaCounts counts = snapshotRoot.cleanSubtree(bsps, snapshot.getId(),
            prior, collectedBlocks, removedINodes, null);
        INodeDirectory parent = snapshotRoot.getParent();
        if (parent != null) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/DirectoryWithSnapshotFeature.java

    private QuotaCounts destroyCreatedList(
        final BlockStoragePolicySuite bsps, final INodeDirectory currentINode,
        final BlocksMapUpdateInfo collectedBlocks,
        final List<INode> removedINodes, List<Long> removedUCFiles) {
      QuotaCounts counts = new QuotaCounts.Builder().build();
      final List<INode> createdList = getList(ListType.CREATED);
      for (INode c : createdList) {
        c.computeQuotaUsage(bsps, counts, true);
        c.destroyAndCollectBlocks(bsps, collectedBlocks, removedINodes,
                                  removedUCFiles);
        currentINode.removeChild(c);
      }
    private QuotaCounts destroyDeletedList(
        final BlockStoragePolicySuite bsps,
        final BlocksMapUpdateInfo collectedBlocks,
        final List<INode> removedINodes, List<Long> removedUCFiles) {
      QuotaCounts counts = new QuotaCounts.Builder().build();
      final List<INode> deletedList = getList(ListType.DELETED);
      for (INode d : deletedList) {
        d.computeQuotaUsage(bsps, counts, false);
        d.destroyAndCollectBlocks(bsps, collectedBlocks, removedINodes,
                                  removedUCFiles);
      }
      deletedList.clear();
      return counts;

    @Override
    QuotaCounts combinePosteriorAndCollectBlocks(
        final BlockStoragePolicySuite bsps, final INodeDirectory currentDir,
        final DirectoryDiff posterior,
        final BlocksMapUpdateInfo collectedBlocks,
        final List<INode> removedINodes) {
      final QuotaCounts counts = new QuotaCounts.Builder().build();
        public void process(INode inode) {
          if (inode != null) {
            inode.computeQuotaUsage(bsps, counts, false);
            inode.destroyAndCollectBlocks(bsps, collectedBlocks, removedINodes,
                                          null);
          }
        }
      });
        BlocksMapUpdateInfo collectedBlocks, final List<INode> removedINodes) {
      QuotaCounts counts = new QuotaCounts.Builder().build();
      counts.add(diff.destroyDeletedList(bsps, collectedBlocks, removedINodes,
                                         null));
      INodeDirectoryAttributes snapshotINode = getSnapshotINode();
      if (snapshotINode != null && snapshotINode.getAclFeature() != null) {
        AclStorage.removeAclFeature(snapshotINode.getAclFeature());
  public static void destroyDstSubtree(
      final BlockStoragePolicySuite bsps, INode inode, final int snapshot,
      final int prior, final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, List<Long> removedUCFiles) throws QuotaExceededException {
    Preconditions.checkArgument(prior != Snapshot.NO_SNAPSHOT_ID);
    if (inode.isReference()) {
      if (inode instanceof INodeReference.WithName
          && snapshot != Snapshot.CURRENT_STATE_ID) {
        inode.cleanSubtree(bsps, snapshot, prior, collectedBlocks, removedINodes,
                           removedUCFiles);
      } else { 
        destroyDstSubtree(bsps, inode.asReference().getReferredINode(), snapshot,
            prior, collectedBlocks, removedINodes, removedUCFiles);
      }
    } else if (inode.isFile()) {
      inode.cleanSubtree(bsps, snapshot, prior, collectedBlocks, removedINodes,
                         removedUCFiles);
    } else if (inode.isDirectory()) {
      Map<INode, INode> excludedNodes = null;
      INodeDirectory dir = inode.asDirectory();
        priorDiff = diffList.getDiffById(prior);
        if (priorDiff != null && priorDiff.getSnapshotId() == prior) {
          priorDiff.diff.destroyCreatedList(bsps, dir, collectedBlocks,
              removedINodes, removedUCFiles);
        }
      }
      for (INode child : inode.asDirectory().getChildrenList(prior)) {
          continue;
        }
        destroyDstSubtree(bsps, child, snapshot, prior, collectedBlocks,
            removedINodes, removedUCFiles);
      }
    }
  }
  private static QuotaCounts cleanDeletedINode(
      final BlockStoragePolicySuite bsps, INode inode, final int post, final int prior,
      final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, List<Long> removedUCFiles) {
    QuotaCounts counts = new QuotaCounts.Builder().build();
    Deque<INode> queue = new ArrayDeque<INode>();
    queue.addLast(inode);
      if (topNode instanceof INodeReference.WithName) {
        INodeReference.WithName wn = (INodeReference.WithName) topNode;
        if (wn.getLastSnapshotId() >= post) {
          wn.cleanSubtree(bsps, post, prior, collectedBlocks, removedINodes,
                          removedUCFiles);
        }
          if (priorDiff != null && priorDiff.getSnapshotId() == prior) {
            priorChildrenDiff = priorDiff.getChildrenDiff();
            counts.add(priorChildrenDiff.destroyCreatedList(bsps, dir,
                collectedBlocks, removedINodes, removedUCFiles));
          }
        }
        
  }

  public void clear(BlockStoragePolicySuite bsps, INodeDirectory currentINode,
      final BlocksMapUpdateInfo collectedBlocks, final List<INode>
      removedINodes, final List<Long> removedUCFiles) {
    for (DirectoryDiff diff : diffs) {
      diff.destroyDiffAndCollectBlocks(bsps, currentINode, collectedBlocks,
    }
  }

  public QuotaCounts cleanDirectory(
      final BlockStoragePolicySuite bsps, final INodeDirectory currentINode,
      final int snapshot, int prior, final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, List<Long> removedUCFiles) {
    QuotaCounts counts = new QuotaCounts.Builder().build();
    Map<INode, INode> priorCreated = null;
    Map<INode, INode> priorDeleted = null;
      DirectoryDiff lastDiff = diffs.getLast();
      if (lastDiff != null) {
        counts.add(lastDiff.diff.destroyCreatedList(bsps, currentINode,
            collectedBlocks, removedINodes, removedUCFiles));
      }
      counts.add(currentINode.cleanSubtreeRecursively(bsps, snapshot, prior,
          collectedBlocks, removedINodes, removedUCFiles, priorDeleted));
    } else {
      prior = getDiffs().updatePrior(snapshot, prior);
      counts.add(getDiffs().deleteSnapshotDiff(bsps, snapshot, prior,
          currentINode, collectedBlocks, removedINodes));
      counts.add(currentINode.cleanSubtreeRecursively(bsps, snapshot, prior,
          collectedBlocks, removedINodes, removedUCFiles, priorDeleted));

      if (prior != Snapshot.NO_SNAPSHOT_ID) {
                ListType.CREATED)) {
              if (priorCreated.containsKey(cNode)) {
                counts.add(cNode.cleanSubtree(bsps, snapshot, Snapshot.NO_SNAPSHOT_ID,
                    collectedBlocks, removedINodes, removedUCFiles));
              }
            }
          }
              ListType.DELETED)) {
            if (priorDeleted == null || !priorDeleted.containsKey(dNode)) {
              counts.add(cleanDeletedINode(bsps, dNode, snapshot, prior,
                  collectedBlocks, removedINodes, removedUCFiles));
            }
          }
        }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FileWithSnapshotFeature.java
      final BlocksMapUpdateInfo info, final List<INode> removedINodes) {
    if (isCurrentFileDeleted() && getDiffs().asList().isEmpty()) {
      file.destroyAndCollectBlocks(bsps, info, removedINodes, null);
      return;
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/SnapshotManager.java
hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestLease.java

public class TestLease {
  static boolean hasLease(MiniDFSCluster cluster, Path src) {
    return NameNodeAdapter.getLeaseForPath(cluster.getNameNode(),
            src.toString()) != null;
  }

  static int leaseCount(MiniDFSCluster cluster) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/NameNodeAdapter.java
    namesystem.leaseManager.triggerMonitorCheckNow();
  }

  public static Lease getLeaseForPath(NameNode nn, String path) {
    final FSNamesystem fsn = nn.getNamesystem();
    INode inode;
    try {
      inode = fsn.getFSDirectory().getINode(path, false);
    } catch (UnresolvedLinkException e) {
      throw new RuntimeException("Lease manager should not support symlinks");
    }
    return inode == null ? null : fsn.leaseManager.getLease((INodeFile) inode);
  }

  public static String getLeaseHolderForPath(NameNode namenode, String path) {
    Lease l = getLeaseForPath(namenode, path);
    return l == null? null: l.getHolder();
  }

  public static long getLeaseRenewalTime(NameNode nn, String path) {
    Lease l = getLeaseForPath(nn, path);
    return l == null ? -1 : l.getLastUpdate();
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestDiskspaceQuotaUpdate.java
    }

    LeaseManager lm = cluster.getNamesystem().getLeaseManager();
    INodeFile inode = fsdir.getINode(file.toString()).asFile();
    Assert.assertNotNull(inode);
    Assert.assertFalse("should not be UC", inode.isUnderConstruction());
    Assert.assertNull("should not have a lease", lm.getLease(inode));
    final long newSpaceUsed = dirNode.getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getStorageSpace();
    }

    LeaseManager lm = cluster.getNamesystem().getLeaseManager();
    INodeFile inode = fsdir.getINode(file.toString()).asFile();
    Assert.assertNotNull(inode);
    Assert.assertFalse("should not be UC", inode.isUnderConstruction());
    Assert.assertNull("should not have a lease", lm.getLease(inode));
    final long newSpaceUsed = dirNode.getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getStorageSpace();
    }

    LeaseManager lm = cluster.getNamesystem().getLeaseManager();
    INodeFile inode = fsdir.getINode(file.toString()).asFile();
    Assert.assertNotNull(inode);
    Assert.assertFalse("should not be UC", inode.isUnderConstruction());
    Assert.assertNull("should not have a lease", lm.getLease(inode));
    final long newSpaceUsed = dirNode.getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getStorageSpace();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFSImage.java
      assertEquals(1, blks.length);
      assertEquals(BlockUCState.UNDER_CONSTRUCTION, blks[0].getBlockUCState());
      Lease lease = fsn.leaseManager.getLease(file2Node);
      Assert.assertNotNull(lease);
    } finally {
      if (cluster != null) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFSNamesystem.java
    DFSTestUtil.formatNameNode(conf);
    FSNamesystem fsn = FSNamesystem.loadFromDisk(conf);
    LeaseManager leaseMan = fsn.getLeaseManager();
    leaseMan.addLease("client1", fsn.getFSDirectory().allocateNewInodeId());
    assertEquals(1, leaseMan.countLease());
    fsn.clear();
    leaseMan = fsn.getLeaseManager();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestGetBlockLocations.java
      public Void answer(InvocationOnMock invocation) throws Throwable {
        INodesInPath iip = fsd.getINodesInPath(FILE_PATH, true);
        FSDirDeleteOp.delete(fsd, iip, new INode.BlocksMapUpdateInfo(),
                             new ArrayList<INode>(), new ArrayList<Long>(),
                             now());
        invocation.callRealMethod();
        return null;
      }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestLeaseManager.java
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;

import static org.mockito.Mockito.*;

public class TestLeaseManager {
  @Test
  public void testRemoveLeases() throws Exception {
    FSNamesystem fsn = mock(FSNamesystem.class);
    LeaseManager lm = new LeaseManager(fsn);
    ArrayList<Long> ids = Lists.newArrayList(INodeId.ROOT_INODE_ID + 1,
            INodeId.ROOT_INODE_ID + 2, INodeId.ROOT_INODE_ID + 3,
            INodeId.ROOT_INODE_ID + 4);
    for (long id : ids) {
      lm.addLease("foo", id);
    }

    assertEquals(4, lm.getINodeIdWithLeases().size());
    synchronized (lm) {
      lm.removeLeases(ids);
    }
    assertEquals(0, lm.getINodeIdWithLeases().size());
  }

    lm.setLeasePeriod(0, 0);

    lm.addLease("holder1", INodeId.ROOT_INODE_ID + 1);
    lm.addLease("holder2", INodeId.ROOT_INODE_ID + 2);
    lm.addLease("holder3", INodeId.ROOT_INODE_ID + 3);
    assertEquals(lm.countLease(), 3);

    lm.checkLeases();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestSaveNamespace.java
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    try {
      cluster.getNamesystem().leaseManager.addLease("me",
              INodeId.ROOT_INODE_ID + 1);
      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.getNameNodeRpc().saveNamespace();
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestINodeFileUnderConstructionWithSnapshot.java
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.apache.log4j.Level;
import org.junit.After;
    assertEquals(BLOCKSIZE - 1, lastBlock.getBlockSize());
    out.close();
  }

  @Test
  public void testLease() throws Exception {
    try {
      NameNodeAdapter.setLeasePeriod(fsn, 100, 200);
      final Path foo = new Path(dir, "foo");
      final Path bar = new Path(foo, "bar");
      DFSTestUtil.createFile(hdfs, bar, BLOCKSIZE, REPLICATION, 0);
      HdfsDataOutputStream out = appendFileWithoutClosing(bar, 100);
      out.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
      SnapshotTestHelper.createSnapshot(hdfs, dir, "s0");

      hdfs.delete(foo, true);
      Thread.sleep(1000);
      try {
        fsn.writeLock();
        NameNodeAdapter.getLeaseManager(fsn).runLeaseChecks();
      } finally {
        fsn.writeUnlock();
      }
    } finally {
      NameNodeAdapter.setLeasePeriod(
          fsn,
          HdfsServerConstants.LEASE_SOFTLIMIT_PERIOD,
          HdfsServerConstants.LEASE_HARDLIMIT_PERIOD);
    }
  }
}
\No newline at end of file

