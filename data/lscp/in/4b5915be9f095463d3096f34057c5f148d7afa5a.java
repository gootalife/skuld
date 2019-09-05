hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirDeleteOp.java
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INode.ReclaimContext;
import org.apache.hadoop.util.ChunkedArrayList;

import java.io.IOException;
  static long delete(FSDirectory fsd, INodesInPath iip,
      BlocksMapUpdateInfo collectedBlocks, List<INode> removedINodes,
      List<Long> removedUCFiles, long mtime) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: " + iip.getPath());
    }
    long filesRemoved = -1;
    fsd.writeLock();
    try {
      if (deleteAllowed(iip, iip.getPath()) ) {
        List<INodeDirectory> snapshottableDirs = new ArrayList<>();
        FSDirSnapshotOp.checkSnapshot(iip.getLastINode(), snapshottableDirs);
        ReclaimContext context = new ReclaimContext(
            fsd.getBlockStoragePolicySuite(), collectedBlocks, removedINodes,
            removedUCFiles);
        if (unprotectedDelete(fsd, iip, context, mtime)) {
          filesRemoved = context.quotaDelta().getNsDelta();
        }
        fsd.getFSNamesystem().removeSnapshottableDirs(snapshottableDirs);
        fsd.updateCount(iip, context.quotaDelta(), false);
      }
    } finally {
      fsd.writeUnlock();
    }
    List<INodeDirectory> snapshottableDirs = new ArrayList<>();
    FSDirSnapshotOp.checkSnapshot(iip.getLastINode(), snapshottableDirs);
    boolean filesRemoved = unprotectedDelete(fsd, iip,
        new ReclaimContext(fsd.getBlockStoragePolicySuite(),
            collectedBlocks, removedINodes, removedUCFiles),
        mtime);
    fsn.removeSnapshottableDirs(snapshottableDirs);

    if (filesRemoved) {
      fsn.removeLeasesAndINodes(removedUCFiles, removedINodes, false);
      fsn.removeBlocksAndUpdateSafemodeTotal(collectedBlocks);
    }
  private static boolean unprotectedDelete(FSDirectory fsd, INodesInPath iip,
      ReclaimContext reclaimContext, long mtime) {
    assert fsd.hasWriteLock();

    INode targetNode = iip.getLastINode();
    if (targetNode == null) {
      return false;
    }

    long removed = fsd.removeLastINode(iip);
    if (removed == -1) {
      return false;
    }

    final INodeDirectory parent = targetNode.getParent();
    parent.updateModificationTime(mtime, latestSnapshot);

    if (!targetNode.isInLatestSnapshot(latestSnapshot)) {
      targetNode.destroyAndCollectBlocks(reclaimContext);
    } else {
      targetNode.cleanSubtree(reclaimContext, CURRENT_STATE_ID, latestSnapshot);
    }

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
          + iip.getPath() + " is removed");
    }
    return true;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirRenameOp.java
  static void renameForEditLog(
      FSDirectory fsd, String src, String dst, long timestamp,
      Options.Rename... options)
      throws IOException {
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    final INodesInPath srcIIP = fsd.getINodesInPath4Write(src, false);
    final INodesInPath dstIIP = fsd.getINodesInPath4Write(dst, false);
    unprotectedRenameTo(fsd, src, dst, srcIIP, dstIIP, timestamp,
        collectedBlocks, options);
    if (!collectedBlocks.getToDeleteList().isEmpty()) {
      fsd.getFSNamesystem().removeBlocksAndUpdateSafemodeTotal(collectedBlocks);
    }
  }

        this.srcIIP = INodesInPath.replace(srcIIP, srcIIP.length() - 1,
            srcChild);
        oldSrcCounts.add(withCount.getReferredINode().computeQuotaUsage(bsps));
      } else if (srcChildIsReference) {
        withCount = (INodeReference.WithCount) srcChild.asReference()
      Preconditions.checkState(oldDstChild != null);
      List<INode> removedINodes = new ChunkedArrayList<>();
      List<Long> removedUCFiles = new ChunkedArrayList<>();
      INode.ReclaimContext context = new INode.ReclaimContext(bsps,
          collectedBlocks, removedINodes, removedUCFiles);
      final boolean filesDeleted;
      if (!oldDstChild.isInLatestSnapshot(dstIIP.getLatestSnapshotId())) {
        oldDstChild.destroyAndCollectBlocks(context);
        filesDeleted = true;
      } else {
        oldDstChild.cleanSubtree(context, Snapshot.CURRENT_STATE_ID,
            dstIIP.getLatestSnapshotId());
        filesDeleted = context.quotaDelta().getNsDelta() >= 0;
      }
      fsd.getFSNamesystem().removeLeasesAndINodes(
          removedUCFiles, removedINodes, false);
      if (isSrcInSnapshot) {
        QuotaCounts newSrcCounts = srcChild.computeQuotaUsage(bsps, false);
        newSrcCounts.subtract(oldSrcCounts);
        srcParent.addSpaceConsumed(newSrcCounts, false);
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirSnapshotOp.java
    }

    INode.BlocksMapUpdateInfo collectedBlocks = new INode.BlocksMapUpdateInfo();
    ChunkedArrayList<INode> removedINodes = new ChunkedArrayList<>();
    INode.ReclaimContext context = new INode.ReclaimContext(
        fsd.getBlockStoragePolicySuite(), collectedBlocks, removedINodes, null);
    fsd.writeLock();
    try {
      snapshotManager.deleteSnapshot(iip, snapshotName, context);
      fsd.updateCount(iip, context.quotaDelta(), false);
      fsd.removeFromInodeMap(removedINodes);
    } finally {
      fsd.writeUnlock();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirectory.java
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
    }
  }

  public void updateCount(INodesInPath iip, INode.QuotaDelta quotaDelta,
      boolean check) throws QuotaExceededException {
    QuotaCounts counts = quotaDelta.getCountsCopy();
    updateCount(iip, iip.length() - 1, counts.negation(), check);
    Map<INode, QuotaCounts> deltaInOtherPaths = quotaDelta.getUpdateMap();
    for (Map.Entry<INode, QuotaCounts> entry : deltaInOtherPaths.entrySet()) {
      INodesInPath path = INodesInPath.fromINode(entry.getKey());
      updateCount(path, path.length() - 1, entry.getValue().negation(), check);
    }
    for (Map.Entry<INodeDirectory, QuotaCounts> entry :
        quotaDelta.getQuotaDirMap().entrySet()) {
      INodeDirectory quotaDir = entry.getKey();
      quotaDir.getDirectoryWithQuotaFeature().addSpaceConsumed2Cache(
          entry.getValue().negation());
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogLoader.java
          renameReservedPathsOnUpgrade(deleteSnapshotOp.snapshotRoot,
              logVersion);
      INodesInPath iip = fsDir.getINodesInPath4Write(snapshotRoot);
      fsNamesys.getSnapshotManager().deleteSnapshot(iip,
          deleteSnapshotOp.snapshotName,
          new INode.ReclaimContext(fsNamesys.dir.getBlockStoragePolicySuite(),
              collectedBlocks, removedINodes, null));
      fsNamesys.removeBlocksAndUpdateSafemodeTotal(collectedBlocks);
      collectedBlocks.clear();
      fsNamesys.dir.removeFromInodeMap(removedINodes);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImage.java
            child.asDirectory(), counts);
      } else {
        counts.add(child.computeQuotaUsage(bsps, childPolicyId, false,
            Snapshot.CURRENT_STATE_ID));
      }
    }
      

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INode.java
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;

  public final boolean isInLatestSnapshot(final int latestSnapshotId) {
    if (latestSnapshotId == Snapshot.CURRENT_STATE_ID ||
        latestSnapshotId == Snapshot.NO_SNAPSHOT_ID) {
      return false;
    }
  public abstract void cleanSubtree(ReclaimContext reclaimContext,
      final int snapshotId, int priorSnapshotId);

  public final QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps) {
    final byte storagePolicyId = isSymlink() ?
        HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED : getStoragePolicyID();
    return computeQuotaUsage(bsps, storagePolicyId, true,
        Snapshot.CURRENT_STATE_ID);
  }

  public abstract QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
      byte blockStoragePolicyId, boolean useCache, int lastSnapshotId);

  public final QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
      boolean useCache) {
    final byte storagePolicyId = isSymlink() ?
        HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED : getStoragePolicyID();
    return computeQuotaUsage(bsps, storagePolicyId, useCache,
        Snapshot.CURRENT_STATE_ID);
  }

    out.print(", " + getPermissionStatus(snapshotId));
  }

  public static class QuotaDelta {
    private final QuotaCounts counts;
    private final Map<INode, QuotaCounts> updateMap;

    private final Map<INodeDirectory, QuotaCounts> quotaDirMap;

    public QuotaDelta() {
      counts = new QuotaCounts.Builder().build();
      updateMap = Maps.newHashMap();
      quotaDirMap = Maps.newHashMap();
    }

    public void add(QuotaCounts update) {
      counts.add(update);
    }

    public void addUpdatePath(INodeReference inode, QuotaCounts update) {
      QuotaCounts c = updateMap.get(inode);
      if (c == null) {
        c = new QuotaCounts.Builder().build();
        updateMap.put(inode, c);
      }
      c.add(update);
    }

    public void addQuotaDirUpdate(INodeDirectory dir, QuotaCounts update) {
      Preconditions.checkState(dir.isQuotaSet());
      QuotaCounts c = quotaDirMap.get(dir);
      if (c == null) {
        quotaDirMap.put(dir, update);
      } else {
        c.add(update);
      }
    }

    public QuotaCounts getCountsCopy() {
      final QuotaCounts copy = new QuotaCounts.Builder().build();
      copy.add(counts);
      return copy;
    }

    public void setCounts(QuotaCounts c) {
      this.counts.setNameSpace(c.getNameSpace());
      this.counts.setStorageSpace(c.getStorageSpace());
      this.counts.setTypeSpaces(c.getTypeSpaces());
    }

    public long getNsDelta() {
      long nsDelta = counts.getNameSpace();
      for (Map.Entry<INode, QuotaCounts> entry : updateMap.entrySet()) {
        nsDelta += entry.getValue().getNameSpace();
      }
      return nsDelta;
    }

    public Map<INode, QuotaCounts> getUpdateMap() {
      return ImmutableMap.copyOf(updateMap);
    }

    public Map<INodeDirectory, QuotaCounts> getQuotaDirMap() {
      return ImmutableMap.copyOf(quotaDirMap);
    }
  }

    protected final BlocksMapUpdateInfo collectedBlocks;
    protected final List<INode> removedINodes;
    protected final List<Long> removedUCFiles;
    private final QuotaDelta quotaDelta;

      this.collectedBlocks = collectedBlocks;
      this.removedINodes = removedINodes;
      this.removedUCFiles = removedUCFiles;
      this.quotaDelta = new QuotaDelta();
    }

    public BlockStoragePolicySuite storagePolicySuite() {
    public BlocksMapUpdateInfo collectedBlocks() {
      return collectedBlocks;
    }

    public QuotaDelta quotaDelta() {
      return quotaDelta;
    }

    public ReclaimContext getCopy() {
      return new ReclaimContext(bsps, collectedBlocks, removedINodes,
          removedUCFiles);
    }
  }

    private final List<Block> toDeleteList;
    
    public BlocksMapUpdateInfo() {
      toDeleteList = new ChunkedArrayList<>();
    }
    

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeDirectory.java
  private void addChild(final INode node, final int insertionPoint) {
    if (children == null) {
      children = new ArrayList<>(DEFAULT_FILES_PER_DIRECTORY);
    }
    node.setParent(this);
    children.add(-insertionPoint - 1, node);

  @Override
  public QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
      byte blockStoragePolicyId, boolean useCache, int lastSnapshotId) {
    final DirectoryWithSnapshotFeature sf = getDirectoryWithSnapshotFeature();

    QuotaCounts counts = new QuotaCounts.Builder().build();
        && !(useCache && isQuotaSet())) {
      ReadOnlyList<INode> childrenList = getChildrenList(lastSnapshotId);
      for (INode child : childrenList) {
        final byte childPolicyId = child.getStoragePolicyIDForQuota(
            blockStoragePolicyId);
        counts.add(child.computeQuotaUsage(bsps, childPolicyId, useCache,
            lastSnapshotId));
      }
      counts.addNameSpace(1);
      return counts;
      int lastSnapshotId) {
    if (children != null) {
      for (INode child : children) {
        final byte childPolicyId = child.getStoragePolicyIDForQuota(
            blockStoragePolicyId);
        counts.add(child.computeQuotaUsage(bsps, childPolicyId, useCache,
            lastSnapshotId));
      }
    }
    return computeQuotaUsage4CurrentDirectory(bsps, blockStoragePolicyId,
    DirectoryWithSnapshotFeature sf = getDirectoryWithSnapshotFeature();
    if (sf != null) {
      counts.add(sf.computeQuotaUsage4CurrentDirectory(bsps, storagePolicyId));
    }
    return counts;
  }
  public void undoRename4ScrParent(final INodeReference oldChild,
      final INode newChild) throws QuotaExceededException {
    DirectoryWithSnapshotFeature sf = getDirectoryWithSnapshotFeature();
    assert sf != null : "Directory does not have snapshot feature";
    sf.getDiffs().removeChild(ListType.DELETED, oldChild);
    sf.getDiffs().replaceChild(ListType.CREATED, oldChild, newChild);
    addChild(newChild, true, Snapshot.CURRENT_STATE_ID);
      final INode deletedChild,
      int latestSnapshotId) throws QuotaExceededException {
    DirectoryWithSnapshotFeature sf = getDirectoryWithSnapshotFeature();
    assert sf != null : "Directory does not have snapshot feature";
    boolean removeDeletedChild = sf.getDiffs().removeChild(ListType.DELETED,
        deletedChild);
    int sid = removeDeletedChild ? Snapshot.CURRENT_STATE_ID : latestSnapshotId;
  }

  public void cleanSubtreeRecursively(
      ReclaimContext reclaimContext, final int snapshot, int prior,
      final Map<INode, INode> excludedNodes) {
    int s = snapshot != Snapshot.CURRENT_STATE_ID
        && prior != Snapshot.NO_SNAPSHOT_ID ? prior : snapshot;
    for (INode child : getChildrenList(s)) {
      if (snapshot == Snapshot.CURRENT_STATE_ID || excludedNodes == null ||
          !excludedNodes.containsKey(child)) {
        child.cleanSubtree(reclaimContext, snapshot, prior);
      }
    }
  }

  @Override
  public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
    reclaimContext.quotaDelta().add(
        new QuotaCounts.Builder().nameSpace(1).build());
    final DirectoryWithSnapshotFeature sf = getDirectoryWithSnapshotFeature();
    if (sf != null) {
      sf.clear(reclaimContext, this);
  }
  
  @Override
  public void cleanSubtree(ReclaimContext reclaimContext, final int snapshotId,
      int priorSnapshotId) {
    DirectoryWithSnapshotFeature sf = getDirectoryWithSnapshotFeature();
    if (sf != null) {
      sf.cleanDirectory(reclaimContext, this, snapshotId, priorSnapshotId);
    } else {
      if (priorSnapshotId == Snapshot.NO_SNAPSHOT_ID &&
          snapshotId == Snapshot.CURRENT_STATE_ID) {
        destroyAndCollectBlocks(reclaimContext);
      } else {
        QuotaCounts old = reclaimContext.quotaDelta().getCountsCopy();
        cleanSubtreeRecursively(reclaimContext, snapshotId, priorSnapshotId,
            null);
        QuotaCounts current = reclaimContext.quotaDelta().getCountsCopy();
        current.subtract(old);
        if (isQuotaSet()) {
          reclaimContext.quotaDelta().addQuotaDirUpdate(this, current);
        }
      }
    }
  }
  

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeFile.java
  }

  @Override
  public void cleanSubtree(ReclaimContext reclaimContext,
      final int snapshot, int priorSnapshotId) {
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      sf.cleanFile(reclaimContext, this, snapshot, priorSnapshotId,
          getStoragePolicyID());
    } else {
      if (snapshot == CURRENT_STATE_ID) {
        if (priorSnapshotId == NO_SNAPSHOT_ID) {
          destroyAndCollectBlocks(reclaimContext);
        } else {
          FileUnderConstructionFeature uc = getFileUnderConstructionFeature();
          if (uc != null) {
            uc.cleanZeroSizeBlock(this, reclaimContext.collectedBlocks);
          }
        }
      }
    }
  }

  @Override
  public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
    reclaimContext.quotaDelta().add(computeQuotaUsage(reclaimContext.bsps,
        false));
    clearFile(reclaimContext);
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      sf.getDiffs().destroyAndCollectSnapshotBlocks(
          reclaimContext.collectedBlocks);
      sf.clearDiffs();
    }
    if (isUnderConstruction() && reclaimContext.removedUCFiles != null) {
      reclaimContext.removedUCFiles.add(getId());
    }
  }

  public void clearFile(ReclaimContext reclaimContext) {
    if (blocks != null && reclaimContext.collectedBlocks != null) {
      for (BlockInfoContiguous blk : blocks) {
        reclaimContext.collectedBlocks.addDeleteBlock(blk);
    }
    clear();
    reclaimContext.removedINodes.add(this);
  }

  @Override
  @Override
  public final QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
      byte blockStoragePolicyId, boolean useCache, int lastSnapshotId) {
    final QuotaCounts counts = new QuotaCounts.Builder().nameSpace(1).build();

    final BlockStoragePolicy bsp = bsps.getPolicy(blockStoragePolicyId);
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf == null) {
      counts.add(storagespaceConsumed(bsp));
      final ContentSummaryComputationContext summary) {
    final ContentCounts counts = summary.getCounts();
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    final long fileLen;
    if (sf == null) {
      fileLen = computeFileSize();
      counts.addContent(Content.FILE, 1);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeMap.java
  static INodeMap newInstance(INodeDirectory rootDir) {
    int capacity = LightWeightGSet.computeCapacity(1, "INodeMap");
    GSet<INode, INodeWithAdditionalFields> map =
        new LightWeightGSet<>(capacity);
    map.put(rootDir);
    return new INodeMap(map);
  }
      @Override
      public QuotaCounts computeQuotaUsage(
          BlockStoragePolicySuite bsps, byte blockStoragePolicyId,
          boolean useCache, int lastSnapshotId) {
        return null;
      }

      }
      
      @Override
      public void cleanSubtree(
          ReclaimContext reclaimContext, int snapshotId, int priorSnapshotId) {
      }

      @Override

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeReference.java

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import com.google.common.base.Preconditions;

  }

  @Override // used by WithCount
  public void cleanSubtree(
      ReclaimContext reclaimContext, int snapshot, int prior) {
    referred.cleanSubtree(reclaimContext, snapshot, prior);
  }

  @Override // used by WithCount
  }

  @Override
  public QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
      byte blockStoragePolicyId, boolean useCache, int lastSnapshotId) {
    return referred.computeQuotaUsage(bsps, blockStoragePolicyId, useCache,
        lastSnapshotId);
  }

  @Override
  public static class WithCount extends INodeReference {

    private final List<WithName> withNameList = new ArrayList<>();

    public final ContentSummaryComputationContext computeContentSummary(
        ContentSummaryComputationContext summary) {
      final QuotaCounts q = computeQuotaUsage(
          summary.getBlockStoragePolicySuite(), getStoragePolicyID(), false,
          lastSnapshotId);
      summary.getCounts().addContent(Content.DISKSPACE, q.getStorageSpace());
      summary.getCounts().addTypeSpaces(q.getTypeSpaces());
      return summary;

    @Override
    public final QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
        byte blockStoragePolicyId, boolean useCache, int lastSnapshotId) {
      int id = lastSnapshotId != Snapshot.CURRENT_STATE_ID ? 
          lastSnapshotId : this.lastSnapshotId;
      return referred.computeQuotaUsage(bsps, blockStoragePolicyId, false, id);
    }
    
    @Override
    public void cleanSubtree(ReclaimContext reclaimContext, final int snapshot,
        int prior) {
      Preconditions.checkArgument(snapshot != Snapshot.CURRENT_STATE_ID);
      
      if (prior != Snapshot.NO_SNAPSHOT_ID
          && Snapshot.ID_INTEGER_COMPARATOR.compare(snapshot, prior) <= 0) {
        return;
      }

      QuotaCounts old = reclaimContext.quotaDelta().getCountsCopy();
      getReferredINode().cleanSubtree(reclaimContext, snapshot, prior);
      INodeReference ref = getReferredINode().getParentReference();
      if (ref != null) {
        QuotaCounts current = reclaimContext.quotaDelta().getCountsCopy();
        current.subtract(old);
        reclaimContext.quotaDelta().addUpdatePath(ref, current);
      }
      
      if (snapshot < lastSnapshotId) {
        reclaimContext.quotaDelta().setCounts(old);
      }
    }
    
    @Override
    public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
      int snapshot = getSelfSnapshot();
      reclaimContext.quotaDelta().add(computeQuotaUsage(reclaimContext.bsps));
      if (removeReference(this) <= 0) {
        getReferredINode().destroyAndCollectBlocks(reclaimContext.getCopy());
      } else {
        int prior = getPriorSnapshot(this);
        INode referred = getReferredINode().asReference().getReferredINode();
            return;
          }
          ReclaimContext newCtx = reclaimContext.getCopy();
          referred.cleanSubtree(newCtx, snapshot, prior);
          INodeReference ref = getReferredINode().getParentReference();
          if (ref != null) {
            reclaimContext.quotaDelta().addUpdatePath(ref,
                newCtx.quotaDelta().getCountsCopy());
          }
        }
      }
    }
    
    @Override
    public void cleanSubtree(ReclaimContext reclaimContext, int snapshot,
        int prior) {
      if (snapshot == Snapshot.CURRENT_STATE_ID
          && prior == Snapshot.NO_SNAPSHOT_ID) {
        destroyAndCollectBlocks(reclaimContext);
      } else {
        if (snapshot != Snapshot.CURRENT_STATE_ID
            && prior != Snapshot.NO_SNAPSHOT_ID
            && Snapshot.ID_INTEGER_COMPARATOR.compare(snapshot, prior) <= 0) {
          return;
        }
        getReferredINode().cleanSubtree(reclaimContext, snapshot, prior);
      }
    }
    
    @Override
    public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
      reclaimContext.quotaDelta().add(computeQuotaUsage(reclaimContext.bsps));
      ReclaimContext newCtx = reclaimContext.getCopy();

      if (removeReference(this) <= 0) {
        getReferredINode().destroyAndCollectBlocks(newCtx);
      } else {
          referred.cleanSubtree(newCtx, snapshot, prior);
        } else if (referred.isDirectory()) {
          INodeDirectory dir = referred.asDirectory();
          Preconditions.checkState(dir.isWithSnapshot());
          DirectoryWithSnapshotFeature.destroyDstSubtree(newCtx, dir,
              snapshot, prior);
        }
      }
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeSymlink.java
  }
  
  @Override
  public void cleanSubtree(ReclaimContext reclaimContext, final int snapshotId,
      int priorSnapshotId) {
    if (snapshotId == Snapshot.CURRENT_STATE_ID
        && priorSnapshotId == Snapshot.NO_SNAPSHOT_ID) {
      destroyAndCollectBlocks(reclaimContext);
    }
  }
  
  @Override
  public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
    reclaimContext.removedINodes.add(this);
    reclaimContext.quotaDelta().add(
        new QuotaCounts.Builder().nameSpace(1).build());
  }

  @Override
  public QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
      byte blockStoragePolicyId, boolean useCache, int lastSnapshotId) {
    return new QuotaCounts.Builder().nameSpace(1).build();
  }

  @Override
    out.println();
  }

  @Override
  public void removeAclFeature() {
    throw new UnsupportedOperationException("ACLs are not supported on symlinks");

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/QuotaCounts.java
    this.tsCounts = builder.tsCounts;
  }

  public QuotaCounts add(QuotaCounts that) {
    this.nsSsCounts.add(that.nsSsCounts);
    this.tsCounts.add(that.tsCounts);
    return this;
  }

  public QuotaCounts subtract(QuotaCounts that) {
    this.nsSsCounts.subtract(that.nsSsCounts);
    this.tsCounts.subtract(that.tsCounts);
    return this;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/AbstractINodeDiff.java

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat.ReferenceMap;

import com.google.common.base.Preconditions;
  }

  abstract void combinePosteriorAndCollectBlocks(
      INode.ReclaimContext reclaimContext, final N currentINode,
      final D posterior);
  
  abstract void destroyDiffAndCollectBlocks(INode.ReclaimContext reclaimContext,
      final N currentINode);

  @Override
  public String toString() {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/AbstractINodeDiffList.java

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;

  public final void deleteSnapshotDiff(INode.ReclaimContext reclaimContext,
      final int snapshot, final int prior, final N currentINode) {
    int snapshotIndex = Collections.binarySearch(diffs, snapshot);

    D removed;
    if (snapshotIndex == 0) {
      if (prior != Snapshot.NO_SNAPSHOT_ID) { // there is still snapshot before
        diffs.get(snapshotIndex).setSnapshotId(prior);
      } else { // there is no snapshot before
        removed = diffs.remove(0);
        removed.destroyDiffAndCollectBlocks(reclaimContext, currentINode);
      }
    } else if (snapshotIndex > 0) {
      final AbstractINodeDiff<N, A, D> previous = diffs.get(snapshotIndex - 1);
          previous.snapshotINode = removed.snapshotINode;
        }

        previous.combinePosteriorAndCollectBlocks(reclaimContext, currentINode,
            removed);
        previous.setPosterior(removed.getPosterior());
        removed.setPosterior(null);
      }
    }
  }

  }

  private D addLast(D diff) {
    final D last = getLast();
    diffs.add(diff);
    if (last != null) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/DirectorySnapshottableFeature.java
import org.apache.hadoop.hdfs.server.namenode.Content;
import org.apache.hadoop.hdfs.server.namenode.ContentSummaryComputationContext;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory.SnapshotAndINode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithName;
import org.apache.hadoop.hdfs.util.Diff.ListType;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.util.Time;
    } else {
      final Snapshot snapshot = snapshotsByNames.get(i);
      int prior = Snapshot.findLatestSnapshot(snapshotRoot, snapshot.getId());
      snapshotRoot.cleanSubtree(reclaimContext, snapshot.getId(), prior);
      snapshotsByNames.remove(i);
      return snapshot;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/DirectoryWithSnapshotFeature.java

import com.google.common.base.Preconditions;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.NO_SNAPSHOT_ID;

    }

    private void destroyCreatedList(INode.ReclaimContext reclaimContext,
        final INodeDirectory currentINode) {
      final List<INode> createdList = getList(ListType.CREATED);
      for (INode c : createdList) {
        c.destroyAndCollectBlocks(reclaimContext);
        currentINode.removeChild(c);
      }
      createdList.clear();
    }

    private void destroyDeletedList(INode.ReclaimContext reclaimContext) {
      final List<INode> deletedList = getList(ListType.DELETED);
      for (INode d : deletedList) {
        d.destroyAndCollectBlocks(reclaimContext);
      }
      deletedList.clear();
    }

    }

    @Override
    void combinePosteriorAndCollectBlocks(
        final INode.ReclaimContext reclaimContext,
        final INodeDirectory currentDir,
        final DirectoryDiff posterior) {
      diff.combinePosterior(posterior.diff, new Diff.Processor<INode>() {
        @Override
        public void process(INode inode) {
          if (inode != null) {
            inode.destroyAndCollectBlocks(reclaimContext);
          }
        }
      });
    }

    }

    @Override
    void destroyDiffAndCollectBlocks(
        INode.ReclaimContext reclaimContext, INodeDirectory currentINode) {
      diff.destroyDeletedList(reclaimContext);
      INodeDirectoryAttributes snapshotINode = getSnapshotINode();
      if (snapshotINode != null && snapshotINode.getAclFeature() != null) {
        AclStorage.removeAclFeature(snapshotINode.getAclFeature());
      }
    }
  }

          return diffList.get(i).getSnapshotId();
        }
      }
      return NO_SNAPSHOT_ID;
    }
  }
  
    if (diffList == null || diffList.size() == 0) {
      return null;
    }
    Map<INode, INode> map = new HashMap<>(diffList.size());
    for (INode node : diffList) {
      map.put(node, node);
    }
  public static void destroyDstSubtree(INode.ReclaimContext reclaimContext,
      INode inode, final int snapshot, final int prior) {
    Preconditions.checkArgument(prior != NO_SNAPSHOT_ID);
    if (inode.isReference()) {
      if (inode instanceof INodeReference.WithName
          && snapshot != Snapshot.CURRENT_STATE_ID) {
        inode.cleanSubtree(reclaimContext, snapshot, prior);
      } else {
        destroyDstSubtree(reclaimContext,
            inode.asReference().getReferredINode(), snapshot, prior);
      }
    } else if (inode.isFile()) {
      inode.cleanSubtree(reclaimContext, snapshot, prior);
  private static void cleanDeletedINode(INode.ReclaimContext reclaimContext,
      INode inode, final int post, final int prior) {
    Deque<INode> queue = new ArrayDeque<>();
    queue.addLast(inode);
    while (!queue.isEmpty()) {
      INode topNode = queue.pollFirst();
      } else if (topNode.isFile() && topNode.asFile().isWithSnapshot()) {
        INodeFile file = topNode.asFile();
        file.getDiffs().deleteSnapshotDiff(reclaimContext, post, prior, file);
      } else if (topNode.isDirectory()) {
        INodeDirectory dir = topNode.asDirectory();
        ChildrenDiff priorChildrenDiff = null;
          DirectoryDiff priorDiff = sf.getDiffs().getDiffById(prior);
          if (priorDiff != null && priorDiff.getSnapshotId() == prior) {
            priorChildrenDiff = priorDiff.getChildrenDiff();
            priorChildrenDiff.destroyCreatedList(reclaimContext, dir);
          }
        }

        for (INode child : dir.getChildrenList(prior)) {
          if (priorChildrenDiff != null && priorChildrenDiff.search(
              ListType.DELETED, child.getLocalNameBytes()) != null) {
            continue;
          }
          queue.addLast(child);
        }
      }
    }
  }

  }

  public QuotaCounts computeQuotaUsage4CurrentDirectory(
      BlockStoragePolicySuite bsps, byte storagePolicyId) {
    final QuotaCounts counts = new QuotaCounts.Builder().build();
    for(DirectoryDiff d : diffs) {
      for(INode deleted : d.getChildrenDiff().getList(ListType.DELETED)) {
        final byte childPolicyId = deleted.getStoragePolicyIDForQuota(
            storagePolicyId);
        counts.add(deleted.computeQuotaUsage(bsps, childPolicyId, false,
            Snapshot.CURRENT_STATE_ID));
      }
    }
    return counts;
    }
  }

  public void cleanDirectory(INode.ReclaimContext reclaimContext,
      final INodeDirectory currentINode, final int snapshot, int prior) {
    Map<INode, INode> priorCreated = null;
    Map<INode, INode> priorDeleted = null;
    QuotaCounts old = reclaimContext.quotaDelta().getCountsCopy();
    if (snapshot == Snapshot.CURRENT_STATE_ID) { // delete the current directory
      currentINode.recordModification(prior);
      DirectoryDiff lastDiff = diffs.getLast();
      if (lastDiff != null) {
        lastDiff.diff.destroyCreatedList(reclaimContext, currentINode);
      }
      currentINode.cleanSubtreeRecursively(reclaimContext, snapshot, prior,
          null);
    } else {
      prior = getDiffs().updatePrior(snapshot, prior);
      if (prior != NO_SNAPSHOT_ID) {
        DirectoryDiff priorDiff = this.getDiffs().getDiffById(prior);
        if (priorDiff != null && priorDiff.getSnapshotId() == prior) {
          List<INode> cList = priorDiff.diff.getList(ListType.CREATED);
        }
      }

      getDiffs().deleteSnapshotDiff(reclaimContext, snapshot, prior,
          currentINode);
      currentINode.cleanSubtreeRecursively(reclaimContext, snapshot, prior,
          priorDeleted);

      if (prior != NO_SNAPSHOT_ID) {
        DirectoryDiff priorDiff = this.getDiffs().getDiffById(prior);
        if (priorDiff != null && priorDiff.getSnapshotId() == prior) {
            for (INode cNode : priorDiff.getChildrenDiff().getList(
                ListType.CREATED)) {
              if (priorCreated.containsKey(cNode)) {
                cNode.cleanSubtree(reclaimContext, snapshot, NO_SNAPSHOT_ID);
              }
            }
          }
          for (INode dNode : priorDiff.getChildrenDiff().getList(
              ListType.DELETED)) {
            if (priorDeleted == null || !priorDeleted.containsKey(dNode)) {
              cleanDeletedINode(reclaimContext, dNode, snapshot, prior);
            }
          }
        }
      }
    }

    QuotaCounts current = reclaimContext.quotaDelta().getCountsCopy();
    current.subtract(old);
    if (currentINode.isQuotaSet()) {
      reclaimContext.quotaDelta().addQuotaDirUpdate(currentINode, current);
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FileDiff.java
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileAttributes;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat.ReferenceMap;

  }

  @Override
  void combinePosteriorAndCollectBlocks(
      INode.ReclaimContext reclaimContext, INodeFile currentINode,
      FileDiff posterior) {
    FileWithSnapshotFeature sf = currentINode.getFileWithSnapshotFeature();
    assert sf != null : "FileWithSnapshotFeature is null";
    sf.updateQuotaAndCollectBlocks(reclaimContext, currentINode, posterior);
  }
  
  @Override
  }

  @Override
  void destroyDiffAndCollectBlocks(INode.ReclaimContext reclaimContext,
      INodeFile currentINode) {
    currentINode.getFileWithSnapshotFeature().updateQuotaAndCollectBlocks(
        reclaimContext, currentINode, this);
  }

  public void destroyAndCollectSnapshotBlocks(
      BlocksMapUpdateInfo collectedBlocks) {
    if (blocks == null || collectedBlocks == null) {
      return;
    }
    for (BlockInfoContiguous blk : blocks) {
      collectedBlocks.addDeleteBlock(blk);
    }
    blocks = null;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FileDiffList.java

  public void destroyAndCollectSnapshotBlocks(
      BlocksMapUpdateInfo collectedBlocks) {
    for (FileDiff d : asList()) {
      d.destroyAndCollectSnapshotBlocks(collectedBlocks);
    }
  }

  public void saveSelf2Snapshot(int latestSnapshotId, INodeFile iNodeFile,
      INodeFileAttributes snapshotCopy, boolean withBlocks) {
    final FileDiff diff =
        super.saveSelf2Snapshot(latestSnapshotId, iNodeFile, snapshotCopy);
    if (withBlocks) {  // Store blocks if this is the first update
      diff.setBlocks(iNodeFile.getBlocks());
    }
  }

  public BlockInfoContiguous[] findEarlierSnapshotBlocks(int snapshotId) {
    assert snapshotId != Snapshot.NO_SNAPSHOT_ID : "Wrong snapshot id";
    int p = getPrior(removed.getSnapshotId(), true);
    FileDiff earlierDiff = p == Snapshot.NO_SNAPSHOT_ID ? null : getDiffById(p);
    if (earlierDiff != null) {
      earlierDiff.setBlocks(removedBlocks);
    }
    BlockInfoContiguous[] earlierBlocks =
        (earlierDiff == null ? new BlockInfoContiguous[]{} : earlierDiff.getBlocks());

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FileWithSnapshotFeature.java
    return (isCurrentFileDeleted()? "(DELETED), ": ", ") + diffs;
  }
  
  public void cleanFile(INode.ReclaimContext reclaimContext,
      final INodeFile file, final int snapshotId, int priorSnapshotId,
      byte storagePolicyId) {
    if (snapshotId == Snapshot.CURRENT_STATE_ID) {
      if (!isCurrentFileDeleted()) {
        file.recordModification(priorSnapshotId);
        deleteCurrentFile();
      }
      final BlockStoragePolicy policy = reclaimContext.storagePolicySuite()
          .getPolicy(storagePolicyId);
      QuotaCounts old = file.storagespaceConsumed(policy);
      collectBlocksAndClear(reclaimContext, file);
      QuotaCounts current = file.storagespaceConsumed(policy);
      reclaimContext.quotaDelta().add(old.subtract(current));
    } else { // delete the snapshot
      priorSnapshotId = getDiffs().updatePrior(snapshotId, priorSnapshotId);
      diffs.deleteSnapshotDiff(reclaimContext, snapshotId, priorSnapshotId,
          file);
    }
  }
  
    this.diffs.clear();
  }
  
  public void updateQuotaAndCollectBlocks(INode.ReclaimContext reclaimContext,
      INodeFile file, FileDiff removed) {
    byte storagePolicyID = file.getStoragePolicyID();
    BlockStoragePolicy bsp = null;
    if (storagePolicyID != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
    getDiffs().combineAndCollectSnapshotBlocks(reclaimContext, file, removed);

    QuotaCounts current = file.storagespaceConsumed(bsp);
    reclaimContext.quotaDelta().add(oldCounts.subtract(current));
  }

      INode.ReclaimContext reclaimContext, final INodeFile file) {
    if (isCurrentFileDeleted() && getDiffs().asList().isEmpty()) {
      file.clearFile(reclaimContext);
      return;
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/SnapshotManager.java
import org.apache.hadoop.hdfs.server.namenode.FSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.metrics2.util.MBeans;
      throw new SnapshotException(
          "Failed to create the snapshot. The FileSystem has run out of " +
          "snapshot IDs and ID rollover is not supported.");
  public void deleteSnapshot(final INodesInPath iip, final String snapshotName,
      INode.ReclaimContext reclaimContext) throws IOException {
    INodeDirectory srcRoot = getSnapshottableRoot(iip);
    srcRoot.removeSnapshot(reclaimContext, snapshotName);
    numSnapshots.getAndDecrement();
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestQuotaByStorageType.java
  private static final long seed = 0L;
  private static final Path dir = new Path("/TestQuotaByStorageType");

  private MiniDFSCluster cluster;
  private FSDirectory fsdir;
  private DistributedFileSystem dfs;

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);

        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(0, storageTypeConsumed);

    QuotaCounts counts = fnode.computeQuotaUsage(
        fsn.getBlockManager().getStoragePolicySuite(), true);
    assertEquals(fnode.dumpTreeRecursively().toString(), 0,
        counts.getTypeSpaces().get(StorageType.SSD));

    assertEquals(0, cntAfterDelete.getStorageSpace());

    QuotaCounts counts = fnode.computeQuotaUsage(
        fsn.getBlockManager().getStoragePolicySuite(), true);
    assertEquals(fnode.dumpTreeRecursively().toString(), 1,
        counts.getNameSpace());
    assertEquals(fnode.dumpTreeRecursively().toString(), 0,
        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumed);

    QuotaCounts counts1 = sub1Node.computeQuotaUsage(
        fsn.getBlockManager().getStoragePolicySuite(), true);
    assertEquals(sub1Node.dumpTreeRecursively().toString(), file1Len,
        counts1.getTypeSpaces().get(StorageType.SSD));

        .getSpaceConsumed().getTypeSpaces().get(StorageType.SSD);
    assertEquals(0, ssdConsumed);

    QuotaCounts counts2 = sub1Node.computeQuotaUsage(
        fsn.getBlockManager().getStoragePolicySuite(), true);
    assertEquals(sub1Node.dumpTreeRecursively().toString(), 0,
        counts2.getTypeSpaces().get(StorageType.SSD));

    assertEquals(file1Len, ssdConsumed);

    int newFile1Len = BLOCKSIZE;
    dfs.truncate(createdFile1, newFile1Len);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestFileWithSnapshotFeature.java
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import com.google.common.collect.Lists;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.QuotaCounts;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

    ArrayList<INode> removedINodes = new ArrayList<>();
    INode.ReclaimContext ctx = new INode.ReclaimContext(
        bsps, collectedBlocks, removedINodes, null);
    sf.updateQuotaAndCollectBlocks(ctx, file, diff);
    QuotaCounts counts = ctx.quotaDelta().getCountsCopy();
    Assert.assertEquals(0, counts.getStorageSpace());
    Assert.assertTrue(counts.getTypeSpaces().allLessOrEqual(0));

        .thenReturn(Lists.newArrayList(SSD));
    when(bsp.chooseStorageTypes(REPL_3))
        .thenReturn(Lists.newArrayList(DISK));
    sf.updateQuotaAndCollectBlocks(ctx, file, diff);
    counts = ctx.quotaDelta().getCountsCopy();
    Assert.assertEquals((REPL_3 - REPL_1) * BLOCK_SIZE,
                        counts.getStorageSpace());
    Assert.assertEquals(BLOCK_SIZE, counts.getTypeSpaces().get(DISK));

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestRenameWithSnapshots.java
    
    hdfs.delete(foo_dir1, true);
    restartClusterAndCheckImage(true);
    hdfs.delete(bar2_dir1, true);
    

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestSnapshotDeletion.java
        q.getNameSpace());
    assertEquals(dirNode.dumpTreeRecursively().toString(), expectedDs,
        q.getStorageSpace());
    QuotaCounts counts = dirNode.computeQuotaUsage(fsdir.getBlockStoragePolicySuite(), false);
    assertEquals(dirNode.dumpTreeRecursively().toString(), expectedNs,
        counts.getNameSpace());
    assertEquals(dirNode.dumpTreeRecursively().toString(), expectedDs,
    DFSTestUtil.createFile(hdfs, metaChangeFile2, BLOCKSIZE, REPLICATION, seed);
    
    hdfs.setQuota(dir, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);
    checkQuotaUsageComputation(dir, 10, BLOCKSIZE * REPLICATION * 4);
    hdfs.delete(deleteDir, true);
    checkQuotaUsageComputation(dir, 8, BLOCKSIZE * REPLICATION * 3);

    SnapshotTestHelper.createSnapshot(hdfs, dir, "s0");
    
    try {
      hdfs.getFileStatus(toDeleteFile);
      fail("should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      GenericTestUtils.assertExceptionContains("File does not exist: "
    final Path toDeleteFileInSnapshot = SnapshotTestHelper.getSnapshotPath(dir,
        "s0", toDeleteFile.toString().substring(dir.toString().length()));
    try {
      hdfs.getFileStatus(toDeleteFileInSnapshot);
      fail("should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      GenericTestUtils.assertExceptionContains("File does not exist: "

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestSnapshotManager.java
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INode;

    sm.deleteSnapshot(iip, "", mock(INode.ReclaimContext.class));


