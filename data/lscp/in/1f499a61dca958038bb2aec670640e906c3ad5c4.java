hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirDeleteOp.java
    }

    INode.ReclaimContext reclaimContext = new INode.ReclaimContext(
        fsd.getBlockStoragePolicySuite(), collectedBlocks,
        removedINodes, removedUCFiles);
    if (!targetNode.isInLatestSnapshot(latestSnapshot)) {
      targetNode.destroyAndCollectBlocks(reclaimContext);
    } else {
      QuotaCounts counts = targetNode.cleanSubtree(reclaimContext,
          CURRENT_STATE_ID, latestSnapshot);
      removed = counts.getNameSpace();
      fsd.updateCountNoQuotaCheck(iip, iip.length() -1, counts.negation());
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirRenameOp.java
      List<Long> removedUCFiles = new ChunkedArrayList<>();
      final boolean filesDeleted;
      if (!oldDstChild.isInLatestSnapshot(dstIIP.getLatestSnapshotId())) {
        oldDstChild.destroyAndCollectBlocks(
            new INode.ReclaimContext(bsps, collectedBlocks, removedINodes, removedUCFiles));
        filesDeleted = true;
      } else {
        filesDeleted = oldDstChild.cleanSubtree(
            new INode.ReclaimContext(bsps, collectedBlocks, removedINodes,
                                     removedUCFiles),
            Snapshot.CURRENT_STATE_ID,
            dstIIP.getLatestSnapshotId())
            .getNameSpace() >= 0;
      }
      fsd.getFSNamesystem().removeLeasesAndINodes(
          removedUCFiles, removedINodes, false);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INode.java
  public abstract QuotaCounts cleanSubtree(
      ReclaimContext reclaimContext, final int snapshotId, int priorSnapshotId);
  
  public abstract void destroyAndCollectBlocks(ReclaimContext reclaimContext);

  public final ContentSummary computeContentSummary(BlockStoragePolicySuite bsps) {
    out.print(", " + getPermissionStatus(snapshotId));
  }

  public static class ReclaimContext {
    protected final BlockStoragePolicySuite bsps;
    protected final BlocksMapUpdateInfo collectedBlocks;
    protected final List<INode> removedINodes;
    protected final List<Long> removedUCFiles;
    public ReclaimContext(
        BlockStoragePolicySuite bsps, BlocksMapUpdateInfo collectedBlocks,
        List<INode> removedINodes, List<Long> removedUCFiles) {
      this.bsps = bsps;
      this.collectedBlocks = collectedBlocks;
      this.removedINodes = removedINodes;
      this.removedUCFiles = removedUCFiles;
    }

    public BlockStoragePolicySuite storagePolicySuite() {
      return bsps;
    }

    public BlocksMapUpdateInfo collectedBlocks() {
      return collectedBlocks;
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeDirectory.java
    return getDirectorySnapshottableFeature().addSnapshot(this, id, name);
  }

  public Snapshot removeSnapshot(
      ReclaimContext reclaimContext, String snapshotName)
      throws SnapshotException {
    return getDirectorySnapshottableFeature().removeSnapshot(
        reclaimContext, this, snapshotName);
  }

  public void renameSnapshot(String path, String oldName, String newName)

  public QuotaCounts cleanSubtreeRecursively(
      ReclaimContext reclaimContext, final int snapshot, int prior,
      final Map<INode, INode> excludedNodes) {
    QuotaCounts counts = new QuotaCounts.Builder().build();
          && excludedNodes.containsKey(child)) {
        continue;
      } else {
        QuotaCounts childCounts = child.cleanSubtree(reclaimContext, snapshot, prior);
        counts.add(childCounts);
      }
    }
  }

  @Override
  public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
    final DirectoryWithSnapshotFeature sf = getDirectoryWithSnapshotFeature();
    if (sf != null) {
      sf.clear(reclaimContext, this);
    }
    for (INode child : getChildrenList(Snapshot.CURRENT_STATE_ID)) {
      child.destroyAndCollectBlocks(reclaimContext);
    }
    if (getAclFeature() != null) {
      AclStorage.removeAclFeature(getAclFeature());
    }
    clear();
    reclaimContext.removedINodes.add(this);
  }
  
  @Override
  public QuotaCounts cleanSubtree(
      ReclaimContext reclaimContext, final int snapshotId, int priorSnapshotId) {
    DirectoryWithSnapshotFeature sf = getDirectoryWithSnapshotFeature();
    if (sf != null) {
      return sf.cleanDirectory(reclaimContext, this, snapshotId,
                               priorSnapshotId);
    }
    if (priorSnapshotId == Snapshot.NO_SNAPSHOT_ID
        && snapshotId == Snapshot.CURRENT_STATE_ID) {
      QuotaCounts counts = new QuotaCounts.Builder().build();
      this.computeQuotaUsage(reclaimContext.bsps, counts, true);
      destroyAndCollectBlocks(reclaimContext);
      return counts; 
    } else {
      QuotaCounts counts = cleanSubtreeRecursively(
          reclaimContext, snapshotId, priorSnapshotId, null);
      if (isQuotaSet()) {
        getDirectoryWithQuotaFeature().addSpaceConsumed2Cache(counts.negation());
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeFile.java

  @Override
  public QuotaCounts cleanSubtree(
      ReclaimContext reclaimContext, final int snapshot, int priorSnapshotId) {
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      return sf.cleanFile(reclaimContext, this, snapshot, priorSnapshotId);
    }
    QuotaCounts counts = new QuotaCounts.Builder().build();

      if (priorSnapshotId == NO_SNAPSHOT_ID) {
        computeQuotaUsage(reclaimContext.bsps, counts, false);
        destroyAndCollectBlocks(reclaimContext);
      } else {
        FileUnderConstructionFeature uc = getFileUnderConstructionFeature();
        if (uc != null) {
          uc.cleanZeroSizeBlock(this, reclaimContext.collectedBlocks);
          if (reclaimContext.removedUCFiles != null) {
            reclaimContext.removedUCFiles.add(getId());
          }
        }
      }
  }

  @Override
  public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
    if (blocks != null && reclaimContext.collectedBlocks != null) {
      for (BlockInfoContiguous blk : blocks) {
        reclaimContext.collectedBlocks.addDeleteBlock(blk);
        blk.setBlockCollection(null);
      }
    }
      AclStorage.removeAclFeature(getAclFeature());
    }
    clear();
    reclaimContext.removedINodes.add(this);
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


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeMap.java
package org.apache.hadoop.hdfs.server.namenode;

import java.util.Iterator;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
      }
      
      @Override
      public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
      }

      
      @Override
      public QuotaCounts cleanSubtree(
          ReclaimContext reclaimContext, int snapshotId, int priorSnapshotId) {
          return null;
      }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeReference.java

  @Override // used by WithCount
  public QuotaCounts cleanSubtree(
      ReclaimContext reclaimContext, int snapshot, int prior) {
    return referred.cleanSubtree(reclaimContext,
                                 snapshot, prior);
  }

  @Override // used by WithCount
  public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
    if (removeReference(this) <= 0) {
      referred.destroyAndCollectBlocks(reclaimContext);
    }
  }

    
    @Override
    public QuotaCounts cleanSubtree(
        ReclaimContext reclaimContext, final int snapshot, int prior) {
      Preconditions.checkArgument(snapshot != Snapshot.CURRENT_STATE_ID);
        return new QuotaCounts.Builder().build();
      }

      QuotaCounts counts = getReferredINode().cleanSubtree(reclaimContext,
          snapshot, prior);
      INodeReference ref = getReferredINode().getParentReference();
      if (ref != null) {
        try {
    }
    
    @Override
    public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
      int snapshot = getSelfSnapshot();
      if (removeReference(this) <= 0) {
        getReferredINode().destroyAndCollectBlocks(reclaimContext);
      } else {
        int prior = getPriorSnapshot(this);
        INode referred = getReferredINode().asReference().getReferredINode();
            return;
          }
          try {
            QuotaCounts counts = referred.cleanSubtree(reclaimContext,
                snapshot, prior);
            INodeReference ref = getReferredINode().getParentReference();
            if (ref != null) {
              ref.addSpaceConsumed(counts.negation(), true);
    
    @Override
    public QuotaCounts cleanSubtree(
        ReclaimContext reclaimContext, int snapshot, int prior) {
      if (snapshot == Snapshot.CURRENT_STATE_ID
          && prior == Snapshot.NO_SNAPSHOT_ID) {
        QuotaCounts counts = new QuotaCounts.Builder().build();
        this.computeQuotaUsage(reclaimContext.bsps, counts, true);
        destroyAndCollectBlocks(reclaimContext);
        return counts;
      } else {
            && Snapshot.ID_INTEGER_COMPARATOR.compare(snapshot, prior) <= 0) {
          return new QuotaCounts.Builder().build();
        }
        return getReferredINode().cleanSubtree(reclaimContext, snapshot, prior);
      }
    }
    
    @Override
    public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
      if (removeReference(this) <= 0) {
        getReferredINode().destroyAndCollectBlocks(reclaimContext);
      } else {
          referred.cleanSubtree(reclaimContext, snapshot, prior);
        } else if (referred.isDirectory()) {
          INodeDirectory dir = referred.asDirectory();
          Preconditions.checkState(dir.isWithSnapshot());
          try {
            DirectoryWithSnapshotFeature.destroyDstSubtree(
                reclaimContext, dir, snapshot, prior);
          } catch (QuotaExceededException e) {
            LOG.error("should not exceed quota while snapshot deletion", e);
          }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeSymlink.java
package org.apache.hadoop.hdfs.server.namenode;

import java.io.PrintWriter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.PermissionStatus;
  
  @Override
  public QuotaCounts cleanSubtree(
      ReclaimContext reclaimContext, final int snapshotId, int priorSnapshotId) {
    if (snapshotId == Snapshot.CURRENT_STATE_ID
        && priorSnapshotId == Snapshot.NO_SNAPSHOT_ID) {
      destroyAndCollectBlocks(reclaimContext);
    }
    return new QuotaCounts.Builder().nameSpace(1).build();
  }
  
  @Override
  public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
    reclaimContext.removedINodes.add(this);
  }

  @Override

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/AbstractINodeDiff.java

  abstract QuotaCounts combinePosteriorAndCollectBlocks(
      INode.ReclaimContext reclaimContext, final N currentINode,
      final D posterior);
  
  abstract QuotaCounts destroyDiffAndCollectBlocks(
      INode.ReclaimContext reclaimContext, final N currentINode);

  @Override
  public String toString() {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/AbstractINodeDiffList.java
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.QuotaCounts;

  public final QuotaCounts deleteSnapshotDiff(
      INode.ReclaimContext reclaimContext, final int snapshot, final int prior,
      final N currentINode) {
    int snapshotIndex = Collections.binarySearch(diffs, snapshot);
    
    QuotaCounts counts = new QuotaCounts.Builder().build();
        diffs.get(snapshotIndex).setSnapshotId(prior);
      } else { // there is no snapshot before
        removed = diffs.remove(0);
        counts.add(removed.destroyDiffAndCollectBlocks(reclaimContext,
            currentINode));
      }
    } else if (snapshotIndex > 0) {
      final AbstractINodeDiff<N, A, D> previous = diffs.get(snapshotIndex - 1);
          previous.snapshotINode = removed.snapshotINode;
        }

        counts.add(previous.combinePosteriorAndCollectBlocks(reclaimContext,
            currentINode, removed));
        previous.setPosterior(removed.getPosterior());
        removed.setPosterior(null);
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/DirectorySnapshottableFeature.java
  public Snapshot removeSnapshot(
      INode.ReclaimContext reclaimContext, INodeDirectory snapshotRoot,
      String snapshotName) throws SnapshotException {
    final int i = searchSnapshot(DFSUtil.string2Bytes(snapshotName));
    if (i < 0) {
      throw new SnapshotException("Cannot delete snapshot " + snapshotName
      final Snapshot snapshot = snapshotsByNames.get(i);
      int prior = Snapshot.findLatestSnapshot(snapshotRoot, snapshot.getId());
      try {
        QuotaCounts counts = snapshotRoot.cleanSubtree(reclaimContext,
            snapshot.getId(), prior);
        INodeDirectory parent = snapshotRoot.getParent();
        if (parent != null) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/DirectoryWithSnapshotFeature.java
import org.apache.hadoop.hdfs.server.namenode.ContentSummaryComputationContext;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

    private QuotaCounts destroyCreatedList(
        INode.ReclaimContext reclaimContext, final INodeDirectory currentINode) {
      QuotaCounts counts = new QuotaCounts.Builder().build();
      final List<INode> createdList = getList(ListType.CREATED);
      for (INode c : createdList) {
        c.computeQuotaUsage(reclaimContext.storagePolicySuite(), counts, true);
        c.destroyAndCollectBlocks(reclaimContext);
        currentINode.removeChild(c);
      }
    }

    private QuotaCounts destroyDeletedList(INode.ReclaimContext reclaimContext) {
      QuotaCounts counts = new QuotaCounts.Builder().build();
      final List<INode> deletedList = getList(ListType.DELETED);
      for (INode d : deletedList) {
        d.computeQuotaUsage(reclaimContext.storagePolicySuite(), counts, false);
        d.destroyAndCollectBlocks(reclaimContext);
      }
      deletedList.clear();
      return counts;

    @Override
    QuotaCounts combinePosteriorAndCollectBlocks(
        final INode.ReclaimContext reclaimContext,
        final INodeDirectory currentDir,
        final DirectoryDiff posterior) {
      final QuotaCounts counts = new QuotaCounts.Builder().build();
      diff.combinePosterior(posterior.diff, new Diff.Processor<INode>() {
        @Override
        public void process(INode inode) {
          if (inode != null) {
            inode.computeQuotaUsage(reclaimContext.storagePolicySuite(), counts, false);
            inode.destroyAndCollectBlocks(reclaimContext);
          }
        }
      });

    @Override
    QuotaCounts destroyDiffAndCollectBlocks(
        INode.ReclaimContext reclaimContext, INodeDirectory currentINode) {
      QuotaCounts counts = new QuotaCounts.Builder().build();
      counts.add(diff.destroyDeletedList(reclaimContext));
      INodeDirectoryAttributes snapshotINode = getSnapshotINode();
      if (snapshotINode != null && snapshotINode.getAclFeature() != null) {
        AclStorage.removeAclFeature(snapshotINode.getAclFeature());
  public static void destroyDstSubtree(
      INode.ReclaimContext reclaimContext, INode inode, final int snapshot,
      final int prior) throws QuotaExceededException {
    Preconditions.checkArgument(prior != Snapshot.NO_SNAPSHOT_ID);
    if (inode.isReference()) {
      if (inode instanceof INodeReference.WithName
          && snapshot != Snapshot.CURRENT_STATE_ID) {
        inode.cleanSubtree(reclaimContext,
            snapshot, prior);
      } else { 
        destroyDstSubtree(reclaimContext,
                          inode.asReference().getReferredINode(), snapshot,
                          prior);
      }
    } else if (inode.isFile()) {
      inode.cleanSubtree(reclaimContext, snapshot, prior);
    } else if (inode.isDirectory()) {
      Map<INode, INode> excludedNodes = null;
      INodeDirectory dir = inode.asDirectory();
        }
        
        if (snapshot != Snapshot.CURRENT_STATE_ID) {
          diffList.deleteSnapshotDiff(reclaimContext,
              snapshot, prior, dir);
        }
        priorDiff = diffList.getDiffById(prior);
        if (priorDiff != null && priorDiff.getSnapshotId() == prior) {
          priorDiff.diff.destroyCreatedList(reclaimContext, dir);
        }
      }
      for (INode child : inode.asDirectory().getChildrenList(prior)) {
        if (excludedNodes != null && excludedNodes.containsKey(child)) {
          continue;
        }
        destroyDstSubtree(reclaimContext, child, snapshot, prior);
      }
    }
  }
  private static QuotaCounts cleanDeletedINode(
      INode.ReclaimContext reclaimContext, INode inode, final int post,
      final int prior) {
    QuotaCounts counts = new QuotaCounts.Builder().build();
    Deque<INode> queue = new ArrayDeque<INode>();
    queue.addLast(inode);
      if (topNode instanceof INodeReference.WithName) {
        INodeReference.WithName wn = (INodeReference.WithName) topNode;
        if (wn.getLastSnapshotId() >= post) {
          wn.cleanSubtree(reclaimContext, post, prior);
        }
      } else if (topNode.isFile() && topNode.asFile().isWithSnapshot()) {
        INodeFile file = topNode.asFile();
        counts.add(file.getDiffs().deleteSnapshotDiff(reclaimContext, post, prior, file));
      } else if (topNode.isDirectory()) {
        INodeDirectory dir = topNode.asDirectory();
        ChildrenDiff priorChildrenDiff = null;
          DirectoryDiff priorDiff = sf.getDiffs().getDiffById(prior);
          if (priorDiff != null && priorDiff.getSnapshotId() == prior) {
            priorChildrenDiff = priorDiff.getChildrenDiff();
            counts.add(priorChildrenDiff.destroyCreatedList(reclaimContext,
                dir));
          }
        }
        
    return child;
  }

  public void clear(
      INode.ReclaimContext reclaimContext, INodeDirectory currentINode) {
    for (DirectoryDiff diff : diffs) {
      diff.destroyDiffAndCollectBlocks(reclaimContext, currentINode);
    }
    diffs.clear();
  }
  }

  public QuotaCounts cleanDirectory(
      INode.ReclaimContext reclaimContext, final INodeDirectory currentINode,
      final int snapshot, int prior) {
    QuotaCounts counts = new QuotaCounts.Builder().build();
    Map<INode, INode> priorCreated = null;
    Map<INode, INode> priorDeleted = null;
      DirectoryDiff lastDiff = diffs.getLast();
      if (lastDiff != null) {
        counts.add(lastDiff.diff.destroyCreatedList(reclaimContext,
            currentINode));
      }
      counts.add(currentINode.cleanSubtreeRecursively(reclaimContext,
          snapshot, prior, priorDeleted));
    } else {
      prior = getDiffs().updatePrior(snapshot, prior);
        }
      }
      
      counts.add(getDiffs().deleteSnapshotDiff(reclaimContext, snapshot, prior,
          currentINode));
      counts.add(currentINode.cleanSubtreeRecursively(reclaimContext,
          snapshot, prior, priorDeleted));

      if (prior != Snapshot.NO_SNAPSHOT_ID) {
            for (INode cNode : priorDiff.getChildrenDiff().getList(
                ListType.CREATED)) {
              if (priorCreated.containsKey(cNode)) {
                counts.add(cNode.cleanSubtree(reclaimContext,
                    snapshot, Snapshot.NO_SNAPSHOT_ID));
              }
            }
          }
          for (INode dNode : priorDiff.getChildrenDiff().getList(
              ListType.DELETED)) {
            if (priorDeleted == null || !priorDeleted.containsKey(dNode)) {
              counts.add(cleanDeletedINode(reclaimContext,
                  dNode, snapshot, prior));
            }
          }
        }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FileDiff.java
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;

  @Override
  QuotaCounts combinePosteriorAndCollectBlocks(
      INode.ReclaimContext reclaimContext, INodeFile currentINode,
      FileDiff posterior) {
    FileWithSnapshotFeature sf = currentINode.getFileWithSnapshotFeature();
    assert sf != null : "FileWithSnapshotFeature is null";
    return sf.updateQuotaAndCollectBlocks(reclaimContext,
        currentINode, posterior);
  }
  
  @Override
  }

  @Override
  QuotaCounts destroyDiffAndCollectBlocks(
      INode.ReclaimContext reclaimContext, INodeFile currentINode) {
    return currentINode.getFileWithSnapshotFeature()
        .updateQuotaAndCollectBlocks(reclaimContext, currentINode, this);
  }

  public void destroyAndCollectSnapshotBlocks(

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FileDiffList.java
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
  void combineAndCollectSnapshotBlocks(
      INode.ReclaimContext reclaimContext, INodeFile file, FileDiff removed) {
    BlockInfoContiguous[] removedBlocks = removed.getBlocks();
    if(removedBlocks == null) {
      FileWithSnapshotFeature sf = file.getFileWithSnapshotFeature();
      assert sf != null : "FileWithSnapshotFeature is null";
      if(sf.isCurrentFileDeleted())
        sf.collectBlocksAndClear(reclaimContext, file);
      return;
    }
    int p = getPrior(removed.getSnapshotId(), true);
    for(;i < removedBlocks.length; i++) {
      if(dontRemoveBlock == null || !removedBlocks[i].equals(dontRemoveBlock)) {
        reclaimContext.collectedBlocks().addDeleteBlock(removedBlocks[i]);
      }
    }
  }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FileWithSnapshotFeature.java
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.AclStorage;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileAttributes;
    return (isCurrentFileDeleted()? "(DELETED), ": ", ") + diffs;
  }
  
  public QuotaCounts cleanFile(INode.ReclaimContext reclaimContext,
      final INodeFile file, final int snapshotId,
      int priorSnapshotId) {
    if (snapshotId == Snapshot.CURRENT_STATE_ID) {
      if (!isCurrentFileDeleted()) {
        file.recordModification(priorSnapshotId);
        deleteCurrentFile();
      }
      collectBlocksAndClear(reclaimContext, file);
      return new QuotaCounts.Builder().build();
    } else { // delete the snapshot
      priorSnapshotId = getDiffs().updatePrior(snapshotId, priorSnapshotId);
      return diffs.deleteSnapshotDiff(reclaimContext,
          snapshotId, priorSnapshotId, file);
    }
  }
  
    this.diffs.clear();
  }
  
  public QuotaCounts updateQuotaAndCollectBlocks(
      INode.ReclaimContext reclaimContext, INodeFile file, FileDiff removed) {
    byte storagePolicyID = file.getStoragePolicyID();
    BlockStoragePolicy bsp = null;
    if (storagePolicyID != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      bsp = reclaimContext.storagePolicySuite().getPolicy(file.getStoragePolicyID());
    }


      }
    }

    getDiffs().combineAndCollectSnapshotBlocks(reclaimContext, file, removed);

    QuotaCounts current = file.storagespaceConsumed(bsp);
    oldCounts.subtract(current);
  public void collectBlocksAndClear(
      INode.ReclaimContext reclaimContext, final INodeFile file) {
    if (isCurrentFileDeleted() && getDiffs().asList().isEmpty()) {
      file.destroyAndCollectBlocks(reclaimContext);
      return;
    }
    FileDiff last = diffs.getLast();
    BlockInfoContiguous[] snapshotBlocks = last == null ? null : last.getBlocks();
    if(snapshotBlocks == null)
      file.collectBlocksBeyondMax(max, reclaimContext.collectedBlocks());
    else
      file.collectBlocksBeyondSnapshot(snapshotBlocks,
                                       reclaimContext.collectedBlocks());
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/SnapshotManager.java
      BlocksMapUpdateInfo collectedBlocks, final List<INode> removedINodes)
      throws IOException {
    INodeDirectory srcRoot = getSnapshottableRoot(iip);
    srcRoot.removeSnapshot(
        new INode.ReclaimContext(fsdir.getBlockStoragePolicySuite(),
                                 collectedBlocks, removedINodes, null),
        snapshotName);
    numSnapshots.getAndDecrement();
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestFileWithSnapshotFeature.java
    INode.BlocksMapUpdateInfo collectedBlocks = mock(
        INode.BlocksMapUpdateInfo.class);
    ArrayList<INode> removedINodes = new ArrayList<>();
    INode.ReclaimContext ctx = new INode.ReclaimContext(
        bsps, collectedBlocks, removedINodes, null);
    QuotaCounts counts = sf.updateQuotaAndCollectBlocks(ctx, file, diff);
    Assert.assertEquals(0, counts.getStorageSpace());
    Assert.assertTrue(counts.getTypeSpaces().allLessOrEqual(0));

        .thenReturn(Lists.newArrayList(SSD));
    when(bsp.chooseStorageTypes(REPL_3))
        .thenReturn(Lists.newArrayList(DISK));
    counts = sf.updateQuotaAndCollectBlocks(ctx, file, diff);
    Assert.assertEquals((REPL_3 - REPL_1) * BLOCK_SIZE,
                        counts.getStorageSpace());
    Assert.assertEquals(BLOCK_SIZE, counts.getTypeSpaces().get(DISK));

