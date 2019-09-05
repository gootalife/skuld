hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirStatAndListingOp.java
import org.apache.hadoop.fs.DirectoryListingStartAfterNotFoundException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
          .BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;

      if (!targetNode.isDirectory()) {
        INodeAttributes nodeAttrs = getINodeAttributes(
            fsd, src, HdfsFileStatus.EMPTY_NAME, targetNode,
            snapshot);
        return new DirectoryListing(
            new HdfsFileStatus[]{ createFileStatus(
                fsd, HdfsFileStatus.EMPTY_NAME, targetNode, nodeAttrs,
                needLocation, parentStoragePolicy, snapshot, isRawPath, iip)
            }, 0);
      }

      final INodeDirectory dirInode = targetNode.asDirectory();
        byte curPolicy = isSuperUser && !cur.isSymlink()?
            cur.getLocalStoragePolicyID():
            HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
        INodeAttributes nodeAttrs = getINodeAttributes(
            fsd, src, cur.getLocalNameBytes(), cur,
            snapshot);
        listing[i] = createFileStatus(fsd, cur.getLocalNameBytes(),
            cur, nodeAttrs, needLocation, getStoragePolicyID(curPolicy,
                parentStoragePolicy), snapshot, isRawPath, iip);
        listingCnt++;
        if (needLocation) {
    final HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
    for (int i = 0; i < numOfListing; i++) {
      Snapshot.Root sRoot = snapshots.get(i + skipSize).getRoot();
      INodeAttributes nodeAttrs = getINodeAttributes(
          fsd, src, sRoot.getLocalNameBytes(),
          node, Snapshot.CURRENT_STATE_ID);
      listing[i] = createFileStatus(
          fsd, sRoot.getLocalNameBytes(),
          sRoot, nodeAttrs,
          HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED,
          Snapshot.CURRENT_STATE_ID, false,
          INodesInPath.fromINode(sRoot));
    }
    return new DirectoryListing(
        listing, snapshots.size() - skipSize - numOfListing);
    fsd.readLock();
    try {
      final INode i = src.getLastINode();
      if (i == null) {
        return null;
      }

      byte policyId = includeStoragePolicy && !i.isSymlink() ?
          i.getStoragePolicyID() : HdfsConstantsClient
          .BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      INodeAttributes nodeAttrs = getINodeAttributes(
          fsd, path, HdfsFileStatus.EMPTY_NAME, i, src.getPathSnapshotId());
      return createFileStatus(
          fsd, HdfsFileStatus.EMPTY_NAME,
          i, nodeAttrs, policyId,
          src.getPathSnapshotId(),
          isRawPath, src);
    } finally {
      fsd.readUnlock();
    }
    }
  }


  private static HdfsFileStatus createFileStatus(
      FSDirectory fsd, byte[] path, INode node, INodeAttributes nodeAttrs,
      boolean needLocation, byte storagePolicy, int snapshot, boolean isRawPath,
      INodesInPath iip)
      throws IOException {
    if (needLocation) {
      return createLocatedFileStatus(fsd, path, node, nodeAttrs, storagePolicy,
                                     snapshot, isRawPath, iip);
    } else {
      return createFileStatus(fsd, path, node, nodeAttrs, storagePolicy,
                              snapshot, isRawPath, iip);
    }
  }

  static HdfsFileStatus createFileStatusForEditLog(
      FSDirectory fsd, String fullPath, byte[] path, INode node,
      byte storagePolicy, int snapshot, boolean isRawPath,
      INodesInPath iip) throws IOException {
    INodeAttributes nodeAttrs = getINodeAttributes(
        fsd, fullPath, path, node, snapshot);
    return createFileStatus(fsd, path, node, nodeAttrs,
                            storagePolicy, snapshot, isRawPath, iip);
  }

  static HdfsFileStatus createFileStatus(
      FSDirectory fsd, byte[] path, INode node,
      INodeAttributes nodeAttrs, byte storagePolicy, int snapshot,
      boolean isRawPath, INodesInPath iip) throws IOException {
    long size = 0;     // length is zero for directories
    short replication = 0;
    long blocksize = 0;
    int childrenNum = node.isDirectory() ?
        node.asDirectory().getChildrenNum(snapshot) : 0;

    return new HdfsFileStatus(
        size,
        node.isDirectory(),
        storagePolicy);
  }

  private static INodeAttributes getINodeAttributes(
      FSDirectory fsd, String fullPath, byte[] path, INode node, int snapshot) {
    return fsd.getAttributes(fullPath, path, node, snapshot);
  }

  private static HdfsLocatedFileStatus createLocatedFileStatus(
      FSDirectory fsd, byte[] path, INode node, INodeAttributes nodeAttrs,
      byte storagePolicy, int snapshot,
      boolean isRawPath, INodesInPath iip) throws IOException {
    assert fsd.hasReadLock();
    long size = 0; // length is zero for directories
    short replication = 0;
    int childrenNum = node.isDirectory() ?
        node.asDirectory().getChildrenNum(snapshot) : 0;

    HdfsLocatedFileStatus status =
        new HdfsLocatedFileStatus(size, node.isDirectory(), replication,
          blocksize, node.getModificationTime(snapshot),

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogLoader.java

        if (toAddRetryCache) {
          HdfsFileStatus stat = FSDirStatAndListingOp.createFileStatusForEditLog(
              fsNamesys.dir, path, HdfsFileStatus.EMPTY_NAME, newFile,
              HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED, Snapshot.CURRENT_STATE_ID,
              false, iip);
              false);
          if (toAddRetryCache) {
            HdfsFileStatus stat = FSDirStatAndListingOp.createFileStatusForEditLog(
                fsNamesys.dir, path,
                HdfsFileStatus.EMPTY_NAME, newFile,
                HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED,
            false, false);
        if (toAddRetryCache) {
          HdfsFileStatus stat = FSDirStatAndListingOp.createFileStatusForEditLog(
              fsNamesys.dir, path, HdfsFileStatus.EMPTY_NAME, file,
              HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED,
              Snapshot.CURRENT_STATE_ID, false, iip);

