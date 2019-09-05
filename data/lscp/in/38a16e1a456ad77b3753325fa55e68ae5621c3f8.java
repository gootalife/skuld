hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirAttrOp.java

      if (atime <= inodeTime + fsd.getAccessTimePrecision() && !force) {
        status =  false;
      } else {
        inode.setAccessTime(atime, latest);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirStatAndListingOp.java
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FsPermissionExtension;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import java.io.IOException;
import java.util.Arrays;

import static org.apache.hadoop.util.Time.now;

class FSDirStatAndListingOp {
  static DirectoryListing getListingInt(FSDirectory fsd, final String srcArg,
      byte[] startAfter, boolean needLocation) throws IOException {
    return getContentSummaryInt(fsd, iip);
  }

  static GetBlockLocationsResult getBlockLocations(
      FSDirectory fsd, FSPermissionChecker pc, String src, long offset,
      long length, boolean needBlockToken) throws IOException {
    Preconditions.checkArgument(offset >= 0,
        "Negative offset is not supported. File: " + src);
    Preconditions.checkArgument(length >= 0,
        "Negative length is not supported. File: " + src);
    CacheManager cm = fsd.getFSNamesystem().getCacheManager();
    BlockManager bm = fsd.getBlockManager();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    boolean isReservedName = FSDirectory.isReservedRawName(src);
    fsd.readLock();
    try {
      src = fsd.resolvePath(pc, src, pathComponents);
      final INodesInPath iip = fsd.getINodesInPath(src, true);
      final INodeFile inode = INodeFile.valueOf(iip.getLastINode(), src);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.READ);
        fsd.checkUnreadableBySuperuser(pc, inode, iip.getPathSnapshotId());
      }

      final long fileSize = iip.isSnapshot()
          ? inode.computeFileSize(iip.getPathSnapshotId())
          : inode.computeFileSizeNotIncludingLastUcBlock();

      boolean isUc = inode.isUnderConstruction();
      if (iip.isSnapshot()) {
        length = Math.min(length, fileSize - offset);
        isUc = false;
      }

      final FileEncryptionInfo feInfo = isReservedName ? null
          : fsd.getFileEncryptionInfo(inode, iip.getPathSnapshotId(), iip);

      final LocatedBlocks blocks = bm.createLocatedBlocks(
          inode.getBlocks(iip.getPathSnapshotId()), fileSize, isUc, offset,
          length, needBlockToken, iip.isSnapshot(), feInfo);

      for (LocatedBlock lb : blocks.getLocatedBlocks()) {
        cm.setCachedLocations(lb);
      }

      final long now = now();
      boolean updateAccessTime = fsd.isAccessTimeSupported()
          && !iip.isSnapshot()
          && now > inode.getAccessTime() + fsd.getAccessTimePrecision();
      return new GetBlockLocationsResult(updateAccessTime, blocks);
    } finally {
      fsd.readUnlock();
    }
  }

  private static byte getStoragePolicyID(byte inodePolicy, byte parentPolicy) {
    return inodePolicy != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED
        ? inodePolicy : parentPolicy;
  }

      byte policyId = includeStoragePolicy && !i.isSymlink() ?
          i.getStoragePolicyID() :
          HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      INodeAttributes nodeAttrs = getINodeAttributes(fsd, path,
                                                     HdfsFileStatus.EMPTY_NAME,
                                                     i, src.getPathSnapshotId());
      return createFileStatus(fsd, HdfsFileStatus.EMPTY_NAME, i, nodeAttrs,
                              policyId, src.getPathSnapshotId(), isRawPath, src);
    } finally {
      fsd.readUnlock();
    }
      fsd.readUnlock();
    }
  }

  static class GetBlockLocationsResult {
    final boolean updateAccessTime;
    final LocatedBlocks blocks;
    boolean updateAccessTime() {
      return updateAccessTime;
    }
    private GetBlockLocationsResult(
        boolean updateAccessTime, LocatedBlocks blocks) {
      this.updateAccessTime = updateAccessTime;
      this.blocks = blocks;
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirectory.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_ENCRYPTION_ZONE;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_FILE_ENCRYPTION_INFO;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.SECURITY_XATTR_UNREADABLE_BY_SUPERUSER;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;

  boolean isAccessTimeSupported() {
    return accessTimePrecision > 0;
  }
  long getAccessTimePrecision() {
    return accessTimePrecision;
  }
  boolean isQuotaByStorageTypeEnabled() {
    return quotaByStorageTypeEnabled;
  }
    }
  }

  void checkUnreadableBySuperuser(
      FSPermissionChecker pc, INode inode, int snapshotId)
      throws IOException {
    if (pc.isSuperUser()) {
      for (XAttr xattr : FSDirXAttrOp.getXAttrs(this, inode, snapshotId)) {
        if (XAttrHelper.getPrefixName(xattr).
            equals(SECURITY_XATTR_UNREADABLE_BY_SUPERUSER)) {
          throw new AccessControlException(
              "Access is denied for " + pc.getUser() + " since the superuser "
              + "is not allowed to perform this operation.");
        }
      }
    }
  }

  HdfsFileStatus getAuditFileInfo(INodesInPath iip)
      throws IOException {
    return (namesystem.isAuditEnabled() && namesystem.isExternalInvocation())

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_ASYNC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_ASYNC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SUPPORT_APPEND_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SUPPORT_APPEND_KEY;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.SECURITY_XATTR_UNREADABLE_BY_SUPERUSER;
import static org.apache.hadoop.hdfs.server.namenode.FSDirStatAndListingOp.*;
import static org.apache.hadoop.util.Time.now;
import static org.apache.hadoop.util.Time.monotonicNow;

import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.UnknownCryptoProtocolVersionException;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
  private final long minBlockSize;         // minimum block size
  final long maxBlocksPerFile;     // maximum # of blocks per file

  private final FSNamesystemLock fsLock;

          DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_DEFAULT);
      this.maxBlocksPerFile = conf.getLong(DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_KEY,
          DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_DEFAULT);
      this.supportAppends = conf.getBoolean(DFS_SUPPORT_APPEND_KEY, DFS_SUPPORT_APPEND_DEFAULT);
      LOG.info("Append Enabled: " + supportAppends);

    return serverDefaults;
  }

    logAuditEvent(true, "setOwner", src, null, auditStat);
  }

    readLock();
    try {
      checkOperation(OperationCategory.READ);
      res = FSDirStatAndListingOp.getBlockLocations(
          dir, pc, srcArg, offset, length, true);
      if (isInSafeMode()) {
        for (LocatedBlock b : res.blocks.getLocatedBlocks()) {
          if ((b.getLocations() == null) || (b.getLocations().length == 0)) {
            SafeModeException se = newSafemodeException(
                "Zero blocklocations for " + srcArg);
            if (haEnabled && haContext != null &&
                haContext.getState().getServiceState() == HAServiceState.ACTIVE) {
              throw new RetriableException(se);
            } else {
              throw se;
            }
          }
        }
      }
    } catch (AccessControlException e) {
      logAuditEvent(false, "open", srcArg);
      throw e;

    logAuditEvent(true, "open", srcArg);

    if (!isInSafeMode() && res.updateAccessTime()) {
      byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(
          srcArg);
      String src = srcArg;
        final INodesInPath iip = dir.getINodesInPath(src, true);
        INode inode = iip.getLastINode();
        boolean updateAccessTime = inode != null &&
            now > inode.getAccessTime() + dir.getAccessTimePrecision();
        if (!isInSafeMode() && updateAccessTime) {
          boolean changed = FSDirAttrOp.setTimes(dir,
              inode, -1, now, false, iip.getLatestSnapshotId());
    return blocks;
  }

    readLock();
    try {
      checkOperation(NameNode.OperationCategory.READ);
      dl = getListingInt(dir, src, startAfter, needLocation);
    } catch (AccessControlException e) {
      logAuditEvent(false, "listStatus", src);
      throw e;
    return new PermissionStatus(fsOwner.getShortUserName(), supergroup, permission);
  }

  @Override
  public void checkSuperuserPrivilege()
      throws AccessControlException {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NamenodeFsck.java
    FSNamesystem fsn = namenode.getNamesystem();
    fsn.readLock();
    try {
      blocks = FSDirStatAndListingOp.getBlockLocations(
          fsn.getFSDirectory(), fsn.getPermissionChecker(),
          path, 0, fileLen, false)
          .blocks;
    } catch (FileNotFoundException fnfe) {
      blocks = null;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFsck.java
    Configuration conf = new Configuration();
    NameNode namenode = mock(NameNode.class);
    NetworkTopology nettop = mock(NetworkTopology.class);
    Map<String,String[]> pmap = new HashMap<>();
    Writer result = new StringWriter();
    PrintWriter out = new PrintWriter(result, true);
    InetAddress remoteAddress = InetAddress.getLocalHost();
    FSNamesystem fsName = mock(FSNamesystem.class);
    FSDirectory fsd = mock(FSDirectory.class);
    BlockManager blockManager = mock(BlockManager.class);
    DatanodeManager dnManager = mock(DatanodeManager.class);
    INodesInPath iip = mock(INodesInPath.class);

    when(namenode.getNamesystem()).thenReturn(fsName);
    when(fsName.getBlockManager()).thenReturn(blockManager);
    when(fsName.getFSDirectory()).thenReturn(fsd);
    when(fsd.getFSNamesystem()).thenReturn(fsName);
    when(fsd.getINodesInPath(anyString(), anyBoolean())).thenReturn(iip);
    when(blockManager.getDatanodeManager()).thenReturn(dnManager);

    NamenodeFsck fsck = new NamenodeFsck(conf, namenode, nettop, pmap, out,
    String owner = "foo";
    String group = "bar";
    byte [] symlink = null;
    byte [] path = DFSUtil.string2Bytes(pathString);
    long fileId = 312321L;
    int numChildren = 1;
    byte storagePolicy = 0;

