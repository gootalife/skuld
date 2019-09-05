hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirWriteFileOp.java
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Preconditions;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.ChunkedArrayList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.util.Time.now;

class FSDirWriteFileOp {
  private FSDirWriteFileOp() {}
  static boolean unprotectedRemoveBlock(
    return clientNode;
  }

  static HdfsFileStatus startFile(
      FSNamesystem fsn, FSPermissionChecker pc, String src,
      PermissionStatus permissions, String holder, String clientMachine,
      EnumSet<CreateFlag> flag, boolean createParent,
      short replication, long blockSize,
      EncryptionKeyInfo ezInfo, INode.BlocksMapUpdateInfo toRemoveBlocks,
      boolean logRetryEntry)
      throws IOException {
    assert fsn.hasWriteLock();

    boolean create = flag.contains(CreateFlag.CREATE);
    boolean overwrite = flag.contains(CreateFlag.OVERWRITE);
    boolean isLazyPersist = flag.contains(CreateFlag.LAZY_PERSIST);

    CipherSuite suite = null;
    CryptoProtocolVersion version = null;
    KeyProviderCryptoExtension.EncryptedKeyVersion edek = null;

    if (ezInfo != null) {
      edek = ezInfo.edek;
      suite = ezInfo.suite;
      version = ezInfo.protocolVersion;
    }

    boolean isRawPath = FSDirectory.isReservedRawName(src);
    FSDirectory fsd = fsn.getFSDirectory();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = fsd.resolvePath(pc, src, pathComponents);
    INodesInPath iip = fsd.getINodesInPath4Write(src);

    final INode inode = iip.getLastINode();
    if (inode != null && inode.isDirectory()) {
      throw new FileAlreadyExistsException(src +
          " already exists as a directory");
    }

    final INodeFile myFile = INodeFile.valueOf(inode, src, true);
    if (fsd.isPermissionEnabled()) {
      if (overwrite && myFile != null) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }
      fsd.checkAncestorAccess(pc, iip, FsAction.WRITE);
    }

    if (!createParent) {
      fsd.verifyParentDir(iip, src);
    }

    if (myFile == null && !create) {
      throw new FileNotFoundException("Can't overwrite non-existent " +
          src + " for client " + clientMachine);
    }

    FileEncryptionInfo feInfo = null;

    final EncryptionZone zone = fsd.getEZForPath(iip);
    if (zone != null) {
      if (suite == null || edek == null) {
        throw new RetryStartFileException();
      }
      final String ezKeyName = zone.getKeyName();
      if (!ezKeyName.equals(edek.getEncryptionKeyName())) {
        throw new RetryStartFileException();
      }
      feInfo = new FileEncryptionInfo(suite, version,
          edek.getEncryptedKeyVersion().getMaterial(),
          edek.getEncryptedKeyIv(),
          ezKeyName, edek.getEncryptionKeyVersionName());
    }

    if (myFile != null) {
      if (overwrite) {
        List<INode> toRemoveINodes = new ChunkedArrayList<>();
        List<Long> toRemoveUCFiles = new ChunkedArrayList<>();
        long ret = FSDirDeleteOp.delete(fsd, iip, toRemoveBlocks,
                                        toRemoveINodes, toRemoveUCFiles, now());
        if (ret >= 0) {
          iip = INodesInPath.replace(iip, iip.length() - 1, null);
          FSDirDeleteOp.incrDeletedFileCount(ret);
          fsn.removeLeasesAndINodes(toRemoveUCFiles, toRemoveINodes, true);
        }
      } else {
        fsn.recoverLeaseInternal(FSNamesystem.RecoverLeaseOp.CREATE_FILE, iip,
                                 src, holder, clientMachine, false);
        throw new FileAlreadyExistsException(src + " for client " +
            clientMachine + " already exists");
      }
    }
    fsn.checkFsObjectLimit();
    INodeFile newNode = null;
    Map.Entry<INodesInPath, String> parent = FSDirMkdirOp
        .createAncestorDirectories(fsd, iip, permissions);
    if (parent != null) {
      iip = addFile(fsd, parent.getKey(), parent.getValue(), permissions,
                    replication, blockSize, holder, clientMachine);
      newNode = iip != null ? iip.getLastINode().asFile() : null;
    }
    if (newNode == null) {
      throw new IOException("Unable to add " + src +  " to namespace");
    }
    fsn.leaseManager.addLease(
        newNode.getFileUnderConstructionFeature().getClientName(),
        newNode.getId());
    if (feInfo != null) {
      fsd.setFileEncryptionInfo(src, feInfo);
      newNode = fsd.getInode(newNode.getId()).asFile();
    }
    setNewINodeStoragePolicy(fsn.getBlockManager(), newNode, iip,
                             isLazyPersist);
    fsd.getEditLog().logOpenFile(src, newNode, overwrite, logRetryEntry);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: added " +
          src + " inode " + newNode.getId() + " " + holder);
    }
    return FSDirStatAndListingOp.getFileInfo(fsd, src, false, isRawPath, true);
  }

  static EncryptionKeyInfo getEncryptionKeyInfo(FSNamesystem fsn,
      FSPermissionChecker pc, String src,
      CryptoProtocolVersion[] supportedVersions)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    FSDirectory fsd = fsn.getFSDirectory();
    src = fsd.resolvePath(pc, src, pathComponents);
    INodesInPath iip = fsd.getINodesInPath4Write(src);
    final EncryptionZone zone = fsd.getEZForPath(iip);
    if (zone == null) {
      return null;
    }
    CryptoProtocolVersion protocolVersion = fsn.chooseProtocolVersion(
        zone, supportedVersions);
    CipherSuite suite = zone.getSuite();
    String ezKeyName = zone.getKeyName();

    Preconditions.checkNotNull(protocolVersion);
    Preconditions.checkNotNull(suite);
    Preconditions.checkArgument(!suite.equals(CipherSuite.UNKNOWN),
                                "Chose an UNKNOWN CipherSuite!");
    Preconditions.checkNotNull(ezKeyName);
    return new EncryptionKeyInfo(protocolVersion, suite, ezKeyName);
  }

  static INodeFile addFileForEditLog(
      FSDirectory fsd, long id, INodesInPath existing, byte[] localName,
      PermissionStatus permissions, List<AclEntry> aclEntries,
      List<XAttr> xAttrs, short replication, long modificationTime, long atime,
      long preferredBlockSize, boolean underConstruction, String clientName,
      String clientMachine, byte storagePolicyId) {
    final INodeFile newNode;
    assert fsd.hasWriteLock();
    if (underConstruction) {
      newNode = newINodeFile(id, permissions, modificationTime,
                                              modificationTime, replication,
                                              preferredBlockSize,
                                              storagePolicyId);
      newNode.toUnderConstruction(clientName, clientMachine);
    } else {
      newNode = newINodeFile(id, permissions, modificationTime,
                                              atime, replication,
                                              preferredBlockSize,
                                              storagePolicyId);
    }

    newNode.setLocalName(localName);
    try {
      INodesInPath iip = fsd.addINode(existing, newNode);
      if (iip != null) {
        if (aclEntries != null) {
          AclStorage.updateINodeAcl(newNode, aclEntries, CURRENT_STATE_ID);
        }
        if (xAttrs != null) {
          XAttrStorage.updateINodeXAttrs(newNode, xAttrs, CURRENT_STATE_ID);
        }
        return newNode;
      }
    } catch (IOException e) {
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
            "DIR* FSDirectory.unprotectedAddFile: exception when add "
                + existing.getPath() + " to the file system", e);
      }
    }
    return null;
  }

    }
  }

  private static INodesInPath addFile(
      FSDirectory fsd, INodesInPath existing, String localName,
      PermissionStatus permissions, short replication, long preferredBlockSize,
      String clientName, String clientMachine)
      throws IOException {

    long modTime = now();
    INodeFile newNode = newINodeFile(fsd.allocateNewInodeId(), permissions,
                                     modTime, modTime, replication, preferredBlockSize);
    newNode.setLocalName(localName.getBytes(Charsets.UTF_8));
    newNode.toUnderConstruction(clientName, clientMachine);

    INodesInPath newiip;
    fsd.writeLock();
    try {
      newiip = fsd.addINode(existing, newNode);
    } finally {
      fsd.writeUnlock();
    }
    if (newiip == null) {
      NameNode.stateChangeLog.info("DIR* addFile: failed to add " +
                                       existing.getPath() + "/" + localName);
      return null;
    }

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* addFile: " + localName + " is added");
    }
    return newiip;
  }

  private static FileState analyzeFileState(
      FSNamesystem fsn, String src, long fileId, String clientName,
      ExtendedBlock previous, LocatedBlock[] onRetryBlock)
        src = iip.getPath();
      }
    }
    final INodeFile file = fsn.checkLease(src, clientName, inode, fileId);
    BlockInfoContiguous lastBlockInFile = file.getLastBlock();
    if (!Block.matchingIdAndGenStamp(previousBlock, lastBlockInFile)) {
    return true;
  }

  private static INodeFile newINodeFile(
      long id, PermissionStatus permissions, long mtime, long atime,
      short replication, long preferredBlockSize, byte storagePolicyId) {
    return new INodeFile(id, null, permissions, mtime, atime,
        BlockInfoContiguous.EMPTY_ARRAY, replication, preferredBlockSize,
        storagePolicyId);
  }

  private static INodeFile newINodeFile(long id, PermissionStatus permissions,
      long mtime, long atime, short replication, long preferredBlockSize) {
    return newINodeFile(id, permissions, mtime, atime, replication,
        preferredBlockSize, (byte)0);
  }

    DatanodeStorageInfo.incrementBlocksScheduled(targets);
  }

  private static void setNewINodeStoragePolicy(BlockManager bm, INodeFile
      inode, INodesInPath iip, boolean isLazyPersist)
      throws IOException {

    if (isLazyPersist) {
      BlockStoragePolicy lpPolicy =
          bm.getStoragePolicy("LAZY_PERSIST");

      if (lpPolicy == null) {
        throw new HadoopIllegalArgumentException(
            "The LAZY_PERSIST storage policy has been disabled " +
            "by the administrator.");
      }
      inode.setStoragePolicyID(lpPolicy.getId(),
                                 iip.getLatestSnapshotId());
    } else {
      BlockStoragePolicy effectivePolicy =
          bm.getStoragePolicy(inode.getStoragePolicyID());

      if (effectivePolicy != null &&
          effectivePolicy.isCopyOnCreateFile()) {
        inode.setStoragePolicyID(effectivePolicy.getId(),
                                 iip.getLatestSnapshotId());
      }
    }
  }

  private static class FileState {
    final INodeFile inode;
    final String path;
      this.clientMachine = clientMachine;
    }
  }

  static class EncryptionKeyInfo {
    final CryptoProtocolVersion protocolVersion;
    final CipherSuite suite;
    final String ezKeyName;
    KeyProviderCryptoExtension.EncryptedKeyVersion edek;

    EncryptionKeyInfo(
        CryptoProtocolVersion protocolVersion, CipherSuite suite,
        String ezKeyName) {
      this.protocolVersion = protocolVersion;
      this.suite = suite;
      this.ezKeyName = ezKeyName;
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirectory.java
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_ENCRYPTION_ZONE;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_FILE_ENCRYPTION_INFO;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;

    skipQuotaCheck = true;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogLoader.java

        inodeId = getAndUpdateLastInodeId(addCloseOp.inodeId, logVersion, lastInodeId);
        newFile = FSDirWriteFileOp.addFileForEditLog(fsDir, inodeId,
            iip.getExistingINodes(), iip.getLastLocalName(),
            addCloseOp.permissions, addCloseOp.aclEntries,
            addCloseOp.xAttrs, replication, addCloseOp.mtime,
            addCloseOp.atime, addCloseOp.blockSize, true,
            addCloseOp.clientName, addCloseOp.clientMachine,
            addCloseOp.storagePolicyId);
        iip = INodesInPath.replace(iip, iip.length() - 1, newFile);
        fsNamesys.leaseManager.addLease(addCloseOp.clientName, newFile.getId());

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.ReflectionUtils;
  CryptoProtocolVersion chooseProtocolVersion(
      EncryptionZone zone, CryptoProtocolVersion[] supportedVersions)
      throws UnknownCryptoProtocolVersionException, UnresolvedLinkException,
        SnapshotAccessControlException {
    Preconditions.checkNotNull(zone);
      String holder, String clientMachine, EnumSet<CreateFlag> flag,
      boolean createParent, short replication, long blockSize, 
      CryptoProtocolVersion[] supportedVersions, boolean logRetryCache)
      throws IOException {

    HdfsFileStatus status;
    try {
      status = startFileInt(src, permissions, holder, clientMachine, flag,
          createParent, replication, blockSize, supportedVersions,
      logAuditEvent(false, "create", src);
      throw e;
    }
    logAuditEvent(true, "create", src, null, status);
    return status;
  }

  private HdfsFileStatus startFileInt(final String src,
      PermissionStatus permissions, String holder, String clientMachine,
      EnumSet<CreateFlag> flag, boolean createParent, short replication,
      long blockSize, CryptoProtocolVersion[] supportedVersions,
      boolean logRetryCache)
      throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      StringBuilder builder = new StringBuilder();
      builder.append("DIR* NameSystem.startFile: src=").append(src)
          .append(", holder=").append(holder)
          .append(", clientMachine=").append(clientMachine)
          .append(", createParent=").append(createParent)
          .append(", replication=").append(replication)
          .append(", createFlag=").append(flag.toString())
          .append(", blockSize=").append(blockSize)
          .append(", supportedVersions=")
          .append(supportedVersions == null ? null : Arrays.toString
              (supportedVersions));
      NameNode.stateChangeLog.debug(builder.toString());
    }
    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException(src);
    }
    blockManager.verifyReplication(src, replication, clientMachine);
    checkOperation(OperationCategory.WRITE);
    if (blockSize < minBlockSize) {
      throw new IOException("Specified block size is less than configured" +
          " minimum value (" + DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY
          + "): " + blockSize + " < " + minBlockSize);
    }

    FSPermissionChecker pc = getPermissionChecker();
    waitForLoadingFSImage();

    FSDirWriteFileOp.EncryptionKeyInfo ezInfo = null;

    if (provider != null) {
      readLock();
      try {
        checkOperation(OperationCategory.READ);
        ezInfo = FSDirWriteFileOp
            .getEncryptionKeyInfo(this, pc, src, supportedVersions);
      } finally {
        readUnlock();
      }

      if (ezInfo != null) {
        ezInfo.edek = generateEncryptedDataEncryptionKey(ezInfo.ezKeyName);
      }
      EncryptionFaultInjector.getInstance().startFileAfterGenerateKey();
    }

    boolean skipSync = false;
    HdfsFileStatus stat = null;

    BlocksMapUpdateInfo toRemoveBlocks = new BlocksMapUpdateInfo();
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot create file" + src);
      dir.writeLock();
      try {
        stat = FSDirWriteFileOp.startFile(this, pc, src, permissions, holder,
                                          clientMachine, flag, createParent,
                                          replication, blockSize, ezInfo,
                                          toRemoveBlocks, logRetryCache);
      } finally {
        dir.writeUnlock();
      }
    } catch (IOException e) {
      skipSync = e instanceof StandbyException;
      throw e;
    } finally {
      writeUnlock();
      if (!skipSync) {
        getEditLog().logSync();
        removeBlocks(toRemoveBlocks);
        toRemoveBlocks.clear();
      }
    }

    return stat;
  }

    return false;
  }

  enum RecoverLeaseOp {
    CREATE_FILE,
    APPEND_FILE,
    TRUNCATE_FILE,

