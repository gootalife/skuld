hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsClientConfigKeys.java
  String  DFS_NAMENODE_HTTP_PORT_KEY = "dfs.http.port";
  String  DFS_NAMENODE_HTTPS_PORT_KEY = "dfs.https.port";
  int DFS_NAMENODE_RPC_PORT_DEFAULT = 8020;
  String DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY = "dfs.namenode.kerberos.principal";

  interface Retry {

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/ClientProtocol.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/ClientProtocol.java
package org.apache.hadoop.hdfs.protocol;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;

@InterfaceAudience.Private
@InterfaceStability.Evolving
@KerberosInfo(
    serverPrincipal = DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)
@TokenInfo(DelegationTokenSelector.class)
public interface ClientProtocol {

  public static final long versionID = 69L;
  
  @Idempotent
  public LocatedBlocks getBlockLocations(String src,
                                         long offset,
                                         long length) 
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  @Idempotent
  public FsServerDefaults getServerDefaults() throws IOException;

  @AtMostOnce
  public HdfsFileStatus create(String src, FsPermission masked,
      String clientName, EnumSetWritable<CreateFlag> flag,
      boolean createParent, short replication, long blockSize, 
      CryptoProtocolVersion[] supportedVersions)
      throws AccessControlException, AlreadyBeingCreatedException,
      DSQuotaExceededException, FileAlreadyExistsException,
      FileNotFoundException, NSQuotaExceededException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      SnapshotAccessControlException, IOException;

  @AtMostOnce
  public LastBlockWithStatus append(String src, String clientName,
      EnumSetWritable<CreateFlag> flag) throws AccessControlException,
      DSQuotaExceededException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, SnapshotAccessControlException, IOException;

  @Idempotent
  public boolean setReplication(String src, short replication)
      throws AccessControlException, DSQuotaExceededException,
      FileNotFoundException, SafeModeException, UnresolvedLinkException,
      SnapshotAccessControlException, IOException;

  @Idempotent
  public BlockStoragePolicy[] getStoragePolicies() throws IOException;

  @Idempotent
  public void setStoragePolicy(String src, String policyName)
      throws SnapshotAccessControlException, UnresolvedLinkException,
      FileNotFoundException, QuotaExceededException, IOException;

  @Idempotent
  public void setPermission(String src, FsPermission permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, SnapshotAccessControlException, IOException;

  @Idempotent
  public void setOwner(String src, String username, String groupname)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, SnapshotAccessControlException, IOException;

  @Idempotent
  public void abandonBlock(ExtendedBlock b, long fileId,
      String src, String holder)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  @Idempotent
  public LocatedBlock addBlock(String src, String clientName,
      ExtendedBlock previous, DatanodeInfo[] excludeNodes, long fileId, 
      String[] favoredNodes)
      throws AccessControlException, FileNotFoundException,
      NotReplicatedYetException, SafeModeException, UnresolvedLinkException,
      IOException;

  @Idempotent
  public LocatedBlock getAdditionalDatanode(final String src,
      final long fileId, final ExtendedBlock blk,
      final DatanodeInfo[] existings,
      final String[] existingStorageIDs,
      final DatanodeInfo[] excludes,
      final int numAdditionalNodes, final String clientName
      ) throws AccessControlException, FileNotFoundException,
          SafeModeException, UnresolvedLinkException, IOException;

  @Idempotent
  public boolean complete(String src, String clientName,
                          ExtendedBlock last, long fileId)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException;

  @Idempotent
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException;

  @AtMostOnce
  public boolean rename(String src, String dst) 
      throws UnresolvedLinkException, SnapshotAccessControlException, IOException;

  @AtMostOnce
  public void concat(String trg, String[] srcs) 
      throws IOException, UnresolvedLinkException, SnapshotAccessControlException;

  @AtMostOnce
  public void rename2(String src, String dst, Options.Rename... options)
      throws AccessControlException, DSQuotaExceededException,
      FileAlreadyExistsException, FileNotFoundException,
      NSQuotaExceededException, ParentNotDirectoryException, SafeModeException,
      UnresolvedLinkException, SnapshotAccessControlException, IOException;

  @Idempotent
  public boolean truncate(String src, long newLength, String clientName)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, SnapshotAccessControlException, IOException;

  @AtMostOnce
  public boolean delete(String src, boolean recursive)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, SnapshotAccessControlException, IOException;
  
  @Idempotent
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, NSQuotaExceededException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      SnapshotAccessControlException, IOException;

  @Idempotent
  public DirectoryListing getListing(String src,
                                     byte[] startAfter,
                                     boolean needLocation)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;
  
  @Idempotent
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException;


  @Idempotent
  public void renewLease(String clientName) throws AccessControlException,
      IOException;

  @Idempotent
  public boolean recoverLease(String src, String clientName) throws IOException;

  public int GET_STATS_CAPACITY_IDX = 0;
  public int GET_STATS_USED_IDX = 1;
  public int GET_STATS_REMAINING_IDX = 2;
  public int GET_STATS_UNDER_REPLICATED_IDX = 3;
  public int GET_STATS_CORRUPT_BLOCKS_IDX = 4;
  public int GET_STATS_MISSING_BLOCKS_IDX = 5;
  public int GET_STATS_MISSING_REPL_ONE_BLOCKS_IDX = 6;
  
  @Idempotent
  public long[] getStats() throws IOException;

  @Idempotent
  public DatanodeInfo[] getDatanodeReport(HdfsConstants.DatanodeReportType type)
      throws IOException;

  @Idempotent
  public DatanodeStorageReport[] getDatanodeStorageReport(
      HdfsConstants.DatanodeReportType type) throws IOException;

  @Idempotent
  public long getPreferredBlockSize(String filename) 
      throws IOException, UnresolvedLinkException;

  @Idempotent
  public boolean setSafeMode(HdfsConstants.SafeModeAction action, boolean isChecked) 
      throws IOException;

  @AtMostOnce
  public void saveNamespace() throws AccessControlException, IOException;

  
  @Idempotent
  public long rollEdits() throws AccessControlException, IOException;

  @Idempotent
  public boolean restoreFailedStorage(String arg) 
      throws AccessControlException, IOException;

  @Idempotent
  public void refreshNodes() throws IOException;

  @Idempotent
  public void finalizeUpgrade() throws IOException;

  @Idempotent
  public RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action)
      throws IOException;

  @Idempotent
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException;
  
  @Idempotent
  public void metaSave(String filename) throws IOException;

  @Idempotent
  public void setBalancerBandwidth(long bandwidth) throws IOException;
  
  @Idempotent
  public HdfsFileStatus getFileInfo(String src) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException;
  
  @Idempotent
  public boolean isFileClosed(String src) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException;
  
  @Idempotent
  public HdfsFileStatus getFileLinkInfo(String src)
      throws AccessControlException, UnresolvedLinkException, IOException;
  
  @Idempotent
  public ContentSummary getContentSummary(String path)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  @Idempotent
  public void setQuota(String path, long namespaceQuota, long storagespaceQuota,
      StorageType type) throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, SnapshotAccessControlException, IOException;

  @Idempotent
  public void fsync(String src, long inodeId, String client,
                    long lastBlockLength)
      throws AccessControlException, FileNotFoundException, 
      UnresolvedLinkException, IOException;

  @Idempotent
  public void setTimes(String src, long mtime, long atime)
      throws AccessControlException, FileNotFoundException, 
      UnresolvedLinkException, SnapshotAccessControlException, IOException;

  @AtMostOnce
  public void createSymlink(String target, String link, FsPermission dirPerm,
      boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      SnapshotAccessControlException, IOException;

  @Idempotent
  public String getLinkTarget(String path) throws AccessControlException,
      FileNotFoundException, IOException; 
  
  @Idempotent
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block,
      String clientName) throws IOException;

  @AtMostOnce
  public void updatePipeline(String clientName, ExtendedBlock oldBlock, 
      ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs)
      throws IOException;

  @Idempotent
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) 
      throws IOException;

  @Idempotent
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException;
  
  @Idempotent
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException;
  
  @Idempotent
  public DataEncryptionKey getDataEncryptionKey() throws IOException;
  
  @AtMostOnce
  public String createSnapshot(String snapshotRoot, String snapshotName)
      throws IOException;

  @AtMostOnce
  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException;
  
  @AtMostOnce
  public void renameSnapshot(String snapshotRoot, String snapshotOldName,
      String snapshotNewName) throws IOException;
  
  @Idempotent
  public void allowSnapshot(String snapshotRoot)
      throws IOException;
    
  @Idempotent
  public void disallowSnapshot(String snapshotRoot)
      throws IOException;
  
  @Idempotent
  public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot,
      String fromSnapshot, String toSnapshot) throws IOException;

  @AtMostOnce
  public long addCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags) throws IOException;

  @AtMostOnce
  public void modifyCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags) throws IOException;

  @AtMostOnce
  public void removeCacheDirective(long id) throws IOException;

  @Idempotent
  public BatchedEntries<CacheDirectiveEntry> listCacheDirectives(
      long prevId, CacheDirectiveInfo filter) throws IOException;

  @AtMostOnce
  public void addCachePool(CachePoolInfo info) throws IOException;

  @AtMostOnce
  public void modifyCachePool(CachePoolInfo req) throws IOException;
  
  @AtMostOnce
  public void removeCachePool(String pool) throws IOException;

  @Idempotent
  public BatchedEntries<CachePoolEntry> listCachePools(String prevPool)
      throws IOException;

  @Idempotent
  public void modifyAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException;

  @Idempotent
  public void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException;

  @Idempotent
  public void removeDefaultAcl(String src) throws IOException;

  @Idempotent
  public void removeAcl(String src) throws IOException;

  @Idempotent
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException;

  @Idempotent
  public AclStatus getAclStatus(String src) throws IOException;
  
  @AtMostOnce
  public void createEncryptionZone(String src, String keyName)
    throws IOException;

  @Idempotent
  public EncryptionZone getEZForPath(String src)
    throws IOException;

  @Idempotent
  public BatchedEntries<EncryptionZone> listEncryptionZones(
      long prevId) throws IOException;

  @AtMostOnce
  public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag) 
      throws IOException;
  
  @Idempotent
  public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs) 
      throws IOException;

  @Idempotent
  public List<XAttr> listXAttrs(String src)
      throws IOException;
  
  @AtMostOnce
  public void removeXAttr(String src, XAttr xAttr) throws IOException;

  @Idempotent
  public void checkAccess(String path, FsAction mode) throws IOException;

  @Idempotent
  public long getCurrentEditLogTxid() throws IOException;

  @Idempotent
  public EventBatchList getEditsFromTxid(long txid) throws IOException;
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/server/namenode/SafeModeException.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/server/namenode/SafeModeException.java

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SafeModeException extends IOException {
  private static final long serialVersionUID = 1L;
  public SafeModeException(String msg) {
    super(msg);
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  public static final String  DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS = "dfs.short.circuit.shared.memory.watcher.interrupt.check.ms";
  public static final int     DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT = 60000;
  public static final String  DFS_NAMENODE_KEYTAB_FILE_KEY = "dfs.namenode.keytab.file";
  public static final String  DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY =
      HdfsClientConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
  @Deprecated
  public static final String  DFS_NAMENODE_USER_NAME_KEY = DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
  public static final String  DFS_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY = "dfs.namenode.kerberos.internal.spnego.principal";

