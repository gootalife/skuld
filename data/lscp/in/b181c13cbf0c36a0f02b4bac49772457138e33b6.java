hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsClientConfigKeys.java
  String  DFS_NAMENODE_HTTP_PORT_KEY = "dfs.http.port";
  String  DFS_NAMENODE_HTTPS_PORT_KEY = "dfs.https.port";
  int DFS_NAMENODE_RPC_PORT_DEFAULT = 8020;
  String DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY =
      "dfs.namenode.kerberos.principal";

  interface Retry {

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/ClientProtocol.java
  long versionID = 69L;

  @Idempotent
  LocatedBlocks getBlockLocations(String src, long offset, long length)
      throws IOException;

  @Idempotent
  FsServerDefaults getServerDefaults() throws IOException;

  @AtMostOnce
  HdfsFileStatus create(String src, FsPermission masked,
      String clientName, EnumSetWritable<CreateFlag> flag,
      boolean createParent, short replication, long blockSize,
      CryptoProtocolVersion[] supportedVersions)
      throws IOException;

  @AtMostOnce
  LastBlockWithStatus append(String src, String clientName,
      EnumSetWritable<CreateFlag> flag) throws IOException;

  @Idempotent
  boolean setReplication(String src, short replication)
      throws IOException;

  @Idempotent
  BlockStoragePolicy[] getStoragePolicies() throws IOException;

  @Idempotent
  void setStoragePolicy(String src, String policyName)
      throws IOException;

  @Idempotent
  void setPermission(String src, FsPermission permission)
      throws IOException;

  @Idempotent
  void setOwner(String src, String username, String groupname)
      throws IOException;

  @Idempotent
  void abandonBlock(ExtendedBlock b, long fileId,
      String src, String holder)
      throws IOException;

  @Idempotent
  LocatedBlock addBlock(String src, String clientName,
      ExtendedBlock previous, DatanodeInfo[] excludeNodes, long fileId,
      String[] favoredNodes)
      throws IOException;

  @Idempotent
  LocatedBlock getAdditionalDatanode(final String src,
      final long fileId, final ExtendedBlock blk,
      final DatanodeInfo[] existings,
      final String[] existingStorageIDs,
      final DatanodeInfo[] excludes,
      final int numAdditionalNodes, final String clientName
      ) throws IOException;

  @Idempotent
  boolean complete(String src, String clientName,
                          ExtendedBlock last, long fileId)
      throws IOException;

  @Idempotent
  void reportBadBlocks(LocatedBlock[] blocks) throws IOException;

  @AtMostOnce
  boolean rename(String src, String dst)
      throws IOException;

  @AtMostOnce
  void concat(String trg, String[] srcs)
      throws IOException;

  @AtMostOnce
  void rename2(String src, String dst, Options.Rename... options)
      throws IOException;

  @Idempotent
  boolean truncate(String src, long newLength, String clientName)
      throws IOException;

  @AtMostOnce
  boolean delete(String src, boolean recursive)
      throws IOException;

  @Idempotent
  boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws IOException;

  @Idempotent
  DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) throws IOException;

  @Idempotent
  SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException;

  @Idempotent
  void renewLease(String clientName) throws IOException;

  @Idempotent
  boolean recoverLease(String src, String clientName) throws IOException;

  int GET_STATS_CAPACITY_IDX = 0;
  int GET_STATS_USED_IDX = 1;
  int GET_STATS_REMAINING_IDX = 2;
  int GET_STATS_UNDER_REPLICATED_IDX = 3;
  int GET_STATS_CORRUPT_BLOCKS_IDX = 4;
  int GET_STATS_MISSING_BLOCKS_IDX = 5;
  int GET_STATS_MISSING_REPL_ONE_BLOCKS_IDX = 6;

  @Idempotent
  long[] getStats() throws IOException;

  @Idempotent
  DatanodeInfo[] getDatanodeReport(HdfsConstants.DatanodeReportType type)
      throws IOException;

  @Idempotent
  DatanodeStorageReport[] getDatanodeStorageReport(
      HdfsConstants.DatanodeReportType type) throws IOException;

  @Idempotent
  long getPreferredBlockSize(String filename)
      throws IOException;

  @Idempotent
  boolean setSafeMode(HdfsConstants.SafeModeAction action, boolean isChecked)
      throws IOException;

  @AtMostOnce
  void saveNamespace() throws IOException;

  @Idempotent
  long rollEdits() throws IOException;

  @Idempotent
  boolean restoreFailedStorage(String arg) throws IOException;

  @Idempotent
  void refreshNodes() throws IOException;

  @Idempotent
  void finalizeUpgrade() throws IOException;

  @Idempotent
  RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action)
      throws IOException;

  @Idempotent
  CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException;

  @Idempotent
  void metaSave(String filename) throws IOException;

  @Idempotent
  void setBalancerBandwidth(long bandwidth) throws IOException;

  @Idempotent
  HdfsFileStatus getFileInfo(String src) throws IOException;

  @Idempotent
  boolean isFileClosed(String src) throws IOException;

  @Idempotent
  HdfsFileStatus getFileLinkInfo(String src) throws IOException;

  @Idempotent
  ContentSummary getContentSummary(String path) throws IOException;

  @Idempotent
  void setQuota(String path, long namespaceQuota, long storagespaceQuota,
      StorageType type) throws IOException;

  @Idempotent
  void fsync(String src, long inodeId, String client, long lastBlockLength)
      throws IOException;

  @Idempotent
  void setTimes(String src, long mtime, long atime) throws IOException;

  @AtMostOnce
  void createSymlink(String target, String link, FsPermission dirPerm,
      boolean createParent) throws IOException;

  @Idempotent
  String getLinkTarget(String path) throws IOException;

  @Idempotent
  LocatedBlock updateBlockForPipeline(ExtendedBlock block,
      String clientName) throws IOException;

  @AtMostOnce
  void updatePipeline(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs)
      throws IOException;

  @Idempotent
  Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException;

  @Idempotent
  long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException;

  @Idempotent
  void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException;

  @Idempotent
  DataEncryptionKey getDataEncryptionKey() throws IOException;

  @AtMostOnce
  String createSnapshot(String snapshotRoot, String snapshotName)
      throws IOException;

  @AtMostOnce
  void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException;

  @AtMostOnce
  void renameSnapshot(String snapshotRoot, String snapshotOldName,
      String snapshotNewName) throws IOException;

  @Idempotent
  void allowSnapshot(String snapshotRoot)
      throws IOException;

  @Idempotent
  void disallowSnapshot(String snapshotRoot)
      throws IOException;

  @Idempotent
  SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot,
      String fromSnapshot, String toSnapshot) throws IOException;

  @AtMostOnce
  long addCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags) throws IOException;

  @AtMostOnce
  void modifyCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags) throws IOException;

  @AtMostOnce
  void removeCacheDirective(long id) throws IOException;

  @Idempotent
  BatchedEntries<CacheDirectiveEntry> listCacheDirectives(
      long prevId, CacheDirectiveInfo filter) throws IOException;

  @AtMostOnce
  void addCachePool(CachePoolInfo info) throws IOException;

  @AtMostOnce
  void modifyCachePool(CachePoolInfo req) throws IOException;

  @AtMostOnce
  void removeCachePool(String pool) throws IOException;

  @Idempotent
  BatchedEntries<CachePoolEntry> listCachePools(String prevPool)
      throws IOException;

  @Idempotent
  void modifyAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException;

  @Idempotent
  void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException;

  @Idempotent
  void removeDefaultAcl(String src) throws IOException;

  @Idempotent
  void removeAcl(String src) throws IOException;

  @Idempotent
  void setAcl(String src, List<AclEntry> aclSpec) throws IOException;

  @Idempotent
  AclStatus getAclStatus(String src) throws IOException;

  @AtMostOnce
  void createEncryptionZone(String src, String keyName)
    throws IOException;

  @Idempotent
  EncryptionZone getEZForPath(String src)
    throws IOException;

  @Idempotent
  BatchedEntries<EncryptionZone> listEncryptionZones(
      long prevId) throws IOException;

  @AtMostOnce
  void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
      throws IOException;

  @Idempotent
  List<XAttr> getXAttrs(String src, List<XAttr> xAttrs)
      throws IOException;

  @Idempotent
  List<XAttr> listXAttrs(String src)
      throws IOException;

  @AtMostOnce
  void removeXAttr(String src, XAttr xAttr) throws IOException;

  @Idempotent
  void checkAccess(String path, FsAction mode) throws IOException;

  @Idempotent
  long getCurrentEditLogTxid() throws IOException;

  @Idempotent
  EventBatchList getEditsFromTxid(long txid) throws IOException;
}

