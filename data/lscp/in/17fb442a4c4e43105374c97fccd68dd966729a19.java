hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
  void concat(String target, String [] srcs, boolean logRetryCache)
      throws IOException {
    waitForLoadingFSImage();
    HdfsFileStatus stat = null;
    boolean success = false;
      throw new InvalidPathException(src);
    }
    blockManager.verifyReplication(src, replication, clientMachine);
    if (blockSize < minBlockSize) {
      throw new IOException("Specified block size is less than configured" +
          " minimum value (" + DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY
    LocatedBlock lb = null;
    HdfsFileStatus stat = null;
    FSPermissionChecker pc = getPermissionChecker();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    writeLock();
    try {
  boolean renameTo(String src, String dst, boolean logRetryCache)
      throws IOException {
    waitForLoadingFSImage();
    FSDirRenameOp.RenameOldResult ret = null;
    writeLock();
    try {
                boolean logRetryCache, Options.Rename... options)
      throws IOException {
    waitForLoadingFSImage();
    Map.Entry<BlocksMapUpdateInfo, HdfsFileStatus> res = null;
    writeLock();
    try {
  boolean delete(String src, boolean recursive, boolean logRetryCache)
      throws IOException {
    waitForLoadingFSImage();
    BlocksMapUpdateInfo toRemovedBlocks = null;
    writeLock();
    boolean ret = false;
      String clientName, ExtendedBlock oldBlock, ExtendedBlock newBlock,
      DatanodeID[] newNodes, String[] newStorageIDs, boolean logRetryCache)
      throws IOException {
    LOG.info("updatePipeline(" + oldBlock.getLocalBlock()
             + ", newGS=" + newBlock.getGenerationStamp()
             + ", newLength=" + newBlock.getNumBytes()
  void renameSnapshot(
      String path, String snapshotOldName, String snapshotNewName,
      boolean logRetryCache) throws IOException {
    boolean success = false;
    writeLock();
    try {
  void deleteSnapshot(String snapshotRoot, String snapshotName,
      boolean logRetryCache) throws IOException {
    boolean success = false;
    writeLock();
    BlocksMapUpdateInfo blocksToBeDeleted = null;
  long addCacheDirective(CacheDirectiveInfo directive,
                         EnumSet<CacheFlag> flags, boolean logRetryCache)
      throws IOException {
    CacheDirectiveInfo effectiveDirective = null;
    if (!flags.contains(CacheFlag.FORCE)) {
      cacheManager.waitForRescanIfNeeded();

  void modifyCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags, boolean logRetryCache) throws IOException {
    boolean success = false;
    if (!flags.contains(CacheFlag.FORCE)) {
      cacheManager.waitForRescanIfNeeded();
  }

  void removeCacheDirective(long id, boolean logRetryCache) throws IOException {
    boolean success = false;
    writeLock();
    try {

  void addCachePool(CachePoolInfo req, boolean logRetryCache)
      throws IOException {
    writeLock();
    boolean success = false;
    String poolInfoStr = null;

  void modifyCachePool(CachePoolInfo req, boolean logRetryCache)
      throws IOException {
    writeLock();
    boolean success = false;
    try {

  void removeCachePool(String cachePoolName, boolean logRetryCache)
      throws IOException {
    writeLock();
    boolean success = false;
    try {
    String src = srcArg;
    HdfsFileStatus resultingStat = null;
    checkSuperuserPrivilege();
    final byte[][] pathComponents =
      FSDirectory.getPathComponentsForReservedPath(src);
    FSPermissionChecker pc = getPermissionChecker();
  void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag,
                boolean logRetryCache)
      throws IOException {
    HdfsFileStatus auditStat = null;
    writeLock();
    try {

  void removeXAttr(String src, XAttr xAttr, boolean logRetryCache)
      throws IOException {
    HdfsFileStatus auditStat = null;
    writeLock();
    try {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNodeRpcServer.java
      throw new IOException("create: Pathname too long.  Limit "
          + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntryWithPayload cacheEntry = RetryCache.waitForCompletion(retryCache, null);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return (HdfsFileStatus) cacheEntry.getPayload();
      stateChangeLog.debug("*DIR* NameNode.append: file "
          +src+" for "+clientName+" at "+clientMachine);
    }
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntryWithPayload cacheEntry = RetryCache.waitForCompletion(retryCache,
        null);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs)
      throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
      throw new IOException("rename: Pathname too long.  Limit "
          + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return true; // Return previous response
  @Override // ClientProtocol
  public void concat(String trg, String[] src) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
      throw new IOException("rename: Pathname too long.  Limit "
          + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
      stateChangeLog.debug("*DIR* Namenode.delete: src=" + src
          + ", recursive=" + recursive);
    }
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return true; // Return previous response
  public void createSymlink(String target, String link, FsPermission dirPerms,
      boolean createParent) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
      throw new IOException("createSnapshot: Pathname too long.  Limit "
          + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntryWithPayload cacheEntry = RetryCache.waitForCompletion(retryCache,
      null);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    metrics.incrDeleteSnapshotOps();
    CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
    if (snapshotNewName == null || snapshotNewName.isEmpty()) {
      throw new IOException("The new snapshot name is null or empty.");
    }
    namesystem.checkOperation(OperationCategory.WRITE);
    metrics.incrRenameSnapshotOps();
    CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
  public long addCacheDirective(
      CacheDirectiveInfo path, EnumSet<CacheFlag> flags) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntryWithPayload cacheEntry = RetryCache.waitForCompletion
      (retryCache, null);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
  public void modifyCacheDirective(
      CacheDirectiveInfo directive, EnumSet<CacheFlag> flags) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return;
  @Override // ClientProtocol
  public void removeCacheDirective(long id) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return;
  @Override //ClientProtocol
  public void addCachePool(CachePoolInfo info) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
  @Override // ClientProtocol
  public void modifyCachePool(CachePoolInfo info) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
  @Override // ClientProtocol
  public void removeCachePool(String cachePoolName) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return;
  public void createEncryptionZone(String src, String keyName)
    throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    final CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return;
  public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
      throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
  @Override // ClientProtocol
  public void removeXAttr(String src, XAttr xAttr) throws IOException {
    checkNNStartup();
    namesystem.checkOperation(OperationCategory.WRITE);
    CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestRetryCacheWithHA.java
  abstract class AtMostOnceOp {
    private final String name;
    final DFSClient client;
    int expectedUpdateCount = 0;

    AtMostOnceOp(String name, DFSClient client) {
      this.name = name;
    abstract void invoke() throws Exception;
    abstract boolean checkNamenodeBeforeReturn() throws Exception;
    abstract Object getResult();
    int getExpectedCacheUpdateCount() {
      return expectedUpdateCount;
    }
  }
  
    void prepare() throws Exception {
      Path p = new Path(target);
      if (!dfs.exists(p)) {
        expectedUpdateCount++;
        DFSTestUtil.createFile(dfs, p, BlockSize, DataNodes, 0);
      }
    }

    @Override
    void invoke() throws Exception {
      expectedUpdateCount++;
      deleted = client.delete(target, true);
    }

    void prepare() throws Exception {
      Path p = new Path(target);
      if (!dfs.exists(p)) {
        expectedUpdateCount++;
        DFSTestUtil.createFile(dfs, p, BlockSize, DataNodes, 0);
      }
    }

    @Override
    void invoke() throws Exception {
      expectedUpdateCount++;
      client.createSymlink(target, link, false);
    }


    @Override
    void prepare() throws Exception {
      expectedUpdateCount++;
      dfs.addCachePool(new CachePoolInfo(directive.getPool()));
    }

    @Override
    void invoke() throws Exception {
      expectedUpdateCount++;
      result = client.addCacheDirective(directive, EnumSet.of(CacheFlag.FORCE));
    }


    @Override
    void prepare() throws Exception {
      expectedUpdateCount++;
      dfs.addCachePool(new CachePoolInfo(directive.getPool()));
      expectedUpdateCount++;
      id = client.addCacheDirective(directive, EnumSet.of(CacheFlag.FORCE));
    }

    @Override
    void invoke() throws Exception {
      expectedUpdateCount++;
      client.modifyCacheDirective(
          new CacheDirectiveInfo.Builder().
              setId(id).

    @Override
    void prepare() throws Exception {
      expectedUpdateCount++;
      dfs.addCachePool(new CachePoolInfo(directive.getPool()));
      expectedUpdateCount++;
      id = dfs.addCacheDirective(directive, EnumSet.of(CacheFlag.FORCE));
    }

    @Override
    void invoke() throws Exception {
      expectedUpdateCount++;
      client.removeCacheDirective(id);
    }


    @Override
    void invoke() throws Exception {
      expectedUpdateCount++;
      client.addCachePool(new CachePoolInfo(pool));
    }


    @Override
    void prepare() throws Exception {
      expectedUpdateCount++;
      client.addCachePool(new CachePoolInfo(pool).setLimit(10l));
    }

    @Override
    void invoke() throws Exception {
      expectedUpdateCount++;
      client.modifyCachePool(new CachePoolInfo(pool).setLimit(99l));
    }


    @Override
    void prepare() throws Exception {
      expectedUpdateCount++;
      client.addCachePool(new CachePoolInfo(pool));
    }

    @Override
    void invoke() throws Exception {
      expectedUpdateCount++;
      client.removeCachePool(pool);
    }

    void prepare() throws Exception {
      Path p = new Path(src);
      if (!dfs.exists(p)) {
        expectedUpdateCount++;
        DFSTestUtil.createFile(dfs, p, BlockSize, DataNodes, 0);
      }
    }

    @Override
    void invoke() throws Exception {
      expectedUpdateCount++;
      client.setXAttr(src, "user.key", "value".getBytes(),
          EnumSet.of(XAttrSetFlag.CREATE));
    }
    void prepare() throws Exception {
      Path p = new Path(src);
      if (!dfs.exists(p)) {
        expectedUpdateCount++;
        DFSTestUtil.createFile(dfs, p, BlockSize, DataNodes, 0);
        expectedUpdateCount++;
        client.setXAttr(src, "user.key", "value".getBytes(),
          EnumSet.of(XAttrSetFlag.CREATE));
      }

    @Override
    void invoke() throws Exception {
      expectedUpdateCount++;
      client.removeXAttr(src, "user.key");
    }

    assertTrue("CacheUpdated on NN0: " + updatedNN0, updatedNN0 > 0);
    assertTrue("CacheUpdated on NN1: " + updatedNN1, updatedNN1 > 0);
    long expectedUpdateCount = op.getExpectedCacheUpdateCount();
    if (expectedUpdateCount > 0) {
      assertEquals("CacheUpdated on NN0: " + updatedNN0, expectedUpdateCount,
          updatedNN0);
      assertEquals("CacheUpdated on NN0: " + updatedNN1, expectedUpdateCount,
          updatedNN1);
    }
  }


