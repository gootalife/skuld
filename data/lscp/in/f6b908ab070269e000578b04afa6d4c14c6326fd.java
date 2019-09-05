hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot add cache directive");
      effectiveDirective = FSNDNCacheOp.addCacheDirective(this, cacheManager,
          directive, flags, logRetryCache);
    } finally {
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot add cache directive");
      FSNDNCacheOp.modifyCacheDirective(this, cacheManager, directive, flags,
          logRetryCache);
      success = true;
    writeLock();
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot remove cache directives");
      FSNDNCacheOp.removeCacheDirective(this, cacheManager, id, logRetryCache);
      success = true;
    } finally {
    String poolInfoStr = null;
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot add cache pool"
          + (req == null ? null : req.getPoolName()));
      CachePoolInfo info = FSNDNCacheOp.addCachePool(this, cacheManager, req,
          logRetryCache);
      poolInfoStr = info.toString();
    boolean success = false;
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot modify cache pool"
          + (req == null ? null : req.getPoolName()));
      FSNDNCacheOp.modifyCachePool(this, cacheManager, req, logRetryCache);
      success = true;
    } finally {
    boolean success = false;
    try {
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot modify cache pool" + cachePoolName);
      FSNDNCacheOp.removeCachePool(this, cacheManager, cachePoolName,
          logRetryCache);
      success = true;

