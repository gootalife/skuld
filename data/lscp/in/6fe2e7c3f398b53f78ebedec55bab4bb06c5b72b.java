hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/FsDatasetSpi.java
  abstract class Factory<D extends FsDatasetSpi<?>> {
    public static Factory<?> getFactory(Configuration conf) {
      @SuppressWarnings("rawtypes")
  FsVolumeReferences getFsVolumeReferences();

  void addVolume(
      final StorageLocation location,
      final List<NamespaceInfo> nsInfos) throws IOException;

  void removeVolumes(Set<File> volumes, boolean clearFailure);

  DatanodeStorage getStorage(final String storageUuid);

  StorageReport[] getStorageReports(String bpid)
      throws IOException;

  V getVolume(ExtendedBlock b);

  Map<String, Object> getVolumeInfoMap();

  VolumeFailureSummary getVolumeFailureSummary();

  List<FinalizedReplica> getFinalizedBlocks(String bpid);

  List<FinalizedReplica> getFinalizedBlocksOnPersistentStorage(String bpid);

  void checkAndUpdate(String bpid, long blockId, File diskFile,
      File diskMetaFile, FsVolumeSpi vol) throws IOException;

  LengthInputStream getMetaDataInputStream(ExtendedBlock b
      ) throws IOException;

  long getLength(ExtendedBlock b) throws IOException;

  @Deprecated
  Replica getReplica(String bpid, long blockId);

  String getReplicaString(String bpid, long blockId);

  Block getStoredBlock(String bpid, long blkid) throws IOException;

  InputStream getBlockInputStream(ExtendedBlock b, long seekOffset)
            throws IOException;

  ReplicaInputStreams getTmpInputStreams(ExtendedBlock b, long blkoff,
      long ckoff) throws IOException;

  ReplicaHandler createTemporary(StorageType storageType,
      ExtendedBlock b) throws IOException;

  ReplicaHandler createRbw(StorageType storageType,
      ExtendedBlock b, boolean allowLazyPersist) throws IOException;

  ReplicaHandler recoverRbw(ExtendedBlock b,
      long newGS, long minBytesRcvd, long maxBytesRcvd) throws IOException;

  ReplicaInPipelineInterface convertTemporaryToRbw(
      ExtendedBlock temporary) throws IOException;

  ReplicaHandler append(ExtendedBlock b, long newGS,
      long expectedBlockLen) throws IOException;

  ReplicaHandler recoverAppend(
      ExtendedBlock b, long newGS, long expectedBlockLen) throws IOException;
  
  String recoverClose(ExtendedBlock b, long newGS, long expectedBlockLen
      ) throws IOException;
  
  void finalizeBlock(ExtendedBlock b) throws IOException;

  void unfinalizeBlock(ExtendedBlock b) throws IOException;

  Map<DatanodeStorage, BlockListAsLongs> getBlockReports(String bpid);

  List<Long> getCacheReport(String bpid);

  boolean contains(ExtendedBlock block);

  void checkBlock(ExtendedBlock b, long minLength, ReplicaState state)
      throws ReplicaNotFoundException, UnexpectedReplicaStateException,
      FileNotFoundException, EOFException, IOException;
      
  boolean isValidBlock(ExtendedBlock b);

  boolean isValidRbw(ExtendedBlock b);

  void invalidate(String bpid, Block invalidBlks[]) throws IOException;

  void cache(String bpid, long[] blockIds);

  void uncache(String bpid, long[] blockIds);

  boolean isCached(String bpid, long blockId);

  Set<File> checkDataDir();

  void shutdown();

  void adjustCrcChannelPosition(ExtendedBlock b,
      ReplicaOutputStreams outs, int checksumSize) throws IOException;

  boolean hasEnoughResource();

  ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock
      ) throws IOException;

  String updateReplicaUnderRecovery(ExtendedBlock oldBlock,
      long recoveryId, long newBlockId, long newLength) throws IOException;

  void addBlockPool(String bpid, Configuration conf) throws IOException;

  void shutdownBlockPool(String bpid) ;

  void deleteBlockPool(String bpid, boolean force) throws IOException;

  BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock b
      ) throws IOException;

  HdfsBlocksMetadata getHdfsBlocksMetadata(String bpid,
      long[] blockIds) throws IOException;

  void enableTrash(String bpid);

  void clearTrash(String bpid);

  boolean trashEnabled(String bpid);

  void setRollingUpgradeMarker(String bpid) throws IOException;

  void clearRollingUpgradeMarker(String bpid) throws IOException;

  void submitBackgroundSyncFileRangeRequest(final ExtendedBlock block,
      final FileDescriptor fd, final long offset, final long nbytes,
      final int flags);

  void onCompleteLazyPersist(String bpId, long blockId,
      long creationTime, File[] savedFiles, V targetVolume);

   void onFailLazyPersist(String bpId, long blockId);

   ReplicaInfo moveBlockAcrossStorage(final ExtendedBlock block,
        StorageType targetStorageType) throws IOException;

  void setPinning(ExtendedBlock block) throws IOException;

  boolean getPinning(ExtendedBlock block) throws IOException;

  boolean isDeletingBlock(String bpid, long blockId);
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/FsVolumeSpi.java
  FsVolumeReference obtainReference() throws ClosedChannelException;

  String getStorageID();

  String[] getBlockPoolList();

  long getAvailable() throws IOException;

  String getBasePath();

  String getPath(String bpid) throws IOException;

  File getFinalizedDir(String bpid) throws IOException;
  
  StorageType getStorageType();

  boolean isTransientStorage();

  void reserveSpaceForRbw(long bytesToReserve);

  void releaseReservedSpace(long bytesToRelease);

  void releaseLockedMemory(long bytesToRelease);

  interface BlockIterator extends Closeable {
    ExtendedBlock nextBlock() throws IOException;

    boolean atEnd();

    void rewind();

    void save() throws IOException;

    void setMaxStalenessMs(long maxStalenessMs);

    long getIterStartMs();

    long getLastSavedMs();

    String getBlockPoolId();
  }

  BlockIterator newBlockIterator(String bpid, String name);

  BlockIterator loadBlockIterator(String bpid, String name) throws IOException;

  FsDatasetSpi getDataset();
}

