hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/FsDatasetSpi.java
  public static abstract class Factory<D extends FsDatasetSpi<?>> {
    public static Factory<?> getFactory(Configuration conf) {
      @SuppressWarnings("rawtypes")
  public FsVolumeReferences getFsVolumeReferences();

  public void addVolume(
      final StorageLocation location,
      final List<NamespaceInfo> nsInfos) throws IOException;

  public void removeVolumes(Set<File> volumes, boolean clearFailure);

  public DatanodeStorage getStorage(final String storageUuid);

  public StorageReport[] getStorageReports(String bpid)
      throws IOException;

  public V getVolume(ExtendedBlock b);

  public Map<String, Object> getVolumeInfoMap();

  VolumeFailureSummary getVolumeFailureSummary();

  public List<FinalizedReplica> getFinalizedBlocks(String bpid);

  public List<FinalizedReplica> getFinalizedBlocksOnPersistentStorage(String bpid);

  public void checkAndUpdate(String bpid, long blockId, File diskFile,
      File diskMetaFile, FsVolumeSpi vol) throws IOException;

  public LengthInputStream getMetaDataInputStream(ExtendedBlock b
      ) throws IOException;

  public long getLength(ExtendedBlock b) throws IOException;

  @Deprecated
  public Replica getReplica(String bpid, long blockId);

  public String getReplicaString(String bpid, long blockId);

  public Block getStoredBlock(String bpid, long blkid) throws IOException;
  
  public InputStream getBlockInputStream(ExtendedBlock b, long seekOffset)
            throws IOException;

  public ReplicaInputStreams getTmpInputStreams(ExtendedBlock b, long blkoff,
      long ckoff) throws IOException;

  public ReplicaHandler createTemporary(StorageType storageType,
      ExtendedBlock b) throws IOException;

  public ReplicaHandler createRbw(StorageType storageType,
      ExtendedBlock b, boolean allowLazyPersist) throws IOException;

  public ReplicaHandler recoverRbw(ExtendedBlock b,
      long newGS, long minBytesRcvd, long maxBytesRcvd) throws IOException;

  public ReplicaInPipelineInterface convertTemporaryToRbw(
      ExtendedBlock temporary) throws IOException;

  public ReplicaHandler append(ExtendedBlock b, long newGS,
      long expectedBlockLen) throws IOException;

  public ReplicaHandler recoverAppend(
      ExtendedBlock b, long newGS, long expectedBlockLen) throws IOException;
  
  public String recoverClose(ExtendedBlock b, long newGS, long expectedBlockLen
      ) throws IOException;
  
  public void finalizeBlock(ExtendedBlock b) throws IOException;

  public void unfinalizeBlock(ExtendedBlock b) throws IOException;

  public Map<DatanodeStorage, BlockListAsLongs> getBlockReports(String bpid);

  public List<Long> getCacheReport(String bpid);

  public boolean contains(ExtendedBlock block);

  public void checkBlock(ExtendedBlock b, long minLength, ReplicaState state)
      throws ReplicaNotFoundException, UnexpectedReplicaStateException,
      FileNotFoundException, EOFException, IOException;
      
  public boolean isValidBlock(ExtendedBlock b);

  public boolean isValidRbw(ExtendedBlock b);

  public void invalidate(String bpid, Block invalidBlks[]) throws IOException;

  public void cache(String bpid, long[] blockIds);

  public void uncache(String bpid, long[] blockIds);

  public boolean isCached(String bpid, long blockId);

  public Set<File> checkDataDir();

  public void shutdown();

  public void adjustCrcChannelPosition(ExtendedBlock b,
      ReplicaOutputStreams outs, int checksumSize) throws IOException;

  public boolean hasEnoughResource();

  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock
      ) throws IOException;

  public String updateReplicaUnderRecovery(ExtendedBlock oldBlock,
      long recoveryId, long newBlockId, long newLength) throws IOException;

  public void addBlockPool(String bpid, Configuration conf) throws IOException;
  
  public void shutdownBlockPool(String bpid) ;
  
  public void deleteBlockPool(String bpid, boolean force) throws IOException;
  
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock b
      ) throws IOException;

  public HdfsBlocksMetadata getHdfsBlocksMetadata(String bpid,
      long[] blockIds) throws IOException;

  public void enableTrash(String bpid);

  public void clearTrash(String bpid);

  public boolean trashEnabled(String bpid);

  public void setRollingUpgradeMarker(String bpid) throws IOException;

  public void clearRollingUpgradeMarker(String bpid) throws IOException;

  public void submitBackgroundSyncFileRangeRequest(final ExtendedBlock block,
      final FileDescriptor fd, final long offset, final long nbytes,
      final int flags);

   public void onCompleteLazyPersist(String bpId, long blockId,
      long creationTime, File[] savedFiles, V targetVolume);

   public void onFailLazyPersist(String bpId, long blockId);

    public ReplicaInfo moveBlockAcrossStorage(final ExtendedBlock block,
        StorageType targetStorageType) throws IOException;

  public void setPinning(ExtendedBlock block) throws IOException;

  public boolean getPinning(ExtendedBlock block) throws IOException;
  
  public boolean isDeletingBlock(String bpid, long blockId);
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/FsVolumeSpi.java
  FsVolumeReference obtainReference() throws ClosedChannelException;

  public String getStorageID();

  public String[] getBlockPoolList();

  public long getAvailable() throws IOException;

  public String getBasePath();

  public String getPath(String bpid) throws IOException;

  public File getFinalizedDir(String bpid) throws IOException;
  
  public StorageType getStorageType();

  public void reserveSpaceForRbw(long bytesToReserve);

  public void releaseReservedSpace(long bytesToRelease);

  public boolean isTransientStorage();
  public void releaseLockedMemory(long bytesToRelease);

  public interface BlockIterator extends Closeable {
    public ExtendedBlock nextBlock() throws IOException;

    public boolean atEnd();

    public void rewind();

    public void save() throws IOException;

    public void setMaxStalenessMs(long maxStalenessMs);

    public long getIterStartMs();

    public long getLastSavedMs();

    public String getBlockPoolId();
  }

  public BlockIterator newBlockIterator(String bpid, String name);

  public BlockIterator loadBlockIterator(String bpid, String name)
      throws IOException;

  public FsDatasetSpi getDataset();
}

