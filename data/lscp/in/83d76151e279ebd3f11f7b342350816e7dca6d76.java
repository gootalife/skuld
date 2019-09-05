hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfo.java
  public abstract int numNodes();

  abstract boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock);

  abstract boolean removeStorage(DatanodeStorageInfo storage);

  abstract void replaceBlock(BlockInfo newBlock);


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoContiguous.java
  }

  @Override
  boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    return ContiguousBlockStorageOp.addStorage(this, storage);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoUnderConstructionContiguous.java
  }

  @Override
  boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    return ContiguousBlockStorageOp.addStorage(this, storage);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
  public final Map<String, LightWeightLinkedSet<BlockInfo>> excessReplicateMap =
    new TreeMap<>();

  public void metaSave(PrintWriter out) {
    assert namesystem.hasWriteLock();
    final List<DatanodeDescriptor> live = new ArrayList<>();
    final List<DatanodeDescriptor> dead = new ArrayList<>();
    datanodeManager.fetchDatanodes(live, dead, false);
    out.println("Live Datanodes: " + live.size());
    out.println("Dead Datanodes: " + dead.size());
    List<DatanodeDescriptor> containingNodes =
                                      new ArrayList<DatanodeDescriptor>();
    List<DatanodeStorageInfo> containingLiveReplicasNodes =
      new ArrayList<>();

    NumberReplicas numReplicas = new NumberReplicas();
    Collection<DatanodeDescriptor> corruptNodes = 
                                  corruptReplicas.getNodes(block);
    
    for (DatanodeStorageInfo storage : getStorages(block)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      String state = "";
      if (corruptNodes != null && corruptNodes.contains(node)) {
    return maxReplicationStreams;
  }

  public int getDefaultStorageNum(BlockInfo block) {
    return defaultReplication;
  }

  public short getMinStorageNum(BlockInfo block) {
    return minReplication;
  }

  public boolean hasMinStorage(BlockInfo block) {
    return hasMinStorage(block, countNodes(block).liveReplicas());
  }

  public boolean hasMinStorage(BlockInfo block, int liveNum) {
    return liveNum >= getMinStorageNum(block);
  }

  private static boolean commitBlock(
      final BlockInfoUnderConstruction block, final Block commitBlock)
      throws IOException {
    if (block.getBlockUCState() == BlockUCState.COMMITTED) {
      return false;
    }
    assert block.getNumBytes() <= commitBlock.getNumBytes() :
      "commitBlock length is less than the stored one "
      + commitBlock.getNumBytes() + " vs. " + block.getNumBytes();
  public boolean commitOrCompleteLastBlock(BlockCollection bc,
      Block commitBlock) throws IOException {
    if (commitBlock == null) {
      return false; // not committing, this is a block allocation retry
    }
    BlockInfo lastBlock = bc.getLastBlock();
    if (lastBlock == null) {
      return false; // no blocks in file yet
    }
    if (lastBlock.isComplete()) {
      return false; // already completed (e.g. by syncBlock)
    }

    final boolean b = commitBlock(
        (BlockInfoUnderConstruction) lastBlock, commitBlock);
    if(hasMinStorage(lastBlock)) {
      completeBlock(bc, bc.numBlocks()-1, false);
    }
    return b;
  }

  private BlockInfo completeBlock(final BlockCollection bc,
      final int blkIndex, boolean force) throws IOException {
    if (blkIndex < 0) {
      return null;
    }
    BlockInfo curBlock = bc.getBlocks()[blkIndex];
    if(curBlock.isComplete()) {
      return curBlock;
    }
    BlockInfoUnderConstruction ucBlock =
        (BlockInfoUnderConstruction) curBlock;
    int numNodes = ucBlock.numNodes();
    if (!force && !hasMinStorage(curBlock, numNodes)) {
      throw new IOException("Cannot complete block: " +
          "block does not satisfy minimal replication requirement.");
    }
    if(!force && ucBlock.getBlockUCState() != BlockUCState.COMMITTED) {
      throw new IOException(
          "Cannot complete block: block has not been COMMITTED by the client");
    }
    BlockInfo completeBlock = ucBlock.convertToCompleteBlock();
    bc.setBlock(blkIndex, completeBlock);
    namesystem.adjustSafeModeBlockTotals(
        hasMinStorage(oldBlock, targets.length) ? -1 : 0,
        -1);

  private List<DatanodeStorageInfo> getValidLocations(Block block) {
    final List<DatanodeStorageInfo> locations
        = new ArrayList<>(blocksMap.numNodes(block));
    for(DatanodeStorageInfo storage : getStorages(block)) {
      if(!invalidateBlocks.contains(storage.getDatanodeDescriptor(), block)) {
        locations.add(storage);
      final BlockInfo[] blocks,
      final long offset, final long length, final int nrBlocksToReturn,
      final AccessMode mode) throws IOException {
    int curBlk;
    long curPos = 0, blkSize = 0;
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
    }

    if (nrBlocks > 0 && curBlk == nrBlocks)   // offset >= end of file
      return Collections.emptyList();

    long endOff = offset + length;
    List<LocatedBlock> results = new ArrayList<>(blocks.length);
    do {
      results.add(createLocatedBlock(blocks[curBlk], curPos, mode));
      curPos += blocks[curBlk].getNumBytes();

  private LocatedBlock createLocatedBlock(final BlockInfo[] blocks,
      final long endPos, final AccessMode mode) throws IOException {
    int curBlk;
    long curPos = 0;
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
  }

  private LocatedBlock createLocatedBlock(final BlockInfo blk, final long pos)
      throws IOException {
    if (blk instanceof BlockInfoUnderConstruction) {
      if (blk.isComplete()) {
        throw new IOException(
      final BlockInfoUnderConstruction uc =
          (BlockInfoUnderConstruction) blk;
      final DatanodeStorageInfo[] storages = uc.getExpectedStorageLocations();
      final ExtendedBlock eb =
          new ExtendedBlock(namesystem.getBlockPoolId(), blk);
      return newLocatedBlock(eb, storages, pos, false);
    }

    final DatanodeStorageInfo[] machines = new DatanodeStorageInfo[numMachines];
    int j = 0;
    if (numMachines > 0) {
      for(DatanodeStorageInfo storage : getStorages(blk)) {
        final DatanodeDescriptor d = storage.getDatanodeDescriptor();
        final boolean replicaCorrupt = corruptReplicas.isReplicaCorrupt(blk, d);
        if (isCorrupt || (!replicaCorrupt)) {
          machines[j++] = storage;
        }
      }
    }
    assert j == machines.length :
      "isCorrupt: " + isCorrupt + 
      " numMachines: " + numMachines +
    for(int i=0; i<startBlock; i++) {
      iter.next();
    }
    List<BlockWithLocations> results = new ArrayList<>();
    long totalSize = 0;
    BlockInfo curBlock;
    while(totalSize<size && iter.hasNext()) {
   
  void removeBlocksAssociatedTo(final DatanodeDescriptor node) {
    final Iterator<BlockInfo> it = node.getBlockIterator();
    while(it.hasNext()) {
      removeStoredBlock(it.next(), node);
    }
  void removeBlocksAssociatedTo(final DatanodeStorageInfo storageInfo) {
    assert namesystem.hasWriteLock();
    final Iterator<BlockInfo> it = storageInfo.getBlockIterator();
    DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();
    while(it.hasNext()) {
      BlockInfo block = it.next();
      removeStoredBlock(block, node);
      invalidateBlocks.remove(node, block);
    }
  private void addToInvalidates(BlockInfo storedBlock) {
    if (!namesystem.isPopulatingReplQueues()) {
      return;
    }
    StringBuilder datanodes = new StringBuilder();
    for(DatanodeStorageInfo storage : blocksMap.getStorages(storedBlock,
        State.NORMAL)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      invalidateBlocks.add(storedBlock, node, false);
      datanodes.append(node).append(" ");
    }
    if (datanodes.length() != 0) {
      blockLog.info("BLOCK* addToInvalidates: {} {}", storedBlock,
          datanodes.toString());
    }
  }

  public void findAndMarkBlockAsCorrupt(final ExtendedBlock blk,
      final DatanodeInfo dn, String storageID, String reason) throws IOException {
    assert namesystem.hasWriteLock();
    final Block reportedBlock = blk.getLocalBlock();
    final BlockInfo storedBlock = getStoredBlock(reportedBlock);
    if (storedBlock == null) {
          + ") does not exist");
    }

    markBlockAsCorrupt(new BlockToMarkCorrupt(reportedBlock, storedBlock,
            blk.getGenerationStamp(), reason, Reason.CORRUPTION_REPORTED),
        storageID == null ? null : node.getStorageInfo(storageID),
        node);
      DatanodeStorageInfo storageInfo,
      DatanodeDescriptor node) throws IOException {

    if (b.stored.isDeleted()) {
      blockLog.info("BLOCK markBlockAsCorrupt: {} cannot be marked as" +
          " corrupt as it does not belong to any file", b);
      addToInvalidates(b.corrupted, node);
      return;
    } 
    short expectedReplicas =
        getExpectedReplicaNum(b.stored.getBlockCollection(), b.stored);

    if (storageInfo != null) {
      storageInfo.addBlock(b.stored, b.corrupted);
    }

    NumberReplicas numberOfReplicas = countNodes(b.stored);
    boolean hasEnoughLiveReplicas = numberOfReplicas.liveReplicas() >=
        expectedReplicas;
    boolean minReplicationSatisfied = hasMinStorage(b.stored,
        numberOfReplicas.liveReplicas());
    boolean hasMoreCorruptReplicas = minReplicationSatisfied &&
        (numberOfReplicas.liveReplicas() + numberOfReplicas.corruptReplicas()) >
        expectedReplicas;
    int additionalReplRequired;

    int scheduledWork = 0;
    List<ReplicationWork> work = new LinkedList<>();

    namesystem.writeLock();
    try {
              continue;
            }

            requiredReplication = getExpectedReplicaNum(bc, block);

            containingNodes = new ArrayList<>();
            List<DatanodeStorageInfo> liveReplicaNodes = new ArrayList<>();
            NumberReplicas numReplicas = new NumberReplicas();
            srcNode = chooseSourceDatanode(
                block, containingNodes, liveReplicaNodes, numReplicas,
      
            if (numEffectiveReplicas >= requiredReplication) {
              if ( (pendingReplications.getNumReplicas(block) > 0) ||
                   (blockHasEnoughRacks(block, requiredReplication)) ) {
                neededReplications.remove(block, priority); // remove from neededReplications
                blockLog.info("BLOCK* Removing {} from neededReplications as" +
                        " it has enough replicas", block);
      namesystem.writeUnlock();
    }

    final Set<Node> excludedNodes = new HashSet<>();
    for(ReplicationWork rw : work){
            rw.targets = null;
            continue;
          }
          requiredReplication = getExpectedReplicaNum(bc, block);

          NumberReplicas numReplicas = countNodes(block);

          if (numEffectiveReplicas >= requiredReplication) {
            if ( (pendingReplications.getNumReplicas(block) > 0) ||
                 (blockHasEnoughRacks(block, requiredReplication)) ) {
              neededReplications.remove(block, priority); // remove from neededReplications
              rw.targets = null;
              blockLog.info("BLOCK* Removing {} from neededReplications as" +
          }

          if ( (numReplicas.liveReplicas() >= requiredReplication) &&
               (!blockHasEnoughRacks(block, requiredReplication)) ) {
            if (rw.srcNode.getNetworkLocation().equals(
                targets[0].getDatanodeDescriptor().getNetworkLocation())) {
  List<DatanodeDescriptor> getDatanodeDescriptors(List<String> nodes) {
    List<DatanodeDescriptor> datanodeDescriptors = null;
    if (nodes != null) {
      datanodeDescriptors = new ArrayList<>(nodes.size());
      for (int i = 0; i < nodes.size(); i++) {
        DatanodeDescriptor node = datanodeManager.getDatanodeDescriptor(nodes.get(i));
        if (node != null) {
    int excess = 0;
    
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(block);
    for(DatanodeStorageInfo storage : getStorages(block)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      LightWeightLinkedSet<BlockInfo> excessBlocks =
        excessReplicateMap.get(node.getDatanodeUuid());
      int countableReplica = storage.getState() == State.NORMAL ? 1 : 0; 
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node)))
          BlockInfo bi = getStoredBlock(timedOutItems[i]);
          if (bi == null) {
            continue;
          }
    }
  }

  private static class BlockInfoToAdd {
    private final BlockInfo stored;
    private final Block reported;

    BlockInfoToAdd(BlockInfo stored, Block reported) {
      this.stored = stored;
      this.reported = reported;
    }

    public BlockInfo getStored() {
      return stored;
    }

    public Block getReported() {
      return reported;
    }
  }

  private static class BlockToMarkCorrupt {
    final Block corrupted;
    final BlockInfo stored;
    final Reason reasonCode;

    BlockToMarkCorrupt(Block corrupted,
        BlockInfo stored, String reason,
        Reason reasonCode) {
      Preconditions.checkNotNull(corrupted, "corrupted is null");
      this.reasonCode = reasonCode;
    }

    BlockToMarkCorrupt(Block corrupted, BlockInfo stored, long gs,
        String reason, Reason reasonCode) {
      this(corrupted, stored, reason, reasonCode);
      corrupted.setGenerationStamp(gs);
    }
          break;
        }

        BlockInfo bi = getStoredBlock(b);
        if (bi == null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("BLOCK* rescanPostponedMisreplicatedBlocks: " +
    Collection<BlockInfoToAdd> toAdd = new LinkedList<>();
    Collection<BlockInfo> toRemove = new TreeSet<>();
    Collection<Block> toInvalidate = new LinkedList<>();
    Collection<BlockToMarkCorrupt> toCorrupt = new LinkedList<>();
    Collection<StatefulBlockInfo> toUC = new LinkedList<>();
    reportDiff(storageInfo, report,
        toAdd, toRemove, toInvalidate, toCorrupt, toUC);

    for (StatefulBlockInfo b : toUC) {
      addStoredBlockUnderConstruction(b, storageInfo);
    }
    for (BlockInfo b : toRemove) {
      removeStoredBlock(b, node);
    }
    int numBlocksLogged = 0;
    for (BlockInfoToAdd b : toAdd) {
      addStoredBlock(b.getStored(), b.getReported(), storageInfo, null,
          numBlocksLogged < maxNumBlocksToLog);
      numBlocksLogged++;
    }
    if (numBlocksLogged > maxNumBlocksToLog) {
  public void markBlockReplicasAsCorrupt(Block oldBlock, BlockInfo block,
      long oldGenerationStamp, long oldNumBytes,
      DatanodeStorageInfo[] newStorages) throws IOException {
    assert namesystem.hasWriteLock();
    BlockToMarkCorrupt b = null;
    if (block.getGenerationStamp() != oldGenerationStamp) {
      b = new BlockToMarkCorrupt(oldBlock, block, oldGenerationStamp,
          "genstamp does not match " + oldGenerationStamp
          + " : " + block.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
    } else if (block.getNumBytes() != oldNumBytes) {
      b = new BlockToMarkCorrupt(oldBlock, block,
          "length does not match " + oldNumBytes
          + " : " + block.getNumBytes(), Reason.SIZE_MISMATCH);
    } else {
        continue;
      }
      
      BlockInfo storedBlock = getStoredBlock(iblk);
      if (storedBlock == null) continue;
      
      }      
      if (reportedState == ReplicaState.FINALIZED) {
        addStoredBlockImmediate(storedBlock, iblk, storageInfo);
      }
    }
  }

  private void reportDiff(DatanodeStorageInfo storageInfo,
      BlockListAsLongs newReport,
      Collection<BlockInfoToAdd> toAdd,     // add to DatanodeDescriptor
      Collection<BlockInfo> toRemove,       // remove from DatanodeDescriptor
      Collection<Block> toInvalidate,       // should be removed from DN
      Collection<BlockToMarkCorrupt> toCorrupt, // add to corrupt replicas list
      Collection<StatefulBlockInfo> toUC) { // add to under-construction list

    Block delimiterBlock = new Block();
    BlockInfo delimiter = new BlockInfoContiguous(delimiterBlock,
        (short) 1);
    AddBlockResult result = storageInfo.addBlock(delimiter, delimiterBlock);
    assert result == AddBlockResult.ADDED
        : "Delimiting block cannot be present in the node";
    int headIndex = 0; //currently the delimiter is in the head of the list
      if (storedBlock != null &&
          (curIndex = storedBlock.findStorageInfo(storageInfo)) >= 0) {
        headIndex =
            storageInfo.moveBlockToHead(storedBlock, curIndex, headIndex);
      }
    }

    Iterator<BlockInfo> it =
        storageInfo.new BlockIterator(delimiter.getNext(0));
    while (it.hasNext()) {
      toRemove.add(it.next());
    }
    storageInfo.removeBlock(delimiter);
  }

  private BlockInfo processReportedBlock(
      final DatanodeStorageInfo storageInfo,
      final Block block, final ReplicaState reportedState,
      final Collection<BlockInfoToAdd> toAdd,
      final Collection<Block> toInvalidate,
      final Collection<BlockToMarkCorrupt> toCorrupt,
      final Collection<StatefulBlockInfo> toUC) {
    }

    BlockInfo storedBlock = getStoredBlock(block);
    if(storedBlock == null) {
    if (reportedState == ReplicaState.FINALIZED
        && (storedBlock.findStorageInfo(storageInfo) == -1 ||
        corruptReplicas.isReplicaCorrupt(storedBlock, dn))) {
      toAdd.add(new BlockInfoToAdd(storedBlock, block));
    }
    return storedBlock;
  }
      if (rbi.getReportedState() == null) {
        DatanodeStorageInfo storageInfo = rbi.getStorageInfo();
        removeStoredBlock(getStoredBlock(rbi.getBlock()),
            storageInfo.getDatanodeDescriptor());
      } else {
        processAndHandleReportedBlock(rbi.getStorageInfo(),
      case COMMITTED:
        if (storedBlock.getGenerationStamp() != reported.getGenerationStamp()) {
          final long reportedGS = reported.getGenerationStamp();
          return new BlockToMarkCorrupt(new Block(reported), storedBlock, reportedGS,
              "block is " + ucState + " and reported genstamp " + reportedGS
                  + " does not match genstamp in block map "
                  + storedBlock.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
        } else if (storedBlock.getNumBytes() != reported.getNumBytes()) {
          return new BlockToMarkCorrupt(new Block(reported), storedBlock,
              "block is " + ucState + " and reported length " +
                  reported.getNumBytes() + " does not match " +
                  "length in block map " + storedBlock.getNumBytes(),
      case UNDER_CONSTRUCTION:
        if (storedBlock.getGenerationStamp() > reported.getGenerationStamp()) {
          final long reportedGS = reported.getGenerationStamp();
          return new BlockToMarkCorrupt(new Block(reported), storedBlock,
              reportedGS, "block is " + ucState + " and reported state "
              + reportedState + ", But reported genstamp " + reportedGS
              + " does not match genstamp in block map "
              + storedBlock.getGenerationStamp(),
              Reason.GENSTAMP_MISMATCH);
        }
        return null;
      default:
    case RWR:
      if (!storedBlock.isComplete()) {
        return null; // not corrupt
      } else if (storedBlock.getGenerationStamp() !=
          reported.getGenerationStamp()) {
        final long reportedGS = reported.getGenerationStamp();
        return new BlockToMarkCorrupt(
            new Block(reported), storedBlock, reportedGS,
            "reported " + reportedState +
                " replica with genstamp " + reportedGS +
                " does not match COMPLETE block's genstamp in block map " +
                storedBlock.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
      } else { // COMPLETE block, same genstamp
        if (reportedState == ReplicaState.RBW) {
              "complete with the same genstamp");
          return null;
        } else {
          return new BlockToMarkCorrupt(new Block(reported), storedBlock,
              "reported replica has invalid state " + reportedState,
              Reason.INVALID_STATE);
        }
      " on " + dn + " size " + storedBlock.getNumBytes();
      LOG.warn(msg);
      return new BlockToMarkCorrupt(new Block(reported), storedBlock, msg,
          Reason.INVALID_STATE);
    }
  }


    if (ucBlock.reportedState == ReplicaState.FINALIZED &&
        (block.findStorageInfo(storageInfo) < 0)) {
      addStoredBlock(block, ucBlock.reportedBlock, storageInfo, null, true);
    }
  } 

  private void addStoredBlockImmediate(BlockInfo storedBlock, Block reported,
      DatanodeStorageInfo storageInfo)
      throws IOException {
    assert (storedBlock != null && namesystem.hasWriteLock());
    if (!namesystem.isInStartupSafeMode()
        || namesystem.isPopulatingReplQueues()) {
      addStoredBlock(storedBlock, reported, storageInfo, null, false);
      return;
    }

    AddBlockResult result = storageInfo.addBlock(storedBlock, reported);

    int numCurrentReplica = countLiveNodes(storedBlock);
    if (storedBlock.getBlockUCState() == BlockUCState.COMMITTED
        && hasMinStorage(storedBlock, numCurrentReplica)) {
      completeBlock(storedBlock.getBlockCollection(), storedBlock, false);
    } else if (storedBlock.isComplete() && result == AddBlockResult.ADDED) {
  private Block addStoredBlock(final BlockInfo block,
      final Block reportedBlock,
      DatanodeStorageInfo storageInfo,
      DatanodeDescriptor delNodeHint,
      boolean logEveryBlock)
    DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();
    if (block instanceof BlockInfoUnderConstruction) {
      storedBlock = getStoredBlock(block);
    } else {
      storedBlock = block;
    }
      return block;
    }
    BlockCollection bc = storedBlock.getBlockCollection();

    AddBlockResult result = storageInfo.addBlock(storedBlock, reportedBlock);

    int curReplicaDelta;
    if (result == AddBlockResult.ADDED) {
        + pendingReplications.getNumReplicas(storedBlock);

    if(storedBlock.getBlockUCState() == BlockUCState.COMMITTED &&
        hasMinStorage(storedBlock, numLiveReplicas)) {
      storedBlock = completeBlock(bc, storedBlock, false);
    } else if (storedBlock.isComplete() && result == AddBlockResult.ADDED) {
    }

    short fileReplication = getExpectedReplicaNum(bc, storedBlock);
    if (!isNeededReplication(storedBlock, fileReplication, numCurrentReplica)) {
      neededReplications.remove(storedBlock, numCurrentReplica,
          num.decommissionedAndDecommissioning(), fileReplication);
    int numCorruptNodes = num.corruptReplicas();
    if (numCorruptNodes != corruptReplicasCount) {
      LOG.warn("Inconsistent number of corrupt replicas for " +
          storedBlock + ". blockMap has " + numCorruptNodes +
          " but corrupt replicas map has " + corruptReplicasCount);
    }
    if ((corruptReplicasCount > 0) && (numLiveReplicas >= fileReplication)) {
      invalidateCorruptReplicas(storedBlock, reportedBlock);
    }
    return storedBlock;
  }

  private void invalidateCorruptReplicas(BlockInfo blk, Block reported) {
    Collection<DatanodeDescriptor> nodes = corruptReplicas.getNodes(blk);
    boolean removedFromBlocksMap = true;
    if (nodes == null)
    DatanodeDescriptor[] nodesCopy = nodes.toArray(new DatanodeDescriptor[0]);
    for (DatanodeDescriptor node : nodesCopy) {
      try {
        if (!invalidateBlock(new BlockToMarkCorrupt(reported, blk, null,
            Reason.ANY), node)) {
          removedFromBlocksMap = false;
        }
    }
    short expectedReplication =
        getExpectedReplicaNum(block.getBlockCollection(), block);
    NumberReplicas num = countNodes(block);
    int numCurrentReplica = num.liveReplicas();
  private void processOverReplicatedBlock(final BlockInfo block,
      final short replication, final DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint) {
    assert namesystem.hasWriteLock();
    if (addedNode == delNodeHint) {
      delNodeHint = null;
    }
    Collection<DatanodeStorageInfo> nonExcess = new ArrayList<>();
    Collection<DatanodeDescriptor> corruptNodes = corruptReplicas
        .getNodes(block);
    for(DatanodeStorageInfo storage : blocksMap.getStorages(block, State.NORMAL)) {
        postponeBlock(block);
        return;
      }
      LightWeightLinkedSet<BlockInfo> excessBlocks = excessReplicateMap.get(
          cur.getDatanodeUuid());
      if (excessBlocks == null || !excessBlocks.contains(block)) {
        if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
  private void chooseExcessReplicates(
      final Collection<DatanodeStorageInfo> nonExcess,
      BlockInfo storedBlock, short replication,
      DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint,
      BlockPlacementPolicy replicator) {
    assert namesystem.hasWriteLock();
    BlockCollection bc = getBlockCollection(storedBlock);
    final BlockStoragePolicy storagePolicy = storagePolicySuite.getPolicy(
        bc.getStoragePolicyID());
    final List<StorageType> excessTypes = storagePolicy.chooseExcess(
        replication, DatanodeStorageInfo.toStorageTypes(nonExcess));

    final Map<String, List<DatanodeStorageInfo>> rackMap = new HashMap<>();
    final List<DatanodeStorageInfo> moreThanOne = new ArrayList<>();
    final List<DatanodeStorageInfo> exactlyOne = new ArrayList<>();

          moreThanOne, excessTypes)) {
        cur = delNodeHintStorage;
      } else { // regular excessive replica removal
        cur = replicator.chooseReplicaToDelete(bc, storedBlock, replication,
            moreThanOne, exactlyOne, excessTypes);
      }
      firstOne = false;
      replicator.adjustSetsWithChosenReplica(rackMap, moreThanOne,
          exactlyOne, cur);

      processChosenExcessReplica(nonExcess, cur, storedBlock);
    }
  }

  private void processChosenExcessReplica(
      final Collection<DatanodeStorageInfo> nonExcess,
      final DatanodeStorageInfo chosen, BlockInfo storedBlock) {
    nonExcess.remove(chosen);
    addToExcessReplicate(chosen.getDatanodeDescriptor(), storedBlock);
    addToInvalidates(storedBlock, chosen.getDatanodeDescriptor());
    blockLog.info("BLOCK* chooseExcessReplicates: "
        +"({}, {}) is added to invalidated blocks set", chosen, storedBlock);
  }

    }
  }

  private void addToExcessReplicate(DatanodeInfo dn, BlockInfo storedBlock) {
    assert namesystem.hasWriteLock();
    LightWeightLinkedSet<BlockInfo> excessBlocks = excessReplicateMap.get(
        dn.getDatanodeUuid());
    if (excessBlocks == null) {
      excessBlocks = new LightWeightLinkedSet<>();
      excessReplicateMap.put(dn.getDatanodeUuid(), excessBlocks);
    }
    if (excessBlocks.add(storedBlock)) {
      excessBlocksCount.incrementAndGet();
      blockLog.debug("BLOCK* addToExcessReplicate: ({}, {}) is added to"
          + " excessReplicateMap", dn, storedBlock);
    }
  }

          QUEUE_REASON_FUTURE_GENSTAMP);
      return;
    }
    removeStoredBlock(getStoredBlock(block), node);
  }

  public void removeStoredBlock(BlockInfo storedBlock,
      DatanodeDescriptor node) {
    blockLog.debug("BLOCK* removeStoredBlock: {} from {}", storedBlock, node);
    assert (namesystem.hasWriteLock());
    {
      if (storedBlock == null || !blocksMap.removeNode(storedBlock, node)) {
        blockLog.debug("BLOCK* removeStoredBlock: {} has already been" +
            " removed from node {}", storedBlock, node);
        return;
      }

      CachedBlock cblock = namesystem.getCacheManager().getCachedBlocks()
          .get(new CachedBlock(storedBlock.getBlockId(), (short) 0, false));
      if (cblock != null) {
        boolean removed = false;
        removed |= node.getPendingCached().remove(cblock);
        removed |= node.getPendingUncached().remove(cblock);
        if (removed) {
          blockLog.debug("BLOCK* removeStoredBlock: {} removed from caching "
              + "related lists on node {}", storedBlock, node);
        }
      }

      BlockCollection bc = storedBlock.getBlockCollection();
      if (bc != null) {
        namesystem.decrementSafeBlockCount(storedBlock);
        updateNeededReplications(storedBlock, -1, 0);
      LightWeightLinkedSet<BlockInfo> excessBlocks = excessReplicateMap.get(
          node.getDatanodeUuid());
      if (excessBlocks != null) {
        if (excessBlocks.remove(storedBlock)) {
          excessBlocksCount.decrementAndGet();
          blockLog.debug("BLOCK* removeStoredBlock: {} is removed from " +
              "excessBlocks", storedBlock);
          if (excessBlocks.size() == 0) {
            excessReplicateMap.remove(node.getDatanodeUuid());
          }
      }

      corruptReplicas.removeFromCorruptReplicasMap(storedBlock, node);
    }
  }

  private long addBlock(BlockInfo block, List<BlockWithLocations> results) {
    final List<DatanodeStorageInfo> locations = getValidLocations(block);
    if(locations.size() == 0) {
      return 0;
      ReplicaState reportedState, DatanodeDescriptor delHintNode)
      throws IOException {
    Collection<BlockInfoToAdd> toAdd = new LinkedList<>();
    Collection<Block> toInvalidate = new LinkedList<>();
    Collection<BlockToMarkCorrupt> toCorrupt = new LinkedList<>();
    Collection<StatefulBlockInfo> toUC = new LinkedList<>();
    final DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();

    processReportedBlock(storageInfo, block, reportedState, toAdd, toInvalidate,
        toCorrupt, toUC);
    assert toUC.size() + toAdd.size() + toInvalidate.size() + toCorrupt.size() <= 1
      addStoredBlockUnderConstruction(b, storageInfo);
    }
    long numBlocksLogged = 0;
    for (BlockInfoToAdd b : toAdd) {
      addStoredBlock(b.getStored(), b.getReported(), storageInfo, delHintNode,
          numBlocksLogged < maxNumBlocksToLog);
      numBlocksLogged++;
    }
    if (numBlocksLogged > maxNumBlocksToLog) {
      } else if (node.isDecommissioned()) {
        decommissioned++;
      } else {
        LightWeightLinkedSet<BlockInfo> blocksExcess =
            excessReplicateMap.get(node.getDatanodeUuid());
        if (blocksExcess != null && blocksExcess.contains(b)) {
          excess++;
        } else {
    int numOverReplicated = 0;
    while(it.hasNext()) {
      final BlockInfo block = it.next();
      int expectedReplication = this.getReplication(block);
      NumberReplicas num = countNodes(block);
      int numCurrentReplica = num.liveReplicas();
      if (numCurrentReplica > expectedReplication) {
        processOverReplicatedBlock(block, (short) expectedReplication, null,
            null);
        numOverReplicated++;
      }
    }
    return blocksMap.size();
  }


  public Iterable<DatanodeStorageInfo> getStorages(final Block block) {
    return blocksMap.getStorages(block);
  }

  public DatanodeStorageInfo[] getStorages(BlockInfo block) {
    final DatanodeStorageInfo[] storages = new DatanodeStorageInfo[block.numNodes()];
    int i = 0;
        final BlockInfoUnderConstruction uc =
            (BlockInfoUnderConstruction)b;
        final int numNodes = b.numNodes();
        final int min = getMinStorageNum(b);
        final BlockUCState state = b.getBlockUCState();
        LOG.info("BLOCK* " + b + " is not COMPLETE (ucState = " + state
            + ", replication# = " + numNodes
            + (numNodes < min ? " < " : " >= ")
            + " minimum = " + min + ") in file " + src);
        return false;
      }
    }
  private int getReplication(BlockInfo block) {
    final BlockCollection bc = blocksMap.getBlockCollection(block);
    return bc == null? 0: getExpectedReplicaNum(bc, block);
  }


    return toInvalidate.size();
  }

  boolean blockHasEnoughRacks(BlockInfo storedBlock, int expectedStorageNum) {
    if (!this.shouldCheckForEnoughRacks) {
      return true;
    }
    boolean enoughRacks = false;
    Collection<DatanodeDescriptor> corruptNodes =
        corruptReplicas.getNodes(storedBlock);
    String rackName = null;
    for(DatanodeStorageInfo storage : getStorages(storedBlock)) {
      final DatanodeDescriptor cur = storage.getDatanodeDescriptor();
      if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
        if ((corruptNodes == null ) || !corruptNodes.contains(cur)) {
          if (expectedStorageNum == 1 || (expectedStorageNum > 1 &&
              !datanodeManager.hasClusterEverBeenMultiRack())) {
            enoughRacks = true;
            break;
  boolean isNeededReplication(BlockInfo storedBlock, int expected,
      int current) {
    return current < expected || !blockHasEnoughRacks(storedBlock, expected);
  }

  public short getExpectedReplicaNum(BlockCollection bc, BlockInfo block) {
    return bc.getPreferredBlockReplication();
  }
  
  public long getMissingBlocksCount() {
    return blocksMap.getBlockCollection(b);
  }

  public int numCorruptReplicas(Block block) {
    return corruptReplicas.numCorruptReplicas(block);
  }
  private void removeFromExcessReplicateMap(Block block) {
    for (DatanodeStorageInfo info : getStorages(block)) {
      String uuid = info.getDatanodeDescriptor().getDatanodeUuid();
      LightWeightLinkedSet<BlockInfo> excessReplicas =
          excessReplicateMap.get(uuid);
      if (excessReplicas != null) {
        if (excessReplicas.remove(block)) {
          excessBlocksCount.decrementAndGet();
    return blocksMap.getCapacity();
  }


  enum MisReplicationResult {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DatanodeStorageInfo.java
import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
    return blockPoolUsed;
  }

  public AddBlockResult addBlock(BlockInfo b, Block reportedBlock) {
    AddBlockResult result = AddBlockResult.ADDED;
    }

    b.addStorage(this, reportedBlock);
    insertToList(b);
    return result;
  }

  AddBlockResult addBlock(BlockInfo b) {
    return addBlock(b, b);
  }

  public void insertToList(BlockInfo b) {
    blockList = b.listInsert(blockList, this);
    numBlocks++;
  }

  public boolean removeBlock(BlockInfo b) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
      if (trackBlockCounts) {
        if (b.isComplete()) {
          numRemovedComplete++;
          if (blockManager.hasMinStorage(b)) {
            numRemovedSafe++;
          }
        }
      curBlock = blocks[nrCompleteBlocks];
      if(!curBlock.isComplete())
        break;
      assert blockManager.hasMinStorage(curBlock) :
              "A COMPLETE block is not minimally replicated in " + src;
    }


    boolean penultimateBlockMinReplication = penultimateBlock == null ? true :
        blockManager.hasMinStorage(penultimateBlock);

    switch(lastBlockState) {
    case COMPLETE:
    case COMMITTED:
      if(penultimateBlockMinReplication &&
          blockManager.hasMinStorage(lastBlock)) {
        finalizeINodeFileUnderConstruction(src, pendingFile,
            iip.getLatestSnapshotId());
        NameNode.stateChangeLog.warn("BLOCK*"
                trimmedTargets.get(i).getStorageInfo(trimmedStorages.get(i));
            if (storageInfo != null) {
              if(copyTruncate) {
                storageInfo.addBlock(truncatedBlock, truncatedBlock);
              } else {
                storageInfo.addBlock(storedBlock, storedBlock);
              }
            }
          }
        } else {
          iFile.setLastBlock(storedBlock, trimmedStorageInfos);
          if (closeFile) {
            blockManager.markBlockReplicasAsCorrupt(oldBlock.getLocalBlock(),
                storedBlock, oldGenerationStamp, oldNumBytes,
                trimmedStorageInfos);
          }
        }
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NamenodeFsck.java
                  .getStorageType()));
            }
            if (showReplicaDetails) {
              LightWeightLinkedSet<BlockInfo> blocksExcess =
                  bm.excessReplicateMap.get(dnDesc.getDatanodeUuid());
              Collection<DatanodeDescriptor> corruptReplicas =
                  bm.getCorruptReplicas(block.getLocalBlock());

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockInfo.java

    final DatanodeStorageInfo storage = DFSTestUtil.createDatanodeStorageInfo("storageID", "127.0.0.1");

    boolean added = blockInfo.addStorage(storage, blockInfo);

    Assert.assertTrue(added);
    Assert.assertEquals(storage, blockInfo.getStorageInfo(0));

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockManager.java
    for (int i = 1; i < pipeline.length; i++) {
      DatanodeStorageInfo storage = pipeline[i];
      bm.addBlock(storage, blockInfo, null);
      blockInfo.addStorage(storage, blockInfo);
    }
  }


    for (DatanodeDescriptor dn : nodes) {
      for (DatanodeStorageInfo storage : dn.getStorageInfos()) {
        blockInfo.addStorage(storage, blockInfo);
      }
    }
    return blockInfo;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestNodeCount.java
      DatanodeDescriptor nonExcessDN = null;
      for(DatanodeStorageInfo storage : bm.blocksMap.getStorages(block.getLocalBlock())) {
        final DatanodeDescriptor dn = storage.getDatanodeDescriptor();
        Collection<BlockInfo> blocks = bm.excessReplicateMap.get(dn.getDatanodeUuid());
        if (blocks == null || !blocks.contains(block.getLocalBlock()) ) {
          nonExcessDN = dn;
          break;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestOverReplicatedBlocks.java
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.junit.Test;

public class TestOverReplicatedBlocks {
      namesystem.readLock();
      Collection<BlockInfo> dnBlocks =
        namesystem.getBlockManager().excessReplicateMap.get(lastDNid);
      assertEquals("Replicas on node " + lastDNid + " should have been deleted",
          SMALL_FILE_LENGTH / SMALL_BLOCK_SIZE, dnBlocks.size());

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestReplicationPolicy.java
    when(storage.removeBlock(any(BlockInfo.class))).thenReturn(true);
    when(storage.addBlock(any(BlockInfo.class))).thenReturn
        (DatanodeStorageInfo.AddBlockResult.ADDED);
    ucBlock.addStorage(storage, ucBlock);

    when(mbc.setLastBlock((BlockInfo) any(), (DatanodeStorageInfo[]) any()))
    .thenReturn(ucBlock);

