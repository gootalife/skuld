hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfo.java
  public abstract int numNodes();

  abstract boolean addStorage(DatanodeStorageInfo storage);

  abstract boolean removeStorage(DatanodeStorageInfo storage);


  abstract void replaceBlock(BlockInfo newBlock);


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoContiguous.java
  }

  @Override
  boolean addStorage(DatanodeStorageInfo storage) {
    return ContiguousBlockStorageOp.addStorage(this, storage);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoUnderConstructionContiguous.java
  }

  @Override
  boolean addStorage(DatanodeStorageInfo storage) {
    return ContiguousBlockStorageOp.addStorage(this, storage);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
  public final Map<String, LightWeightLinkedSet<Block>> excessReplicateMap =
    new TreeMap<String, LightWeightLinkedSet<Block>>();

  public void metaSave(PrintWriter out) {
    assert namesystem.hasWriteLock();
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    datanodeManager.fetchDatanodes(live, dead, false);
    out.println("Live Datanodes: " + live.size());
    out.println("Dead Datanodes: " + dead.size());
    List<DatanodeDescriptor> containingNodes =
                                      new ArrayList<DatanodeDescriptor>();
    List<DatanodeStorageInfo> containingLiveReplicasNodes =
      new ArrayList<DatanodeStorageInfo>();
    
    NumberReplicas numReplicas = new NumberReplicas();
    Collection<DatanodeDescriptor> corruptNodes = 
                                  corruptReplicas.getNodes(block);
    
    for (DatanodeStorageInfo storage : blocksMap.getStorages(block)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      String state = "";
      if (corruptNodes != null && corruptNodes.contains(node)) {
    return maxReplicationStreams;
  }

  public boolean checkMinReplication(BlockInfo block) {
    return (countNodes(block).liveReplicas() >= minReplication);
  }

  private static boolean commitBlock(
      final BlockInfoUnderConstruction block, final Block commitBlock)
      throws IOException {
    if (block.getBlockUCState() == BlockUCState.COMMITTED)
      return false;
    assert block.getNumBytes() <= commitBlock.getNumBytes() :
      "commitBlock length is less than the stored one "
      + commitBlock.getNumBytes() + " vs. " + block.getNumBytes();
  public boolean commitOrCompleteLastBlock(BlockCollection bc,
      Block commitBlock) throws IOException {
    if(commitBlock == null)
      return false; // not committing, this is a block allocation retry
    BlockInfo lastBlock = bc.getLastBlock();
    if(lastBlock == null)
      return false; // no blocks in file yet
    if(lastBlock.isComplete())
      return false; // already completed (e.g. by syncBlock)
    
    final boolean b = commitBlock(
        (BlockInfoUnderConstruction) lastBlock, commitBlock);
    if(countNodes(lastBlock).liveReplicas() >= minReplication)
      completeBlock(bc, bc.numBlocks()-1, false);
    return b;
  }

  private BlockInfo completeBlock(final BlockCollection bc,
      final int blkIndex, boolean force) throws IOException {
    if(blkIndex < 0)
      return null;
    BlockInfo curBlock = bc.getBlocks()[blkIndex];
    if(curBlock.isComplete())
      return curBlock;
    BlockInfoUnderConstruction ucBlock =
        (BlockInfoUnderConstruction) curBlock;
    int numNodes = ucBlock.numNodes();
    if (!force && numNodes < minReplication)
      throw new IOException("Cannot complete block: " +
          "block does not satisfy minimal replication requirement.");
    if(!force && ucBlock.getBlockUCState() != BlockUCState.COMMITTED)
      throw new IOException(
          "Cannot complete block: block has not been COMMITTED by the client");
    BlockInfo completeBlock = ucBlock.convertToCompleteBlock();
    bc.setBlock(blkIndex, completeBlock);
    namesystem.adjustSafeModeBlockTotals(
        targets.length >= minReplication ? -1 : 0,
        -1);

  private List<DatanodeStorageInfo> getValidLocations(Block block) {
    final List<DatanodeStorageInfo> locations
        = new ArrayList<DatanodeStorageInfo>(blocksMap.numNodes(block));
    for(DatanodeStorageInfo storage : blocksMap.getStorages(block)) {
      if(!invalidateBlocks.contains(storage.getDatanodeDescriptor(), block)) {
        locations.add(storage);
      final BlockInfo[] blocks,
      final long offset, final long length, final int nrBlocksToReturn,
      final AccessMode mode) throws IOException {
    int curBlk = 0;
    long curPos = 0, blkSize = 0;
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
    }

    if (nrBlocks > 0 && curBlk == nrBlocks)   // offset >= end of file
      return Collections.<LocatedBlock>emptyList();

    long endOff = offset + length;
    List<LocatedBlock> results = new ArrayList<LocatedBlock>(blocks.length);
    do {
      results.add(createLocatedBlock(blocks[curBlk], curPos, mode));
      curPos += blocks[curBlk].getNumBytes();

  private LocatedBlock createLocatedBlock(final BlockInfo[] blocks,
      final long endPos, final AccessMode mode) throws IOException {
    int curBlk = 0;
    long curPos = 0;
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
  }

  private LocatedBlock createLocatedBlock(final BlockInfo blk, final long pos
      ) throws IOException {
    if (blk instanceof BlockInfoUnderConstruction) {
      if (blk.isComplete()) {
        throw new IOException(
      final BlockInfoUnderConstruction uc =
          (BlockInfoUnderConstruction) blk;
      final DatanodeStorageInfo[] storages = uc.getExpectedStorageLocations();
      final ExtendedBlock eb = new ExtendedBlock(namesystem.getBlockPoolId(), blk);
      return newLocatedBlock(eb, storages, pos, false);
    }

    final DatanodeStorageInfo[] machines = new DatanodeStorageInfo[numMachines];
    int j = 0;
    if (numMachines > 0) {
      for(DatanodeStorageInfo storage : blocksMap.getStorages(blk)) {
        final DatanodeDescriptor d = storage.getDatanodeDescriptor();
        final boolean replicaCorrupt = corruptReplicas.isReplicaCorrupt(blk, d);
        if (isCorrupt || (!replicaCorrupt))
          machines[j++] = storage;
      }
    }
    assert j == machines.length :
      "isCorrupt: " + isCorrupt + 
      " numMachines: " + numMachines +
    for(int i=0; i<startBlock; i++) {
      iter.next();
    }
    List<BlockWithLocations> results = new ArrayList<BlockWithLocations>();
    long totalSize = 0;
    BlockInfo curBlock;
    while(totalSize<size && iter.hasNext()) {
   
  void removeBlocksAssociatedTo(final DatanodeDescriptor node) {
    final Iterator<? extends Block> it = node.getBlockIterator();
    while(it.hasNext()) {
      removeStoredBlock(it.next(), node);
    }
  void removeBlocksAssociatedTo(final DatanodeStorageInfo storageInfo) {
    assert namesystem.hasWriteLock();
    final Iterator<? extends Block> it = storageInfo.getBlockIterator();
    DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();
    while(it.hasNext()) {
      Block block = it.next();
      removeStoredBlock(block, node);
      invalidateBlocks.remove(node, block);
    }
  private void addToInvalidates(Block b) {
    if (!namesystem.isPopulatingReplQueues()) {
      return;
    }
    StringBuilder datanodes = new StringBuilder();
    for(DatanodeStorageInfo storage : blocksMap.getStorages(b, State.NORMAL)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      invalidateBlocks.add(b, node, false);
      datanodes.append(node).append(" ");
    }
    if (datanodes.length() != 0) {
      blockLog.debug("BLOCK* addToInvalidates: {} {}", b,
          datanodes.toString());
    }
  }
  public void findAndMarkBlockAsCorrupt(final ExtendedBlock blk,
      final DatanodeInfo dn, String storageID, String reason) throws IOException {
    assert namesystem.hasWriteLock();
    final BlockInfo storedBlock = getStoredBlock(blk.getLocalBlock());
    if (storedBlock == null) {
          + ") does not exist");
    }
    
    markBlockAsCorrupt(new BlockToMarkCorrupt(storedBlock,
            blk.getGenerationStamp(), reason, Reason.CORRUPTION_REPORTED),
        storageID == null ? null : node.getStorageInfo(storageID),
        node);
      DatanodeStorageInfo storageInfo,
      DatanodeDescriptor node) throws IOException {

    if (b.corrupted.isDeleted()) {
      blockLog.debug("BLOCK markBlockAsCorrupt: {} cannot be marked as" +
          " corrupt as it does not belong to any file", b);
      addToInvalidates(b.corrupted, node);
      return;
    } 
    short expectedReplicas =
        b.corrupted.getBlockCollection().getPreferredBlockReplication();

    if (storageInfo != null) {
      storageInfo.addBlock(b.stored);
    }

    NumberReplicas numberOfReplicas = countNodes(b.stored);
    boolean hasEnoughLiveReplicas = numberOfReplicas.liveReplicas() >=
        expectedReplicas;
    boolean minReplicationSatisfied =
        numberOfReplicas.liveReplicas() >= minReplication;
    boolean hasMoreCorruptReplicas = minReplicationSatisfied &&
        (numberOfReplicas.liveReplicas() + numberOfReplicas.corruptReplicas()) >
        expectedReplicas;
    int additionalReplRequired;

    int scheduledWork = 0;
    List<ReplicationWork> work = new LinkedList<ReplicationWork>();

    namesystem.writeLock();
    try {
              continue;
            }

            requiredReplication = bc.getPreferredBlockReplication();

            containingNodes = new ArrayList<DatanodeDescriptor>();
            List<DatanodeStorageInfo> liveReplicaNodes = new ArrayList<DatanodeStorageInfo>();
            NumberReplicas numReplicas = new NumberReplicas();
            srcNode = chooseSourceDatanode(
                block, containingNodes, liveReplicaNodes, numReplicas,
      
            if (numEffectiveReplicas >= requiredReplication) {
              if ( (pendingReplications.getNumReplicas(block) > 0) ||
                   (blockHasEnoughRacks(block)) ) {
                neededReplications.remove(block, priority); // remove from neededReplications
                blockLog.debug("BLOCK* Removing {} from neededReplications as" +
                        " it has enough replicas", block);
      namesystem.writeUnlock();
    }

    final Set<Node> excludedNodes = new HashSet<Node>();
    for(ReplicationWork rw : work){
            rw.targets = null;
            continue;
          }
          requiredReplication = bc.getPreferredBlockReplication();

          NumberReplicas numReplicas = countNodes(block);

          if (numEffectiveReplicas >= requiredReplication) {
            if ( (pendingReplications.getNumReplicas(block) > 0) ||
                 (blockHasEnoughRacks(block)) ) {
              neededReplications.remove(block, priority); // remove from neededReplications
              rw.targets = null;
              blockLog.debug("BLOCK* Removing {} from neededReplications as" +
          }

          if ( (numReplicas.liveReplicas() >= requiredReplication) &&
               (!blockHasEnoughRacks(block)) ) {
            if (rw.srcNode.getNetworkLocation().equals(
                targets[0].getDatanodeDescriptor().getNetworkLocation())) {
  List<DatanodeDescriptor> getDatanodeDescriptors(List<String> nodes) {
    List<DatanodeDescriptor> datanodeDescriptors = null;
    if (nodes != null) {
      datanodeDescriptors = new ArrayList<DatanodeDescriptor>(nodes.size());
      for (int i = 0; i < nodes.size(); i++) {
        DatanodeDescriptor node = datanodeManager.getDatanodeDescriptor(nodes.get(i));
        if (node != null) {
    int excess = 0;
    
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(block);
    for(DatanodeStorageInfo storage : blocksMap.getStorages(block)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      LightWeightLinkedSet<Block> excessBlocks =
        excessReplicateMap.get(node.getDatanodeUuid());
      int countableReplica = storage.getState() == State.NORMAL ? 1 : 0; 
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node)))
          BlockInfo bi = blocksMap.getStoredBlock(timedOutItems[i]);
          if (bi == null) {
            continue;
          }
    }
  }
  
  private static class BlockToMarkCorrupt {
    final BlockInfo corrupted;
    final BlockInfo stored;
    final Reason reasonCode;

    BlockToMarkCorrupt(BlockInfo corrupted,
        BlockInfo stored, String reason,
        Reason reasonCode) {
      Preconditions.checkNotNull(corrupted, "corrupted is null");
      this.reasonCode = reasonCode;
    }

    BlockToMarkCorrupt(BlockInfo stored, String reason,
        Reason reasonCode) {
      this(stored, stored, reason, reasonCode);
    }

    BlockToMarkCorrupt(BlockInfo stored, long gs, String reason,
        Reason reasonCode) {
      this(new BlockInfoContiguous(stored), stored,
          reason, reasonCode);
      corrupted.setGenerationStamp(gs);
    }
          break;
        }

        BlockInfo bi = blocksMap.getStoredBlock(b);
        if (bi == null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("BLOCK* rescanPostponedMisreplicatedBlocks: " +
    Collection<BlockInfo> toAdd = new LinkedList<BlockInfo>();
    Collection<Block> toRemove = new TreeSet<Block>();
    Collection<Block> toInvalidate = new LinkedList<Block>();
    Collection<BlockToMarkCorrupt> toCorrupt = new LinkedList<BlockToMarkCorrupt>();
    Collection<StatefulBlockInfo> toUC = new LinkedList<StatefulBlockInfo>();
    reportDiff(storageInfo, report,
        toAdd, toRemove, toInvalidate, toCorrupt, toUC);
   
    for (StatefulBlockInfo b : toUC) { 
      addStoredBlockUnderConstruction(b, storageInfo);
    }
    for (Block b : toRemove) {
      removeStoredBlock(b, node);
    }
    int numBlocksLogged = 0;
    for (BlockInfo b : toAdd) {
      addStoredBlock(b, storageInfo, null, numBlocksLogged < maxNumBlocksToLog);
      numBlocksLogged++;
    }
    if (numBlocksLogged > maxNumBlocksToLog) {
  public void markBlockReplicasAsCorrupt(BlockInfo block,
      long oldGenerationStamp, long oldNumBytes, 
      DatanodeStorageInfo[] newStorages) throws IOException {
    assert namesystem.hasWriteLock();
    BlockToMarkCorrupt b = null;
    if (block.getGenerationStamp() != oldGenerationStamp) {
      b = new BlockToMarkCorrupt(block, oldGenerationStamp,
          "genstamp does not match " + oldGenerationStamp
          + " : " + block.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
    } else if (block.getNumBytes() != oldNumBytes) {
      b = new BlockToMarkCorrupt(block,
          "length does not match " + oldNumBytes
          + " : " + block.getNumBytes(), Reason.SIZE_MISMATCH);
    } else {
        continue;
      }
      
      BlockInfo storedBlock = blocksMap.getStoredBlock(iblk);
      if (storedBlock == null) continue;
      
      }      
      if (reportedState == ReplicaState.FINALIZED) {
        addStoredBlockImmediate(storedBlock, storageInfo);
      }
    }
  }

  private void reportDiff(DatanodeStorageInfo storageInfo, 
      BlockListAsLongs newReport, 
      Collection<BlockInfo> toAdd,              // add to DatanodeDescriptor
      Collection<Block> toRemove,           // remove from DatanodeDescriptor
      Collection<Block> toInvalidate,       // should be removed from DN
      Collection<BlockToMarkCorrupt> toCorrupt, // add to corrupt replicas list
      Collection<StatefulBlockInfo> toUC) { // add to under-construction list

    BlockInfo delimiter = new BlockInfoContiguous(new Block(), (short) 1);
    AddBlockResult result = storageInfo.addBlock(delimiter);
    assert result == AddBlockResult.ADDED 
        : "Delimiting block cannot be present in the node";
    int headIndex = 0; //currently the delimiter is in the head of the list
      if (storedBlock != null &&
          (curIndex = storedBlock.findStorageInfo(storageInfo)) >= 0) {
        headIndex = storageInfo.moveBlockToHead(storedBlock, curIndex, headIndex);
      }
    }

    Iterator<BlockInfo> it =
        storageInfo.new BlockIterator(delimiter.getNext(0));
    while(it.hasNext())
      toRemove.add(it.next());
    storageInfo.removeBlock(delimiter);
  }

  private BlockInfo processReportedBlock(
      final DatanodeStorageInfo storageInfo,
      final Block block, final ReplicaState reportedState, 
      final Collection<BlockInfo> toAdd,
      final Collection<Block> toInvalidate, 
      final Collection<BlockToMarkCorrupt> toCorrupt,
      final Collection<StatefulBlockInfo> toUC) {
    }
    
    BlockInfo storedBlock = blocksMap.getStoredBlock(block);
    if(storedBlock == null) {
    if (reportedState == ReplicaState.FINALIZED
        && (storedBlock.findStorageInfo(storageInfo) == -1 ||
            corruptReplicas.isReplicaCorrupt(storedBlock, dn))) {
      toAdd.add(storedBlock);
    }
    return storedBlock;
  }
      if (rbi.getReportedState() == null) {
        DatanodeStorageInfo storageInfo = rbi.getStorageInfo();
        removeStoredBlock(rbi.getBlock(),
            storageInfo.getDatanodeDescriptor());
      } else {
        processAndHandleReportedBlock(rbi.getStorageInfo(),
      case COMMITTED:
        if (storedBlock.getGenerationStamp() != reported.getGenerationStamp()) {
          final long reportedGS = reported.getGenerationStamp();
          return new BlockToMarkCorrupt(storedBlock, reportedGS,
              "block is " + ucState + " and reported genstamp " + reportedGS
              + " does not match genstamp in block map "
              + storedBlock.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
        } else if (storedBlock.getNumBytes() != reported.getNumBytes()) {
          return new BlockToMarkCorrupt(storedBlock,
              "block is " + ucState + " and reported length " +
              reported.getNumBytes() + " does not match " +
              "length in block map " + storedBlock.getNumBytes(),
      case UNDER_CONSTRUCTION:
        if (storedBlock.getGenerationStamp() > reported.getGenerationStamp()) {
          final long reportedGS = reported.getGenerationStamp();
          return new BlockToMarkCorrupt(storedBlock, reportedGS, "block is "
              + ucState + " and reported state " + reportedState
              + ", But reported genstamp " + reportedGS
              + " does not match genstamp in block map "
              + storedBlock.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
        }
        return null;
      default:
    case RWR:
      if (!storedBlock.isComplete()) {
        return null; // not corrupt
      } else if (storedBlock.getGenerationStamp() != reported.getGenerationStamp()) {
        final long reportedGS = reported.getGenerationStamp();
        return new BlockToMarkCorrupt(storedBlock, reportedGS,
            "reported " + reportedState + " replica with genstamp " + reportedGS
            + " does not match COMPLETE block's genstamp in block map "
            + storedBlock.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
      } else { // COMPLETE block, same genstamp
        if (reportedState == ReplicaState.RBW) {
              "complete with the same genstamp");
          return null;
        } else {
          return new BlockToMarkCorrupt(storedBlock,
              "reported replica has invalid state " + reportedState,
              Reason.INVALID_STATE);
        }
      " on " + dn + " size " + storedBlock.getNumBytes();
      LOG.warn(msg);
      return new BlockToMarkCorrupt(storedBlock, msg, Reason.INVALID_STATE);
    }
  }


    if (ucBlock.reportedState == ReplicaState.FINALIZED &&
        (block.findStorageInfo(storageInfo) < 0)) {
      addStoredBlock(block, storageInfo, null, true);
    }
  } 

  private void addStoredBlockImmediate(BlockInfo storedBlock,
      DatanodeStorageInfo storageInfo)
  throws IOException {
    assert (storedBlock != null && namesystem.hasWriteLock());
    if (!namesystem.isInStartupSafeMode() 
        || namesystem.isPopulatingReplQueues()) {
      addStoredBlock(storedBlock, storageInfo, null, false);
      return;
    }

    AddBlockResult result = storageInfo.addBlock(storedBlock);

    int numCurrentReplica = countLiveNodes(storedBlock);
    if (storedBlock.getBlockUCState() == BlockUCState.COMMITTED
        && numCurrentReplica >= minReplication) {
      completeBlock(storedBlock.getBlockCollection(), storedBlock, false);
    } else if (storedBlock.isComplete() && result == AddBlockResult.ADDED) {
  private Block addStoredBlock(final BlockInfo block,
                               DatanodeStorageInfo storageInfo,
                               DatanodeDescriptor delNodeHint,
                               boolean logEveryBlock)
    DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();
    if (block instanceof BlockInfoUnderConstruction) {
      storedBlock = blocksMap.getStoredBlock(block);
    } else {
      storedBlock = block;
    }
      return block;
    }
    BlockCollection bc = storedBlock.getBlockCollection();
    assert bc != null : "Block must belong to a file";

    AddBlockResult result = storageInfo.addBlock(storedBlock);

    int curReplicaDelta;
    if (result == AddBlockResult.ADDED) {
      + pendingReplications.getNumReplicas(storedBlock);

    if(storedBlock.getBlockUCState() == BlockUCState.COMMITTED &&
        numLiveReplicas >= minReplication) {
      storedBlock = completeBlock(bc, storedBlock, false);
    } else if (storedBlock.isComplete() && result == AddBlockResult.ADDED) {
    }

    short fileReplication = bc.getPreferredBlockReplication();
    if (!isNeededReplication(storedBlock, fileReplication, numCurrentReplica)) {
      neededReplications.remove(storedBlock, numCurrentReplica,
          num.decommissionedAndDecommissioning(), fileReplication);
    int numCorruptNodes = num.corruptReplicas();
    if (numCorruptNodes != corruptReplicasCount) {
      LOG.warn("Inconsistent number of corrupt replicas for " +
          storedBlock + "blockMap has " + numCorruptNodes + 
          " but corrupt replicas map has " + corruptReplicasCount);
    }
    if ((corruptReplicasCount > 0) && (numLiveReplicas >= fileReplication))
      invalidateCorruptReplicas(storedBlock);
    return storedBlock;
  }

  private void invalidateCorruptReplicas(BlockInfo blk) {
    Collection<DatanodeDescriptor> nodes = corruptReplicas.getNodes(blk);
    boolean removedFromBlocksMap = true;
    if (nodes == null)
    DatanodeDescriptor[] nodesCopy = nodes.toArray(new DatanodeDescriptor[0]);
    for (DatanodeDescriptor node : nodesCopy) {
      try {
        if (!invalidateBlock(new BlockToMarkCorrupt(blk, null,
              Reason.ANY), node)) {
          removedFromBlocksMap = false;
        }
    }
    short expectedReplication =
        block.getBlockCollection().getPreferredBlockReplication();
    NumberReplicas num = countNodes(block);
    int numCurrentReplica = num.liveReplicas();
  private void processOverReplicatedBlock(final Block block,
      final short replication, final DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint) {
    assert namesystem.hasWriteLock();
    if (addedNode == delNodeHint) {
      delNodeHint = null;
    }
    Collection<DatanodeStorageInfo> nonExcess = new ArrayList<DatanodeStorageInfo>();
    Collection<DatanodeDescriptor> corruptNodes = corruptReplicas
        .getNodes(block);
    for(DatanodeStorageInfo storage : blocksMap.getStorages(block, State.NORMAL)) {
        postponeBlock(block);
        return;
      }
      LightWeightLinkedSet<Block> excessBlocks = excessReplicateMap.get(cur
          .getDatanodeUuid());
      if (excessBlocks == null || !excessBlocks.contains(block)) {
        if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
  private void chooseExcessReplicates(final Collection<DatanodeStorageInfo> nonExcess, 
                              Block b, short replication,
                              DatanodeDescriptor addedNode,
                              DatanodeDescriptor delNodeHint,
                              BlockPlacementPolicy replicator) {
    assert namesystem.hasWriteLock();
    BlockCollection bc = getBlockCollection(b);
    final BlockStoragePolicy storagePolicy = storagePolicySuite.getPolicy(bc.getStoragePolicyID());
    final List<StorageType> excessTypes = storagePolicy.chooseExcess(
        replication, DatanodeStorageInfo.toStorageTypes(nonExcess));


    final Map<String, List<DatanodeStorageInfo>> rackMap
        = new HashMap<String, List<DatanodeStorageInfo>>();
    final List<DatanodeStorageInfo> moreThanOne = new ArrayList<DatanodeStorageInfo>();
    final List<DatanodeStorageInfo> exactlyOne = new ArrayList<DatanodeStorageInfo>();
    
          moreThanOne, excessTypes)) {
        cur = delNodeHintStorage;
      } else { // regular excessive replica removal
        cur = replicator.chooseReplicaToDelete(bc, b, replication,
            moreThanOne, exactlyOne, excessTypes);
      }
      firstOne = false;
      replicator.adjustSetsWithChosenReplica(rackMap, moreThanOne,
          exactlyOne, cur);

      nonExcess.remove(cur);
      addToExcessReplicate(cur.getDatanodeDescriptor(), b);

      addToInvalidates(b, cur.getDatanodeDescriptor());
      blockLog.debug("BLOCK* chooseExcessReplicates: "
                +"({}, {}) is added to invalidated blocks set", cur, b);
    }
  }

    }
  }

  private void addToExcessReplicate(DatanodeInfo dn, Block block) {
    assert namesystem.hasWriteLock();
    LightWeightLinkedSet<Block> excessBlocks = excessReplicateMap.get(dn.getDatanodeUuid());
    if (excessBlocks == null) {
      excessBlocks = new LightWeightLinkedSet<Block>();
      excessReplicateMap.put(dn.getDatanodeUuid(), excessBlocks);
    }
    if (excessBlocks.add(block)) {
      excessBlocksCount.incrementAndGet();
      blockLog.debug("BLOCK* addToExcessReplicate: ({}, {}) is added to"
          + " excessReplicateMap", dn, block);
    }
  }

          QUEUE_REASON_FUTURE_GENSTAMP);
      return;
    }
    removeStoredBlock(block, node);
  }

  public void removeStoredBlock(Block block, DatanodeDescriptor node) {
    blockLog.debug("BLOCK* removeStoredBlock: {} from {}", block, node);
    assert (namesystem.hasWriteLock());
    {
      BlockInfo storedBlock = getStoredBlock(block);
      if (storedBlock == null || !blocksMap.removeNode(storedBlock, node)) {
        blockLog.debug("BLOCK* removeStoredBlock: {} has already been" +
            " removed from node {}", block, node);
        return;
      }

      CachedBlock cblock = namesystem.getCacheManager().getCachedBlocks()
          .get(new CachedBlock(block.getBlockId(), (short) 0, false));
      if (cblock != null) {
        boolean removed = false;
        removed |= node.getPendingCached().remove(cblock);
        removed |= node.getPendingUncached().remove(cblock);
        if (removed) {
          blockLog.debug("BLOCK* removeStoredBlock: {} removed from caching "
              + "related lists on node {}", block, node);
        }
      }

      BlockCollection bc = blocksMap.getBlockCollection(block);
      if (bc != null) {
        namesystem.decrementSafeBlockCount(storedBlock);
        updateNeededReplications(storedBlock, -1, 0);
      LightWeightLinkedSet<Block> excessBlocks = excessReplicateMap.get(node
          .getDatanodeUuid());
      if (excessBlocks != null) {
        if (excessBlocks.remove(block)) {
          excessBlocksCount.decrementAndGet();
          blockLog.debug("BLOCK* removeStoredBlock: {} is removed from " +
              "excessBlocks", block);
          if (excessBlocks.size() == 0) {
            excessReplicateMap.remove(node.getDatanodeUuid());
          }
      }

      corruptReplicas.removeFromCorruptReplicasMap(block, node);
    }
  }

  private long addBlock(Block block, List<BlockWithLocations> results) {
    final List<DatanodeStorageInfo> locations = getValidLocations(block);
    if(locations.size() == 0) {
      return 0;
      ReplicaState reportedState, DatanodeDescriptor delHintNode)
      throws IOException {
    Collection<BlockInfo> toAdd = new LinkedList<BlockInfo>();
    Collection<Block> toInvalidate = new LinkedList<Block>();
    Collection<BlockToMarkCorrupt> toCorrupt = new LinkedList<BlockToMarkCorrupt>();
    Collection<StatefulBlockInfo> toUC = new LinkedList<StatefulBlockInfo>();
    final DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();

    processReportedBlock(storageInfo, block, reportedState,
                              toAdd, toInvalidate, toCorrupt, toUC);
    assert toUC.size() + toAdd.size() + toInvalidate.size() + toCorrupt.size() <= 1
      addStoredBlockUnderConstruction(b, storageInfo);
    }
    long numBlocksLogged = 0;
    for (BlockInfo b : toAdd) {
      addStoredBlock(b, storageInfo, delHintNode, numBlocksLogged < maxNumBlocksToLog);
      numBlocksLogged++;
    }
    if (numBlocksLogged > maxNumBlocksToLog) {
      } else if (node.isDecommissioned()) {
        decommissioned++;
      } else {
        LightWeightLinkedSet<Block> blocksExcess = excessReplicateMap.get(node
            .getDatanodeUuid());
        if (blocksExcess != null && blocksExcess.contains(b)) {
          excess++;
        } else {
    int numOverReplicated = 0;
    while(it.hasNext()) {
      final BlockInfo block = it.next();
      BlockCollection bc = blocksMap.getBlockCollection(block);
      short expectedReplication = bc.getPreferredBlockReplication();
      NumberReplicas num = countNodes(block);
      int numCurrentReplica = num.liveReplicas();
      if (numCurrentReplica > expectedReplication) {
        processOverReplicatedBlock(block, expectedReplication, null, null);
        numOverReplicated++;
      }
    }
    return blocksMap.size();
  }

  public DatanodeStorageInfo[] getStorages(BlockInfo block) {
    final DatanodeStorageInfo[] storages = new DatanodeStorageInfo[block.numNodes()];
    int i = 0;
      String src, BlockInfo[] blocks) {
    for (BlockInfo b: blocks) {
      if (!b.isComplete()) {
        final BlockInfoUnderConstruction uc =
            (BlockInfoUnderConstruction)b;
        final int numNodes = b.numNodes();
        LOG.info("BLOCK* " + b + " is not COMPLETE (ucState = "
          + uc.getBlockUCState() + ", replication# = " + numNodes
          + (numNodes < minReplication ? " < ": " >= ")
          + " minimum = " + minReplication + ") in file " + src);
        return false;
      }
    }
  private int getReplication(Block block) {
    final BlockCollection bc = blocksMap.getBlockCollection(block);
    return bc == null? 0: bc.getPreferredBlockReplication();
  }


    return toInvalidate.size();
  }

  boolean blockHasEnoughRacks(Block b) {
    if (!this.shouldCheckForEnoughRacks) {
      return true;
    }
    boolean enoughRacks = false;;
    Collection<DatanodeDescriptor> corruptNodes = 
                                  corruptReplicas.getNodes(b);
    int numExpectedReplicas = getReplication(b);
    String rackName = null;
    for(DatanodeStorageInfo storage : blocksMap.getStorages(b)) {
      final DatanodeDescriptor cur = storage.getDatanodeDescriptor();
      if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
        if ((corruptNodes == null ) || !corruptNodes.contains(cur)) {
          if (numExpectedReplicas == 1 ||
              (numExpectedReplicas > 1 &&
                  !datanodeManager.hasClusterEverBeenMultiRack())) {
            enoughRacks = true;
            break;
  boolean isNeededReplication(Block b, int expected, int current) {
    return current < expected || !blockHasEnoughRacks(b);
  }
  
  public long getMissingBlocksCount() {
    return blocksMap.getBlockCollection(b);
  }

  public Iterable<DatanodeStorageInfo> getStorages(final Block block) {
    return blocksMap.getStorages(block);
  }

  public int numCorruptReplicas(Block block) {
    return corruptReplicas.numCorruptReplicas(block);
  }
  private void removeFromExcessReplicateMap(Block block) {
    for (DatanodeStorageInfo info : blocksMap.getStorages(block)) {
      String uuid = info.getDatanodeDescriptor().getDatanodeUuid();
      LightWeightLinkedSet<Block> excessReplicas = excessReplicateMap.get(uuid);
      if (excessReplicas != null) {
        if (excessReplicas.remove(block)) {
          excessBlocksCount.decrementAndGet();

  enum MisReplicationResult {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DatanodeStorageInfo.java
import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
    return blockPoolUsed;
  }

  public AddBlockResult addBlock(BlockInfo b) {
    AddBlockResult result = AddBlockResult.ADDED;
    }

    b.addStorage(this);
    blockList = b.listInsert(blockList, this);
    numBlocks++;
    return result;
  }

  public boolean removeBlock(BlockInfo b) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
      if (trackBlockCounts) {
        if (b.isComplete()) {
          numRemovedComplete++;
          if (blockManager.checkMinReplication(b)) {
            numRemovedSafe++;
          }
        }
      curBlock = blocks[nrCompleteBlocks];
      if(!curBlock.isComplete())
        break;
      assert blockManager.checkMinReplication(curBlock) :
              "A COMPLETE block is not minimally replicated in " + src;
    }


    boolean penultimateBlockMinReplication = penultimateBlock == null ? true :
        blockManager.checkMinReplication(penultimateBlock);

    switch(lastBlockState) {
    case COMPLETE:
    case COMMITTED:
      if(penultimateBlockMinReplication &&
          blockManager.checkMinReplication(lastBlock)) {
        finalizeINodeFileUnderConstruction(src, pendingFile,
            iip.getLatestSnapshotId());
        NameNode.stateChangeLog.warn("BLOCK*"
                trimmedTargets.get(i).getStorageInfo(trimmedStorages.get(i));
            if (storageInfo != null) {
              if(copyTruncate) {
                storageInfo.addBlock(truncatedBlock);
              } else {
                storageInfo.addBlock(storedBlock);
              }
            }
          }
        } else {
          iFile.setLastBlock(storedBlock, trimmedStorageInfos);
          if (closeFile) {
            blockManager.markBlockReplicasAsCorrupt(storedBlock,
                oldGenerationStamp, oldNumBytes, trimmedStorageInfos);
          }
        }
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NamenodeFsck.java
                  .getStorageType()));
            }
            if (showReplicaDetails) {
              LightWeightLinkedSet<Block> blocksExcess =
                  bm.excessReplicateMap.get(dnDesc.getDatanodeUuid());
              Collection<DatanodeDescriptor> corruptReplicas =
                  bm.getCorruptReplicas(block.getLocalBlock());

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockInfo.java

    final DatanodeStorageInfo storage = DFSTestUtil.createDatanodeStorageInfo("storageID", "127.0.0.1");

    boolean added = blockInfo.addStorage(storage);

    Assert.assertTrue(added);
    Assert.assertEquals(storage, blockInfo.getStorageInfo(0));

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockManager.java
    for (int i = 1; i < pipeline.length; i++) {
      DatanodeStorageInfo storage = pipeline[i];
      bm.addBlock(storage, blockInfo, null);
      blockInfo.addStorage(storage);
    }
  }


    for (DatanodeDescriptor dn : nodes) {
      for (DatanodeStorageInfo storage : dn.getStorageInfos()) {
        blockInfo.addStorage(storage);
      }
    }
    return blockInfo;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestNodeCount.java
      DatanodeDescriptor nonExcessDN = null;
      for(DatanodeStorageInfo storage : bm.blocksMap.getStorages(block.getLocalBlock())) {
        final DatanodeDescriptor dn = storage.getDatanodeDescriptor();
        Collection<Block> blocks = bm.excessReplicateMap.get(dn.getDatanodeUuid());
        if (blocks == null || !blocks.contains(block.getLocalBlock()) ) {
          nonExcessDN = dn;
          break;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestOverReplicatedBlocks.java
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.util.Time;
import org.junit.Test;

public class TestOverReplicatedBlocks {
      namesystem.readLock();
      Collection<Block> dnBlocks = 
        namesystem.getBlockManager().excessReplicateMap.get(lastDNid);
      assertEquals("Replicas on node " + lastDNid + " should have been deleted",
          SMALL_FILE_LENGTH / SMALL_BLOCK_SIZE, dnBlocks.size());

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestReplicationPolicy.java
    when(storage.removeBlock(any(BlockInfo.class))).thenReturn(true);
    when(storage.addBlock(any(BlockInfo.class))).thenReturn
        (DatanodeStorageInfo.AddBlockResult.ADDED);
    ucBlock.addStorage(storage);

    when(mbc.setLastBlock((BlockInfo) any(), (DatanodeStorageInfo[]) any()))
    .thenReturn(ucBlock);

