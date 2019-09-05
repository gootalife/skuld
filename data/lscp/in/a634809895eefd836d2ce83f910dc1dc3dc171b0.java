hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockCollection.java
  public BlockInfo getLastBlock();

  public BlockInfo[] getBlocks();

  public void setBlock(int index, BlockInfo blk);

  public BlockInfoContiguousUnderConstruction setLastBlock(BlockInfo lastBlock,
      DatanodeStorageInfo[] targets) throws IOException;


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfo.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfo.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.util.LightWeightGSet;

@InterfaceAudience.Private
public class BlockInfo extends Block
    implements LightWeightGSet.LinkedElement {
  public static final BlockInfo[] EMPTY_ARRAY = {};

  private BlockCollection bc;

  private LightWeightGSet.LinkedElement nextLinkedElement;

  private Object[] triplets;

  public BlockInfo(short replication) {
    this.triplets = new Object[3*replication];
    this.bc = null;
  }

  public BlockInfo(Block blk, short replication) {
    super(blk);
    this.triplets = new Object[3*replication];
    this.bc = null;
  }

  protected BlockInfo(BlockInfo from) {
    super(from);
    this.triplets = new Object[from.triplets.length];
    this.bc = from.bc;
  }

  public BlockCollection getBlockCollection() {
    return bc;
  }

  public void setBlockCollection(BlockCollection bc) {
    this.bc = bc;
  }

  public boolean isDeleted() {
    return (bc == null);
  }

  public DatanodeDescriptor getDatanode(int index) {
    DatanodeStorageInfo storage = getStorageInfo(index);
    return storage == null ? null : storage.getDatanodeDescriptor();
  }

  DatanodeStorageInfo getStorageInfo(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3 < triplets.length : "Index is out of bound";
    return (DatanodeStorageInfo)triplets[index*3];
  }

  private BlockInfo getPrevious(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+1 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo)triplets[index*3+1];
    assert info == null ||
        info.getClass().getName().startsWith(BlockInfo.class.getName()) :
              "BlockInfo is expected at " + index*3;
    return info;
  }

  BlockInfo getNext(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+2 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo)triplets[index*3+2];
    assert info == null || info.getClass().getName().startsWith(
        BlockInfo.class.getName()) :
        "BlockInfo is expected at " + index*3;
    return info;
  }

  private void setStorageInfo(int index, DatanodeStorageInfo storage) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3 < triplets.length : "Index is out of bound";
    triplets[index*3] = storage;
  }

  private BlockInfo setPrevious(int index, BlockInfo to) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+1 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo)triplets[index*3+1];
    triplets[index*3+1] = to;
    return info;
  }

  private BlockInfo setNext(int index, BlockInfo to) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+2 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo)triplets[index*3+2];
    triplets[index*3+2] = to;
    return info;
  }

  public int getCapacity() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";
    return triplets.length / 3;
  }

  private int ensureCapacity(int num) {
    assert this.triplets != null : "BlockInfo is not initialized";
    int last = numNodes();
    if(triplets.length >= (last+num)*3)
      return last;
    Object[] old = triplets;
    triplets = new Object[(last+num)*3];
    System.arraycopy(old, 0, triplets, 0, last*3);
    return last;
  }

  public int numNodes() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";
    for(int idx = getCapacity()-1; idx >= 0; idx--) {
      if(getDatanode(idx) != null)
        return idx+1;
    }
    return 0;
  }

  boolean addStorage(DatanodeStorageInfo storage) {
    int lastNode = ensureCapacity(1);
    setStorageInfo(lastNode, storage);
    setNext(lastNode, null);
    setPrevious(lastNode, null);
    return true;
  }

  boolean removeStorage(DatanodeStorageInfo storage) {
    int dnIndex = findStorageInfo(storage);
    if(dnIndex < 0) // the node is not found
      return false;
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null :
      "Block is still in the list and must be removed first.";
    int lastNode = numNodes()-1;
    setStorageInfo(dnIndex, getStorageInfo(lastNode));
    setNext(dnIndex, getNext(lastNode));
    setPrevious(dnIndex, getPrevious(lastNode));
    setStorageInfo(lastNode, null);
    setNext(lastNode, null);
    setPrevious(lastNode, null);
    return true;
  }

  DatanodeStorageInfo findStorageInfo(DatanodeDescriptor dn) {
    int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if(cur == null)
        break;
      if(cur.getDatanodeDescriptor() == dn)
        return cur;
    }
    return null;
  }

  int findStorageInfo(DatanodeStorageInfo storageInfo) {
    int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if (cur == storageInfo) {
        return idx;
      }
      if (cur == null) {
        break;
      }
    }
    return -1;
  }

  BlockInfo listInsert(BlockInfo head,
      DatanodeStorageInfo storage) {
    int dnIndex = this.findStorageInfo(storage);
    assert dnIndex >= 0 : "Data node is not found: current";
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null :
            "Block is already in the list and cannot be inserted.";
    this.setPrevious(dnIndex, null);
    this.setNext(dnIndex, head);
    if(head != null)
      head.setPrevious(head.findStorageInfo(storage), this);
    return this;
  }

  BlockInfo listRemove(BlockInfo head,
      DatanodeStorageInfo storage) {
    if(head == null)
      return null;
    int dnIndex = this.findStorageInfo(storage);
    if(dnIndex < 0) // this block is not on the data-node list
      return head;

    BlockInfo next = this.getNext(dnIndex);
    BlockInfo prev = this.getPrevious(dnIndex);
    this.setNext(dnIndex, null);
    this.setPrevious(dnIndex, null);
    if(prev != null)
      prev.setNext(prev.findStorageInfo(storage), next);
    if(next != null)
      next.setPrevious(next.findStorageInfo(storage), prev);
    if(this == head)  // removing the head
      head = next;
    return head;
  }

  public BlockInfo moveBlockToHead(BlockInfo head,
      DatanodeStorageInfo storage, int curIndex, int headIndex) {
    if (head == this) {
      return this;
    }
    BlockInfo next = this.setNext(curIndex, head);
    BlockInfo prev = this.setPrevious(curIndex, null);

    head.setPrevious(headIndex, this);
    prev.setNext(prev.findStorageInfo(storage), next);
    if (next != null) {
      next.setPrevious(next.findStorageInfo(storage), prev);
    }
    return this;
  }

  public BlockUCState getBlockUCState() {
    return BlockUCState.COMPLETE;
  }

  public boolean isComplete() {
    return getBlockUCState().equals(BlockUCState.COMPLETE);
  }

  public BlockInfoContiguousUnderConstruction convertToBlockUnderConstruction(
      BlockUCState s, DatanodeStorageInfo[] targets) {
    if(isComplete()) {
      BlockInfoContiguousUnderConstruction ucBlock =
          new BlockInfoContiguousUnderConstruction(this,
          getBlockCollection().getPreferredBlockReplication(), s, targets);
      ucBlock.setBlockCollection(getBlockCollection());
      return ucBlock;
    }
    BlockInfoContiguousUnderConstruction ucBlock =
        (BlockInfoContiguousUnderConstruction)this;
    ucBlock.setBlockUCState(s);
    ucBlock.setExpectedLocations(targets);
    ucBlock.setBlockCollection(getBlockCollection());
    return ucBlock;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return (this == obj) || super.equals(obj);
  }

  @Override
  public LightWeightGSet.LinkedElement getNext() {
    return nextLinkedElement;
  }

  @Override
  public void setNext(LightWeightGSet.LinkedElement next) {
    this.nextLinkedElement = next;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoContiguousUnderConstruction.java
public class BlockInfoContiguousUnderConstruction extends BlockInfo {
  private BlockUCState blockUCState;

  BlockInfo convertToCompleteBlock() throws IOException {
    assert getBlockUCState() != BlockUCState.COMPLETE :
      "Trying to convert a COMPLETE block";
    return new BlockInfo(this);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
    int usableReplicas = numReplicas.liveReplicas() +
                         numReplicas.decommissionedAndDecommissioning();
    
    if (block instanceof BlockInfo) {
      BlockCollection bc = ((BlockInfo) block).getBlockCollection();
      String fileName = (bc == null) ? "[orphaned]" : bc.getName();
      out.print(fileName + ": ");
    }
      Block commitBlock) throws IOException {
    if(commitBlock == null)
      return false; // not committing, this is a block allocation retry
    BlockInfo lastBlock = bc.getLastBlock();
    if(lastBlock == null)
      return false; // no blocks in file yet
    if(lastBlock.isComplete())
  private BlockInfo completeBlock(final BlockCollection bc,
      final int blkIndex, boolean force) throws IOException {
    if(blkIndex < 0)
      return null;
    BlockInfo curBlock = bc.getBlocks()[blkIndex];
    if(curBlock.isComplete())
      return curBlock;
    BlockInfoContiguousUnderConstruction ucBlock =
    if(!force && ucBlock.getBlockUCState() != BlockUCState.COMMITTED)
      throw new IOException(
          "Cannot complete block: block has not been COMMITTED by the client");
    BlockInfo completeBlock = ucBlock.convertToCompleteBlock();
    bc.setBlock(blkIndex, completeBlock);
    
    return blocksMap.replaceBlock(completeBlock);
  }

  private BlockInfo completeBlock(final BlockCollection bc,
      final BlockInfo block, boolean force) throws IOException {
    BlockInfo[] fileBlocks = bc.getBlocks();
    for(int idx = 0; idx < fileBlocks.length; idx++)
      if(fileBlocks[idx] == block) {
        return completeBlock(bc, idx, force);
  public BlockInfo forceCompleteBlock(final BlockCollection bc,
      final BlockInfoContiguousUnderConstruction block) throws IOException {
    block.commitBlock(block);
    return completeBlock(bc, block, true);
  public LocatedBlock convertLastBlockToUnderConstruction(
      BlockCollection bc, long bytesToRemove) throws IOException {
    BlockInfo oldBlock = bc.getLastBlock();
    if(oldBlock == null ||
       bc.getPreferredBlockSize() == oldBlock.getNumBytes() - bytesToRemove)
      return null;
  }
  
  private List<LocatedBlock> createLocatedBlockList(
      final BlockInfo[] blocks,
      final long offset, final long length, final int nrBlocksToReturn,
      final AccessMode mode) throws IOException {
    int curBlk = 0;
    return results;
  }

  private LocatedBlock createLocatedBlock(final BlockInfo[] blocks,
      final long endPos, final AccessMode mode) throws IOException {
    int curBlk = 0;
    long curPos = 0;
    return createLocatedBlock(blocks[curBlk], curPos, mode);
  }
  
  private LocatedBlock createLocatedBlock(final BlockInfo blk, final long pos,
    final AccessMode mode) throws IOException {
    final LocatedBlock lb = createLocatedBlock(blk, pos);
    if (mode != null) {
  }

  private LocatedBlock createLocatedBlock(final BlockInfo blk, final long pos
      ) throws IOException {
    if (blk instanceof BlockInfoContiguousUnderConstruction) {
      if (blk.isComplete()) {
  }

  public LocatedBlocks createLocatedBlocks(final BlockInfo[] blocks,
      final long fileSizeExcludeBlocksUnderConstruction,
      final boolean isFileUnderConstruction, final long offset,
      final long length, final boolean needBlockToken,
      final LocatedBlock lastlb;
      final boolean isComplete;
      if (!inSnapshot) {
        final BlockInfo last = blocks[blocks.length - 1];
        final long lastPos = last.isComplete()?
            fileSizeExcludeBlocksUnderConstruction - last.getNumBytes()
            : fileSizeExcludeBlocksUnderConstruction;
  public boolean isSufficientlyReplicated(BlockInfo b) {
    final int replication =
        Math.min(minReplication, getDatanodeManager().getNumLiveDataNodes());
    if(numBlocks == 0) {
      return new BlocksWithLocations(new BlockWithLocations[0]);
    }
    Iterator<BlockInfo> iter = node.getBlockIterator();
    int startBlock = ThreadLocalRandom.current().nextInt(numBlocks);
    }
    List<BlockWithLocations> results = new ArrayList<BlockWithLocations>();
    long totalSize = 0;
    BlockInfo curBlock;
    while(totalSize<size && iter.hasNext()) {
      curBlock = iter.next();
      if(!curBlock.isComplete())  continue;
  public void findAndMarkBlockAsCorrupt(final ExtendedBlock blk,
      final DatanodeInfo dn, String storageID, String reason) throws IOException {
    assert namesystem.hasWriteLock();
    final BlockInfo storedBlock = getStoredBlock(blk.getLocalBlock());
    if (storedBlock == null) {
          BlockInfo bi = blocksMap.getStoredBlock(timedOutItems[i]);
          if (bi == null) {
            continue;
          }
  private static class BlockToMarkCorrupt {
    final BlockInfo corrupted;
    final BlockInfo stored;
    final String reason;
    final Reason reasonCode;

    BlockToMarkCorrupt(BlockInfo corrupted,
        BlockInfo stored, String reason,
        Reason reasonCode) {
      Preconditions.checkNotNull(corrupted, "corrupted is null");
      Preconditions.checkNotNull(stored, "stored is null");
      this.reasonCode = reasonCode;
    }

    BlockToMarkCorrupt(BlockInfo stored, String reason,
        Reason reasonCode) {
      this(stored, stored, reason, reasonCode);
    }

    BlockToMarkCorrupt(BlockInfo stored, long gs, String reason,
        Reason reasonCode) {
      this(new BlockInfo(stored), stored, reason, reasonCode);
      corrupted.setGenerationStamp(gs);
    }
             "longer exists on the DataNode.",
              Long.toHexString(context.getReportId()), zombie.getStorageID());
    assert(namesystem.hasWriteLock());
    Iterator<BlockInfo> iter = zombie.getBlockIterator();
    int prevBlocks = zombie.numBlocks();
    while (iter.hasNext()) {
      BlockInfo block = iter.next();
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
      removeStoredBlock(b, node);
    }
    int numBlocksLogged = 0;
    for (BlockInfo b : toAdd) {
      addStoredBlock(b, storageInfo, null, numBlocksLogged < maxNumBlocksToLog);
      numBlocksLogged++;
    }
  public void markBlockReplicasAsCorrupt(BlockInfo block,
      long oldGenerationStamp, long oldNumBytes, 
      DatanodeStorageInfo[] newStorages) throws IOException {
    assert namesystem.hasWriteLock();
        continue;
      }
      
      BlockInfo storedBlock = blocksMap.getStoredBlock(iblk);
      if (storedBlock == null) continue;
      

  private void reportDiff(DatanodeStorageInfo storageInfo, 
      BlockListAsLongs newReport, 
      Collection<BlockInfo> toAdd,              // add to DatanodeDescriptor
      Collection<Block> toRemove,           // remove from DatanodeDescriptor
      Collection<Block> toInvalidate,       // should be removed from DN
      Collection<BlockToMarkCorrupt> toCorrupt, // add to corrupt replicas list

    BlockInfo delimiter = new BlockInfo(new Block(), (short) 1);
    AddBlockResult result = storageInfo.addBlock(delimiter);
    assert result == AddBlockResult.ADDED 
        : "Delimiting block cannot be present in the node";
    for (BlockReportReplica iblk : newReport) {
      ReplicaState iState = iblk.getState();
      BlockInfo storedBlock = processReportedBlock(storageInfo,
          iblk, iState, toAdd, toInvalidate, toCorrupt, toUC);


    Iterator<BlockInfo> it =
        storageInfo.new BlockIterator(delimiter.getNext(0));
    while(it.hasNext())
      toRemove.add(it.next());
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
  private BlockToMarkCorrupt checkReplicaCorrupt(
      Block reported, ReplicaState reportedState, 
      BlockInfo storedBlock, BlockUCState ucState,
      DatanodeDescriptor dn) {
    switch(reportedState) {
    case FINALIZED:
    }
  }

  private boolean isBlockUnderConstruction(BlockInfo storedBlock,
      BlockUCState ucState, ReplicaState reportedState) {
    switch(reportedState) {
    case FINALIZED:
  private void addStoredBlockImmediate(BlockInfo storedBlock,
      DatanodeStorageInfo storageInfo)
  throws IOException {
    assert (storedBlock != null && namesystem.hasWriteLock());
  private Block addStoredBlock(final BlockInfo block,
                               DatanodeStorageInfo storageInfo,
                               DatanodeDescriptor delNodeHint,
                               boolean logEveryBlock)
  throws IOException {
    assert block != null && namesystem.hasWriteLock();
    BlockInfo storedBlock;
    DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();
    if (block instanceof BlockInfoContiguousUnderConstruction) {
    return storedBlock;
  }

  private void logAddStoredBlock(BlockInfo storedBlock,
      DatanodeDescriptor node) {
    if (!blockLog.isInfoEnabled()) {
      return;
  private void invalidateCorruptReplicas(BlockInfo blk) {
    Collection<DatanodeDescriptor> nodes = corruptReplicas.getNodes(blk);
    boolean removedFromBlocksMap = true;
    if (nodes == null)
    long nrInvalid = 0, nrOverReplicated = 0;
    long nrUnderReplicated = 0, nrPostponed = 0, nrUnderConstruction = 0;
    long startTimeMisReplicatedScan = Time.monotonicNow();
    Iterator<BlockInfo> blocksItr = blocksMap.getBlocks().iterator();
    long totalBlocks = blocksMap.size();
    replicationQueuesInitProgress = 0;
    long totalProcessed = 0;
      namesystem.writeLockInterruptibly();
      try {
        while (processed < numBlocksPerIteration && blocksItr.hasNext()) {
          BlockInfo block = blocksItr.next();
          MisReplicationResult res = processMisReplicatedBlock(block);
          if (LOG.isTraceEnabled()) {
            LOG.trace("block " + block + ": " + res);
  private MisReplicationResult processMisReplicatedBlock(BlockInfo block) {
    if (block.isDeleted()) {
      addToInvalidates(block);
      ReplicaState reportedState, DatanodeDescriptor delHintNode)
      throws IOException {
    Collection<BlockInfo> toAdd = new LinkedList<BlockInfo>();
    Collection<Block> toInvalidate = new LinkedList<Block>();
    Collection<BlockToMarkCorrupt> toCorrupt = new LinkedList<BlockToMarkCorrupt>();
    Collection<StatefulBlockInfo> toUC = new LinkedList<StatefulBlockInfo>();
      addStoredBlockUnderConstruction(b, storageInfo);
    }
    long numBlocksLogged = 0;
    for (BlockInfo b : toAdd) {
      addStoredBlock(b, storageInfo, delHintNode, numBlocksLogged < maxNumBlocksToLog);
      numBlocksLogged++;
    }
  int countLiveNodes(BlockInfo b) {
    if (!namesystem.isInStartupSafeMode()) {
      return countNodes(b).liveReplicas();
    }
    return blocksMap.size();
  }

  public DatanodeStorageInfo[] getStorages(BlockInfo block) {
    final DatanodeStorageInfo[] storages = new DatanodeStorageInfo[block.numNodes()];
    int i = 0;
    for(DatanodeStorageInfo s : blocksMap.getStorages(block)) {
    }
  }

  public BlockInfo getStoredBlock(Block block) {
    return blocksMap.getStoredBlock(block);
  }

  public boolean checkBlocksProperlyReplicated(
      String src, BlockInfo[] blocks) {
    for (BlockInfo b: blocks) {
      if (!b.isComplete()) {
        final BlockInfoContiguousUnderConstruction uc =
            (BlockInfoContiguousUnderConstruction)b;
    return this.neededReplications.getCorruptReplOneBlockSize();
  }

  public BlockInfo addBlockCollection(BlockInfo block,
      BlockCollection bc) {
    return blocksMap.addBlockCollection(block, bc);
  }

  enum MisReplicationResult {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlocksMap.java
class BlocksMap {
  private static class StorageIterator implements Iterator<DatanodeStorageInfo> {
    private final BlockInfo blockInfo;
    private int nextIdx = 0;
      
    StorageIterator(BlockInfo blkInfo) {
      this.blockInfo = blkInfo;
    }

  private final int capacity;
  
  private GSet<Block, BlockInfo> blocks;

  BlocksMap(int capacity) {
    this.capacity = capacity;
    this.blocks = new LightWeightGSet<Block, BlockInfo>(capacity) {
      @Override
      public Iterator<BlockInfo> iterator() {
        SetIterator iterator = new SetIterator();
  }

  BlockCollection getBlockCollection(Block b) {
    BlockInfo info = blocks.get(b);
    return (info != null) ? info.getBlockCollection() : null;
  }

  BlockInfo addBlockCollection(BlockInfo b, BlockCollection bc) {
    BlockInfo info = blocks.get(b);
    if (info != b) {
      info = b;
      blocks.put(info);
  void removeBlock(Block block) {
    BlockInfo blockInfo = blocks.remove(block);
    if (blockInfo == null)
      return;

  }
  
  BlockInfo getStoredBlock(Block b) {
    return blocks.get(b);
  }

  Iterable<DatanodeStorageInfo> getStorages(final BlockInfo storedBlock) {
    return new Iterable<DatanodeStorageInfo>() {
      @Override
      public Iterator<DatanodeStorageInfo> iterator() {

  int numNodes(Block b) {
    BlockInfo info = blocks.get(b);
    return info == null ? 0 : info.numNodes();
  }

  boolean removeNode(Block b, DatanodeDescriptor node) {
    BlockInfo info = blocks.get(b);
    if (info == null)
      return false;

    return blocks.size();
  }

  Iterable<BlockInfo> getBlocks() {
    return blocks;
  }
  
  BlockInfo replaceBlock(BlockInfo newBlock) {
    BlockInfo currentBlock = blocks.get(newBlock);
    assert currentBlock != null : "the block if not in blocksMap";
    for (int i = currentBlock.numNodes() - 1; i >= 0; i--) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/CacheReplicationMonitor.java
  private void rescanFile(CacheDirective directive, INodeFile file) {
    BlockInfo[] blockInfos = file.getBlocks();

    directive.addFilesNeeded(1);
    }

    long cachedTotal = 0;
    for (BlockInfo blockInfo : blockInfos) {
      if (!blockInfo.getBlockUCState().equals(BlockUCState.COMPLETE)) {
        LOG.trace("Directive {}: can't cache block {} because it is in state "
  }

  private String findReasonForNotCaching(CachedBlock cblock, 
          BlockInfo blockInfo) {
    if (blockInfo == null) {
          iter.remove();
        }
      }
      BlockInfo blockInfo = blockManager.
            getStoredBlock(new Block(cblock.getBlockId()));
      String reason = findReasonForNotCaching(cblock, blockInfo);
      int neededCached = 0;
      List<DatanodeDescriptor> pendingCached) {
    BlockInfo blockInfo = blockManager.
          getStoredBlock(new Block(cachedBlock.getBlockId()));
    if (blockInfo == null) {
      LOG.debug("Block {}: can't add new cached replicas," +
      Iterator<CachedBlock> it = datanode.getPendingCached().iterator();
      while (it.hasNext()) {
        CachedBlock cBlock = it.next();
        BlockInfo info =
            blockManager.getStoredBlock(new Block(cBlock.getBlockId()));
        if (info != null) {
          pendingBytes -= info.getNumBytes();
      while (it.hasNext()) {
        CachedBlock cBlock = it.next();
        BlockInfo info =
            blockManager.getStoredBlock(new Block(cBlock.getBlockId()));
        if (info != null) {
          pendingBytes += info.getNumBytes();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DatanodeDescriptor.java
  boolean removeBlock(BlockInfo b) {
    final DatanodeStorageInfo s = b.findStorageInfo(this);
    if (s != null) {
  boolean removeBlock(String storageID, BlockInfo b) {
    DatanodeStorageInfo s = getStorageInfo(storageID);
    if (s != null) {
      return s.removeBlock(b);
    }
  }

  private static class BlockIterator implements Iterator<BlockInfo> {
    private int index = 0;
    private final List<Iterator<BlockInfo>> iterators;
    
    private BlockIterator(final DatanodeStorageInfo... storages) {
      List<Iterator<BlockInfo>> iterators = new ArrayList<Iterator<BlockInfo>>();
      for (DatanodeStorageInfo e : storages) {
        iterators.add(e.getBlockIterator());
      }
    }

    @Override
    public BlockInfo next() {
      update();
      return iterators.get(index).next();
    }
    }
  }

  Iterator<BlockInfo> getBlockIterator() {
    return new BlockIterator(getStorageInfos());
  }
  Iterator<BlockInfo> getBlockIterator(final String storageID) {
    return new BlockIterator(getStorageInfo(storageID));
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DatanodeStorageInfo.java

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
  class BlockIterator implements Iterator<BlockInfo> {
    private BlockInfo current;

    BlockIterator(BlockInfo head) {
      this.current = head;
    }

      return current != null;
    }

    public BlockInfo next() {
      BlockInfo res = current;
      current = current.getNext(current.findStorageInfo(DatanodeStorageInfo.this));
      return res;
    }
  private volatile long remaining;
  private long blockPoolUsed;

  private volatile BlockInfo blockList = null;
  private int numBlocks = 0;

    return blockPoolUsed;
  }

  public AddBlockResult addBlock(BlockInfo b) {
    AddBlockResult result = AddBlockResult.ADDED;
    return result;
  }

  public boolean removeBlock(BlockInfo b) {
    blockList = b.listRemove(blockList, this);
    if (b.removeStorage(this)) {
      numBlocks--;
    return numBlocks;
  }
  
  Iterator<BlockInfo> getBlockIterator() {
    return new BlockIterator(blockList);

  }
  int moveBlockToHead(BlockInfo b, int curIndex, int headIndex) {
    blockList = b.moveBlockToHead(blockList, this, curIndex, headIndex);
    return curIndex;
  }
  @VisibleForTesting
  BlockInfo getBlockListHeadForTesting(){
    return blockList;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DecommissionManager.java
  private final TreeMap<DatanodeDescriptor, AbstractList<BlockInfo>>
      decomNodeBlocks;

  private boolean isSufficientlyReplicated(BlockInfo block,
      BlockCollection bc,
      NumberReplicas numberReplicas) {
    final int numExpected = bc.getPreferredBlockReplication();
    }

    private void check() {
      final Iterator<Map.Entry<DatanodeDescriptor, AbstractList<BlockInfo>>>
          it = new CyclicIteration<>(decomNodeBlocks, iterkey).iterator();
      final LinkedList<DatanodeDescriptor> toRemove = new LinkedList<>();

          && !exceededNumBlocksPerCheck()
          && !exceededNumNodesPerCheck()) {
        numNodesChecked++;
        final Map.Entry<DatanodeDescriptor, AbstractList<BlockInfo>>
            entry = it.next();
        final DatanodeDescriptor dn = entry.getKey();
        AbstractList<BlockInfo> blocks = entry.getValue();
        boolean fullScan = false;
        if (blocks == null) {
    private void pruneSufficientlyReplicated(final DatanodeDescriptor datanode,
        AbstractList<BlockInfo> blocks) {
      processBlocksForDecomInternal(datanode, blocks.iterator(), null, true);
    }

    private AbstractList<BlockInfo> handleInsufficientlyReplicated(
        final DatanodeDescriptor datanode) {
      AbstractList<BlockInfo> insufficient = new ChunkedArrayList<>();
      processBlocksForDecomInternal(datanode, datanode.getBlockIterator(),
          insufficient, false);
      return insufficient;
    private void processBlocksForDecomInternal(
        final DatanodeDescriptor datanode,
        final Iterator<BlockInfo> it,
        final List<BlockInfo> insufficientlyReplicated,
        boolean pruneSufficientlyReplicated) {
      boolean firstReplicationLog = true;
      int underReplicatedBlocks = 0;
      int underReplicatedInOpenFiles = 0;
      while (it.hasNext()) {
        numBlocksChecked++;
        final BlockInfo block = it.next();
        if (blockManager.blocksMap.getStoredBlock(block) == null) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirWriteFileOp.java
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
        return onRetryBlock[0];
      } else {
        BlockInfo lastBlockInFile = pendingFile.getLastBlock();
        ((BlockInfoContiguousUnderConstruction) lastBlockInFile)
            .setExpectedLocations(targets);
        offset = pendingFile.computeFileSize();
  private static BlockInfo addBlock(
      FSDirectory fsd, String path, INodesInPath inodesInPath, Block block,
      DatanodeStorageInfo[] targets) throws IOException {
    fsd.writeLock();
      }
    }
    final INodeFile file = fsn.checkLease(src, clientName, inode, fileId);
    BlockInfo lastBlockInFile = file.getLastBlock();
    if (!Block.matchingIdAndGenStamp(previousBlock, lastBlockInFile)) {

      BlockInfo penultimateBlock = file.getPenultimateBlock();
      if (previous == null &&
          lastBlockInFile != null &&
          lastBlockInFile.getNumBytes() >= file.getPreferredBlockSize() &&
      long id, PermissionStatus permissions, long mtime, long atime,
      short replication, long preferredBlockSize, byte storagePolicyId) {
    return new INodeFile(id, null, permissions, mtime, atime,
        BlockInfo.EMPTY_ARRAY, replication, preferredBlockSize,
        storagePolicyId);
  }

      DatanodeStorageInfo[] targets)
      throws IOException {
    assert fsn.hasWriteLock();
    BlockInfo b = addBlock(fsn.dir, src, inodesInPath, newBlock,
                                     targets);
    NameNode.stateChangeLog.info("BLOCK* allocate " + b + " for " + src);
    DatanodeStorageInfo.incrementBlocksScheduled(targets);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirectory.java
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
        unprotectedTruncate(iip, newLength, collectedBlocks, mtime, null);

    if(! onBlockBoundary) {
      BlockInfo oldBlock = file.getLastBlock();
      Block tBlk =
      getFSNamesystem().prepareFileForTruncate(iip,
          clientName, clientMachine, file.computeFileSize() - newLength,

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLog.java
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
  
  public void logAddBlock(String path, INodeFile file) {
    Preconditions.checkArgument(file.isUnderConstruction());
    BlockInfo[] blocks = file.getBlocks();
    Preconditions.checkState(blocks != null && blocks.length > 0);
    BlockInfo pBlock = blocks.length > 1 ? blocks[blocks.length - 2] : null;
    BlockInfo lastBlock = blocks[blocks.length - 1];
    AddBlockOp op = AddBlockOp.getInstance(cache.get()).setPath(path)
        .setPenultimateBlock(pBlock).setLastBlock(lastBlock);
    logEdit(op);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogLoader.java
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
  private void addNewBlock(FSDirectory fsDir, AddBlockOp op, INodeFile file)
      throws IOException {
    BlockInfo[] oldBlocks = file.getBlocks();
    Block pBlock = op.getPenultimateBlock();
    Block newBlock= op.getLastBlock();
    
      Preconditions.checkState(oldBlocks == null || oldBlocks.length == 0);
    }
    BlockInfo newBI = new BlockInfoContiguousUnderConstruction(
          newBlock, file.getPreferredBlockReplication());
    fsNamesys.getBlockManager().addBlockCollection(newBI, file);
    file.addBlock(newBI);
  private void updateBlocks(FSDirectory fsDir, BlockListUpdatingOp op,
      INodesInPath iip, INodeFile file) throws IOException {
    BlockInfo[] oldBlocks = file.getBlocks();
    Block[] newBlocks = op.getBlocks();
    String path = op.getPath();
    
    
    for (int i = 0; i < oldBlocks.length && i < newBlocks.length; i++) {
      BlockInfo oldBlock = oldBlocks[i];
      Block newBlock = newBlocks[i];
      
      boolean isLastBlock = i == newBlocks.length - 1;
      for (int i = oldBlocks.length; i < newBlocks.length; i++) {
        Block newBlock = newBlocks[i];
        BlockInfo newBI;
        if (!op.shouldCompleteLastBlock()) {
          newBI = new BlockInfo(newBlock,
              file.getPreferredBlockReplication());
        }
        fsNamesys.getBlockManager().addBlockCollection(newBI, file);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageFormat.java
import org.apache.hadoop.hdfs.protocol.LayoutFlags;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

    public void updateBlocksMap(INodeFile file) {
      final BlockInfo[] blocks = file.getBlocks();
      if (blocks != null) {
        final BlockManager bm = namesystem.getBlockManager();
        for (int i = 0; i < blocks.length; i++) {
      
      BlockInfo[] blocks = new BlockInfo[numBlocks];
      for (int j = 0; j < numBlocks; j++) {
        blocks[j] = new BlockInfo(replication);
        blocks[j].readFields(in);
      }

            clientMachine = FSImageSerialization.readString(in);
            if (blocks.length > 0) {
              BlockInfo lastBlk = blocks[blocks.length - 1];
              blocks[blocks.length - 1] = new BlockInfoContiguousUnderConstruction(
                  lastBlk, replication);
            }
        FileUnderConstructionFeature uc = cons.getFileUnderConstructionFeature();
        oldnode.toUnderConstruction(uc.getClientName(), uc.getClientMachine());
        if (oldnode.numBlocks() > 0) {
          BlockInfo ucBlock = cons.getLastBlock();
          BlockInfo info = namesystem.getBlockManager().addBlockCollection(
              ucBlock, oldnode);
          oldnode.setBlock(oldnode.numBlocks() - 1, info);
        }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageFormatPBINode.java
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.LoaderContext;

    public static void updateBlocksMap(INodeFile file, BlockManager bm) {
      final BlockInfo[] blocks = file.getBlocks();
      if (blocks != null) {
        for (int i = 0; i < blocks.length; i++) {
          file.setBlock(i, bm.addBlockCollection(blocks[i], file));
      short replication = (short) f.getReplication();
      LoaderContext state = parent.getLoaderContext();

      BlockInfo[] blocks = new BlockInfo[bp.size()];
      for (int i = 0, e = bp.size(); i < e; ++i) {
        blocks[i] = new BlockInfo(PBHelper.convert(bp.get(i)), replication);
      }
      final PermissionStatus permissions = loadPermission(f.getPermission(),
          parent.getLoaderContext().getStringTable());
        INodeSection.FileUnderConstructionFeature uc = f.getFileUC();
        file.toUnderConstruction(uc.getClientName(), uc.getClientMachine());
        if (blocks.length > 0) {
          BlockInfo lastBlk = file.getLastBlock();
          file.setBlock(file.numBlocks() - 1, new BlockInfoContiguousUnderConstruction(
              lastBlk, replication));

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageSerialization.java
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat;
    long preferredBlockSize = in.readLong();
  
    int numBlocks = in.readInt();
    BlockInfo[] blocks = new BlockInfo[numBlocks];
    Block blk = new Block();
    int i = 0;
    for (; i < numBlocks-1; i++) {
      blk.readFields(in);
      blocks[i] = new BlockInfo(blk, blockReplication);
    }
    if(numBlocks > 0) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager.SecretManagerState;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
    }

    final BlockInfo last = file.getLastBlock();
    if (last != null && last.getBlockUCState() == BlockUCState.UNDER_RECOVERY) {
      final Block truncateBlock
          = ((BlockInfoContiguousUnderConstruction)last).getTruncateBlock();
    leaseManager.addLease(
        file.getFileUnderConstructionFeature().getClientName(), file.getId());
    boolean shouldRecoverNow = (newBlock == null);
    BlockInfo oldBlock = file.getLastBlock();
    boolean shouldCopyOnTruncate = shouldCopyOnTruncate(file, oldBlock);
    if(newBlock == null) {
      newBlock = (shouldCopyOnTruncate) ? createNewBlock() :
  boolean shouldCopyOnTruncate(INodeFile file, BlockInfo blk) {
    if(!isUpgradeFinalized()) {
      return true;
    }
      recoverLeaseInternal(RecoverLeaseOp.APPEND_FILE, iip, src, holder,
                           clientMachine, false);
      
      final BlockInfo lastBlock = myFile.getLastBlock();
      if(lastBlock != null && lastBlock.isComplete() &&
          !getBlockManager().isSufficientlyReplicated(lastBlock)) {
        }
      }
    } else {
      BlockInfo lastBlock = file.getLastBlock();
      if (lastBlock != null) {
        ExtendedBlock blk = new ExtendedBlock(this.getBlockPoolId(), lastBlock);
        ret = new LocatedBlock(blk, new DatanodeInfo[0]);
  private QuotaCounts computeQuotaDeltaForUCBlock(INodeFile file) {
    final QuotaCounts delta = new QuotaCounts.Builder().build();
    final BlockInfo lastBlock = file.getLastBlock();
    if (lastBlock != null) {
      final long diff = file.getPreferredBlockSize() - lastBlock.getNumBytes();
      final short repl = file.getPreferredBlockReplication();
                op.getExceptionMessage(src, holder, clientMachine,
                    "lease recovery is in progress. Try again later."));
        } else {
          final BlockInfo lastBlock = file.getLastBlock();
          if (lastBlock != null
              && lastBlock.getBlockUCState() == BlockUCState.UNDER_RECOVERY) {
            throw new RecoveryInProgressException(
          .getBlocks());
    } else {
      BlockInfo b = v.getPenultimateBlock();
      return b == null ||
          blockManager.checkBlocksProperlyReplicated(
              src, new BlockInfo[] { b });
    }
  }


    for (Block b : blocks.getToDeleteList()) {
      if (trackBlockCounts) {
        BlockInfo bi = getStoredBlock(b);
        if (bi.isComplete()) {
          numRemovedComplete++;
          if (bi.numNodes() >= blockManager.minReplication) {

    final INodeFile pendingFile = iip.getLastINode().asFile();
    int nrBlocks = pendingFile.numBlocks();
    BlockInfo[] blocks = pendingFile.getBlocks();

    int nrCompleteBlocks;
    BlockInfo curBlock = null;
    for(nrCompleteBlocks = 0; nrCompleteBlocks < nrBlocks; nrCompleteBlocks++) {
      curBlock = blocks[nrCompleteBlocks];
      if(!curBlock.isComplete())

    final BlockInfo lastBlock = pendingFile.getLastBlock();
    BlockUCState lastBlockState = lastBlock.getBlockUCState();
    BlockInfo penultimateBlock = pendingFile.getPenultimateBlock();

    boolean penultimateBlockMinReplication = penultimateBlock == null ? true :
  }

  @VisibleForTesting
  BlockInfo getStoredBlock(Block block) {
    return blockManager.getStoredBlock(block);
  }
  
  
      checkNameNodeSafeMode(
          "Cannot commitBlockSynchronization while in safe mode");
      final BlockInfo storedBlock = getStoredBlock(
          ExtendedBlock.getLocalBlock(oldBlock));
      if (storedBlock == null) {
        if (deleteblock) {
  @VisibleForTesting
  String closeFileCommitBlocks(INodeFile pendingFile, BlockInfo storedBlock)
      throws IOException {
    final INodesInPath iip = INodesInPath.fromINode(pendingFile);
    final String src = iip.getPath();

        while (it.hasNext()) {
          Block b = it.next();
          BlockInfo blockInfo = blockManager.getStoredBlock(b);
          if (blockInfo.getBlockCollection().getStoragePolicyID()
              == lpPolicy.getId()) {
            filesToDelete.add(blockInfo.getBlockCollection());
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) // mostly true
      return;
    BlockInfo storedBlock = getStoredBlock(b);
    if (storedBlock.isComplete()) {
      safeMode.decrementSafeBlockCount((short)blockManager.countNodes(b).liveReplicas());
    }
        + "access token for block " + block);
    
    BlockInfo storedBlock = getStoredBlock(ExtendedBlock.getLocalBlock(block));
    if (storedBlock == null || 
        storedBlock.getBlockUCState() != BlockUCState.UNDER_CONSTRUCTION) {
        throw new IOException(block + 

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FileUnderConstructionFeature.java
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;

  void updateLengthOfLastBlock(INodeFile f, long lastBlockLength)
      throws IOException {
    BlockInfo lastBlock = f.getLastBlock();
    assert (lastBlock != null) : "The last block for path "
        + f.getFullPathName() + " is null when updating its length";
    assert (lastBlock instanceof BlockInfoContiguousUnderConstruction)
  void cleanZeroSizeBlock(final INodeFile f,
      final BlocksMapUpdateInfo collectedBlocks) {
    final BlockInfo[] blocks = f.getBlocks();
    if (blocks != null && blocks.length > 0
        && blocks[blocks.length - 1] instanceof BlockInfoContiguousUnderConstruction) {
      BlockInfoContiguousUnderConstruction lastUC =

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeFile.java
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;

  private long header = 0L;

  private BlockInfo[] blocks;

  INodeFile(long id, byte[] name, PermissionStatus permissions, long mtime,
            long atime, BlockInfo[] blklist, short replication,
            long preferredBlockSize) {
    this(id, name, permissions, mtime, atime, blklist, replication,
         preferredBlockSize, (byte) 0);
  }

  INodeFile(long id, byte[] name, PermissionStatus permissions, long mtime,
      long atime, BlockInfo[] blklist, short replication,
      long preferredBlockSize, byte storagePolicyID) {
    super(id, name, permissions, mtime, atime);
    header = HeaderFormat.toLong(preferredBlockSize, replication,
  }

  @Override // BlockCollection
  public void setBlock(int index, BlockInfo blk) {
    this.blocks[index] = blk;
  }

  @Override // BlockCollection, the file should be under construction
  public BlockInfoContiguousUnderConstruction setLastBlock(
      BlockInfo lastBlock, DatanodeStorageInfo[] locations)
      throws IOException {
    Preconditions.checkState(isUnderConstruction(),
        "file is no longer under construction");
    BlockInfoContiguousUnderConstruction uc =
        (BlockInfoContiguousUnderConstruction)blocks[size_1];
    BlockInfo[] newlist = new BlockInfo[size_1];
    System.arraycopy(blocks, 0, newlist, 0, size_1);
    setBlocks(newlist);
    return uc;

  @Override
  public BlockInfo[] getBlocks() {
    return this.blocks;
  }

  public BlockInfo[] getBlocks(int snapshot) {
    if(snapshot == CURRENT_STATE_ID || getDiffs() == null)
      return getBlocks();
    FileDiff diff = getDiffs().getDiffById(snapshot);
    BlockInfo[] snapshotBlocks =
        diff == null ? getBlocks() : diff.getBlocks();
    if(snapshotBlocks != null)
      return snapshotBlocks;

  void updateBlockCollection() {
    if (blocks != null) {
      for(BlockInfo b : blocks) {
        b.setBlockCollection(this);
      }
    }
      totalAddedBlocks += f.blocks.length;
    }
    
    BlockInfo[] newlist =
        new BlockInfo[size + totalAddedBlocks];
    System.arraycopy(this.blocks, 0, newlist, 0, size);
    
    for(INodeFile in: inodes) {
  void addBlock(BlockInfo newblock) {
    if (this.blocks == null) {
      this.setBlocks(new BlockInfo[]{newblock});
    } else {
      int size = this.blocks.length;
      BlockInfo[] newlist = new BlockInfo[size + 1];
      System.arraycopy(this.blocks, 0, newlist, 0, size);
      newlist[size] = newblock;
      this.setBlocks(newlist);
  }

  public void setBlocks(BlockInfo[] blocks) {
    this.blocks = blocks;
  }


  public void clearFile(ReclaimContext reclaimContext) {
    if (blocks != null && reclaimContext.collectedBlocks != null) {
      for (BlockInfo blk : blocks) {
        reclaimContext.collectedBlocks.addDeleteBlock(blk);
        blk.setBlockCollection(null);
      }
  public final QuotaCounts storagespaceConsumed(BlockStoragePolicy bsp) {
    QuotaCounts counts = new QuotaCounts.Builder().build();
    final Iterable<BlockInfo> blocks;
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf == null) {
      blocks = Arrays.asList(getBlocks());
    } else {
      Set<BlockInfo> allBlocks = new HashSet<>(Arrays.asList(getBlocks()));
      List<FileDiff> diffs = sf.getDiffs().asList();
      for(FileDiff diff : diffs) {
        BlockInfo[] diffBlocks = diff.getBlocks();
        if (diffBlocks != null) {
          allBlocks.addAll(Arrays.asList(diffBlocks));
        }
    }

    final short replication = getPreferredBlockReplication();
    for (BlockInfo b : blocks) {
      long blockSize = b.isComplete() ? b.getNumBytes() :
          getPreferredBlockSize();
      counts.addStorageSpace(blockSize * replication);
  BlockInfo getPenultimateBlock() {
    if (blocks == null || blocks.length <= 1) {
      return null;
    }
  }

  @Override
  public BlockInfo getLastBlock() {
    return blocks == null || blocks.length == 0? null: blocks[blocks.length-1];
  }

  public long collectBlocksBeyondMax(final long max,
      final BlocksMapUpdateInfo collectedBlocks) {
    final BlockInfo[] oldBlocks = getBlocks();
    if (oldBlocks == null)
      return 0;
  void computeQuotaDeltaForTruncate(
      long newLength, BlockStoragePolicy bsps,
      QuotaCounts delta) {
    final BlockInfo[] blocks = getBlocks();
    if (blocks == null || blocks.length == 0) {
      return;
    }

    long size = 0;
    for (BlockInfo b : blocks) {
      size += b.getNumBytes();
    }

    BlockInfo[] sblocks = null;
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      FileDiff diff = sf.getDiffs().getLast();

    for (int i = blocks.length - 1; i >= 0 && size > newLength;
         size -= blocks[i].getNumBytes(), --i) {
      BlockInfo bi = blocks[i];
      long truncatedBytes;
      if (size - newLength < bi.getNumBytes()) {
  }

  void truncateBlocksTo(int n) {
    final BlockInfo[] newBlocks;
    if (n == 0) {
      newBlocks = BlockInfo.EMPTY_ARRAY;
    } else {
      newBlocks = new BlockInfo[n];
      System.arraycopy(getBlocks(), 0, newBlocks, 0, n);
    }
    setBlocks(newBlocks);
  }

  public void collectBlocksBeyondSnapshot(BlockInfo[] snapshotBlocks,
                                          BlocksMapUpdateInfo collectedBlocks) {
    BlockInfo[] oldBlocks = getBlocks();
    if(snapshotBlocks == null || oldBlocks == null)
      return;
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if(sf == null)
      return;
    BlockInfo[] snapshotBlocks =
        getDiffs().findEarlierSnapshotBlocks(snapshotId);
    if(snapshotBlocks == null)
      return;
  boolean isBlockInLatestSnapshot(BlockInfo block) {
    FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
    if (sf == null || sf.getDiffs() == null) {
      return false;
    }
    BlockInfo[] snapshotBlocks = getDiffs()
        .findEarlierSnapshotBlocks(getDiffs().getLastSnapshotId());
    return snapshotBlocks != null &&
        Arrays.asList(snapshotBlocks).contains(block);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/LeaseManager.java
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.util.Daemon;

    for (Long id : getINodeIdWithLeases()) {
      final INodeFile cons = fsnamesystem.getFSDirectory().getInode(id).asFile();
      Preconditions.checkState(cons.isUnderConstruction());
      BlockInfo[] blocks = cons.getBlocks();
      if(blocks == null) {
        continue;
      }
      for(BlockInfo b : blocks) {
        if(!b.isComplete())
          numUCBlocks++;
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NamenodeFsck.java
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementStatus;
      Block block = new Block(Block.getBlockId(blockId));
      BlockInfo blockInfo = bm.getStoredBlock(block);
      if(blockInfo == null) {
        out.println("Block "+ blockId +" " + NONEXISTENT_STATUS);
        LOG.warn("Block "+ blockId + " " + NONEXISTENT_STATUS);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FSImageFormatPBSnapshot.java
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.AclEntryStatusFormat;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
        FileDiff diff = new FileDiff(pbf.getSnapshotId(), copy, null,
            pbf.getFileSize());
        List<BlockProto> bpl = pbf.getBlocksList();
        BlockInfo[] blocks = new BlockInfo[bpl.size()];
        for(int j = 0, e = bpl.size(); j < e; ++j) {
          Block blk = PBHelper.convert(bpl.get(j));
          BlockInfo storedBlock =  fsn.getBlockManager().getStoredBlock(blk);
          if(storedBlock == null) {
            storedBlock = fsn.getBlockManager().addBlockCollection(
                new BlockInfo(blk, copy.getFileReplication()), file);
          }
          blocks[j] = storedBlock;
        }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FileDiff.java
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
  private final long fileSize;
  private BlockInfo[] blocks;

  FileDiff(int snapshotId, INodeFile file) {
    super(snapshotId, null, null);
  public void setBlocks(BlockInfo[] blocks) {
    if(this.blocks != null)
      return;
    int numBlocks = 0;
    this.blocks = Arrays.copyOf(blocks, numBlocks);
  }

  public BlockInfo[] getBlocks() {
    return blocks;
  }

    if (blocks == null || collectedBlocks == null) {
      return;
    }
    for (BlockInfo blk : blocks) {
      collectedBlocks.addDeleteBlock(blk);
    }
    blocks = null;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FileDiffList.java
import java.util.List;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.INode;
    }
  }

  public BlockInfo[] findEarlierSnapshotBlocks(int snapshotId) {
    assert snapshotId != Snapshot.NO_SNAPSHOT_ID : "Wrong snapshot id";
    if (snapshotId == Snapshot.CURRENT_STATE_ID) {
      return null;
    }
    List<FileDiff> diffs = this.asList();
    int i = Collections.binarySearch(diffs, snapshotId);
    BlockInfo[] blocks = null;
    for(i = i >= 0 ? i : -i-2; i >= 0; i--) {
      blocks = diffs.get(i).getBlocks();
      if(blocks != null) {
    return blocks;
  }

  public BlockInfo[] findLaterSnapshotBlocks(int snapshotId) {
    assert snapshotId != Snapshot.NO_SNAPSHOT_ID : "Wrong snapshot id";
    if (snapshotId == Snapshot.CURRENT_STATE_ID) {
      return null;
    }
    List<FileDiff> diffs = this.asList();
    int i = Collections.binarySearch(diffs, snapshotId);
    BlockInfo[] blocks = null;
    for (i = i >= 0 ? i+1 : -i-1; i < diffs.size(); i++) {
      blocks = diffs.get(i).getBlocks();
      if (blocks != null) {
  void combineAndCollectSnapshotBlocks(
      INode.ReclaimContext reclaimContext, INodeFile file, FileDiff removed) {
    BlockInfo[] removedBlocks = removed.getBlocks();
    if (removedBlocks == null) {
      FileWithSnapshotFeature sf = file.getFileWithSnapshotFeature();
      assert sf != null : "FileWithSnapshotFeature is null";
    if (earlierDiff != null) {
      earlierDiff.setBlocks(removedBlocks);
    }
    BlockInfo[] earlierBlocks =
        (earlierDiff == null ? new BlockInfo[]{} : earlierDiff.getBlocks());
    BlockInfo[] laterBlocks = findLaterSnapshotBlocks(removed.getSnapshotId());
    laterBlocks = (laterBlocks==null) ? file.getBlocks() : laterBlocks;
    int i = 0;
      break;
    }
    BlockInfo lastBlock = file.getLastBlock();
    Block dontRemoveBlock = null;
    if (lastBlock != null && lastBlock.getBlockUCState().equals(
        HdfsServerConstants.BlockUCState.UNDER_RECOVERY)) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FileWithSnapshotFeature.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.AclStorage;

    FileDiff last = diffs.getLast();
    BlockInfo[] snapshotBlocks = last == null ? null : last.getBlocks();
    if(snapshotBlocks == null)
      file.collectBlocksBeyondMax(max, reclaimContext.collectedBlocks());
    else

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/DFSTestUtil.java
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
  public static DatanodeDescriptor getExpectedPrimaryNode(NameNode nn,
      ExtendedBlock blk) {
    BlockManager bm0 = nn.getNamesystem().getBlockManager();
    BlockInfo storedBlock = bm0.getStoredBlock(blk.getLocalBlock());
    assertTrue("Block " + blk + " should be under construction, " +
        "got: " + storedBlock,
        storedBlock instanceof BlockInfoContiguousUnderConstruction);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDecommission.java
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          BlockInfo info =
              blockManager.getStoredBlock(b.getLocalBlock());
          int count = 0;
          StringBuilder sb = new StringBuilder("Replica locations: ");

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockInfo.java

  @Test
  public void testIsDeleted() {
    BlockInfo blockInfo = new BlockInfo((short) 3);
    BlockCollection bc = Mockito.mock(BlockCollection.class);
    blockInfo.setBlockCollection(bc);
    Assert.assertFalse(blockInfo.isDeleted());

  @Test
  public void testAddStorage() throws Exception {
    BlockInfo blockInfo = new BlockInfo((short) 3);

    final DatanodeStorageInfo storage = DFSTestUtil.createDatanodeStorageInfo("storageID", "127.0.0.1");


  @Test
  public void testCopyConstructor() {
    BlockInfo old = new BlockInfo((short) 3);
    try {
      BlockInfo copy = new BlockInfo(old);
      assertEquals(old.getBlockCollection(), copy.getBlockCollection());
      assertEquals(old.getCapacity(), copy.getCapacity());
    } catch (Exception e) {
    final DatanodeStorageInfo storage1 = DFSTestUtil.createDatanodeStorageInfo("storageID1", "127.0.0.1");
    final DatanodeStorageInfo storage2 = new DatanodeStorageInfo(storage1.getDatanodeDescriptor(), new DatanodeStorage("storageID2"));
    final int NUM_BLOCKS = 10;
    BlockInfo[] blockInfos = new BlockInfo[NUM_BLOCKS];

    for (int i = 0; i < NUM_BLOCKS; ++i) {
      blockInfos[i] = new BlockInfo((short) 3);
      storage1.addBlock(blockInfos[i]);
    }


    DatanodeStorageInfo dd = DFSTestUtil.createDatanodeStorageInfo("s1", "1.1.1.1");
    ArrayList<Block> blockList = new ArrayList<Block>(MAX_BLOCKS);
    ArrayList<BlockInfo> blockInfoList = new ArrayList<BlockInfo>();
    int headIndex;
    int curIndex;

    LOG.info("Building block list...");
    for (int i = 0; i < MAX_BLOCKS; i++) {
      blockList.add(new Block(i, 0, GenerationStamp.LAST_RESERVED_STAMP));
      blockInfoList.add(new BlockInfo(blockList.get(i), (short) 3));
      dd.addBlock(blockInfoList.get(i));

    LOG.info("Checking list length...");
    assertEquals("Length should be MAX_BLOCK", MAX_BLOCKS, dd.numBlocks());
    Iterator<BlockInfo> it = dd.getBlockIterator();
    int len = 0;
    while (it.hasNext()) {
      it.next();
    LOG.info("Moving head to the head...");

    BlockInfo temp = dd.getBlockListHeadForTesting();
    curIndex = 0;
    headIndex = 0;
    dd.moveBlockToHead(temp, curIndex, headIndex);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockManager.java
  private void doBasicTest(int testIndex) {
    List<DatanodeStorageInfo> origStorages = getStorages(0, 1);
    List<DatanodeDescriptor> origNodes = getNodes(origStorages);
    BlockInfo blockInfo = addBlockOnNodes(testIndex, origNodes);

    DatanodeStorageInfo[] pipeline = scheduleSingleReplication(blockInfo);
    assertEquals(2, pipeline.length);
    List<DatanodeStorageInfo> origStorages = getStorages(0, 1, 3);
    List<DatanodeDescriptor> origNodes = getNodes(origStorages);
    BlockInfo blockInfo = addBlockOnNodes(testIndex, origNodes);
    
    List<DatanodeDescriptor> decomNodes = startDecommission(0, 1);
    List<DatanodeStorageInfo> origStorages = getStorages(0, 1, 3);
    List<DatanodeDescriptor> origNodes = getNodes(origStorages);
    BlockInfo blockInfo = addBlockOnNodes(testIndex, origNodes);
    
    List<DatanodeDescriptor> decomNodes = startDecommission(0, 1, 3);
    List<DatanodeStorageInfo> origStorages = getStorages(0, 1, 3);
    List<DatanodeDescriptor> origNodes = getNodes(origStorages);
    BlockInfo blockInfo = addBlockOnNodes(testIndex, origNodes);
    
    List<DatanodeDescriptor> decomNodes = startDecommission(0, 1, 2);
  private void doTestSufficientlyReplBlocksUsesNewRack(int testIndex) {
    List<DatanodeDescriptor> origNodes = rackA;
    BlockInfo blockInfo = addBlockOnNodes(testIndex, origNodes);
    DatanodeStorageInfo pipeline[] = scheduleSingleReplication(blockInfo);
    
    assertEquals(2, pipeline.length); // single new copy
  private void fulfillPipeline(BlockInfo blockInfo,
      DatanodeStorageInfo[] pipeline) throws IOException {
    for (int i = 1; i < pipeline.length; i++) {
      DatanodeStorageInfo storage = pipeline[i];
    }
  }

  private BlockInfo blockOnNodes(long blkId, List<DatanodeDescriptor> nodes) {
    Block block = new Block(blkId);
    BlockInfo blockInfo = new BlockInfo(block, (short) 3);

    for (DatanodeDescriptor dn : nodes) {
      for (DatanodeStorageInfo storage : dn.getStorageInfos()) {
    return nodes;
  }
  
  private BlockInfo addBlockOnNodes(long blockId, List<DatanodeDescriptor> nodes) {
    BlockCollection bc = Mockito.mock(BlockCollection.class);
    Mockito.doReturn((short)3).when(bc).getPreferredBlockReplication();
    BlockInfo blockInfo = blockOnNodes(blockId, nodes);

    bm.blocksMap.addBlockCollection(blockInfo, bc);
    return blockInfo;

    long receivedBlockId = 42;  // arbitrary
    BlockInfo receivedBlock = addBlockToBM(receivedBlockId);
    rdbiList.add(new ReceivedDeletedBlockInfo(new Block(receivedBlock),
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, null));
    builder.add(new FinalizedReplica(receivedBlock, null, null));

    long receivingBlockId = 43;
    BlockInfo receivingBlock = addUcBlockToBM(receivingBlockId);
    rdbiList.add(new ReceivedDeletedBlockInfo(new Block(receivingBlock),
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVING_BLOCK, null));
    builder.add(new ReplicaBeingWritten(receivingBlock, null, null, null));

    long receivingReceivedBlockId = 44;
    BlockInfo receivingReceivedBlock = addBlockToBM(receivingReceivedBlockId);
    rdbiList.add(new ReceivedDeletedBlockInfo(new Block(receivingReceivedBlock),
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVING_BLOCK, null));
    rdbiList.add(new ReceivedDeletedBlockInfo(new Block(receivingReceivedBlock),

    long existedBlockId = 46;
    BlockInfo existedBlock = addBlockToBM(existedBlockId);
    builder.add(new FinalizedReplica(existedBlock, null, null));

        (ds) >= 0);
  }

  private BlockInfo addBlockToBM(long blkId) {
    Block block = new Block(blkId);
    BlockInfo blockInfo =
        new BlockInfo(block, (short) 3);
    BlockCollection bc = Mockito.mock(BlockCollection.class);
    Mockito.doReturn((short) 3).when(bc).getPreferredBlockReplication();
    bm.blocksMap.addBlockCollection(blockInfo, bc);
    return blockInfo;
  }

  private BlockInfo addUcBlockToBM(long blkId) {
    Block block = new Block(blkId);
    BlockInfoContiguousUnderConstruction blockInfo =
        new BlockInfoContiguousUnderConstruction(block, (short) 3);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestDatanodeDescriptor.java
  public void testBlocksCounter() throws Exception {
    DatanodeDescriptor dd = BlockManagerTestUtil.getLocalDatanodeDescriptor(true);
    assertEquals(0, dd.numBlocks());
    BlockInfo blk = new BlockInfo(new Block(1L), (short) 1);
    BlockInfo blk1 = new BlockInfo(new Block(2L), (short) 2);
    DatanodeStorageInfo[] storages = dd.getStorageInfos();
    assertTrue(storages.length > 0);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestPendingReplication.java
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY, TIMEOUT);
    MiniDFSCluster cluster = null;
    Block block;
    BlockInfo blockInfo;
    try {
      cluster =
          new MiniDFSCluster.Builder(conf).numDataNodes(DATANODE_COUNT).build();

      block = new Block(1, 1, 0);
      blockInfo = new BlockInfo(block, (short) 3);

      pendingReplications.increment(block,
          DatanodeStorageInfo.toDatanodeDescriptors(

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/CreateEditsLog.java
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.Storage;

    INodeDirectory dirInode = new INodeDirectory(inodeId.nextValue(), null, p,
      0L);
    editLog.logMkDir(BASE_PATH, dirInode);
    BlockInfo[] blocks = new BlockInfo[blocksPerFile];
    for (int iB = 0; iB < blocksPerFile; ++iB) {
      blocks[iB] = 
       new BlockInfo(new Block(0, blockSize, BLOCK_GENERATION_STAMP),
                               replication);
    }
    
        editLog.logMkDir(currentDir, dirInode);
      }
      INodeFile fileUc = new INodeFile(inodeId.nextValue(), null,
          p, 0L, 0L, BlockInfo.EMPTY_ARRAY, replication, blockSize);
      fileUc.toUnderConstruction("", "");
      editLog.logOpenFile(filePath, fileUc, false, false);
      editLog.logCloseFile(filePath, inode);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestAddBlock.java
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.junit.After;
import org.junit.Before;
    
    INodeFile file1Node = fsdir.getINode4Write(file1.toString()).asFile();
    BlockInfo[] file1Blocks = file1Node.getBlocks();
    assertEquals(1, file1Blocks.length);
    assertEquals(BLOCKSIZE - 1, file1Blocks[0].getNumBytes());
    assertEquals(BlockUCState.COMPLETE, file1Blocks[0].getBlockUCState());
    
    INodeFile file2Node = fsdir.getINode4Write(file2.toString()).asFile();
    BlockInfo[] file2Blocks = file2Node.getBlocks();
    assertEquals(1, file2Blocks.length);
    assertEquals(BLOCKSIZE, file2Blocks[0].getNumBytes());
    assertEquals(BlockUCState.COMPLETE, file2Blocks[0].getBlockUCState());
    
    INodeFile file3Node = fsdir.getINode4Write(file3.toString()).asFile();
    BlockInfo[] file3Blocks = file3Node.getBlocks();
    assertEquals(2, file3Blocks.length);
    assertEquals(BLOCKSIZE, file3Blocks[0].getNumBytes());
    assertEquals(BlockUCState.COMPLETE, file3Blocks[0].getBlockUCState());
    
    INodeFile file4Node = fsdir.getINode4Write(file4.toString()).asFile();
    BlockInfo[] file4Blocks = file4Node.getBlocks();
    assertEquals(2, file4Blocks.length);
    assertEquals(BLOCKSIZE, file4Blocks[0].getNumBytes());
    assertEquals(BlockUCState.COMPLETE, file4Blocks[0].getBlockUCState());
      FSDirectory fsdir = cluster.getNamesystem().getFSDirectory();
      
      INodeFile fileNode = fsdir.getINode4Write(file1.toString()).asFile();
      BlockInfo[] fileBlocks = fileNode.getBlocks();
      assertEquals(2, fileBlocks.length);
      assertEquals(BLOCKSIZE, fileBlocks[0].getNumBytes());
      assertEquals(BlockUCState.COMPLETE, fileBlocks[0].getBlockUCState());

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestBlockUnderConstruction.java
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
        " isUnderConstruction = " + inode.isUnderConstruction() +
        " expected to be " + isFileOpen,
        inode.isUnderConstruction() == isFileOpen);
    BlockInfo[] blocks = inode.getBlocks();
    assertTrue("File does not have blocks: " + inode.toString(),
        blocks != null && blocks.length > 0);
    
    int idx = 0;
    BlockInfo curBlock;
    for(; idx < blocks.length - 2; idx++) {
      curBlock = blocks[idx];

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestCommitBlockSynchronization.java
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
    doReturn(blockInfo).when(namesystemSpy).getStoredBlock(any(Block.class));
    doReturn(blockInfo).when(file).getLastBlock();
    doReturn("").when(namesystemSpy).closeFileCommitBlocks(
        any(INodeFile.class), any(BlockInfo.class));
    doReturn(mock(FSEditLog.class)).when(namesystemSpy).getEditLog();

    return namesystemSpy;
        lastBlock, genStamp, length, false, false, newTargets, null);

    BlockInfo completedBlockInfo = new BlockInfo(block, (short) 1);
    completedBlockInfo.setBlockCollection(file);
    completedBlockInfo.setGenerationStamp(genStamp);
    doReturn(completedBlockInfo).when(namesystemSpy)
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, true, false, newTargets, null);

    BlockInfo completedBlockInfo = new BlockInfo(block, (short) 1);
    completedBlockInfo.setBlockCollection(file);
    completedBlockInfo.setGenerationStamp(genStamp);
    doReturn(completedBlockInfo).when(namesystemSpy)

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestEditLog.java
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;

      for (int i = 0; i < numTransactions; i++) {
        INodeFile inode = new INodeFile(namesystem.dir.allocateNewInodeId(), null,
            p, 0L, 0L, BlockInfo.EMPTY_ARRAY, replication, blockSize);
        inode.toUnderConstruction("", "");

        editLog.logOpenFile("/filename" + (startIndex + i), inode, false, false);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFSImage.java
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
      INodeFile file2Node = fsn.dir.getINode4Write(file2.toString()).asFile();
      assertEquals("hello".length(), file2Node.computeFileSize());
      assertTrue(file2Node.isUnderConstruction());
      BlockInfo[] blks = file2Node.getBlocks();
      assertEquals(1, blks.length);
      assertEquals(BlockUCState.UNDER_CONSTRUCTION, blks[0].getBlockUCState());

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFsck.java
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
      INodeFile node = (INodeFile) cluster.getNamesystem().dir.getINode
          (fileName, true);
      final BlockInfo[] blocks = node.getBlocks();
      assertEquals(blocks.length, 1);
      blocks[0].setNumBytes(-1L);  // set the block length to be negative
      

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestGetBlockLocations.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
        FsPermission.createImmutable((short) 0x1ff));
    final INodeFile file = new INodeFile(
        MOCK_INODE_ID, FILE_NAME.getBytes(Charsets.UTF_8),
        perm, 1, 1, new BlockInfo[] {}, (short) 1,
        DFS_BLOCK_SIZE_DEFAULT);
    fsn.getFSDirectory().addINode(iip, file);
    return fsn;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestINodeFile.java
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.IOUtils;
      iNodes[i] = new INodeFile(i, null, perm, 0L, 0L, null, replication,
          preferredBlockSize, (byte)0);
      iNodes[i].setLocalName(DFSUtil.string2Bytes(fileNamePrefix + i));
      BlockInfo newblock = new BlockInfo(replication);
      iNodes[i].addBlock(newblock);
    }
    

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestTruncateQuotaUpdate.java
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshotFeature;
  @Test
  public void testTruncateWithSnapshotAndDivergence() {
    INodeFile file = createMockFile(BLOCKSIZE * 2 + BLOCKSIZE / 2, REPLICATION);
    BlockInfo[] blocks = new BlockInfo
        [file.getBlocks().length];
    System.arraycopy(file.getBlocks(), 0, blocks, 0, blocks.length);
    addSnapshotFeature(file, blocks);
  }

  private INodeFile createMockFile(long size, short replication) {
    ArrayList<BlockInfo> blocks = new ArrayList<>();
    long createdSize = 0;
    while (createdSize < size) {
      long blockSize = Math.min(BLOCKSIZE, size - createdSize);
      BlockInfo bi = newBlock(blockSize, replication);
      blocks.add(bi);
      createdSize += BLOCKSIZE;
    }
        .createImmutable((short) 0x1ff));
    return new INodeFile(
        ++nextMockINodeId, new byte[0], perm, 0, 0,
        blocks.toArray(new BlockInfo[blocks.size()]), replication,
        BLOCKSIZE);
  }

  private BlockInfo newBlock(long size, short replication) {
    Block b = new Block(++nextMockBlockId, size, ++nextMockGenstamp);
    return new BlockInfo(b, replication);
  }

  private static void addSnapshotFeature(INodeFile file, BlockInfo[] blocks) {
    FileDiff diff = mock(FileDiff.class);
    when(diff.getBlocks()).thenReturn(blocks);
    FileDiffList diffList = new FileDiffList();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/SnapshotTestHelper.java
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.datanode.BlockPoolSliceStorage;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestFileWithSnapshotFeature.java
import com.google.common.collect.Lists;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
    FileDiff diff = mock(FileDiff.class);
    BlockStoragePolicySuite bsps = mock(BlockStoragePolicySuite.class);
    BlockStoragePolicy bsp = mock(BlockStoragePolicy.class);
    BlockInfo[] blocks = new BlockInfo[] {
        new BlockInfo(new Block(1, BLOCK_SIZE, 1), REPL_1)
    };


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestSnapshotBlocksMap.java
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
     final FSDirectory dir, final BlockManager blkManager) throws Exception {
    final INodeFile file = INodeFile.valueOf(dir.getINode(path), path);
    assertEquals(numBlocks, file.getBlocks().length);
    for(BlockInfo b : file.getBlocks()) {
      assertBlockCollection(blkManager, file, b);
    }
    return file;
  }

  static void assertBlockCollection(final BlockManager blkManager,
      final INodeFile file, final BlockInfo b) {
    Assert.assertSame(b, blkManager.getStoredBlock(b));
    Assert.assertSame(file, blkManager.getBlockCollection(b));
    Assert.assertSame(file, b.getBlockCollection());
    {
      final INodeFile f2 = assertBlockCollection(file2.toString(), 3, fsdir,
          blockmanager);
      BlockInfo[] blocks = f2.getBlocks();
      hdfs.delete(sub2, true);
      for(BlockInfo b : blocks) {
        assertNull(blockmanager.getBlockCollection(b));
      }
    }
    final INodeFile f0 = assertBlockCollection(file0.toString(), 4, fsdir,
        blockmanager);
    BlockInfo[] blocks0 = f0.getBlocks();
    
    Path snapshotFile0 = SnapshotTestHelper.getSnapshotPath(sub1, "s0",
    hdfs.delete(file0, true);
    for(BlockInfo b : blocks0) {
      assertNotNull(blockmanager.getBlockCollection(b));
    }
    assertBlockCollection(snapshotFile0.toString(), 4, fsdir, blockmanager);
    hdfs.deleteSnapshot(sub1, "s1");

    for(BlockInfo b : blocks0) {
      assertNotNull(blockmanager.getBlockCollection(b));
    }
    assertBlockCollection(snapshotFile0.toString(), 4, fsdir, blockmanager);
    hdfs.append(bar);

    INodeFile barNode = fsdir.getINode4Write(bar.toString()).asFile();
    BlockInfo[] blks = barNode.getBlocks();
    assertEquals(1, blks.length);
    assertEquals(BLOCKSIZE, blks[0].getNumBytes());
    ExtendedBlock previous = new ExtendedBlock(fsn.getBlockPoolId(), blks[0]);
    hdfs.append(bar);

    INodeFile barNode = fsdir.getINode4Write(bar.toString()).asFile();
    BlockInfo[] blks = barNode.getBlocks();
    assertEquals(1, blks.length);
    ExtendedBlock previous = new ExtendedBlock(fsn.getBlockPoolId(), blks[0]);
    cluster.getNameNodeRpc()
    hdfs.append(bar);

    INodeFile barNode = fsdir.getINode4Write(bar.toString()).asFile();
    BlockInfo[] blks = barNode.getBlocks();
    assertEquals(1, blks.length);
    ExtendedBlock previous = new ExtendedBlock(fsn.getBlockPoolId(), blks[0]);
    cluster.getNameNodeRpc()
    out.write(testData);
    out.close();
    INodeFile barNode = fsdir.getINode4Write(bar.toString()).asFile();
    BlockInfo[] blks = barNode.getBlocks();
    assertEquals(1, blks.length);
    assertEquals(testData.length, blks[0].getNumBytes());
    

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestSnapshotDeletion.java
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
    DFSTestUtil.createFile(hdfs, tempFile, BLOCKSIZE, REPLICATION, seed);
    final INodeFile temp = TestSnapshotBlocksMap.assertBlockCollection(
        tempFile.toString(), 1, fsdir, blockmanager);
    BlockInfo[] blocks = temp.getBlocks();
    hdfs.delete(tempDir, true);
    checkQuotaUsageComputation(dir, 8, BLOCKSIZE * REPLICATION * 3);
    for (BlockInfo b : blocks) {
      assertNull(blockmanager.getBlockCollection(b));
    }
    
    checkQuotaUsageComputation(dir, 9L, BLOCKSIZE * REPLICATION * 4);
    for (BlockInfo b : blocks) {
      assertNull(blockmanager.getBlockCollection(b));
    }
    
    
    final INodeFile toDeleteFileNode = TestSnapshotBlocksMap
        .assertBlockCollection(toDeleteFile.toString(), 1, fsdir, blockmanager);
    BlockInfo[] blocks = toDeleteFileNode.getBlocks();
    
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s0");
    checkQuotaUsageComputation(dir, 6, 2 * BLOCKSIZE * REPLICATION - BLOCKSIZE);
    for (BlockInfo b : blocks) {
      assertNull(blockmanager.getBlockCollection(b));
    }
    
    FileStatus statusBeforeDeletion13 = hdfs.getFileStatus(file13_s1);
    INodeFile file14Node = TestSnapshotBlocksMap.assertBlockCollection(
        file14_s2.toString(), 1, fsdir, blockmanager);
    BlockInfo[] blocks_14 = file14Node.getBlocks();
    TestSnapshotBlocksMap.assertBlockCollection(file15_s2.toString(), 1, fsdir,
        blockmanager);
    
        modDirStr + "file15");
    assertFalse(hdfs.exists(file14_s1));
    assertFalse(hdfs.exists(file15_s1));
    for (BlockInfo b : blocks_14) {
      assertNull(blockmanager.getBlockCollection(b));
    }
    

