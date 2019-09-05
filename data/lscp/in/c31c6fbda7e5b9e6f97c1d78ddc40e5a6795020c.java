hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockCollection.java
  public short getPreferredBlockReplication();


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoContiguous.java
    if(isComplete()) {
      BlockInfoContiguousUnderConstruction ucBlock =
          new BlockInfoContiguousUnderConstruction(this,
          getBlockCollection().getPreferredBlockReplication(), s, targets);
      ucBlock.setBlockCollection(getBlockCollection());
      return ucBlock;
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
      return;
    } 
    short expectedReplicas =
        b.corrupted.getBlockCollection().getPreferredBlockReplication();

    if (storageInfo != null) {
              continue;
            }

            requiredReplication = bc.getPreferredBlockReplication();

            containingNodes = new ArrayList<DatanodeDescriptor>();
            neededReplications.decrementReplicationIndex(priority);
            continue;
          }
          requiredReplication = bc.getPreferredBlockReplication();

          NumberReplicas numReplicas = countNodes(block);
    }

    short fileReplication = bc.getPreferredBlockReplication();
    if (!isNeededReplication(storedBlock, fileReplication, numCurrentReplica)) {
      neededReplications.remove(storedBlock, numCurrentReplica,
          num.decommissionedAndDecommissioning(), fileReplication);
    }
    short expectedReplication =
        block.getBlockCollection().getPreferredBlockReplication();
    NumberReplicas num = countNodes(block);
    int numCurrentReplica = num.liveReplicas();
    while(it.hasNext()) {
      final Block block = it.next();
      BlockCollection bc = blocksMap.getBlockCollection(block);
      short expectedReplication = bc.getPreferredBlockReplication();
      NumberReplicas num = countNodes(block);
      int numCurrentReplica = num.liveReplicas();
      if (numCurrentReplica > expectedReplication) {
  public void checkReplication(BlockCollection bc) {
    final short expected = bc.getPreferredBlockReplication();
    for (Block block : bc.getBlocks()) {
      final NumberReplicas n = countNodes(block);
      if (isNeededReplication(block, expected, n.liveReplicas())) { 
  private int getReplication(Block block) {
    final BlockCollection bc = blocksMap.getBlockCollection(block);
    return bc == null? 0: bc.getPreferredBlockReplication();
  }



hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DecommissionManager.java
  private boolean isSufficientlyReplicated(BlockInfoContiguous block, 
      BlockCollection bc,
      NumberReplicas numberReplicas) {
    final int numExpected = bc.getPreferredBlockReplication();
    final int numLive = numberReplicas.liveReplicas();
    if (!blockManager.isNeededReplication(block, numExpected, numLive)) {
      DatanodeDescriptor srcNode, NumberReplicas num,
      Iterable<DatanodeStorageInfo> storages) {
    int curReplicas = num.liveReplicas();
    int curExpectedReplicas = bc.getPreferredBlockReplication();
    StringBuilder nodeList = new StringBuilder();
    for (DatanodeStorageInfo storage : storages) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();

        if (blockManager.isNeededReplication(block,
            bc.getPreferredBlockReplication(), liveReplicas)) {
          if (!blockManager.neededReplications.contains(block) &&
              blockManager.pendingReplications.getNumReplicas(block) == 0 &&
              namesystem.isPopulatingReplQueues()) {
            blockManager.neededReplications.add(block,
                curReplicas,
                num.decommissionedAndDecommissioning(),
                bc.getPreferredBlockReplication());
          }
        }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirAttrOp.java
      return null;
    }
    INodeFile file = inode.asFile();
    final short oldBR = file.getPreferredBlockReplication();


    file.setFileReplication(replication, iip.getLatestSnapshotId());

    final short newBR = file.getPreferredBlockReplication();
    if (newBR < oldBR) {
      long dsDelta = file.storagespaceConsumed(null).getStorageSpace() / newBR;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirConcatOp.java
  private static QuotaCounts computeQuotaDeltas(FSDirectory fsd,
      INodeFile target, INodeFile[] srcList) {
    QuotaCounts deltas = new QuotaCounts.Builder().build();
    final short targetRepl = target.getPreferredBlockReplication();
    for (INodeFile src : srcList) {
      short srcRepl = src.getPreferredBlockReplication();
      long fileSize = src.computeFileSize();
      if (targetRepl != srcRepl) {
        deltas.addStorageSpace(fileSize * (targetRepl - srcRepl));

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirectory.java

      updateCount(inodesInPath, 0, fileINode.getPreferredBlockSize(),
          fileINode.getPreferredBlockReplication(), true);

      BlockInfoContiguousUnderConstruction blockInfo =

    updateCount(iip, 0, -fileNode.getPreferredBlockSize(),
        fileNode.getPreferredBlockReplication(), true);
    return true;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogLoader.java
    }
    BlockInfoContiguous newBI = new BlockInfoContiguousUnderConstruction(
          newBlock, file.getPreferredBlockReplication());
    fsNamesys.getBlockManager().addBlockCollection(newBI, file);
    file.addBlock(newBI);
    fsNamesys.getBlockManager().processQueuedMessagesForBlock(newBlock);
          newBI = new BlockInfoContiguousUnderConstruction(
              newBlock, file.getPreferredBlockReplication());
        } else {
          newBI = new BlockInfoContiguous(newBlock,
              file.getPreferredBlockReplication());
        }
        fsNamesys.getBlockManager().addBlockCollection(newBI, file);
        file.addBlock(newBI);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
      truncatedBlockUC = new BlockInfoContiguousUnderConstruction(newBlock,
          file.getPreferredBlockReplication());
      truncatedBlockUC.setNumBytes(oldBlock.getNumBytes() - lastBlockDelta);
      truncatedBlockUC.setTruncateBlock(oldBlock);
      file.setLastBlock(truncatedBlockUC, blockManager.getStorages(oldBlock));
    final BlockInfoContiguous lastBlock = file.getLastBlock();
    if (lastBlock != null) {
      final long diff = file.getPreferredBlockSize() - lastBlock.getNumBytes();
      final short repl = file.getPreferredBlockReplication();
      delta.addStorageSpace(diff * repl);
      final BlockStoragePolicy policy = dir.getBlockStoragePolicySuite()
          .getPolicy(file.getStoragePolicyID());

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeFile.java
  }

  @Override // BlockCollection
  public short getPreferredBlockReplication() {
    short max = getFileReplication(CURRENT_STATE_ID);
    FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
    if (sf != null) {
      blocks = allBlocks;
    }

    final short replication = getPreferredBlockReplication();
    for (BlockInfoContiguous b : blocks) {
      long blockSize = b.isComplete() ? b.getNumBytes() :
          getPreferredBlockSize();
        truncatedBytes -= bi.getNumBytes();
      }

      delta.addStorageSpace(-truncatedBytes * getPreferredBlockReplication());
      if (bsps != null) {
        List<StorageType> types = bsps.chooseStorageTypes(
            getPreferredBlockReplication());
        for (StorageType t : types) {
          if (t.supportTypeQuota()) {
            delta.addTypeSpace(t, -truncatedBytes);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NamenodeFsck.java
      NumberReplicas numberReplicas= bm.countNodes(block);
      out.println("Block Id: " + blockId);
      out.println("Block belongs to: "+iNode.getFullPathName());
      out.println("No. of Expected Replica: " +
          bc.getPreferredBlockReplication());
      out.println("No. of live Replica: " + numberReplicas.liveReplicas());
      out.println("No. of excess Replica: " + numberReplicas.excessReplicas());
      out.println("No. of stale Replica: " +
          numberReplicas.replicasOnStaleNodes());
      out.println("No. of decommissioned Replica: "
          + numberReplicas.decommissioned());
      out.println("No. of decommissioning Replica: "
          + numberReplicas.decommissioning());
      out.println("No. of corrupted Replica: " +
          numberReplicas.corruptReplicas());
      Collection<DatanodeDescriptor> corruptionRecord = null;
      if (bm.getCorruptReplicas(block) != null) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FileWithSnapshotFeature.java
    long oldStoragespace;
    if (removed.snapshotINode != null) {
      short replication = removed.snapshotINode.getFileReplication();
      short currentRepl = file.getPreferredBlockReplication();
      if (replication > currentRepl) {
        long oldFileSizeNoRep = currentRepl == 0
            ? file.computeFileSize(true, true)
            : oldCounts.getStorageSpace() /
            file.getPreferredBlockReplication();
        oldStoragespace = oldFileSizeNoRep * replication;
        oldCounts.setStorageSpace(oldStoragespace);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockManager.java
  
  private BlockInfoContiguous addBlockOnNodes(long blockId, List<DatanodeDescriptor> nodes) {
    BlockCollection bc = Mockito.mock(BlockCollection.class);
    Mockito.doReturn((short)3).when(bc).getPreferredBlockReplication();
    BlockInfoContiguous blockInfo = blockOnNodes(blockId, nodes);

    bm.blocksMap.addBlockCollection(blockInfo, bc);
    BlockInfoContiguous blockInfo =
        new BlockInfoContiguous(block, (short) 3);
    BlockCollection bc = Mockito.mock(BlockCollection.class);
    Mockito.doReturn((short) 3).when(bc).getPreferredBlockReplication();
    bm.blocksMap.addBlockCollection(blockInfo, bc);
    return blockInfo;
  }
    BlockInfoContiguousUnderConstruction blockInfo =
        new BlockInfoContiguousUnderConstruction(block, (short) 3);
    BlockCollection bc = Mockito.mock(BlockCollection.class);
    Mockito.doReturn((short) 3).when(bc).getPreferredBlockReplication();
    bm.blocksMap.addBlockCollection(blockInfo, bc);
    return blockInfo;
  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestFileWithSnapshotFeature.java

    INodeFile snapshotINode = mock(INodeFile.class);
    when(file.getPreferredBlockReplication()).thenReturn(REPL_1);
    Whitebox.setInternalState(snapshotINode, "header", (long) REPL_3 << 48);
    Whitebox.setInternalState(diff, "snapshotINode", snapshotINode);
    when(diff.getSnapshotINode()).thenReturn(snapshotINode);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestSnapshotDeletion.java
    }
    
    INodeFile nodeFile13 = (INodeFile) fsdir.getINode(file13.toString());
    assertEquals(REPLICATION_1, nodeFile13.getPreferredBlockReplication());
    TestSnapshotBlocksMap.assertBlockCollection(file13.toString(), 1, fsdir,
        blockmanager);
    
    INodeFile nodeFile12 = (INodeFile) fsdir.getINode(file12_s1.toString());
    assertEquals(REPLICATION_1, nodeFile12.getPreferredBlockReplication());
  }
  

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestSnapshotReplication.java
public class TestSnapshotReplication {
    short fileReplication = hdfs.getFileStatus(file1).getReplication();
    assertEquals(replication, fileReplication);
    INode inode = fsdir.getINode(file1.toString());
    assertTrue(inode instanceof INodeFile);
    assertEquals(blockReplication,
        ((INodeFile) inode).getPreferredBlockReplication());
  }
  
  private void checkSnapshotFileReplication(Path currentFile,
      Map<Path, Short> snapshotRepMap, short expectedBlockRep) throws Exception {
    final INodeFile inodeOfCurrentFile = getINodeFile(currentFile);
    assertEquals(expectedBlockRep,
        inodeOfCurrentFile.getPreferredBlockReplication());
    for (Path ss : snapshotRepMap.keySet()) {
      final INodesInPath iip = fsdir.getINodesInPath(ss.toString(), true);
      final INodeFile ssInode = iip.getLastINode().asFile();
      assertEquals(expectedBlockRep, ssInode.getPreferredBlockReplication());
      assertEquals(snapshotRepMap.get(ss).shortValue(),
          ssInode.getFileReplication(iip.getPathSnapshotId()));
    for (Path ss : snapshotRepMap.keySet()) {
      final INodeFile ssInode = getINodeFile(ss);
      assertEquals(REPLICATION, ssInode.getPreferredBlockReplication());
      assertEquals(snapshotRepMap.get(ss).shortValue(),
          ssInode.getFileReplication());

