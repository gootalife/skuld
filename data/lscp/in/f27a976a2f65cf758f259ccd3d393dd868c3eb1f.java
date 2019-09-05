hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NamenodeFsck.java
  @VisibleForTesting
  void check(String parent, HdfsFileStatus file, Result res) throws IOException {
    String path = file.getFullName(parent);
    if (file.isDir()) {
      checkDir(path, res);
      return;
    }
    if (file.isSymlink()) {
      if (showFiles) {
        out.println(path + " <symlink>");
      }
      res.totalSymlinks++;
      return;
    }
    LocatedBlocks blocks = getBlockLocations(path, file);
    if (blocks == null) { // the file is deleted
      return;
    }
    collectFileSummary(path, file, res, blocks);
    collectBlocksSummary(parent, file, res, blocks);
  }

  private void checkDir(String path, Result res) throws IOException {
    if (snapshottableDirs != null && snapshottableDirs.contains(path)) {
      String snapshotPath = (path.endsWith(Path.SEPARATOR) ? path : path
          + Path.SEPARATOR)
      }
      lastReturnedName = thisListing.getLastName();
    } while (thisListing.hasMore());
  }

  private LocatedBlocks getBlockLocations(String path, HdfsFileStatus file)
      throws IOException {
    long fileLen = file.getLen();
    LocatedBlocks blocks = null;
    FSNamesystem fsn = namenode.getNamesystem();
    fsn.readLock();
    } finally {
      fsn.readUnlock();
    }
    return blocks;
  }

  private void collectFileSummary(String path, HdfsFileStatus file, Result res,
      LocatedBlocks blocks) throws IOException {
    long fileLen = file.getLen();
    boolean isOpen = blocks.isUnderConstruction();
    if (isOpen && !showOpenFiles) {
      res.totalOpenFilesSize += fileLen;
      out.print('.');
    }
    if (res.totalFiles % 100 == 0) { out.println(); out.flush(); }
  }

  private void collectBlocksSummary(String parent, HdfsFileStatus file, Result res,
      LocatedBlocks blocks) throws IOException {
    String path = file.getFullName(parent);
    boolean isOpen = blocks.isUnderConstruction();
    int missing = 0;
    int corrupt = 0;
    long missize = 0;
    int underReplicatedPerFile = 0;
    int misReplicatedPerFile = 0;
    StringBuilder report = new StringBuilder();
    int blockNumber = 0;
    for (LocatedBlock lBlk : blocks.getLocatedBlocks()) {
      ExtendedBlock block = lBlk.getBlock();
      BlockManager bm = namenode.getNamesystem().getBlockManager();

      NumberReplicas numberReplicas = bm.countNodes(block.getLocalBlock());
      int decommissionedReplicas = numberReplicas.decommissioned();;
      int decommissioningReplicas = numberReplicas.decommissioning();
      res.decommissionedReplicas +=  decommissionedReplicas;
      res.decommissioningReplicas += decommissioningReplicas;

      int liveReplicas = numberReplicas.liveReplicas();
      int totalReplicasPerBlock = liveReplicas + decommissionedReplicas +
          decommissioningReplicas;
      res.totalReplicas += totalReplicasPerBlock;

      short targetFileReplication = file.getReplication();
      res.numExpectedReplicas += targetFileReplication;

      if(totalReplicasPerBlock < minReplication){
        res.numUnderMinReplicatedBlocks++;
      }

      if (liveReplicas > targetFileReplication) {
        res.excessiveReplicas += (liveReplicas - targetFileReplication);
        res.numOverReplicatedBlocks += 1;
      }

      boolean isCorrupt = lBlk.isCorrupt();
      if (isCorrupt) {
        corrupt++;
        res.corruptBlocks++;
        out.print("\n" + path + ": CORRUPT blockpool " + block.getBlockPoolId() + 
            " block " + block.getBlockName()+"\n");
      }

      if (totalReplicasPerBlock >= minReplication)
        res.numMinReplicatedBlocks++;

      if (totalReplicasPerBlock < targetFileReplication && totalReplicasPerBlock > 0) {
        res.missingReplicas += (targetFileReplication - totalReplicasPerBlock);
        res.numUnderReplicatedBlocks += 1;
        underReplicatedPerFile++;
        if (!showFiles) {
                    decommissionedReplicas + " decommissioned replica(s) and " +
                    decommissioningReplicas + " decommissioning replica(s).");
      }

      BlockPlacementStatus blockPlacementStatus = bpPolicy
          .verifyBlockPlacement(path, lBlk, targetFileReplication);
      if (!blockPlacementStatus.isPlacementPolicySatisfied()) {
        out.println(" Replica placement policy is violated for " + 
                    block + ". " + blockPlacementStatus.getErrorDescription());
      }

      if (this.showStoragePolcies && lBlk.getStorageTypes() != null) {
        countStorageTypeSummary(file, lBlk);
      }

      String blkName = block.toString();
      report.append(blockNumber + ". " + blkName + " len=" + block.getNumBytes());
      if (totalReplicasPerBlock == 0) {
        report.append(" MISSING!");
        res.addMissing(block.toString(), block.getNumBytes());
        missing++;
            if (showReplicaDetails) {
              LightWeightLinkedSet<Block> blocksExcess =
                  bm.excessReplicateMap.get(dnDesc.getDatanodeUuid());
              Collection<DatanodeDescriptor> corruptReplicas =
                  bm.getCorruptReplicas(block.getLocalBlock());
              sb.append("(");
              if (dnDesc.isDecommissioned()) {
                sb.append("DECOMMISSIONED)");
        }
      }
      report.append('\n');
      blockNumber++;
    }

    if ((missing > 0) || (corrupt > 0)) {
      if (!showFiles && (missing > 0)) {
        out.print("\n" + path + ": MISSING " + missing
    }
  }

  private void countStorageTypeSummary(HdfsFileStatus file, LocatedBlock lBlk) {
    StorageType[] storageTypes = lBlk.getStorageTypes();
    storageTypeSummary.add(Arrays.copyOf(storageTypes, storageTypes.length),
                           namenode.getNamesystem().getBlockManager()
                               .getStoragePolicy(file.getStoragePolicy()));
  }

  private void deleteCorruptedFile(String path) {
    try {
      namenode.getRpcServer().delete(path, true);

