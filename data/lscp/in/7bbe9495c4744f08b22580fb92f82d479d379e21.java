hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirDeleteOp.java
    incrDeletedFileCount(filesRemoved);

    fsn.removeLeasesAndINodes(src, removedINodes, true);

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* Namesystem.delete: "

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (toRemovedBlocks != null) {
      removeBlocks(toRemovedBlocks); // Incremental deletion of blocks
    }
    private void clearCorruptLazyPersistFiles()
        throws IOException {

      BlockStoragePolicy lpPolicy = blockManager.getStoragePolicy("LAZY_PERSIST");

      List<BlockCollection> filesToDelete = new ArrayList<>();
      boolean changed = false;
      writeLock();
      try {
        final Iterator<Block> it = blockManager.getCorruptReplicaBlockIterator();

        while (it.hasNext()) {
          Block b = it.next();
          BlockInfoContiguous blockInfo = blockManager.getStoredBlock(b);
          if (blockInfo.getBlockCollection().getStoragePolicyID()
              == lpPolicy.getId()) {
            filesToDelete.add(blockInfo.getBlockCollection());
          }
        }
              FSDirDeleteOp.deleteInternal(
                  FSNamesystem.this, bc.getName(),
                  INodesInPath.fromINode((INodeFile) bc), false);
          changed |= toRemoveBlocks != null;
          if (toRemoveBlocks != null) {
            removeBlocks(toRemoveBlocks); // Incremental deletion of blocks
          }
      } finally {
        writeUnlock();
      }
      if (changed) {
        getEditLog().logSync();
      }
    }

    @Override

