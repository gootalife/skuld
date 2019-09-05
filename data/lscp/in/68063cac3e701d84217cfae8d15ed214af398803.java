hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java
    
    LOG.info(who + " calls recoverBlock(" + block
        + ", targets=[" + Joiner.on(", ").join(targets) + "]"
        + ((rb.getNewBlock() == null) ? ", newGenerationStamp="
            + rb.getNewGenerationStamp() : ", newBlock=" + rb.getNewBlock())
        + ")");
  }

  @Override // ClientDataNodeProtocol

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
    String src = "";
    waitForLoadingFSImage();
    writeLock();
    boolean copyTruncate = false;
    BlockInfoContiguousUnderConstruction truncatedBlock = null;
    try {
      checkOperation(OperationCategory.WRITE);
        return;
      }

      truncatedBlock = (BlockInfoContiguousUnderConstruction) iFile
          .getLastBlock();
      long recoveryId = truncatedBlock.getBlockRecoveryId();
      copyTruncate = truncatedBlock.getBlockId() != storedBlock.getBlockId();
      if(recoveryId != newgenerationstamp) {
        throw new IOException("The recovery id " + newgenerationstamp
                              + " does not match current recovery id "
    if (closeFile) {
      LOG.info("commitBlockSynchronization(oldBlock=" + oldBlock
          + ", file=" + src
          + (copyTruncate ? ", newBlock=" + truncatedBlock
              : ", newgenerationstamp=" + newgenerationstamp)
          + ", newlength=" + newlength
          + ", newtargets=" + Arrays.asList(newtargets) + ") successful");
    } else {

