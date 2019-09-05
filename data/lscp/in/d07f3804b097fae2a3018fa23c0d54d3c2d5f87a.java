hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestCommitBlockSynchronization.java
    blockInfo.initializeBlockRecovery(genStamp);
    doReturn(blockInfo).when(file).removeLastBlock(any(Block.class));
    doReturn(true).when(file).isUnderConstruction();
    doReturn(new BlockInfoContiguous[1]).when(file).getBlocks();

    doReturn(blockInfo).when(namesystemSpy).getStoredBlock(any(Block.class));
    doReturn(blockInfo).when(file).getLastBlock();

