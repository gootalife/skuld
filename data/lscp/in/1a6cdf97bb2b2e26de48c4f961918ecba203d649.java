hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
                      long mtime)
      throws IOException, UnresolvedLinkException {
    String src = srcArg;
    NameNode.stateChangeLog.debug(
        "DIR* NameSystem.truncate: src={} newLength={}", src, newLength);
    if (newLength < 0) {
      throw new HadoopIllegalArgumentException(
          "Cannot truncate to a negative file size: " + newLength + ".");
      file.setLastBlock(truncatedBlockUC, blockManager.getStorages(oldBlock));
      getBlockManager().addBlockCollection(truncatedBlockUC, file);

      NameNode.stateChangeLog.debug(
          "BLOCK* prepareFileForTruncate: Scheduling copy-on-truncate to new" +
          " size {} new block {} old block {}", truncatedBlockUC.getNumBytes(),
          newBlock, truncatedBlockUC.getTruncateBlock());
    } else {
      blockManager.convertLastBlockToUnderConstruction(file, lastBlockDelta);
      truncatedBlockUC.getTruncateBlock().setGenerationStamp(
          newBlock.getGenerationStamp());

      NameNode.stateChangeLog.debug(
          "BLOCK* prepareFileForTruncate: Scheduling in-place block" +
          " truncate to new size {} block= {}",
          truncatedBlockUC.getTruncateBlock().getNumBytes(), truncatedBlockUC);
    }
    if (shouldRecoverNow) {
      truncatedBlockUC.initializeBlockRecovery(newBlock.getGenerationStamp());
      String clientMachine, boolean newBlock, boolean logRetryCache)
      throws IOException {
    String src = srcArg;
    NameNode.stateChangeLog.debug(
        "DIR* NameSystem.appendFile: src={}, holder={}, clientMachine={}",
        src, holder, clientMachine);
    boolean skipSync = false;
    if (!supportAppends) {
      throw new UnsupportedOperationException(
      }
    }
    if (lb != null) {
      NameNode.stateChangeLog.debug(
          "DIR* NameSystem.appendFile: file {} for {} at {} block {} block" +
          " size {}", src, holder, clientMachine, lb.getBlock(),
          lb.getBlock().getNumBytes());
    }
    logAuditEvent(true, "append", srcArg);
    return new LastBlockWithStatus(lb, stat);
  LocatedBlock getAdditionalBlock(
      String src, long fileId, String clientName, ExtendedBlock previous,
      DatanodeInfo[] excludedNodes, String[] favoredNodes) throws IOException {
    NameNode.stateChangeLog.debug("BLOCK* getAdditionalBlock: {} inodeId {}" +
        " for {}", src, fileId, clientName);

    waitForLoadingFSImage();
    LocatedBlock[] onRetryBlock = new LocatedBlock[1];
  void abandonBlock(ExtendedBlock b, long fileId, String src, String holder)
      throws IOException {
    NameNode.stateChangeLog.debug(
        "BLOCK* NameSystem.abandonBlock: {} of file {}", b, src);
    waitForLoadingFSImage();
    checkOperation(OperationCategory.WRITE);
    FSPermissionChecker pc = getPermissionChecker();
      checkOperation(OperationCategory.WRITE);
      checkNameNodeSafeMode("Cannot abandon block " + b + " for file" + src);
      FSDirWriteFileOp.abandonBlock(dir, pc, b, fileId, src, holder);
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: {} is" +
          " removed from pendingCreates", b);
    } finally {
      writeUnlock();
    }
    waitForLoadingFSImage();
    getEditLog().logCloseFile(path, file);
    NameNode.stateChangeLog.debug("closeFile: {} with {} bloks is persisted" +
        " to the file system", path, file.getBlocks().length);
  }

      if (cookieTab[0] == null) {
        cookieTab[0] = String.valueOf(getIntCookie(cookieTab[0]));
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("there are no corrupt file blocks.");
      }
      return corruptFiles;
    }

        }
      }
      cookieTab[0] = String.valueOf(skip);
      if (LOG.isDebugEnabled()) {
        LOG.debug("list corrupt file blocks returned: " + count);
      }
      return corruptFiles;
    } finally {
      readUnlock();

