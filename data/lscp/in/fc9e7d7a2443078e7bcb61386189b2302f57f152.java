hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSInputStream.java
            targetBlock.getBlockSize() - 1;
      this.currentLocatedBlock = targetBlock;

      long offsetIntoBlock = target - targetBlock.getStartOffset();

      DNAddrPair retval = chooseDataNode(targetBlock, null);
      StorageType storageType = retval.storageType;

      try {
        blockReader = getBlockReader(targetBlock, offsetIntoBlock,
            targetBlock.getBlockSize() - offsetIntoBlock, targetAddr,
            storageType, chosenNode);
        if(connectFailedOnce) {
          DFSClient.LOG.info("Successfully connected to " + targetAddr +
                             " for " + targetBlock.getBlock());
        }
        return chosenNode;
      } catch (IOException ex) {
    }
  }

  protected BlockReader getBlockReader(LocatedBlock targetBlock,
      long offsetInBlock, long length, InetSocketAddress targetAddr,
      StorageType storageType, DatanodeInfo datanode) throws IOException {
    ExtendedBlock blk = targetBlock.getBlock();
    Token<BlockTokenIdentifier> accessToken = targetBlock.getBlockToken();
    CachingStrategy curCachingStrategy;
    boolean shortCircuitForbidden;
    synchronized (infoLock) {
      curCachingStrategy = cachingStrategy;
      shortCircuitForbidden = shortCircuitForbidden();
    }
    return new BlockReaderFactory(dfsClient.getConf()).
        setInetSocketAddress(targetAddr).
        setRemotePeerFactory(dfsClient).
        setDatanodeInfo(datanode).
        setStorageType(storageType).
        setFileName(src).
        setBlock(blk).
        setBlockToken(accessToken).
        setStartOffset(offsetInBlock).
        setVerifyChecksum(verifyChecksum).
        setClientName(dfsClient.clientName).
        setLength(length).
        setCachingStrategy(curCachingStrategy).
        setAllowShortCircuitLocalReads(!shortCircuitForbidden).
        setClientCacheContext(dfsClient.getClientContext()).
        setUserGroupInformation(dfsClient.ugi).
        setConfiguration(dfsClient.getConfiguration()).
        build();
  }

  private DNAddrPair chooseDataNode(LocatedBlock block,
      Collection<DatanodeInfo> ignoredNodes) throws IOException {
    while (true) {
      DNAddrPair result = getBestNodeDNAddrPair(block, ignoredNodes);
      if (result != null) {
        return result;
      } else {
        String errMsg = getBestNodeDNAddrPairErrorString(block.getLocations(),
          deadNodes, ignoredNodes);
        String blockInfo = block.getBlock() + " file=" + src;
          DFSClient.LOG.info("No node available for " + blockInfo);
        }
        DFSClient.LOG.info("Could not obtain " + block.getBlock()
            + " from any node: " + errMsg
            + ". Will get new block locations from namenode and retry...");
        try {
        openInfo();
        block = getBlockAt(block.getStartOffset());
        failures++;
      }
    }
  }
  private DNAddrPair getBestNodeDNAddrPair(LocatedBlock block,
      Collection<DatanodeInfo> ignoredNodes) {
    DatanodeInfo[] nodes = block.getLocations();
    StorageType[] storageTypes = block.getStorageTypes();
    DatanodeInfo chosenNode = null;
      }
    }
    if (chosenNode == null) {
      DFSClient.LOG.warn("No live nodes contain block " + block.getBlock() +
          " after checking nodes = " + Arrays.toString(nodes) +
          ", ignoredNodes = " + ignoredNodes);
      return null;
    }
    final String dnAddr =
        chosenNode.getXferAddr(dfsClient.getConf().isConnectToDnViaHostname());
      LocatedBlock block = getBlockAt(blockStartOffset);
      BlockReader reader = null;
      try {
        DFSClientFaultInjector.get().fetchFromDatanodeException();
        int len = (int) (end - start + 1);
        reader = getBlockReader(block, start, len, datanode.addr,
            datanode.storageType, datanode.info);
        int nread = reader.readAll(buf, offset, len);
        updateReadStatistics(readStatistics, nread, reader);

      } catch (ChecksumException e) {
        String msg = "fetchBlockByteRange(). Got a checksum exception for "
            + src + " at " + block.getBlock() + ":" + e.getPos() + " from "
            + datanode.info;
        DFSClient.LOG.warn(msg);
        addIntoCorruptedBlockMap(block.getBlock(), datanode.info,
            corruptedBlockMap);
        addToDeadNodes(datanode.info);
        throw new IOException(msg);
      } catch (IOException e) {
        if (e instanceof InvalidEncryptionKeyException && refetchEncryptionKey > 0) {
          DFSClient.LOG.info("Will fetch a new encryption key and retry, " 
              + "encryption key was invalid when connecting to " + datanode.addr
              + " : " + e);
          refetchEncryptionKey--;
          dfsClient.clearDataEncryptionKey();
        } else if (refetchToken > 0 && tokenRefetchNeeded(e, datanode.addr)) {
          refetchToken--;
          try {
            fetchBlockAt(block.getStartOffset());
          } catch (IOException fbae) {
          }
        } else {
          String msg = "Failed to connect to " + datanode.addr + " for file "
              + src + " for block " + block.getBlock() + ":" + e;
          DFSClient.LOG.warn("Connection failure: " + msg, e);
          addToDeadNodes(datanode.info);
          throw new IOException(msg);
        }
      } finally {
  }

  private void hedgedFetchBlockByteRange(long blockStartOffset, long start,
        try {
          chosenNode = getBestNodeDNAddrPair(block, ignored);
          if (chosenNode == null) {
            chosenNode = chooseDataNode(block, ignored);
          }
          bb = ByteBuffer.allocate(len);

