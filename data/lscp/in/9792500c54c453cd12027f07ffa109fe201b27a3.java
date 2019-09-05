hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSInputStream.java
    return errMsgr.toString();
  }

  private void fetchBlockByteRange(long blockStartOffset, long start, long end,
      byte[] buf, int offset,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    LocatedBlock block = getBlockAt(blockStartOffset);
    while (true) {
      DNAddrPair addressPair = chooseDataNode(block, null);
      try {
        actualGetFromOneDataNode(addressPair, blockStartOffset, start, end,
            buf, offset, corruptedBlockMap);
        return;
      } catch (IOException e) {
  }

  private Callable<ByteBuffer> getFromOneDataNode(final DNAddrPair datanode,
      final long blockStartOffset, final long start, final long end,
      final ByteBuffer bb,
      final Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap,
      final int hedgedReadId) {
        TraceScope scope =
            Trace.startSpan("hedgedRead" + hedgedReadId, parentSpan);
        try {
          actualGetFromOneDataNode(datanode, blockStartOffset, start, end, buf,
              offset, corruptedBlockMap);
          return bb;
        } finally {
          scope.close();
  }

  private void actualGetFromOneDataNode(final DNAddrPair datanode,
      long blockStartOffset, final long start, final long end, byte[] buf,
      int offset, Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    DFSClientFaultInjector.get().startFetchFromDatanode();
      CachingStrategy curCachingStrategy;
      boolean allowShortCircuitLocalReads;
      LocatedBlock block = getBlockAt(blockStartOffset);
      synchronized(infoLock) {
        curCachingStrategy = cachingStrategy;
        allowShortCircuitLocalReads = !shortCircuitForbidden();
  private void hedgedFetchBlockByteRange(long blockStartOffset, long start,
      long end, byte[] buf, int offset,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    ByteBuffer bb = null;
    int len = (int) (end - start + 1);
    int hedgedReadId = 0;
    LocatedBlock block = getBlockAt(blockStartOffset);
    while (true) {
      hedgedReadOpsLoopNumForTesting++;
        chosenNode = chooseDataNode(block, ignored);
        bb = ByteBuffer.wrap(buf, offset, len);
        Callable<ByteBuffer> getFromDataNodeCallable = getFromOneDataNode(
            chosenNode, block.getStartOffset(), start, end, bb,
            corruptedBlockMap, hedgedReadId++);
        Future<ByteBuffer> firstRequest = hedgedService
            .submit(getFromDataNodeCallable);
        futures.add(firstRequest);
          }
          bb = ByteBuffer.allocate(len);
          Callable<ByteBuffer> getFromDataNodeCallable = getFromOneDataNode(
              chosenNode, block.getStartOffset(), start, end, bb,
              corruptedBlockMap, hedgedReadId++);
          Future<ByteBuffer> oneMoreRequest = hedgedService
              .submit(getFromDataNodeCallable);
          futures.add(oneMoreRequest);
      long bytesToRead = Math.min(remaining, blk.getBlockSize() - targetStart);
      try {
        if (dfsClient.isHedgedReadsEnabled()) {
          hedgedFetchBlockByteRange(blk.getStartOffset(), targetStart,
              targetStart + bytesToRead - 1, buffer, offset,
              corruptedBlockMap);
        } else {
          fetchBlockByteRange(blk.getStartOffset(), targetStart,
              targetStart + bytesToRead - 1, buffer, offset,
              corruptedBlockMap);
        }
      } finally {

