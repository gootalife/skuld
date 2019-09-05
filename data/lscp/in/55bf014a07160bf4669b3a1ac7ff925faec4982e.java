hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
    TraceScope scope = getPathTraceScope("newDFSInputStream", src);
    try {
      return new DFSInputStream(this, src, verifyChecksum, null);
    } finally {
      scope.close();
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSInputStream.java
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ByteBufferReadable;
  @VisibleForTesting
  public static boolean tcpReadsDisabledForTesting = false;
  private long hedgedReadOpsLoopNumForTesting = 0;
  protected final DFSClient dfsClient;
  protected AtomicBoolean closed = new AtomicBoolean(false);
  protected final String src;
  protected final boolean verifyChecksum;

  private DatanodeInfo currentNode = null;
  protected LocatedBlock currentLocatedBlock = null;
  protected long pos = 0;
  protected long blockEnd = -1;
  private BlockReader blockReader = null;

  protected LocatedBlocks locatedBlocks = null;
  private long lastBlockBeingWrittenLength = 0;
  private FileEncryptionInfo fileEncryptionInfo = null;
  protected CachingStrategy cachingStrategy;

  protected final ReadStatistics readStatistics = new ReadStatistics();
  protected final Object infoLock = new Object();

  protected int failures = 0;

    deadNodes.put(dnInfo, dnInfo);
  }
  
  DFSInputStream(DFSClient dfsClient, String src, boolean verifyChecksum,
      LocatedBlocks locatedBlocks) throws IOException, UnresolvedLinkException {
    this.dfsClient = dfsClient;
    this.verifyChecksum = verifyChecksum;
    this.src = src;
    synchronized (infoLock) {
      this.cachingStrategy = dfsClient.getDefaultReadCachingStrategy();
    }
    this.locatedBlocks = locatedBlocks;
    openInfo(false);
  }

  void openInfo(boolean refreshLocatedBlocks) throws IOException,
      UnresolvedLinkException {
    final DfsClientConf conf = dfsClient.getConf();
    synchronized(infoLock) {
      lastBlockBeingWrittenLength =
          fetchLocatedBlocksAndGetLastBlockLength(refreshLocatedBlocks);
      int retriesForLastBlockLength = conf.getRetryTimesForGetLastBlockLength();
      while (retriesForLastBlockLength > 0) {
              + "Datanodes might not have reported blocks completely."
              + " Will retry for " + retriesForLastBlockLength + " times");
          waitFor(conf.getRetryIntervalForGetLastBlockLength());
          lastBlockBeingWrittenLength =
              fetchLocatedBlocksAndGetLastBlockLength(true);
        } else {
          break;
        }
    }
  }

  private long fetchLocatedBlocksAndGetLastBlockLength(boolean refresh)
      throws IOException {
    LocatedBlocks newInfo = locatedBlocks;
    if (locatedBlocks == null || refresh) {
      newInfo = dfsClient.getLocatedBlocks(src, 0);
    }
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("newInfo = " + newInfo);
    }
  protected LocatedBlock getBlockAt(long offset) throws IOException {
    synchronized(infoLock) {
      assert (locatedBlocks != null) : "locatedBlocks is null";

  }

  protected void fetchBlockAt(long offset) throws IOException {
    synchronized(infoLock) {
      int targetBlockIdx = locatedBlocks.findBlock(offset);
      if (targetBlockIdx < 0) { // block is not cached
    }

    closeCurrentBlockReaders();

        } else {
          connectFailedOnce = true;
          DFSClient.LOG.warn("Failed to connect to " + targetAddr + " for block"
            + ", add to deadNodes and continue. " + ex, ex);
          addToDeadNodes(chosenNode);
        }
          "unreleased ByteBuffers allocated by read().  " +
          "Please release " + builder.toString() + ".");
    }
    closeCurrentBlockReaders();
    super.close();
  }

  interface ReaderStrategy {
    public int doRead(BlockReader blockReader, int off, int len)
        throws ChecksumException, IOException;

    public int copyFrom(ByteBuffer src, int offset, int length);
  }

  protected void updateReadStatistics(ReadStatistics readStatistics,
        int nRead, BlockReader blockReader) {
    if (nRead <= 0) return;
    synchronized(infoLock) {
      updateReadStatistics(readStatistics, nRead, blockReader);
      return nRead;
    }

    @Override
    public int copyFrom(ByteBuffer src, int offset, int length) {
      ByteBuffer writeSlice = src.duplicate();
      writeSlice.get(buf, offset, length);
      return length;
    }
  }

  protected class ByteBufferStrategy implements ReaderStrategy {
    final ByteBuffer buf;
    ByteBufferStrategy(ByteBuffer buf) {
      this.buf = buf;
        int ret = blockReader.read(buf);
        success = true;
        updateReadStatistics(readStatistics, ret, blockReader);
        if (ret == 0) {
          DFSClient.LOG.warn("zero");
        }
        return ret;
      } finally {
        if (!success) {
        }
      } 
    }

    @Override
    public int copyFrom(ByteBuffer src, int offset, int length) {
      ByteBuffer writeSlice = src.duplicate();
      int remaining = Math.min(buf.remaining(), writeSlice.remaining());
      writeSlice.limit(writeSlice.position() + remaining);
      buf.put(writeSlice);
      return remaining;
    }
  }

    }
  }

  protected synchronized int readWithStrategy(ReaderStrategy strategy, int off, int len) throws IOException {
    dfsClient.checkOpen();
    if (closed.get()) {
      throw new IOException("Stream closed");
  protected void addIntoCorruptedBlockMap(ExtendedBlock blk, DatanodeInfo node,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap) {
    Set<DatanodeInfo> dnSet = null;
    if((corruptedBlockMap.containsKey(blk))) {
        } catch (InterruptedException iex) {
        }
        deadNodes.clear(); //2nd option is to remove only nodes[blockId]
        openInfo(true);
        block = refreshLocatedBlock(block);
        failures++;
      }
    }
  protected DNAddrPair getBestNodeDNAddrPair(LocatedBlock block,
      Collection<DatanodeInfo> ignoredNodes) {
    DatanodeInfo[] nodes = block.getLocations();
    StorageType[] storageTypes = block.getStorageTypes();
    return errMsgr.toString();
  }

  protected void fetchBlockByteRange(LocatedBlock block, long start, long end,
      byte[] buf, int offset,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    block = refreshLocatedBlock(block);
    while (true) {
      DNAddrPair addressPair = chooseDataNode(block, null);
      try {
        actualGetFromOneDataNode(addressPair, block, start, end,
            buf, offset, corruptedBlockMap);
        return;
      } catch (IOException e) {
  }

  private Callable<ByteBuffer> getFromOneDataNode(final DNAddrPair datanode,
      final LocatedBlock block, final long start, final long end,
      final ByteBuffer bb,
      final Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap,
      final int hedgedReadId) {
        TraceScope scope =
            Trace.startSpan("hedgedRead" + hedgedReadId, parentSpan);
        try {
          actualGetFromOneDataNode(datanode, block, start, end, buf,
              offset, corruptedBlockMap);
          return bb;
        } finally {
    };
  }

  private void actualGetFromOneDataNode(final DNAddrPair datanode,
      LocatedBlock block, final long start, final long end, byte[] buf,
      int offset, Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    final int length = (int) (end - start + 1);
    actualGetFromOneDataNode(datanode, block, start, end, buf,
        new int[]{offset}, new int[]{length}, corruptedBlockMap);
  }

  void actualGetFromOneDataNode(final DNAddrPair datanode,
      LocatedBlock block, final long startInBlk, final long endInBlk,
      byte[] buf, int[] offsets, int[] lengths,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    DFSClientFaultInjector.get().startFetchFromDatanode();
    int refetchToken = 1; // only need to get a new access token once
    int refetchEncryptionKey = 1; // only need to get a new encryption key once
    final int len = (int) (endInBlk - startInBlk + 1);
    checkReadPortions(offsets, lengths, len);

    while (true) {
      block = refreshLocatedBlock(block);
      BlockReader reader = null;
      try {
        DFSClientFaultInjector.get().fetchFromDatanodeException();
        reader = getBlockReader(block, startInBlk, len, datanode.addr,
            datanode.storageType, datanode.info);
        for (int i = 0; i < offsets.length; i++) {
          int nread = reader.readAll(buf, offsets[i], lengths[i]);
          updateReadStatistics(readStatistics, nread, reader);
          if (nread != lengths[i]) {
            throw new IOException("truncated return from reader.read(): " +
                "excpected " + lengths[i] + ", got " + nread);
          }
        }
        DFSClientFaultInjector.get().readFromDatanodeDelay();
        return;
    }
  }

  protected LocatedBlock refreshLocatedBlock(LocatedBlock block)
      throws IOException {
    return getBlockAt(block.getStartOffset());
  }

  private void checkReadPortions(int[] offsets, int[] lengths, int totalLen) {
    Preconditions.checkArgument(offsets.length == lengths.length && offsets.length > 0);
    int sum = 0;
    for (int i = 0; i < lengths.length; i++) {
      if (i > 0) {
        int gap = offsets[i] - offsets[i - 1];
        Preconditions.checkArgument(gap >= lengths[i - 1]);
      }
      sum += lengths[i];
    }
    Preconditions.checkArgument(sum == totalLen);
  }

  private void hedgedFetchBlockByteRange(LocatedBlock block, long start,
      long end, byte[] buf, int offset,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    ByteBuffer bb = null;
    int len = (int) (end - start + 1);
    int hedgedReadId = 0;
    block = refreshLocatedBlock(block);
    while (true) {
      hedgedReadOpsLoopNumForTesting++;
        chosenNode = chooseDataNode(block, ignored);
        bb = ByteBuffer.wrap(buf, offset, len);
        Callable<ByteBuffer> getFromDataNodeCallable = getFromOneDataNode(
            chosenNode, block, start, end, bb,
            corruptedBlockMap, hedgedReadId++);
        Future<ByteBuffer> firstRequest = hedgedService
            .submit(getFromDataNodeCallable);
          }
          bb = ByteBuffer.allocate(len);
          Callable<ByteBuffer> getFromDataNodeCallable = getFromOneDataNode(
              chosenNode, block, start, end, bb,
              corruptedBlockMap, hedgedReadId++);
          Future<ByteBuffer> oneMoreRequest = hedgedService
              .submit(getFromDataNodeCallable);
  protected static boolean tokenRefetchNeeded(IOException ex,
      InetSocketAddress targetAddr) {
      long bytesToRead = Math.min(remaining, blk.getBlockSize() - targetStart);
      try {
        if (dfsClient.isHedgedReadsEnabled()) {
          hedgedFetchBlockByteRange(blk, targetStart,
              targetStart + bytesToRead - 1, buffer, offset, corruptedBlockMap);
        } else {
          fetchBlockByteRange(blk, targetStart, targetStart + bytesToRead - 1,
              buffer, offset, corruptedBlockMap);
        }
      } finally {
  protected void reportCheckSumFailure(
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap, 
      int dataNodeCount) {
    if (corruptedBlockMap.isEmpty()) {
  @Override
  public synchronized long getPos() {
    return pos;
  }

  }

  static final class DNAddrPair {
    final DatanodeInfo info;
    final InetSocketAddress addr;
    final StorageType storageType;
    }
  }

  protected void closeCurrentBlockReaders() {
    if (blockReader == null) return;
      this.cachingStrategy =
          new CachingStrategy.Builder(this.cachingStrategy).setReadahead(readahead).build();
    }
    closeCurrentBlockReaders();
  }

  @Override
      this.cachingStrategy =
          new CachingStrategy.Builder(this.cachingStrategy).setDropBehind(dropBehind).build();
    }
    closeCurrentBlockReaders();
  }


  @Override
  public synchronized void unbuffer() {
    closeCurrentBlockReaders();
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSClientRetries.java
      doAnswer(new FailNTimesAnswer(preSpyNN, maxBlockAcquires))
        .when(spyNN).getBlockLocations(anyString(), anyLong(), anyLong());
      is.openInfo(true);

