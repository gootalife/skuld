hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/BlockReaderFactory.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf.ShortCircuitConf;
import org.apache.hadoop.hdfs.net.DomainPeer;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
  static ShortCircuitReplicaCreator
      createShortCircuitReplicaInfoCallback = null;

  private final DfsClientConf conf;

  private int remainingCacheTries;

  public BlockReaderFactory(DfsClientConf conf) {
    this.conf = conf;
    this.failureInjector = conf.getShortCircuitConf().brfFailureInjector;
    this.remainingCacheTries = conf.getNumCachedConnRetry();
  }

  public BlockReaderFactory setFileName(String fileName) {
    BlockReader reader = null;

    Preconditions.checkNotNull(configuration);
    final ShortCircuitConf scConf = conf.getShortCircuitConf();
    if (scConf.isShortCircuitLocalReads() && allowShortCircuitLocalReads) {
      if (clientContext.getUseLegacyBlockReaderLocal()) {
        reader = getLegacyBlockReaderLocal();
        if (reader != null) {
        }
      }
    }
    if (scConf.isDomainSocketDataTraffic()) {
      reader = getRemoteBlockReaderFromDomain();
      if (reader != null) {
        if (LOG.isTraceEnabled()) {
          "for short-circuit reads.");
    }
    if (pathInfo == null) {
      pathInfo = clientContext.getDomainSocketFactory()
          .getPathInfo(inetSocketAddress, conf.getShortCircuitConf());
    }
    if (!pathInfo.getPathState().getUsableForShortCircuit()) {
      PerformanceAdvisory.LOG.debug("{}: {} is not usable for short circuit; " +
          "BlockReaderLocal via {}", this, pathInfo.getPath());
      return null;
    }
    return new BlockReaderLocal.Builder(conf.getShortCircuitConf()).
        setFilename(fileName).
        setBlock(block).
        setStartOffset(startOffset).
  private BlockReader getRemoteBlockReaderFromDomain() throws IOException {
    if (pathInfo == null) {
      pathInfo = clientContext.getDomainSocketFactory()
          .getPathInfo(inetSocketAddress, conf.getShortCircuitConf());
    }
    if (!pathInfo.getPathState().getUsableForDataTransfer()) {
      PerformanceAdvisory.LOG.debug("{}: not trying to create a " +
      }
    }
    DomainSocket sock = clientContext.getDomainSocketFactory().
        createSocket(pathInfo, conf.getSocketTimeout());
    if (sock == null) return null;
    return new BlockReaderPeer(new DomainPeer(sock), false);
  }

  @SuppressWarnings("deprecation")
  private BlockReader getRemoteBlockReader(Peer peer) throws IOException {
    if (conf.getShortCircuitConf().isUseLegacyBlockReader()) {
      return RemoteBlockReader.newBlockReader(fileName,
          block, token, startOffset, length, conf.getIoBufferSize(),
          verifyChecksum, clientName, peer, datanode,
          clientContext.getPeerCache(), cachingStrategy);
    } else {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/BlockReaderLocal.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf.ShortCircuitConf;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitReplica;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DirectBufferPool;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
    private ExtendedBlock block;
    private StorageType storageType;

    public Builder(ShortCircuitConf conf) {
      this.maxReadahead = Integer.MAX_VALUE;
      this.verifyChecksum = !conf.isSkipShortCircuitChecksums();
      this.bufferSize = conf.getShortCircuitBufferSize();
    }

    public Builder setVerifyChecksum(boolean verifyChecksum) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/BlockReaderLocalLegacy.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf.ShortCircuitConf;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DirectBufferPool;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
  static BlockReaderLocalLegacy newBlockReader(DfsClientConf conf,
      UserGroupInformation userGroupInformation,
      Configuration configuration, String file, ExtendedBlock blk,
      Token<BlockTokenIdentifier> token, DatanodeInfo node, 
      long startOffset, long length, StorageType storageType)
      throws IOException {
    final ShortCircuitConf scConf = conf.getShortCircuitConf();
    LocalDatanodeInfo localDatanodeInfo = getLocalDatanodeInfo(node
        .getIpcPort());
        userGroupInformation = UserGroupInformation.getCurrentUser();
      }
      pathinfo = getBlockPathInfo(userGroupInformation, blk, node,
          configuration, conf.getSocketTimeout(), token,
          conf.isConnectToDnViaHostname(), storageType);
    }

    FileInputStream dataIn = null;
    FileInputStream checksumIn = null;
    BlockReaderLocalLegacy localBlockReader = null;
    final boolean skipChecksumCheck = scConf.isSkipShortCircuitChecksums()
        || storageType.isTransient();
    try {
      File blkfile = new File(pathinfo.getBlockPath());
            new DataInputStream(checksumIn), blk);
        long firstChunkOffset = startOffset
            - (startOffset % checksum.getBytesPerChecksum());
        localBlockReader = new BlockReaderLocalLegacy(scConf, file, blk, token,
            startOffset, length, pathinfo, checksum, true, dataIn,
            firstChunkOffset, checksumIn);
      } else {
        localBlockReader = new BlockReaderLocalLegacy(scConf, file, blk, token,
            startOffset, length, pathinfo, dataIn);
      }
    } catch (IOException e) {
    return bufferSizeBytes / bytesPerChecksum;
  }

  private BlockReaderLocalLegacy(ShortCircuitConf conf, String hdfsfile,
      ExtendedBlock block, Token<BlockTokenIdentifier> token, long startOffset,
      long length, BlockLocalPathInfo pathinfo, FileInputStream dataIn)
      throws IOException {
        dataIn, startOffset, null);
  }

  private BlockReaderLocalLegacy(ShortCircuitConf conf, String hdfsfile,
      ExtendedBlock block, Token<BlockTokenIdentifier> token, long startOffset,
      long length, BlockLocalPathInfo pathinfo, DataChecksum checksum,
      boolean verifyChecksum, FileInputStream dataIn, long firstChunkOffset,
    this.checksumIn = checksumIn;
    this.offsetFromChunkBoundary = (int) (startOffset-firstChunkOffset);

    final int chunksPerChecksumRead = getSlowReadBufferNumChunks(
        conf.getShortCircuitBufferSize(), bytesPerChecksum);
    slowReadBuff = bufferPool.getBuffer(bytesPerChecksum * chunksPerChecksumRead);
    checksumBuff = bufferPool.getBuffer(checksumSize * chunksPerChecksumRead);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/ClientContext.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf.ShortCircuitConf;
import org.apache.hadoop.hdfs.shortcircuit.DomainSocketFactory;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache;
import org.apache.hadoop.hdfs.util.ByteArrayManager;

import com.google.common.annotations.VisibleForTesting;

  private boolean printedConfWarning = false;

  private ClientContext(String name, DfsClientConf conf) {
    final ShortCircuitConf scConf = conf.getShortCircuitConf();

    this.name = name;
    this.confString = scConf.confAsString();
    this.shortCircuitCache = ShortCircuitCache.fromConf(scConf);
    this.peerCache = new PeerCache(scConf.getSocketCacheCapacity(),
        scConf.getSocketCacheExpiry());
    this.keyProviderCache = new KeyProviderCache(
        scConf.getKeyProviderCacheExpiryMs());
    this.useLegacyBlockReaderLocal = scConf.isUseLegacyBlockReaderLocal();
    this.domainSocketFactory = new DomainSocketFactory(scConf);

    this.byteArrayManager = ByteArrayManager.newInstance(
        conf.getWriteByteArrayManagerConf());
  }

  public static ClientContext get(String name, DfsClientConf conf) {
    ClientContext context;
    synchronized(ClientContext.class) {
      context = CACHES.get(name);
  public static ClientContext getFromConf(Configuration conf) {
    return get(conf.get(DFSConfigKeys.DFS_CLIENT_CONTEXT,
        DFSConfigKeys.DFS_CLIENT_CONTEXT_DEFAULT),
            new DfsClientConf(conf));
  }

  private void printConfWarningIfNeeded(DfsClientConf conf) {
    String existing = this.getConfString();
    String requested = conf.getShortCircuitConf().confAsString();
    if (!existing.equals(requested)) {
      if (!printedConfWarning) {
        printedConfWarning = true;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_WRITES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHE_READAHEAD;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CONTEXT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CONTEXT_DEFAULT;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.LossyRetryInvocationHandler;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RpcInvocationHandler;
  static final int TCP_WINDOW_SIZE = 128 * 1024; // 128 KB

  private final Configuration conf;
  private final DfsClientConf dfsClientConf;
  final ClientProtocol namenode;
  private Text dtService;
  private static ThreadPoolExecutor HEDGED_READ_THREAD_POOL;
  private final Sampler<?> traceSampler;

  public DfsClientConf getConf() {
    return dfsClientConf;
  }

    SpanReceiverHost.getInstance(conf);
    traceSampler = new SamplerBuilder(TraceUtils.wrapHadoopConf(conf)).build();
    this.dfsClientConf = new DfsClientConf(conf);
    this.conf = conf;
    this.stats = stats;
    this.socketFactory = NetUtils.getSocketFactory(conf, ClientProtocol.class);
    this.ugi = UserGroupInformation.getCurrentUser();
    
    this.authority = nameNodeUri == null? "null": nameNodeUri.getAuthority();
    this.clientName = "DFSClient_" + dfsClientConf.getTaskId() + "_" + 
        DFSUtil.getRandom().nextInt()  + "_" + Thread.currentThread().getId();
    int numResponseToDrop = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY,
    return addr;
  }

  int getDatanodeWriteTimeout(int numNodes) {
    final int t = dfsClientConf.getDatanodeSocketWriteTimeout();
    return t > 0? t + HdfsServerConstants.WRITE_TIMEOUT_EXTENSION*numNodes: 0;
  }

  int getDatanodeReadTimeout(int numNodes) {
    final int t = dfsClientConf.getSocketTimeout();
    return t > 0? HdfsServerConstants.READ_TIMEOUT_EXTENSION*numNodes + t: 0;
  }
  
  @VisibleForTesting
    }
  }

    namenode.reportBadBlocks(blocks);
  }
  
  public LocatedBlocks getLocatedBlocks(String src, long start)
      throws IOException {
    return getLocatedBlocks(src, start, dfsClientConf.getPrefetchSize());
  }

  public BlockStorageLocation[] getBlockStorageLocations(
      List<BlockLocation> blockLocations) throws IOException,
      UnsupportedOperationException, InvalidBlockTokenException {
    if (!getConf().isHdfsBlocksMetadataEnabled()) {
      throw new UnsupportedOperationException("Datanode-side support for " +
          "getVolumeBlockLocations() must also be enabled in the client " +
          "configuration.");
    try {
      metadatas = BlockStorageLocationUtil.
          queryDatanodesForHdfsBlocksMetadata(conf, datanodeBlocks,
              getConf().getFileBlockStorageLocationsNumThreads(),
              getConf().getFileBlockStorageLocationsTimeoutMs(),
              getConf().isConnectToDnViaHostname());
      if (LOG.isTraceEnabled()) {
        LOG.trace("metadata returned: "
            + Joiner.on("\n").withKeyValueSeparator("=").join(metadatas));

  public DFSInputStream open(String src) 
      throws IOException, UnresolvedLinkException {
    return open(src, dfsClientConf.getIoBufferSize(), true, null);
  }

  public OutputStream create(String src, boolean overwrite) 
      throws IOException {
    return create(src, overwrite, dfsClientConf.getDefaultReplication(),
        dfsClientConf.getDefaultBlockSize(), null);
  }
    
  public OutputStream create(String src, 
                             boolean overwrite,
                             Progressable progress) throws IOException {
    return create(src, overwrite, dfsClientConf.getDefaultReplication(),
        dfsClientConf.getDefaultBlockSize(), progress);
  }
    
  public OutputStream create(String src, boolean overwrite, short replication,
      long blockSize, Progressable progress) throws IOException {
    return create(src, overwrite, replication, blockSize, progress,
        dfsClientConf.getIoBufferSize());
  }

        progress, buffersize, checksumOpt, null);
  }

  private FsPermission applyUMask(FsPermission permission) {
    if (permission == null) {
      permission = FsPermission.getFileDefault();
    }
    return permission.applyUMask(dfsClientConf.getUMask());
  }

                             ChecksumOpt checksumOpt,
                             InetSocketAddress[] favoredNodes) throws IOException {
    checkOpen();
    final FsPermission masked = applyUMask(permission);
    if(LOG.isDebugEnabled()) {
      LOG.debug(src + ": masked=" + masked);
    }
      throws IOException {
    TraceScope scope = getPathTraceScope("createSymlink", target);
    try {
      final FsPermission dirPerm = applyUMask(null);
      namenode.createSymlink(target, link, dirPerm, createParent);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          new EnumSetWritable<>(flag, CreateFlag.class));
      return DFSOutputStream.newStreamForAppend(this, src, flag, buffersize,
          progress, blkWithStatus.getLastBlock(),
          blkWithStatus.getFileStatus(), dfsClientConf.createChecksum(null),
          favoredNodes);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
      final DatanodeInfo[] datanodes = lb.getLocations();
      
      final int timeout = 3000*datanodes.length + dfsClientConf.getSocketTimeout();
      boolean done = false;
      for(int j = 0; !done && j < datanodes.length; j++) {
        DataOutputStream out = null;
    Socket sock = null;
    try {
      sock = socketFactory.createSocket();
      String dnAddr = dn.getXferAddr(getConf().isConnectToDnViaHostname());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Connecting to datanode " + dnAddr);
      }
  private Type inferChecksumTypeByReading(LocatedBlock lb, DatanodeInfo dn)
      throws IOException {
    IOStreamPair pair = connectToDN(dn, dfsClientConf.getSocketTimeout(), lb);

    try {
      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(pair.out,
  public boolean mkdirs(String src, FsPermission permission,
      boolean createParent) throws IOException {
    final FsPermission masked = applyUMask(permission);
    return primitiveMkdir(src, masked, createParent);
  }

    throws IOException {
    checkOpen();
    if (absPermission == null) {
      absPermission = applyUMask(null);
    } 

    if(LOG.isDebugEnabled()) {
    Peer peer = null;
    boolean success = false;
    Socket sock = null;
    final int socketTimeout = dfsClientConf.getSocketTimeout(); 
    try {
      sock = socketFactory.createSocket();
      NetUtils.connect(sock, addr, getRandomLocalInterfaceAddr(), socketTimeout);
      peer = TcpPeerServer.peerFromSocketAndKey(saslClient, sock, this,
          blockToken, datanodeId);
      peer.setReadTimeout(socketTimeout);
      success = true;
      return peer;
    } finally {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSInputStream.java
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.HasEnhancedByteBufferAccess;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
  void openInfo() throws IOException, UnresolvedLinkException {
    final DfsClientConf conf = dfsClient.getConf();
    synchronized(infoLock) {
      lastBlockBeingWrittenLength = fetchLocatedBlocksAndGetLastBlockLength();
      int retriesForLastBlockLength = conf.getRetryTimesForGetLastBlockLength();
      while (retriesForLastBlockLength > 0) {
          DFSClient.LOG.warn("Last block locations not available. "
              + "Datanodes might not have reported blocks completely."
              + " Will retry for " + retriesForLastBlockLength + " times");
          waitFor(conf.getRetryIntervalForGetLastBlockLength());
          lastBlockBeingWrittenLength = fetchLocatedBlocksAndGetLastBlockLength();
        } else {
          break;
    assert locatedblock != null : "LocatedBlock cannot be null";
    int replicaNotFoundCount = locatedblock.getLocations().length;
    
    final DfsClientConf conf = dfsClient.getConf();
    for(DatanodeInfo datanode : locatedblock.getLocations()) {
      ClientDatanodeProtocol cdp = null;
      
      try {
        cdp = DFSUtil.createClientDatanodeProtocolProxy(datanode,
            dfsClient.getConfiguration(), conf.getSocketTimeout(),
            conf.isConnectToDnViaHostname(), locatedblock);
        
        final long n = cdp.getReplicaVisibleLength(locatedblock.getBlock());
        
        String errMsg = getBestNodeDNAddrPairErrorString(block.getLocations(),
          deadNodes, ignoredNodes);
        String blockInfo = block.getBlock() + " file=" + src;
        if (failures >= dfsClient.getConf().getMaxBlockAcquireFailures()) {
          String description = "Could not obtain block: " + blockInfo;
          DFSClient.LOG.warn(description + errMsg
              + ". Throwing a BlockMissingException");
          final int timeWindow = dfsClient.getConf().getTimeWindow();
          double waitTime = timeWindow * failures +       // grace period for the last round of attempt
            timeWindow * (failures + 1) * DFSUtil.getRandom().nextDouble(); // expanding time window for each failure
          DFSClient.LOG.warn("DFS chooseDataNode: got # " + (failures + 1) + " IOException, will wait for " + waitTime + " msec.");
          ", ignoredNodes = " + ignoredNodes);
    }
    final String dnAddr =
        chosenNode.getXferAddr(dfsClient.getConf().isConnectToDnViaHostname());
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Connecting to datanode " + dnAddr);
    }
      }
    }
    ByteBuffer buffer = null;
    if (dfsClient.getConf().getShortCircuitConf().isShortCircuitMmapEnabled()) {
      buffer = tryReadZeroCopy(maxLength, opts);
    }
    if (buffer != null) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSOutputStream.java
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
    this(dfsClient, src, progress, stat, checksum);
    this.shouldSyncBlock = flag.contains(CreateFlag.SYNC_BLOCK);

    computePacketChunkSize(dfsClient.getConf().getWritePacketSize(), bytesPerChecksum);

    streamer = new DataStreamer(stat, null, dfsClient, src, progress, checksum,
        cachingStrategy, byteArrayManager);
      adjustPacketChunkSize(stat);
      streamer.setPipelineInConstruction(lastBlock);
    } else {
      computePacketChunkSize(dfsClient.getConf().getWritePacketSize(),
          bytesPerChecksum);
      streamer = new DataStreamer(stat, lastBlock != null ? lastBlock.getBlock() : null,
          dfsClient, src, progress, checksum, cachingStrategy, byteArrayManager);
      computePacketChunkSize(
          Math.min(dfsClient.getConf().getWritePacketSize(), freeInLastBlock),
          bytesPerChecksum);
    }
  }

    if (!streamer.getAppendChunk()) {
      int psize = Math.min((int)(blockSize- streamer.getBytesCurBlock()),
          dfsClient.getConf().getWritePacketSize());
      computePacketChunkSize(psize, bytesPerChecksum);
    }
  }
      return;
    }
    streamer.setLastException(new IOException("Lease timeout of "
        + (dfsClient.getConf().getHdfsTimeout()/1000) + " seconds expired."));
    closeThreads(true);
    dfsClient.endFileLease(fileId);
  }
  protected void completeFile(ExtendedBlock last) throws IOException {
    long localstart = Time.monotonicNow();
    final DfsClientConf conf = dfsClient.getConf();
    long sleeptime = conf.getBlockWriteLocateFollowingInitialDelayMs();
    boolean fileComplete = false;
    int retries = conf.getNumBlockWriteLocateFollowingRetry();
    while (!fileComplete) {
      fileComplete =
          dfsClient.namenode.complete(src, dfsClient.clientName, last, fileId);
      if (!fileComplete) {
        final int hdfsTimeout = conf.getHdfsTimeout();
        if (!dfsClient.clientRunning
            || (hdfsTimeout > 0
                && localstart + hdfsTimeout < Time.monotonicNow())) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DataStreamer.java

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
  static Socket createSocketForPipeline(final DatanodeInfo first,
      final int length, final DFSClient client) throws IOException {
    final DfsClientConf conf = client.getConf();
    final String dnAddr = first.getXferAddr(conf.isConnectToDnViaHostname());
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Connecting to datanode " + dnAddr);
    }
    final InetSocketAddress isa = NetUtils.createSocketAddr(dnAddr);
    final Socket sock = client.socketFactory.createSocket();
    final int timeout = client.getDatanodeReadTimeout(length);
    NetUtils.connect(sock, isa, client.getRandomLocalInterfaceAddr(), conf.getSocketTimeout());
    sock.setSoTimeout(timeout);
    sock.setSendBufferSize(HdfsConstants.DEFAULT_DATA_SOCKET_SIZE);
    if(DFSClient.LOG.isDebugEnabled()) {
    this.byteArrayManager = byteArrayManage;
    isLazyPersistFile = isLazyPersist(stat);
    this.dfsclientSlowLogThresholdMs =
        dfsClient.getConf().getSlowIoWarningThresholdMs();
    excludedNodes = initExcludedNodes();
  }

          doSleep = processDatanodeError();
        }

        final int halfSocketTimeout = dfsClient.getConf().getSocketTimeout()/2; 
        synchronized (dataQueue) {
          long now = Time.monotonicNow();
              && dataQueue.size() == 0 &&
              (stage != BlockConstructionStage.DATA_STREAMING ||
                  stage == BlockConstructionStage.DATA_STREAMING &&
                      now - lastPacket < halfSocketTimeout)) || doSleep ) {
            long timeout = halfSocketTimeout - (now-lastPacket);
            timeout = timeout <= 0 ? 1000 : timeout;
            timeout = (stage == BlockConstructionStage.DATA_STREAMING)?
                timeout : 1000;
        boolean firstWait = true;
        try {
          while (!streamerClosed && dataQueue.size() + ackQueue.size() >
              dfsClient.getConf().getWriteMaxPackets()) {
            if (firstWait) {
              Span span = Trace.currentSpan();
              if (span != null) {
            if (PipelineAck.isRestartOOBStatus(reply) &&
                shouldWaitForRestart(i)) {
              restartDeadline = dfsClient.getConf().getDatanodeRestartTimeout()
                  + Time.monotonicNow();
              setRestartingNodeIndex(i);
              String message = "A datanode is restarting: " + targets[i];
        long delay = Math.min(dfsClient.getConf().getDatanodeRestartTimeout(),
            4000L);
        try {
          Thread.sleep(delay);
    LocatedBlock lb = null;
    DatanodeInfo[] nodes = null;
    StorageType[] storageTypes = null;
    int count = dfsClient.getConf().getNumBlockWriteRetry();
    boolean success = false;
    ExtendedBlock oldBlock = block;
    do {
        }
        if (checkRestart && shouldWaitForRestart(errorIndex)) {
          restartDeadline = dfsClient.getConf().getDatanodeRestartTimeout()
              + Time.monotonicNow();
          restartingNodeIndex.set(errorIndex);
          errorIndex = -1;

  protected LocatedBlock locateFollowingBlock(DatanodeInfo[] excludedNodes)
      throws IOException {
    final DfsClientConf conf = dfsClient.getConf(); 
    int retries = conf.getNumBlockWriteLocateFollowingRetry();
    long sleeptime = conf.getBlockWriteLocateFollowingInitialDelayMs();
    while (true) {
      long localstart = Time.monotonicNow();
      while (true) {

  private LoadingCache<DatanodeInfo, DatanodeInfo> initExcludedNodes() {
    return CacheBuilder.newBuilder().expireAfterWrite(
        dfsClient.getConf().getExcludedNodesCacheExpiry(),
        TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener<DatanodeInfo, DatanodeInfo>() {
          @Override
          public void onRemoval(

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DistributedFileSystem.java

  @Override
  public long getDefaultBlockSize() {
    return dfs.getConf().getDefaultBlockSize();
  }

  @Override
  public short getDefaultReplication() {
    return dfs.getConf().getDefaultReplication();
  }

  @Override

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/LeaseRenewer.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/NameNodeProxies.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
          UserGroupInformation.getCurrentUser(), true, fallbackToSimpleAuth);
    } else {
      DfsClientConf config = new DfsClientConf(conf);
      T proxy = (T) RetryProxy.create(xface, failoverProxyProvider,
          RetryPolicies.failoverOnNetworkException(
              RetryPolicies.TRY_ONCE_THEN_FAIL, config.getMaxFailoverAttempts(),
              config.getMaxRetryAttempts(), config.getFailoverSleepBaseMillis(),
              config.getFailoverSleepMaxMillis()));

      Text dtService;
      if (failoverProxyProvider.useLogicalURI()) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/client/impl/DfsClientConf.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/client/impl/DfsClientConf.java
package org.apache.hadoop.hdfs.client.impl;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_RETRIES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_DATANODE_RESTART_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.BlockReaderFactory;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.util.DataChecksum;

import com.google.common.annotations.VisibleForTesting;

public class DfsClientConf {

  private final int hdfsTimeout;    // timeout value for a DFS operation.

  private final int maxFailoverAttempts;
  private final int maxRetryAttempts;
  private final int failoverSleepBaseMillis;
  private final int failoverSleepMaxMillis;
  private final int maxBlockAcquireFailures;
  private final int datanodeSocketWriteTimeout;
  private final int ioBufferSize;
  private final ChecksumOpt defaultChecksumOpt;
  private final int writePacketSize;
  private final int writeMaxPackets;
  private final ByteArrayManager.Conf writeByteArrayManagerConf;
  private final int socketTimeout;
  private final long excludedNodesCacheExpiry;
  private final int timeWindow;
  private final int numCachedConnRetry;
  private final int numBlockWriteRetry;
  private final int numBlockWriteLocateFollowingRetry;
  private final int blockWriteLocateFollowingInitialDelayMs;
  private final long defaultBlockSize;
  private final long prefetchSize;
  private final short defaultReplication;
  private final String taskId;
  private final FsPermission uMask;
  private final boolean connectToDnViaHostname;
  private final boolean hdfsBlocksMetadataEnabled;
  private final int fileBlockStorageLocationsNumThreads;
  private final int fileBlockStorageLocationsTimeoutMs;
  private final int retryTimesForGetLastBlockLength;
  private final int retryIntervalForGetLastBlockLength;
  private final long datanodeRestartTimeout;
  private final long slowIoWarningThresholdMs;

  private final ShortCircuitConf shortCircuitConf;

  public DfsClientConf(Configuration conf) {
    hdfsTimeout = Client.getTimeout(conf);

    maxFailoverAttempts = conf.getInt(
        DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);
    maxRetryAttempts = conf.getInt(
        HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_KEY,
        HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_DEFAULT);
    failoverSleepBaseMillis = conf.getInt(
        DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY,
        DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT);
    failoverSleepMaxMillis = conf.getInt(
        DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY,
        DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT);

    maxBlockAcquireFailures = conf.getInt(
        DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY,
        DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT);
    datanodeSocketWriteTimeout = conf.getInt(DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY,
        HdfsServerConstants.WRITE_TIMEOUT);
    ioBufferSize = conf.getInt(
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
    defaultChecksumOpt = getChecksumOptFromConf(conf);
    socketTimeout = conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY,
        HdfsServerConstants.READ_TIMEOUT);
    writePacketSize = conf.getInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY,
        DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
    writeMaxPackets = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_WRITE_MAX_PACKETS_IN_FLIGHT_KEY,
        DFSConfigKeys.DFS_CLIENT_WRITE_MAX_PACKETS_IN_FLIGHT_DEFAULT);
    
    final boolean byteArrayManagerEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_ENABLED_KEY,
        DFSConfigKeys.DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_ENABLED_DEFAULT);
    if (!byteArrayManagerEnabled) {
      writeByteArrayManagerConf = null;
    } else {
      final int countThreshold = conf.getInt(
          DFSConfigKeys.DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_THRESHOLD_KEY,
          DFSConfigKeys.DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_THRESHOLD_DEFAULT);
      final int countLimit = conf.getInt(
          DFSConfigKeys.DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_LIMIT_KEY,
          DFSConfigKeys.DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_LIMIT_DEFAULT);
      final long countResetTimePeriodMs = conf.getLong(
          DFSConfigKeys.DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_RESET_TIME_PERIOD_MS_KEY,
          DFSConfigKeys.DFS_CLIENT_WRITE_BYTE_ARRAY_MANAGER_COUNT_RESET_TIME_PERIOD_MS_DEFAULT);
      writeByteArrayManagerConf = new ByteArrayManager.Conf(
          countThreshold, countLimit, countResetTimePeriodMs); 
    }
    
    defaultBlockSize = conf.getLongBytes(DFS_BLOCK_SIZE_KEY,
        DFS_BLOCK_SIZE_DEFAULT);
    defaultReplication = (short) conf.getInt(
        DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT);
    taskId = conf.get("mapreduce.task.attempt.id", "NONMAPREDUCE");
    excludedNodesCacheExpiry = conf.getLong(
        DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL,
        DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT);
    prefetchSize = conf.getLong(DFS_CLIENT_READ_PREFETCH_SIZE_KEY,
        10 * defaultBlockSize);
    timeWindow = conf.getInt(
        HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY,
        HdfsClientConfigKeys.Retry.WINDOW_BASE_DEFAULT);
    numCachedConnRetry = conf.getInt(DFS_CLIENT_CACHED_CONN_RETRY_KEY,
        DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT);
    numBlockWriteRetry = conf.getInt(DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY,
        DFS_CLIENT_BLOCK_WRITE_RETRIES_DEFAULT);
    numBlockWriteLocateFollowingRetry = conf.getInt(
        DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY,
        DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT);
    blockWriteLocateFollowingInitialDelayMs = conf.getInt(
        DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_KEY,
        DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_DEFAULT);
    uMask = FsPermission.getUMask(conf);
    connectToDnViaHostname = conf.getBoolean(DFS_CLIENT_USE_DN_HOSTNAME,
        DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
    hdfsBlocksMetadataEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED, 
        DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED_DEFAULT);
    fileBlockStorageLocationsNumThreads = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_NUM_THREADS,
        DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_NUM_THREADS_DEFAULT);
    fileBlockStorageLocationsTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS,
        DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS_DEFAULT);
    retryTimesForGetLastBlockLength = conf.getInt(
        HdfsClientConfigKeys.Retry.TIMES_GET_LAST_BLOCK_LENGTH_KEY,
        HdfsClientConfigKeys.Retry.TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT);
    retryIntervalForGetLastBlockLength = conf.getInt(
        HdfsClientConfigKeys.Retry.INTERVAL_GET_LAST_BLOCK_LENGTH_KEY,
        HdfsClientConfigKeys.Retry.INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT);


    datanodeRestartTimeout = conf.getLong(
        DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY,
        DFS_CLIENT_DATANODE_RESTART_TIMEOUT_DEFAULT) * 1000;
    slowIoWarningThresholdMs = conf.getLong(
        DFSConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY,
        DFSConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT);
    
    shortCircuitConf = new ShortCircuitConf(conf);
  }

  private DataChecksum.Type getChecksumType(Configuration conf) {
    final String checksum = conf.get(
        DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY,
        DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT);
    try {
      return DataChecksum.Type.valueOf(checksum);
    } catch(IllegalArgumentException iae) {
      DFSClient.LOG.warn("Bad checksum type: " + checksum + ". Using default "
          + DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT);
      return DataChecksum.Type.valueOf(
          DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT); 
    }
  }

  private ChecksumOpt getChecksumOptFromConf(Configuration conf) {
    DataChecksum.Type type = getChecksumType(conf);
    int bytesPerChecksum = conf.getInt(DFS_BYTES_PER_CHECKSUM_KEY,
        DFS_BYTES_PER_CHECKSUM_DEFAULT);
    return new ChecksumOpt(type, bytesPerChecksum);
  }

  public DataChecksum createChecksum(ChecksumOpt userOpt) {
    ChecksumOpt opt = ChecksumOpt.processChecksumOpt(
        defaultChecksumOpt, userOpt);
    DataChecksum dataChecksum = DataChecksum.newDataChecksum(
        opt.getChecksumType(),
        opt.getBytesPerChecksum());
    if (dataChecksum == null) {
      throw new HadoopIllegalArgumentException("Invalid checksum type: userOpt="
          + userOpt + ", default=" + defaultChecksumOpt
          + ", effective=null");
    }
    return dataChecksum;
  }

  @VisibleForTesting
  public int getBlockWriteLocateFollowingInitialDelayMs() {
    return blockWriteLocateFollowingInitialDelayMs;
  }

  public int getHdfsTimeout() {
    return hdfsTimeout;
  }

  public int getMaxFailoverAttempts() {
    return maxFailoverAttempts;
  }

  public int getMaxRetryAttempts() {
    return maxRetryAttempts;
  }

  public int getFailoverSleepBaseMillis() {
    return failoverSleepBaseMillis;
  }

  public int getFailoverSleepMaxMillis() {
    return failoverSleepMaxMillis;
  }

  public int getMaxBlockAcquireFailures() {
    return maxBlockAcquireFailures;
  }

  public int getDatanodeSocketWriteTimeout() {
    return datanodeSocketWriteTimeout;
  }

  public int getIoBufferSize() {
    return ioBufferSize;
  }

  public ChecksumOpt getDefaultChecksumOpt() {
    return defaultChecksumOpt;
  }

  public int getWritePacketSize() {
    return writePacketSize;
  }

  public int getWriteMaxPackets() {
    return writeMaxPackets;
  }

  public ByteArrayManager.Conf getWriteByteArrayManagerConf() {
    return writeByteArrayManagerConf;
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }

  public long getExcludedNodesCacheExpiry() {
    return excludedNodesCacheExpiry;
  }

  public int getTimeWindow() {
    return timeWindow;
  }

  public int getNumCachedConnRetry() {
    return numCachedConnRetry;
  }

  public int getNumBlockWriteRetry() {
    return numBlockWriteRetry;
  }

  public int getNumBlockWriteLocateFollowingRetry() {
    return numBlockWriteLocateFollowingRetry;
  }

  public long getDefaultBlockSize() {
    return defaultBlockSize;
  }

  public long getPrefetchSize() {
    return prefetchSize;
  }

  public short getDefaultReplication() {
    return defaultReplication;
  }

  public String getTaskId() {
    return taskId;
  }

  public FsPermission getUMask() {
    return uMask;
  }

  public boolean isConnectToDnViaHostname() {
    return connectToDnViaHostname;
  }

  public boolean isHdfsBlocksMetadataEnabled() {
    return hdfsBlocksMetadataEnabled;
  }

  public int getFileBlockStorageLocationsNumThreads() {
    return fileBlockStorageLocationsNumThreads;
  }

  public int getFileBlockStorageLocationsTimeoutMs() {
    return fileBlockStorageLocationsTimeoutMs;
  }

  public int getRetryTimesForGetLastBlockLength() {
    return retryTimesForGetLastBlockLength;
  }

  public int getRetryIntervalForGetLastBlockLength() {
    return retryIntervalForGetLastBlockLength;
  }

  public long getDatanodeRestartTimeout() {
    return datanodeRestartTimeout;
  }

  public long getSlowIoWarningThresholdMs() {
    return slowIoWarningThresholdMs;
  }

  public ShortCircuitConf getShortCircuitConf() {
    return shortCircuitConf;
  }

  public static class ShortCircuitConf {
    private static final Log LOG = LogFactory.getLog(ShortCircuitConf.class);

    private final int socketCacheCapacity;
    private final long socketCacheExpiry;

    private final boolean useLegacyBlockReader;
    private final boolean useLegacyBlockReaderLocal;
    private final String domainSocketPath;
    private final boolean skipShortCircuitChecksums;

    private final int shortCircuitBufferSize;
    private final boolean shortCircuitLocalReads;
    private final boolean domainSocketDataTraffic;
    private final int shortCircuitStreamsCacheSize;
    private final long shortCircuitStreamsCacheExpiryMs; 
    private final int shortCircuitSharedMemoryWatcherInterruptCheckMs;
    
    private final boolean shortCircuitMmapEnabled;
    private final int shortCircuitMmapCacheSize;
    private final long shortCircuitMmapCacheExpiryMs;
    private final long shortCircuitMmapCacheRetryTimeout;
    private final long shortCircuitCacheStaleThresholdMs;

    private final long keyProviderCacheExpiryMs;

    @VisibleForTesting
    public BlockReaderFactory.FailureInjector brfFailureInjector =
        new BlockReaderFactory.FailureInjector();

    public ShortCircuitConf(Configuration conf) {
      socketCacheCapacity = conf.getInt(
          DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY,
          DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT);
      socketCacheExpiry = conf.getLong(
          DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY,
          DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT);

      useLegacyBlockReader = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADER,
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADER_DEFAULT);
      useLegacyBlockReaderLocal = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL,
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT);
      shortCircuitLocalReads = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY,
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT);
      domainSocketDataTraffic = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC,
          DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT);
      domainSocketPath = conf.getTrimmed(
          DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
          DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_DEFAULT);

      if (LOG.isDebugEnabled()) {
        LOG.debug(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL
            + " = " + useLegacyBlockReaderLocal);
        LOG.debug(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY
            + " = " + shortCircuitLocalReads);
        LOG.debug(DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC
            + " = " + domainSocketDataTraffic);
        LOG.debug(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY
            + " = " + domainSocketPath);
      }

      skipShortCircuitChecksums = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY,
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_DEFAULT);
      shortCircuitBufferSize = conf.getInt(
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_KEY,
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_DEFAULT);
      shortCircuitStreamsCacheSize = conf.getInt(
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_KEY,
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_DEFAULT);
      shortCircuitStreamsCacheExpiryMs = conf.getLong(
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_KEY,
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_DEFAULT);
      shortCircuitMmapEnabled = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_MMAP_ENABLED,
          DFSConfigKeys.DFS_CLIENT_MMAP_ENABLED_DEFAULT);
      shortCircuitMmapCacheSize = conf.getInt(
          DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_SIZE,
          DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_SIZE_DEFAULT);
      shortCircuitMmapCacheExpiryMs = conf.getLong(
          DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS,
          DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS_DEFAULT);
      shortCircuitMmapCacheRetryTimeout = conf.getLong(
          DFSConfigKeys.DFS_CLIENT_MMAP_RETRY_TIMEOUT_MS,
          DFSConfigKeys.DFS_CLIENT_MMAP_RETRY_TIMEOUT_MS_DEFAULT);
      shortCircuitCacheStaleThresholdMs = conf.getLong(
          DFSConfigKeys.DFS_CLIENT_SHORT_CIRCUIT_REPLICA_STALE_THRESHOLD_MS,
          DFSConfigKeys.DFS_CLIENT_SHORT_CIRCUIT_REPLICA_STALE_THRESHOLD_MS_DEFAULT);
      shortCircuitSharedMemoryWatcherInterruptCheckMs = conf.getInt(
          DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS,
          DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT);

      keyProviderCacheExpiryMs = conf.getLong(
          DFSConfigKeys.DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_MS,
          DFSConfigKeys.DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_DEFAULT);
    }

    public int getSocketCacheCapacity() {
      return socketCacheCapacity;
    }

    public long getSocketCacheExpiry() {
      return socketCacheExpiry;
    }

    public boolean isUseLegacyBlockReaderLocal() {
      return useLegacyBlockReaderLocal;
    }

    public String getDomainSocketPath() {
      return domainSocketPath;
    }

    public boolean isShortCircuitLocalReads() {
      return shortCircuitLocalReads;
    }

    public boolean isDomainSocketDataTraffic() {
      return domainSocketDataTraffic;
    }
    public boolean isUseLegacyBlockReader() {
      return useLegacyBlockReader;
    }

    public boolean isSkipShortCircuitChecksums() {
      return skipShortCircuitChecksums;
    }

    public int getShortCircuitBufferSize() {
      return shortCircuitBufferSize;
    }

    public int getShortCircuitStreamsCacheSize() {
      return shortCircuitStreamsCacheSize;
    }

    public long getShortCircuitStreamsCacheExpiryMs() {
      return shortCircuitStreamsCacheExpiryMs;
    }

    public int getShortCircuitSharedMemoryWatcherInterruptCheckMs() {
      return shortCircuitSharedMemoryWatcherInterruptCheckMs;
    }

    public boolean isShortCircuitMmapEnabled() {
      return shortCircuitMmapEnabled;
    }

    public int getShortCircuitMmapCacheSize() {
      return shortCircuitMmapCacheSize;
    }

    public long getShortCircuitMmapCacheExpiryMs() {
      return shortCircuitMmapCacheExpiryMs;
    }

    public long getShortCircuitMmapCacheRetryTimeout() {
      return shortCircuitMmapCacheRetryTimeout;
    }

    public long getShortCircuitCacheStaleThresholdMs() {
      return shortCircuitCacheStaleThresholdMs;
    }

    public long getKeyProviderCacheExpiryMs() {
      return keyProviderCacheExpiryMs;
    }

    public String confAsString() {
      StringBuilder builder = new StringBuilder();
      builder.append("shortCircuitStreamsCacheSize = ").
        append(shortCircuitStreamsCacheSize).
        append(", shortCircuitStreamsCacheExpiryMs = ").
        append(shortCircuitStreamsCacheExpiryMs).
        append(", shortCircuitMmapCacheSize = ").
        append(shortCircuitMmapCacheSize).
        append(", shortCircuitMmapCacheExpiryMs = ").
        append(shortCircuitMmapCacheExpiryMs).
        append(", shortCircuitMmapCacheRetryTimeout = ").
        append(shortCircuitMmapCacheRetryTimeout).
        append(", shortCircuitCacheStaleThresholdMs = ").
        append(shortCircuitCacheStaleThresholdMs).
        append(", socketCacheCapacity = ").
        append(socketCacheCapacity).
        append(", socketCacheExpiry = ").
        append(socketCacheExpiry).
        append(", shortCircuitLocalReads = ").
        append(shortCircuitLocalReads).
        append(", useLegacyBlockReaderLocal = ").
        append(useLegacyBlockReaderLocal).
        append(", domainSocketDataTraffic = ").
        append(domainSocketDataTraffic).
        append(", shortCircuitSharedMemoryWatcherInterruptCheckMs = ").
        append(shortCircuitSharedMemoryWatcherInterruptCheckMs).
        append(", keyProviderCacheExpiryMs = ").
        append(keyProviderCacheExpiryMs);

      return builder.toString();
    }
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/shortcircuit/DomainSocketFactory.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf.ShortCircuitConf;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.util.PerformanceAdvisory;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class DomainSocketFactory {
  private static final Log LOG = LogFactory.getLog(DomainSocketFactory.class);
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build();

  public DomainSocketFactory(ShortCircuitConf conf) {
    final String feature;
    if (conf.isShortCircuitLocalReads() && (!conf.isUseLegacyBlockReaderLocal())) {
      feature = "The short-circuit local reads feature";
  public PathInfo getPathInfo(InetSocketAddress addr, ShortCircuitConf conf) {
    if (conf.getDomainSocketPath().isEmpty()) return PathInfo.NOT_CONFIGURED;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/shortcircuit/ShortCircuitCache.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf.ShortCircuitConf;
import org.apache.hadoop.hdfs.net.DomainPeer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
            DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT));
  }

  public static ShortCircuitCache fromConf(ShortCircuitConf conf) {
    return new ShortCircuitCache(
        conf.getShortCircuitStreamsCacheSize(),
        conf.getShortCircuitStreamsCacheExpiryMs(),
        conf.getShortCircuitMmapCacheSize(),
        conf.getShortCircuitMmapCacheExpiryMs(),
        conf.getShortCircuitMmapCacheRetryTimeout(),
        conf.getShortCircuitCacheStaleThresholdMs(),
        conf.getShortCircuitSharedMemoryWatcherInterruptCheckMs());
  }

  public ShortCircuitCache(int maxTotalSize, long maxNonMmappedEvictableLifespanMs,
      int maxEvictableMmapedSize, long maxEvictableMmapedLifespanMs,
      long mmapRetryTimeoutMs, long staleThresholdMs, int shmInterruptCheckMs) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/fs/TestEnhancedByteBufferAccess.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.BlockReaderTestUtil;
import org.apache.hadoop.hdfs.ClientContext;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
    fsIn.close();
    fsIn = fs.open(TEST_PATH);
    final ShortCircuitCache cache = ClientContext.get(
        CONTEXT, new DfsClientConf(conf)). getShortCircuitCache();
    cache.accept(new CountingVisitor(0, 5, 5, 0));
    results[0] = fsIn.read(null, BLOCK_SIZE,
        EnumSet.of(ReadOption.SKIP_CHECKSUMS));
    final ExtendedBlock firstBlock =
        DFSTestUtil.getFirstBlock(fs, TEST_PATH);
    final ShortCircuitCache cache = ClientContext.get(
        CONTEXT, new DfsClientConf(conf)). getShortCircuitCache();
    waitForReplicaAnchorStatus(cache, firstBlock, true, true, 1);
    fs.removeCacheDirective(directiveId);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockReaderLocal.java
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache;
              Time.now(), shm.allocAndRegisterSlot(
                  ExtendedBlockId.fromExtendedBlock(block)));
      blockReaderLocal = new BlockReaderLocal.Builder(
              new DfsClientConf.ShortCircuitConf(conf)).
          setFilename(TEST_PATH.getName()).
          setBlock(block).
          setShortCircuitReplica(replica).

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSClientRetries.java
      NamenodeProtocols preSpyNN = cluster.getNameNodeRpc();
      NamenodeProtocols spyNN = spy(preSpyNN);
      DFSClient client = new DFSClient(null, spyNN, conf, null);
      int maxBlockAcquires = client.getConf().getMaxBlockAcquireFailures();
      assertTrue(maxBlockAcquires > 0);



hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSOutputStream.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;

  @Test
  public void testCongestionBackoff() throws IOException {
    DfsClientConf dfsClientConf = mock(DfsClientConf.class);
    DFSClient client = mock(DFSClient.class);
    when(client.getConf()).thenReturn(dfsClientConf);
    client.clientRunning = true;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestLeaseRenewer.java
hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestParallelReadUtil.java
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Ignore;
import org.junit.Test;

      testInfo.filepath = new Path("/TestParallelRead.dat." + i);
      testInfo.authenticData = util.writeFile(testInfo.filepath, FILE_SIZE_K);
      testInfo.dis = dfsClient.open(testInfo.filepath.toString(),
          dfsClient.getConf().getIoBufferSize(), verifyChecksums);

      for (int j = 0; j < nWorkerEach; ++j) {
        workers[nWorkers++] = new ReadWorker(testInfo, nWorkers, helper);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockTokenWithDFS.java
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RemotePeerFactory;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
      DatanodeInfo[] nodes = lblock.getLocations();
      targetAddr = NetUtils.createSocketAddr(nodes[0].getXferAddr());

      blockReader = new BlockReaderFactory(new DfsClientConf(conf)).
          setFileName(BlockReaderFactory.getFileName(targetAddr, 
                        "test-blockpoolid", block.getBlockId())).
          setBlock(block).

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDataNodeVolumeFailure.java
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.BlockReaderFactory;
import org.apache.hadoop.hdfs.ClientContext;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RemotePeerFactory;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.Block;
   
    targetAddr = NetUtils.createSocketAddr(datanode.getXferAddr());

    BlockReader blockReader = new BlockReaderFactory(new DfsClientConf(conf)).
      setInetSocketAddress(targetAddr).
      setBlock(block).
      setFileName(BlockReaderFactory.getFileName(targetAddr,

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/shortcircuit/TestShortCircuitCache.java

    fs.getClient().getConf().getShortCircuitConf().brfFailureInjector =
        new TestCleanupFailureInjector();
    try {
      DFSTestUtil.readFileBuffer(fs, TEST_PATH2);

