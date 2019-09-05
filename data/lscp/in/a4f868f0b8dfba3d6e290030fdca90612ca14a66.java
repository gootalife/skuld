hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
      new DFSHedgedReadMetrics();
  private static ThreadPoolExecutor HEDGED_READ_THREAD_POOL;
  private final Sampler<?> traceSampler;
  private final int smallBufferSize;

  public DfsClientConf getConf() {
    return dfsClientConf;
    this.stats = stats;
    this.socketFactory = NetUtils.getSocketFactory(conf, ClientProtocol.class);
    this.dtpReplaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);
    this.smallBufferSize = DFSUtil.getSmallBufferSize(conf);

    this.ugi = UserGroupInformation.getCurrentUser();
    
          IOStreamPair pair = connectToDN(datanodes[j], timeout, lb);
          out = new DataOutputStream(new BufferedOutputStream(pair.out,
              smallBufferSize));
          in = new DataInputStream(pair.in);

          if (LOG.isDebugEnabled()) {

    try {
      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(pair.out,
          smallBufferSize));
      DataInputStream in = new DataInputStream(pair.in);
  
      new Sender(out).readBlock(lb.getBlock(), lb.getBlockToken(), clientName,

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSUtil.java
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
        .createKeyProviderCryptoExtension(keyProvider);
    return cryptoProvider;
  }

  public static int getIoFileBufferSize(Configuration conf) {
    return conf.getInt(
      CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
      CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
  }

  public static int getSmallBufferSize(Configuration conf) {
    return Math.min(getIoFileBufferSize(conf) / 2, 512);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DataStreamer.java
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.htrace.TraceInfo;
import org.apache.htrace.TraceScope;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
      unbufOut = saslStreams.out;
      unbufIn = saslStreams.in;
      out = new DataOutputStream(new BufferedOutputStream(unbufOut,
          DFSUtil.getSmallBufferSize(dfsClient.getConfiguration())));
      in = new DataInputStream(unbufIn);

        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            DFSUtil.getSmallBufferSize(dfsClient.getConfiguration())));
        blockReplyStream = new DataInputStream(unbufIn);


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/balancer/Dispatcher.java
  private final int maxConcurrentMovesPerNode;

  private final int ioFileBufferSize;

  private static class GlobalBlockMap {
    private final Map<Block, DBlock> map = new HashMap<Block, DBlock>();

        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            ioFileBufferSize));
        in = new DataInputStream(new BufferedInputStream(unbufIn,
            ioFileBufferSize));

        sendRequest(out, eb, accessToken);
        receiveResponse(in);
    this.saslClient = new SaslDataTransferClient(conf,
        DataTransferSaslUtil.getSaslPropertiesResolver(conf),
        TrustedChannelResolver.getInstance(conf), nnc.fallbackToSimpleAuth);
    this.ioFileBufferSize = DFSUtil.getIoFileBufferSize(conf);
  }

  public DistributedFileSystem getDistributedFileSystem() {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/common/HdfsServerConstants.java
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
  int MAX_PATH_LENGTH = 8000;
  int MAX_PATH_DEPTH = 1000;
  long INVALID_TXID = -12345;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockMetadataHeader.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;

  private final short version;
  private DataChecksum checksum = null;

  private static final HdfsConfiguration conf = new HdfsConfiguration();
    
  @VisibleForTesting
  public BlockMetadataHeader(short version, DataChecksum checksum) {
    this.checksum = checksum;
    DataInputStream in = null;
    try {
      in = new DataInputStream(new BufferedInputStream(
        new FileInputStream(metaFile), DFSUtil.getIoFileBufferSize(conf)));
      return readDataChecksum(in, metaFile);
    } finally {
      IOUtils.closeStream(in);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockReceiver.java
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
            out.getClass());
      }
      this.checksumOut = new DataOutputStream(new BufferedOutputStream(
          streams.getChecksumOut(), DFSUtil.getSmallBufferSize(
          datanode.getConf())));
      if (isCreate) {
        BlockMetadataHeader.writeHeader(checksumOut, diskChecksum);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockSender.java

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
  private static final int MIN_BUFFER_WITH_TRANSFERTO = 64*1024;
  private static final int IO_FILE_BUFFER_SIZE;
  static {
    HdfsConfiguration conf = new HdfsConfiguration();
    IO_FILE_BUFFER_SIZE = DFSUtil.getIoFileBufferSize(conf);
  }
  private static final int TRANSFERTO_BUFFER_SIZE = Math.max(
      IO_FILE_BUFFER_SIZE, MIN_BUFFER_WITH_TRANSFERTO);
  
  private final ExtendedBlock block;
            if (metaIn.getLength() > BlockMetadataHeader.getHeaderSize()) {
              checksumIn = new DataInputStream(new BufferedInputStream(
                  metaIn, IO_FILE_BUFFER_SIZE));
  
              csum = BlockMetadataHeader.readDataChecksum(checksumIn, block);
              keepMetaInOpen = true;
        pktBufSize += checksumSize * maxChunksPerPacket;
      } else {
        maxChunksPerPacket = Math.max(1,
            numberOfChunks(IO_FILE_BUFFER_SIZE));
        pktBufSize += (chunkSize + checksumSize) * maxChunksPerPacket;
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java
        unbufIn = saslStreams.in;
        
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            DFSUtil.getSmallBufferSize(conf)));
        in = new DataInputStream(unbufIn);
        blockSender = new BlockSender(b, 0, b.getNumBytes(), 
            false, false, true, DataNode.this, null, cachingStrategy);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataXceiver.java

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
  private final InputStream socketIn;
  private OutputStream socketOut;
  private BlockReceiver blockReceiver = null;
  private final int ioFileBufferSize;
  private final int smallBufferSize;

    this.datanode = datanode;
    this.dataXceiverServer = dataXceiverServer;
    this.connectToDnViaHostname = datanode.getDnConf().connectToDnViaHostname;
    this.ioFileBufferSize = DFSUtil.getIoFileBufferSize(datanode.getConf());
    this.smallBufferSize = DFSUtil.getSmallBufferSize(datanode.getConf());
    remoteAddress = peer.getRemoteAddressString();
    final int colonIdx = remoteAddress.indexOf(':');
    remoteAddressWithoutPort =
          socketIn, datanode.getXferAddress().getPort(),
          datanode.getDatanodeId());
        input = new BufferedInputStream(saslStreams.in,
            smallBufferSize);
        socketOut = saslStreams.out;
      } catch (InvalidMagicNumberException imne) {
        if (imne.isHandshake4Encryption()) {
    long read = 0;
    OutputStream baseStream = getOutputStream();
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
        baseStream, smallBufferSize));
    checkAccess(out, true, block, blockToken,
        Op.READ_BLOCK, BlockTokenIdentifier.AccessMode.READ);
  
    final DataOutputStream replyOut = new DataOutputStream(
        new BufferedOutputStream(
            getOutputStream(),
            smallBufferSize));
    checkAccess(replyOut, isClient, block, blockToken,
        Op.WRITE_BLOCK, BlockTokenIdentifier.AccessMode.WRITE);

          unbufMirrorOut = saslStreams.out;
          unbufMirrorIn = saslStreams.in;
          mirrorOut = new DataOutputStream(new BufferedOutputStream(unbufMirrorOut,
              smallBufferSize));
          mirrorIn = new DataInputStream(unbufMirrorIn);

        .getMetaDataInputStream(block);
    
    final DataInputStream checksumIn = new DataInputStream(
        new BufferedInputStream(metadataIn, ioFileBufferSize));
    updateCurrentThreadName("Getting checksum for block " + block);
    try {
      OutputStream baseStream = getOutputStream();
      reply = new DataOutputStream(new BufferedOutputStream(
          baseStream, smallBufferSize));

      writeSuccessWithChecksumInfo(blockSender, reply);
        unbufProxyIn = saslStreams.in;
        
        proxyOut = new DataOutputStream(new BufferedOutputStream(unbufProxyOut,
            smallBufferSize));
        proxyReply = new DataInputStream(new BufferedInputStream(unbufProxyIn,
            ioFileBufferSize));
        
        IoeDuringCopyBlockOperation = true;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/BlockPoolSlice.java
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
  private final File lazypersistDir;
  private final File rbwDir; // directory store RBW replica
  private final File tmpDir; // directory store Temporary replica
  private final int ioFileBufferSize;
  private static final String DU_CACHE_FILE = "dfsUsed";
  private volatile boolean dfsUsedSaved = false;
  private static final int SHUTDOWN_HOOK_PRIORITY = 30;
      }
    }

    this.ioFileBufferSize = DFSUtil.getIoFileBufferSize(conf);

    this.deleteDuplicateReplicas = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_DUPLICATE_REPLICA_DELETION,
        DFSConfigKeys.DFS_DATANODE_DUPLICATE_REPLICA_DELETION_DEFAULT);
      }
      checksumIn = new DataInputStream(
          new BufferedInputStream(new FileInputStream(metaFile),
              ioFileBufferSize));

      final DataChecksum checksum = BlockMetadataHeader.readDataChecksum(

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetImpl.java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;

  private static final int MAX_BLOCK_EVICTIONS_PER_ITERATION = 3;

  private final int smallBufferSize;

  private final Object statsLock = new Object();
    this.datanode = datanode;
    this.dataStorage = storage;
    this.conf = conf;
    this.smallBufferSize = DFSUtil.getSmallBufferSize(conf);
    final int volFailuresTolerated =
  static File[] copyBlockFiles(long blockId, long genStamp, File srcMeta,
      File srcFile, File destRoot, boolean calculateChecksum,
      int smallBufferSize) throws IOException {
    final File destDir = DatanodeUtil.idToBlockDir(destRoot, blockId);
    final File dstFile = new File(destDir, srcFile.getName());
    final File dstMeta = FsDatasetUtil.getMetaFile(dstFile, genStamp);
    return copyBlockFiles(srcMeta, srcFile, dstMeta, dstFile, calculateChecksum,
        smallBufferSize);
  }

  static File[] copyBlockFiles(File srcMeta, File srcFile, File dstMeta,
                               File dstFile, boolean calculateChecksum,
                               int smallBufferSize)
      throws IOException {
    if (calculateChecksum) {
      computeChecksum(srcMeta, dstMeta, srcFile, smallBufferSize);
    } else {
      try {
        Storage.nativeCopyFileUnbuffered(srcMeta, dstMeta, true);
      File[] blockFiles = copyBlockFiles(block.getBlockId(),
          block.getGenerationStamp(), oldMetaFile, oldBlockFile,
          targetVolume.getTmpDir(block.getBlockPoolId()),
          replicaInfo.isOnTransientStorage(), smallBufferSize);

      ReplicaInfo newReplicaInfo = new ReplicaInPipeline(
          replicaInfo.getBlockId(), replicaInfo.getGenerationStamp(),
  private static void computeChecksum(File srcMeta, File dstMeta,
      File blockFile, int smallBufferSize)
      throws IOException {
    final DataChecksum checksum = BlockMetadataHeader.readDataChecksum(srcMeta);
    final byte[] data = new byte[1 << 16];
        }
      }
      metaOut = new DataOutputStream(new BufferedOutputStream(
          new FileOutputStream(dstMeta), smallBufferSize));
      BlockMetadataHeader.writeHeader(metaOut, checksum);

      int offset = 0;
    final File destDir = DatanodeUtil.idToBlockDir(tmpDir, newBlkId);
    final File dstBlockFile = new File(destDir, blockFileName);
    final File dstMetaFile = FsDatasetUtil.getMetaFile(dstBlockFile, newGS);
    return copyBlockFiles(replicaInfo.getMetaFile(),
        replicaInfo.getBlockFile(),
        dstMetaFile, dstBlockFile, true, smallBufferSize);
  }

  @Override // FsDatasetSpi

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/RamDiskAsyncLazyPersistService.java

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;

  private final ThreadGroup threadGroup;
  private Map<File, ThreadPoolExecutor> executors
      = new HashMap<File, ThreadPoolExecutor>();
  private final static HdfsConfiguration EMPTY_HDFS_CONF = new HdfsConfiguration();

    public void run() {
      boolean succeeded = false;
      final FsDatasetImpl dataset = (FsDatasetImpl)datanode.getFSDataset();
      try (FsVolumeReference ref = this.targetVolume) {
        int smallBufferSize = DFSUtil.getSmallBufferSize(EMPTY_HDFS_CONF);
        File targetFiles[] = FsDatasetImpl.copyBlockFiles(
            blockId, genStamp, metaFile, blockFile, lazyPersistDir, true,
            smallBufferSize);

        dataset.onCompleteLazyPersist(bpId, blockId,

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/TransferFsImage.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;

  private final static String CONTENT_TYPE = "Content-Type";
  private final static String CONTENT_TRANSFER_ENCODING = "Content-Transfer-Encoding";
  private final static int IO_FILE_BUFFER_SIZE;

  @VisibleForTesting
  static int timeout = 0;
    connectionFactory = URLConnectionFactory
        .newDefaultURLConnectionFactory(conf);
    isSpnegoEnabled = UserGroupInformation.isSecurityEnabled();
    IO_FILE_BUFFER_SIZE = DFSUtil.getIoFileBufferSize(conf);
  }

  private static final Log LOG = LogFactory.getLog(TransferFsImage.class);
  private static void copyFileToStream(OutputStream out, File localfile,
      FileInputStream infile, DataTransferThrottler throttler,
      Canceler canceler) throws IOException {
    byte buf[] = new byte[IO_FILE_BUFFER_SIZE];
    try {
      CheckpointFaultInjector.getInstance()
          .aboutToSendFile(localfile);
            shouldSendShortFile(localfile)) {
          long len = localfile.length();
          buf = new byte[(int)Math.min(len/2, IO_FILE_BUFFER_SIZE)];
          infile.read(buf);
      }
      
      int num = 1;
      byte[] buf = new byte[IO_FILE_BUFFER_SIZE];
      while (num > 0) {
        num = stream.read(buf);
        if (num > 0) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/DFSTestUtil.java
    final long writeTimeout = dfsClient.getDatanodeWriteTimeout(datanodes.length);
    final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
        NetUtils.getOutputStream(s, writeTimeout),
        DFSUtil.getSmallBufferSize(dfsClient.getConfiguration())));
    final DataInputStream in = new DataInputStream(NetUtils.getInputStream(s));


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestFileStatus.java
      int fileSize, int blockSize) throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
        DFSUtil.getIoFileBufferSize(conf), (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);

