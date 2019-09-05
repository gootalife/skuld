hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/security/token/block/BlockPoolTokenSecretManager.java
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier.AccessMode;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;


  public void checkAccess(BlockTokenIdentifier id, String userId,
      ExtendedBlock block, AccessMode mode) throws InvalidToken {

  public void checkAccess(Token<BlockTokenIdentifier> token,
      String userId, ExtendedBlock block, AccessMode mode) throws InvalidToken {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/security/token/block/BlockTokenIdentifier.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/security/token/block/BlockTokenSecretManager.java
  
  private final SecureRandom nonceGenerator = new SecureRandom();

  ;
  

  public Token<BlockTokenIdentifier> generateToken(ExtendedBlock block,
      EnumSet<BlockTokenIdentifier.AccessMode> modes) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String userID = (ugi == null ? null : ugi.getShortUserName());
    return generateToken(userID, block, modes);

  public Token<BlockTokenIdentifier> generateToken(String userId,
      ExtendedBlock block, EnumSet<BlockTokenIdentifier.AccessMode> modes) throws IOException {
    BlockTokenIdentifier id = new BlockTokenIdentifier(userId, block
        .getBlockPoolId(), block.getBlockId(), modes);
    return new Token<BlockTokenIdentifier>(id, this);
  public void checkAccess(BlockTokenIdentifier id, String userId,
      ExtendedBlock block, BlockTokenIdentifier.AccessMode mode) throws InvalidToken {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking access for user=" + userId + ", block=" + block
          + ", access mode=" + mode + " using " + id.toString());

  public void checkAccess(Token<BlockTokenIdentifier> token, String userId,
      ExtendedBlock block, BlockTokenIdentifier.AccessMode mode) throws InvalidToken {
    BlockTokenIdentifier id = new BlockTokenIdentifier();
    try {
      id.readFields(new DataInputStream(new ByteArrayInputStream(token

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/balancer/KeyManager.java
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
            "Cannot get access token since BlockKeyUpdater is not running");
      }
      return blockTokenSecretManager.generateToken(null, eb,
          EnumSet.of(BlockTokenIdentifier.AccessMode.REPLACE, BlockTokenIdentifier.AccessMode.COPY));
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier.AccessMode;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplicasMap.Reason;

    final long fileLength = bc.computeContentSummary(getStoragePolicySuite()).getLength();
    final long pos = fileLength - ucBlock.getNumBytes();
    return createLocatedBlock(ucBlock, pos, BlockTokenIdentifier.AccessMode.WRITE);
  }

  }
  
  private LocatedBlock createLocatedBlock(final BlockInfoContiguous blk, final long pos,
    final AccessMode mode) throws IOException {
    final LocatedBlock lb = createLocatedBlock(blk, pos);
    if (mode != null) {
      setBlockToken(lb, mode);
      if (LOG.isDebugEnabled()) {
        LOG.debug("blocks = " + java.util.Arrays.asList(blocks));
      }
      final AccessMode mode = needBlockToken? BlockTokenIdentifier.AccessMode.READ: null;
      final List<LocatedBlock> locatedblocks = createLocatedBlockList(
          blocks, offset, length, Integer.MAX_VALUE, mode);


  public void setBlockToken(final LocatedBlock b,
      final AccessMode mode) throws IOException {
    if (isBlockTokenEnabled()) {
      b.setBlockToken(blockTokenSecretManager.generateToken(

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java
import org.apache.hadoop.hdfs.security.token.block.BlockPoolTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier.AccessMode;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.FileChecksumServlets;
import org.apache.hadoop.hdfs.server.namenode.StreamFile;
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block,
      Token<BlockTokenIdentifier> token) throws IOException {
    checkBlockLocalPathAccess();
    checkBlockToken(block, token, BlockTokenIdentifier.AccessMode.READ);
    Preconditions.checkNotNull(data, "Storage not yet initialized");
    BlockLocalPathInfo info = data.getBlockLocalPathInfo(block);
    if (LOG.isDebugEnabled()) {
      throw new ShortCircuitFdsUnsupportedException(
          fileDescriptorPassingDisabledReason);
    }
    checkBlockToken(blk, token, BlockTokenIdentifier.AccessMode.READ);
    int blkVersion = CURRENT_BLOCK_FORMAT_VERSION;
    if (maxVersion < blkVersion) {
      throw new ShortCircuitFdsVersionException("Your client is too old " +
    for (int i = 0; i < blockIds.length; i++) {
      checkBlockToken(new ExtendedBlock(bpId, blockIds[i]),
          tokens.get(i), BlockTokenIdentifier.AccessMode.READ);
    }

    DataNodeFaultInjector.get().getHdfsBlocksMetadata();
        Token<BlockTokenIdentifier> accessToken = BlockTokenSecretManager.DUMMY_TOKEN;
        if (isBlockTokenEnabled) {
          accessToken = blockPoolTokenSecretManager.generateToken(b, 
              EnumSet.of(BlockTokenIdentifier.AccessMode.WRITE));
        }

        long writeTimeout = dnConf.socketWriteTimeout + 
          LOG.debug("Got: " + id.toString());
        }
        blockPoolTokenSecretManager.checkAccess(id, null, block,
            BlockTokenIdentifier.AccessMode.READ);
      }
    }
  }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataXceiver.java
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode.ShortCircuitFdsUnsupportedException;
import org.apache.hadoop.hdfs.server.datanode.DataNode.ShortCircuitFdsVersionException;
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
        baseStream, HdfsConstants.SMALL_BUFFER_SIZE));
    checkAccess(out, true, block, blockToken,
        Op.READ_BLOCK, BlockTokenIdentifier.AccessMode.READ);
  
    BlockSender blockSender = null;
            getOutputStream(),
            HdfsConstants.SMALL_BUFFER_SIZE));
    checkAccess(replyOut, isClient, block, blockToken,
        Op.WRITE_BLOCK, BlockTokenIdentifier.AccessMode.WRITE);

    DataOutputStream mirrorOut = null;  // stream to next target
    DataInputStream mirrorIn = null;    // reply from next target
      final DatanodeInfo[] targets,
      final StorageType[] targetStorageTypes) throws IOException {
    checkAccess(socketOut, true, blk, blockToken,
        Op.TRANSFER_BLOCK, BlockTokenIdentifier.AccessMode.COPY);
    previousOpClientName = clientName;
    updateCurrentThreadName(Op.TRANSFER_BLOCK + " " + blk);

    final DataOutputStream out = new DataOutputStream(
        getOutputStream());
    checkAccess(out, true, block, blockToken,
        Op.BLOCK_CHECKSUM, BlockTokenIdentifier.AccessMode.READ);
    long requestLength = block.getNumBytes();
    Preconditions.checkArgument(requestLength >= 0);
    if (datanode.isBlockTokenEnabled) {
      try {
        datanode.blockPoolTokenSecretManager.checkAccess(blockToken, null, block,
            BlockTokenIdentifier.AccessMode.COPY);
      } catch (InvalidToken e) {
        LOG.warn("Invalid access token in request from " + remoteAddress
            + " for OP_COPY_BLOCK for block " + block + " : "
    if (datanode.isBlockTokenEnabled) {
      try {
        datanode.blockPoolTokenSecretManager.checkAccess(blockToken, null, block,
            BlockTokenIdentifier.AccessMode.REPLACE);
      } catch (InvalidToken e) {
        LOG.warn("Invalid access token in request from " + remoteAddress
            + " for OP_REPLACE_BLOCK for block " + block + " : "
      final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> t,
      final Op op,
      final BlockTokenIdentifier.AccessMode mode) throws IOException {
    if (datanode.isBlockTokenEnabled) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Checking block access token for block '" + blk.getBlockId()
          if (reply) {
            BlockOpResponseProto.Builder resp = BlockOpResponseProto.newBuilder()
              .setStatus(ERROR_ACCESS_TOKEN);
            if (mode == BlockTokenIdentifier.AccessMode.WRITE) {
              DatanodeRegistration dnR = 
                datanode.getDNRegistrationForBP(blk.getBlockPoolId());

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager.SecretManagerState;
    LocatedBlock lBlk = new LocatedBlock(
        getExtendedBlock(blk), locs, offset, false);
    getBlockManager().setBlockToken(
        lBlk, BlockTokenIdentifier.AccessMode.WRITE);
    return lBlk;
  }

        src, numAdditionalNodes, clientnode, chosen, 
        excludes, preferredblocksize, storagePolicyID);
    final LocatedBlock lb = new LocatedBlock(blk, targets, -1, false);
    blockManager.setBlockToken(lb, BlockTokenIdentifier.AccessMode.COPY);
    return lb;
  }

      block.setGenerationStamp(nextGenerationStamp(blockIdManager.isLegacyBlock(block.getLocalBlock())));
      locatedBlock = new LocatedBlock(block, new DatanodeInfo[0]);
      blockManager.setBlockToken(locatedBlock, BlockTokenIdentifier.AccessMode.WRITE);
    } finally {
      writeUnlock();
    }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/security/token/block/TestBlockToken.java
        LOG.info("Got: " + id.toString());
        assertTrue("Received BlockTokenIdentifier is wrong", ident.equals(id));
        sm.checkAccess(id, null, PBHelper.convert(req.getBlock()),
            BlockTokenIdentifier.AccessMode.WRITE);
        result = id.getBlockId();
      }
      return GetReplicaVisibleLengthResponseProto.newBuilder()

  private BlockTokenIdentifier generateTokenId(BlockTokenSecretManager sm,
      ExtendedBlock block,
      EnumSet<BlockTokenIdentifier.AccessMode> accessModes)
      throws IOException {
    Token<BlockTokenIdentifier> token = sm.generateToken(block, accessModes);
    BlockTokenIdentifier id = sm.createIdentifier();
    BlockTokenSecretManager sm = new BlockTokenSecretManager(
        blockKeyUpdateInterval, blockTokenLifetime, 0, "fake-pool", null);
    TestWritable.testWritable(generateTokenId(sm, block1,
        EnumSet.allOf(BlockTokenIdentifier.AccessMode.class)));
    TestWritable.testWritable(generateTokenId(sm, block2,
        EnumSet.of(BlockTokenIdentifier.AccessMode.WRITE)));
    TestWritable.testWritable(generateTokenId(sm, block3,
        EnumSet.noneOf(BlockTokenIdentifier.AccessMode.class)));
  }

  private void tokenGenerationAndVerification(BlockTokenSecretManager master,
      BlockTokenSecretManager slave) throws Exception {
    for (BlockTokenIdentifier.AccessMode mode : BlockTokenIdentifier.AccessMode
        .values()) {
      Token<BlockTokenIdentifier> token1 = master.generateToken(block1,
    }
    Token<BlockTokenIdentifier> mtoken = master.generateToken(block3,
        EnumSet.allOf(BlockTokenIdentifier.AccessMode.class));
    for (BlockTokenIdentifier.AccessMode mode : BlockTokenIdentifier.AccessMode
        .values()) {
      master.checkAccess(mtoken, null, block3, mode);
      slave.checkAccess(mtoken, null, block3, mode);
    BlockTokenSecretManager sm = new BlockTokenSecretManager(
        blockKeyUpdateInterval, blockTokenLifetime, 0, "fake-pool", null);
    Token<BlockTokenIdentifier> token = sm.generateToken(block3,
        EnumSet.allOf(BlockTokenIdentifier.AccessMode.class));

    final Server server = createMockDatanode(sm, token, conf);

    BlockTokenSecretManager sm = new BlockTokenSecretManager(
        blockKeyUpdateInterval, blockTokenLifetime, 0, "fake-pool", null);
    Token<BlockTokenIdentifier> token = sm.generateToken(block3,
        EnumSet.allOf(BlockTokenIdentifier.AccessMode.class));

    final Server server = createMockDatanode(sm, token, conf);
    server.start();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockTokenWithDFS.java
      tryRead(conf, lblock, false);
      lblock.setBlockToken(sm.generateToken(lblock.getBlock(),
              EnumSet.of(BlockTokenIdentifier.AccessMode.READ)));
      tryRead(conf, lblock, true);
      ExtendedBlock wrongBlock = new ExtendedBlock(lblock.getBlock()
          .getBlockPoolId(), lblock.getBlock().getBlockId() + 1);
      lblock.setBlockToken(sm.generateToken(wrongBlock,
          EnumSet.of(BlockTokenIdentifier.AccessMode.READ)));
      tryRead(conf, lblock, false);
      lblock.setBlockToken(sm.generateToken(lblock.getBlock(),
          EnumSet.of(BlockTokenIdentifier.AccessMode.WRITE,
                     BlockTokenIdentifier.AccessMode.COPY,
                     BlockTokenIdentifier.AccessMode.REPLACE)));
      tryRead(conf, lblock, false);


