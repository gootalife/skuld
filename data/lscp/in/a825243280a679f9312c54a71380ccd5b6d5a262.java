hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/HdfsConstantsClient.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocol/Block.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/ClientNamenodeProtocolServerSideTranslatorPB.java
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.SetXAttrResponseProto;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
      boolean result = 
          server.complete(req.getSrc(), req.getClientName(),
          req.hasLast() ? PBHelper.convert(req.getLast()) : null,
          req.hasFileId() ? req.getFileId() : HdfsConstantsClient.GRANDFATHER_INODE_ID);
      return CompleteResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/PBHelper.java
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SafeModeActionProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitShmIdProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitShmSlotProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BalancerBandwidthCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockIdCommandProto;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.protocol.BalancerBandwidthCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockIdCommand;
        fs.getFileType().equals(FileType.IS_SYMLINK) ? 
            fs.getSymlink().toByteArray() : null,
        fs.getPath().toByteArray(),
        fs.hasFileId()? fs.getFileId(): HdfsConstantsClient.GRANDFATHER_INODE_ID,
        fs.hasLocations() ? PBHelper.convert(fs.getLocations()) : null,
        fs.hasChildrenNum() ? fs.getChildrenNum() : -1,
        fs.hasFileEncryptionInfo() ? convert(fs.getFileEncryptionInfo()) : null,

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockIdManager.java
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;

import java.io.IOException;
  private final SequentialBlockIdGenerator blockIdGenerator;

  public BlockIdManager(BlockManager blockManager) {
    this.generationStampV1Limit = HdfsConstantsClient.GRANDFATHER_GENERATION_STAMP;
    this.blockIdGenerator = new SequentialBlockIdGenerator(blockManager);
  }

  public void setGenerationStampV1Limit(long stamp) {
    Preconditions.checkState(generationStampV1Limit == HdfsConstantsClient
      .GRANDFATHER_GENERATION_STAMP);
    generationStampV1Limit = stamp;
  }
    generationStampV2.setCurrentValue(GenerationStamp.LAST_RESERVED_STAMP);
    getBlockIdGenerator().setCurrentValue(SequentialBlockIdGenerator
      .LAST_RESERVED_BLOCK_ID);
    generationStampV1Limit = HdfsConstantsClient.GRANDFATHER_GENERATION_STAMP;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/common/GenerationStamp.java
  public static final long LAST_RESERVED_STAMP = 1000L;


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DirectoryScanner.java
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.util.Daemon;
    public long getGenStamp() {
      return metaSuffix != null ? Block.getGenerationStamp(
          getMetaFile().getName()) : 
            HdfsConstantsClient.GRANDFATHER_GENERATION_STAMP;
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetImpl.java
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;

      final long diskGS = diskMetaFile != null && diskMetaFile.exists() ?
          Block.getGenerationStamp(diskMetaFile.getName()) :
            HdfsConstantsClient.GRANDFATHER_GENERATION_STAMP;

      if (diskFile == null || !diskFile.exists()) {
        if (memBlockInfo == null) {
          long gs = diskMetaFile != null && diskMetaFile.exists()
              && diskMetaFile.getParent().equals(memFile.getParent()) ? diskGS
              : HdfsConstantsClient.GRANDFATHER_GENERATION_STAMP;

          LOG.warn("Updating generation stamp for block " + blockId
              + " from " + memBlockInfo.getGenerationStamp() + " to " + gs);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetUtil.java

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;

      return Block.getGenerationStamp(listdir[j].getName());
    }
    FsDatasetImpl.LOG.warn("Block " + blockFile + " does not have a metafile!");
    return HdfsConstantsClient.GRANDFATHER_GENERATION_STAMP;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogLoader.java
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.protocol.Block;
      long lastInodeId) throws IOException {
    long inodeId = inodeIdFromOp;

    if (inodeId == HdfsConstantsClient.GRANDFATHER_INODE_ID) {
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.ADD_INODE_ID, logVersion)) {
        throw new IOException("The layout version " + logVersion
  @SuppressWarnings("deprecation")
  private long applyEditLogOp(FSEditLogOp op, FSDirectory fsDir,
      StartupOption startOpt, int logVersion, long lastInodeId) throws IOException {
    long inodeId = HdfsConstantsClient.GRANDFATHER_INODE_ID;
    if (LOG.isTraceEnabled()) {
      LOG.trace("replaying edit log: " + op);
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogOp.java
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclEditLogProto;
        this.inodeId = in.readLong();
      } else {
        this.inodeId = HdfsConstantsClient.GRANDFATHER_INODE_ID;
      }
      if ((-17 < logVersion && length != 4) ||
          (logVersion <= -17 && length != 5 && !NameNodeLayoutVersion.supports(
        this.inodeId = FSImageSerialization.readLong(in);
      } else {
        this.inodeId = HdfsConstantsClient.GRANDFATHER_INODE_ID;
      }
      this.path = FSImageSerialization.readString(in);
      if (NameNodeLayoutVersion.supports(
        this.inodeId = FSImageSerialization.readLong(in);
      } else {
        this.inodeId = HdfsConstantsClient.GRANDFATHER_INODE_ID;
      }
      this.path = FSImageSerialization.readString(in);
      this.value = FSImageSerialization.readString(in);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
    Block previousBlock = ExtendedBlock.getLocalBlock(previous);
    final INode inode;
    final INodesInPath iip;
    if (fileId == HdfsConstantsClient.GRANDFATHER_INODE_ID) {

      final INode inode;
      if (fileId == HdfsConstantsClient.GRANDFATHER_INODE_ID) {

      final INode inode;
      final INodesInPath iip;
      if (fileId == HdfsConstantsClient.GRANDFATHER_INODE_ID) {
    final INodesInPath iip;
    INode inode = null;
    try {
      if (fileId == HdfsConstantsClient.GRANDFATHER_INODE_ID) {
      checkNameNodeSafeMode("Cannot fsync file " + src);
      src = dir.resolvePath(pc, src, pathComponents);
      final INode inode;
      if (fileId == HdfsConstantsClient.GRANDFATHER_INODE_ID) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeId.java
import java.io.FileNotFoundException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.util.SequentialNumber;

  public static final long LAST_RESERVED_ID = 2 << 14 - 1;
  public static final long ROOT_INODE_ID = LAST_RESERVED_ID + 1;

  public static void checkId(long requestId, INode inode)
      throws FileNotFoundException {
    if (requestId != HdfsConstantsClient.GRANDFATHER_INODE_ID && requestId != inode.getId()) {
      throw new FileNotFoundException(
          "ID mismatch. Request id and saved id: " + requestId + " , "
              + inode.getId() + " for file " + inode.getFullPathName());

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/ImageLoaderCurrent.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.LayoutFlags;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.ImageVisitor.ImageElement;
import org.apache.hadoop.io.Text;
    final String pathName = readINodePath(in, parentName);
    v.visit(ImageElement.INODE_PATH, pathName);

    long inodeId = HdfsConstantsClient.GRANDFATHER_INODE_ID;
    if (supportInodeId) {
      inodeId = in.readLong();
      v.visit(ImageElement.INODE_ID, inodeId);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/JsonUtilClient.java
hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestFileCreation.java
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

      LocatedBlock location = client.getNamenode().addBlock(file1.toString(),
          client.clientName, null, null, HdfsConstantsClient.GRANDFATHER_INODE_ID, null);
      System.out.println("testFileCreationError2: "
          + "Added block " + location.getBlock());

      createFile(dfs, f, 3);
      try {
        cluster.getNameNodeRpc().addBlock(f.toString(), client.clientName,
            null, null, HdfsConstantsClient.GRANDFATHER_INODE_ID, null);
        fail();
      } catch(IOException ioe) {
        FileSystem.LOG.info("GOOD!", ioe);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestGetBlocks.java
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.Test;


    for (int i = 0; i < blkids.length; i++) {
      Block b = new Block(blkids[i], 0,
          HdfsConstantsClient.GRANDFATHER_GENERATION_STAMP);
      Long v = map.get(b);
      System.out.println(b + " => " + v);
      assertEquals(blkids[i], v.longValue());

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDirectoryScanner.java
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
      long blockId = deleteMetaFile();
      scan(totalBlocks, 1, 1, 0, 0, 1);
      verifyGenStamp(blockId, HdfsConstantsClient.GRANDFATHER_GENERATION_STAMP);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      blockId = createBlockFile();
      totalBlocks++;
      scan(totalBlocks, 1, 1, 0, 1, 0);
      verifyAddition(blockId, HdfsConstantsClient.GRANDFATHER_GENERATION_STAMP, 0);
      scan(totalBlocks, 0, 0, 0, 0, 0);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/NNThroughputBenchmark.java
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
      long end = Time.now();
      for(boolean written = !closeUponCreate; !written; 
        written = nameNodeProto.complete(fileNames[daemonId][inputIdx],
                                    clientName, null, HdfsConstantsClient.GRANDFATHER_INODE_ID));
      return end-start;
    }

            new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)), true, replication,
            BLOCK_SIZE, null);
        ExtendedBlock lastBlock = addBlocks(fileName, clientName);
        nameNodeProto.complete(fileName, clientName, lastBlock, HdfsConstantsClient.GRANDFATHER_INODE_ID);
      }
      for(int idx=0; idx < nrDatanodes; idx++) {
      ExtendedBlock prevBlock = null;
      for(int jdx = 0; jdx < blocksPerFile; jdx++) {
        LocatedBlock loc = nameNodeProto.addBlock(fileName, clientName,
            prevBlock, null, HdfsConstantsClient.GRANDFATHER_INODE_ID, null);
        prevBlock = loc.getBlock();
        for(DatanodeInfo dnInfo : loc.getLocations()) {
          int dnIdx = Arrays.binarySearch(datanodes, dnInfo.getXferAddr());

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestAddBlockRetry.java
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
    LOG.info("Starting first addBlock for " + src);
    LocatedBlock[] onRetryBlock = new LocatedBlock[1];
    DatanodeStorageInfo targets[] = ns.getNewBlockTargets(
        src, HdfsConstantsClient.GRANDFATHER_INODE_ID, "clientName",
        null, null, null, onRetryBlock);
    assertNotNull("Targets must be generated", targets);

    LOG.info("Starting second addBlock for " + src);
    nn.addBlock(src, "clientName", null, null,
        HdfsConstantsClient.GRANDFATHER_INODE_ID, null);
    assertTrue("Penultimate block must be complete",
        checkFileProgress(src, false));
    LocatedBlocks lbs = nn.getBlockLocations(src, 0, Long.MAX_VALUE);

    LocatedBlock newBlock = ns.storeAllocatedBlock(
        src, HdfsConstantsClient.GRANDFATHER_INODE_ID, "clientName", null, targets);
    assertEquals("Blocks are not equal", lb2.getBlock(), newBlock.getBlock());

    LOG.info("Starting first addBlock for " + src);
    LocatedBlock lb1 = nameNodeRpc.addBlock(src, "clientName", null, null,
        HdfsConstantsClient.GRANDFATHER_INODE_ID, null);
    assertTrue("Block locations should be present",
        lb1.getLocations().length > 0);

    cluster.restartNameNode();
    nameNodeRpc = cluster.getNameNodeRpc();
    LocatedBlock lb2 = nameNodeRpc.addBlock(src, "clientName", null, null,
        HdfsConstantsClient.GRANDFATHER_INODE_ID, null);
    assertEquals("Blocks are not equal", lb1.getBlock(), lb2.getBlock());
    assertTrue("Wrong locations with retry", lb2.getLocations().length > 0);
  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFSPermissionChecker.java
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
    PermissionStatus permStatus = PermissionStatus.createImmutable(owner, group,
      FsPermission.createImmutable(perm));
    INodeDirectory inodeDirectory = new INodeDirectory(
      HdfsConstantsClient.GRANDFATHER_INODE_ID, name.getBytes("UTF-8"), permStatus, 0L);
    parent.addChild(inodeDirectory);
    return inodeDirectory;
  }
      String owner, String group, short perm) throws IOException {
    PermissionStatus permStatus = PermissionStatus.createImmutable(owner, group,
      FsPermission.createImmutable(perm));
    INodeFile inodeFile = new INodeFile(HdfsConstantsClient.GRANDFATHER_INODE_ID,
      name.getBytes("UTF-8"), permStatus, 0L, 0L, null, REPLICATION,
      PREFERRED_BLOCK_SIZE, (byte)0);
    parent.addChild(inodeFile);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestINodeFile.java
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
  private long preferredBlockSize = 1024;

  INodeFile createINodeFile(short replication, long preferredBlockSize) {
    return new INodeFile(HdfsConstantsClient.GRANDFATHER_INODE_ID, null, perm, 0L, 0L,
        null, replication, preferredBlockSize, (byte)0);
  }

  private static INodeFile createINodeFile(byte storagePolicyID) {
    return new INodeFile(HdfsConstantsClient.GRANDFATHER_INODE_ID, null, perm, 0L, 0L,
        null, (short)3, 1024L, storagePolicyID);
  }

    INodeFile inf = createINodeFile(replication, preferredBlockSize);
    inf.setLocalName(DFSUtil.string2Bytes("f"));

    INodeDirectory root = new INodeDirectory(HdfsConstantsClient.GRANDFATHER_INODE_ID,
        INodeDirectory.ROOT_NAME, perm, 0L);
    INodeDirectory dir = new INodeDirectory(HdfsConstantsClient.GRANDFATHER_INODE_ID,
        DFSUtil.string2Bytes("d"), perm, 0L);

    assertEquals("f", inf.getFullPathName());

    {//cast from INodeFileUnderConstruction
      final INode from = new INodeFile(
          HdfsConstantsClient.GRANDFATHER_INODE_ID, null, perm, 0L, 0L, null, replication,
          1024L, (byte)0);
      from.asFile().toUnderConstruction("client", "machine");
    
    }

    {//cast from INodeDirectory
      final INode from = new INodeDirectory(HdfsConstantsClient.GRANDFATHER_INODE_ID, null,
          perm, 0L);

  @Test
  public void testFileUnderConstruction() {
    replication = 3;
    final INodeFile file = new INodeFile(HdfsConstantsClient.GRANDFATHER_INODE_ID, null,
        perm, 0L, 0L, null, replication, 1024L, (byte)0);
    assertFalse(file.isUnderConstruction());


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestOpenFilesWithSnapshot.java
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
    String clientName = fs.getClient().getClientName();
    nameNodeRpc.addBlock(fileWithEmptyBlock.toString(), clientName, null, null,
        HdfsConstantsClient.GRANDFATHER_INODE_ID, null);
    fs.createSnapshot(path, "s2");

    fs.rename(new Path("/test/test"), new Path("/test/test-renamed"));

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestJsonUtil.java
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.util.Time;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
    final HdfsFileStatus status = new HdfsFileStatus(1001L, false, 3, 1L << 26,
        now, now + 10, new FsPermission((short) 0644), "user", "group",
        DFSUtil.string2Bytes("bar"), DFSUtil.string2Bytes("foo"),
        HdfsConstantsClient.GRANDFATHER_INODE_ID, 0, null, (byte) 0);
    final FileStatus fstatus = toFileStatus(status, parent);
    System.out.println("status  = " + status);
    System.out.println("fstatus = " + fstatus);

