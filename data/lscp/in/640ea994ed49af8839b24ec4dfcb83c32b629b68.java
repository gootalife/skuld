hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfo.java
@InterfaceAudience.Private
public abstract class  BlockInfo extends Block
    implements LightWeightGSet.LinkedElement {
  public static final BlockInfo[] EMPTY_ARRAY = {};

  protected Object[] triplets;

    return (DatanodeStorageInfo)triplets[index*3];
  }

  BlockInfo getPrevious(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+1 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo)triplets[index*3+1];
    return info;
  }

  void setStorageInfo(int index, DatanodeStorageInfo storage) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3 < triplets.length : "Index is out of bound";
    triplets[index*3] = storage;
  BlockInfo setPrevious(int index, BlockInfo to) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+1 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo)triplets[index*3+1];
  BlockInfo setNext(int index, BlockInfo to) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+2 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo)triplets[index*3+2];
  }

  public abstract int numNodes();

  abstract boolean addStorage(DatanodeStorageInfo storage);

  abstract boolean removeStorage(DatanodeStorageInfo storage);


  abstract void replaceBlock(BlockInfo newBlock);


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoContiguous.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoContiguous.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;

@InterfaceAudience.Private
public class BlockInfoContiguous extends BlockInfo {
  public static final BlockInfoContiguous[] EMPTY_ARRAY = {};

  public BlockInfoContiguous(short size) {
    super(size);
  }

  public BlockInfoContiguous(Block blk, short size) {
    super(blk, size);
  }

  protected BlockInfoContiguous(BlockInfoContiguous from) {
    super(from);
  }

  private int ensureCapacity(int num) {
    assert this.triplets != null : "BlockInfo is not initialized";
    int last = numNodes();
    if (triplets.length >= (last+num)*3) {
      return last;
    }
    Object[] old = triplets;
    triplets = new Object[(last+num)*3];
    System.arraycopy(old, 0, triplets, 0, last * 3);
    return last;
  }

  @Override
  boolean addStorage(DatanodeStorageInfo storage) {
    int lastNode = ensureCapacity(1);
    setStorageInfo(lastNode, storage);
    setNext(lastNode, null);
    setPrevious(lastNode, null);
    return true;
  }

  @Override
  boolean removeStorage(DatanodeStorageInfo storage) {
    int dnIndex = findStorageInfo(storage);
    if (dnIndex < 0) { // the node is not found
      return false;
    }
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null :
        "Block is still in the list and must be removed first.";
    int lastNode = numNodes()-1;
    setStorageInfo(dnIndex, getStorageInfo(lastNode));
    setNext(dnIndex, getNext(lastNode));
    setPrevious(dnIndex, getPrevious(lastNode));
    setStorageInfo(lastNode, null);
    setNext(lastNode, null);
    setPrevious(lastNode, null);
    return true;
  }

  @Override
  public int numNodes() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";

    for (int idx = getCapacity()-1; idx >= 0; idx--) {
      if (getDatanode(idx) != null) {
        return idx + 1;
      }
    }
    return 0;
  }

  @Override
  void replaceBlock(BlockInfo newBlock) {
    assert newBlock instanceof BlockInfoContiguous;
    for (int i = this.numNodes() - 1; i >= 0; i--) {
      final DatanodeStorageInfo storage = this.getStorageInfo(i);
      final boolean removed = storage.removeBlock(this);
      assert removed : "currentBlock not found.";

      final DatanodeStorageInfo.AddBlockResult result = storage.addBlock(
          newBlock);
      assert result == DatanodeStorageInfo.AddBlockResult.ADDED :
          "newBlock already exists.";
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoContiguousUnderConstruction.java
public class BlockInfoContiguousUnderConstruction extends BlockInfoContiguous {
  private BlockUCState blockUCState;

  public BlockInfoContiguousUnderConstruction(Block blk, short replication,
      BlockUCState state, DatanodeStorageInfo[] targets) {
    super(blk, replication);
    assert getBlockUCState() != BlockUCState.COMPLETE :
      "BlockInfoUnderConstruction cannot be in COMPLETE state";
  BlockInfo convertToCompleteBlock() throws IOException {
    assert getBlockUCState() != BlockUCState.COMPLETE :
      "Trying to convert a COMPLETE block";
    return new BlockInfoContiguous(this);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java

    BlockToMarkCorrupt(BlockInfo stored, long gs, String reason,
        Reason reasonCode) {
      this(new BlockInfoContiguous((BlockInfoContiguous)stored), stored,
          reason, reasonCode);
      corrupted.setGenerationStamp(gs);
    }

    BlockInfo delimiter = new BlockInfoContiguous(new Block(), (short) 1);
    AddBlockResult result = storageInfo.addBlock(delimiter);
    assert result == AddBlockResult.ADDED 
        : "Delimiting block cannot be present in the node";

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlocksMap.java
import java.util.Iterator;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

    BlockInfo currentBlock = blocks.get(newBlock);
    assert currentBlock != null : "the block if not in blocksMap";
    currentBlock.replaceBlock(newBlock);
    blocks.put(newBlock);
    return newBlock;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogLoader.java
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
          newBI = new BlockInfoContiguous(newBlock,
              file.getPreferredBlockReplication());
        }
        fsNamesys.getBlockManager().addBlockCollection(newBI, file);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageFormat.java
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
      BlockInfo[] blocks = new BlockInfo[numBlocks];
      for (int j = 0; j < numBlocks; j++) {
        blocks[j] = new BlockInfoContiguous(replication);
        blocks[j].readFields(in);
      }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageFormatPBINode.java
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.LoaderContext;

      BlockInfo[] blocks = new BlockInfo[bp.size()];
      for (int i = 0, e = bp.size(); i < e; ++i) {
        blocks[i] =
            new BlockInfoContiguous(PBHelper.convert(bp.get(i)), replication);
      }
      final PermissionStatus permissions = loadPermission(f.getPermission(),
          parent.getLoaderContext().getStringTable());

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageSerialization.java
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat;
    int i = 0;
    for (; i < numBlocks-1; i++) {
      blk.readFields(in);
      blocks[i] = new BlockInfoContiguous(blk, blockReplication);
    }
    if(numBlocks > 0) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FSImageFormatPBSnapshot.java
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.namenode.AclEntryStatusFormat;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
          BlockInfo storedBlock =  fsn.getBlockManager().getStoredBlock(blk);
          if(storedBlock == null) {
            storedBlock = fsn.getBlockManager().addBlockCollection(
                new BlockInfoContiguous(blk, copy.getFileReplication()), file);
          }
          blocks[j] = storedBlock;
        }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockInfo.java

  @Test
  public void testIsDeleted() {
    BlockInfo blockInfo = new BlockInfoContiguous((short) 3);
    BlockCollection bc = Mockito.mock(BlockCollection.class);
    blockInfo.setBlockCollection(bc);
    Assert.assertFalse(blockInfo.isDeleted());

  @Test
  public void testAddStorage() throws Exception {
    BlockInfo blockInfo = new BlockInfoContiguous((short) 3);

    final DatanodeStorageInfo storage = DFSTestUtil.createDatanodeStorageInfo("storageID", "127.0.0.1");


  @Test
  public void testCopyConstructor() {
    BlockInfo old = new BlockInfoContiguous((short) 3);
    try {
      BlockInfo copy = new BlockInfoContiguous((BlockInfoContiguous)old);
      assertEquals(old.getBlockCollection(), copy.getBlockCollection());
      assertEquals(old.getCapacity(), copy.getCapacity());
    } catch (Exception e) {

    for (int i = 0; i < NUM_BLOCKS; ++i) {
      blockInfos[i] = new BlockInfoContiguous((short) 3);
      storage1.addBlock(blockInfos[i]);
    }

    LOG.info("Building block list...");
    for (int i = 0; i < MAX_BLOCKS; i++) {
      blockList.add(new Block(i, 0, GenerationStamp.LAST_RESERVED_STAMP));
      blockInfoList.add(new BlockInfoContiguous(blockList.get(i), (short) 3));
      dd.addBlock(blockInfoList.get(i));


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockManager.java

  private BlockInfo blockOnNodes(long blkId, List<DatanodeDescriptor> nodes) {
    Block block = new Block(blkId);
    BlockInfo blockInfo = new BlockInfoContiguous(block, (short) 3);

    for (DatanodeDescriptor dn : nodes) {
      for (DatanodeStorageInfo storage : dn.getStorageInfos()) {
  private BlockInfo addBlockToBM(long blkId) {
    Block block = new Block(blkId);
    BlockInfo blockInfo =
        new BlockInfoContiguous(block, (short) 3);
    BlockCollection bc = Mockito.mock(BlockCollection.class);
    Mockito.doReturn((short) 3).when(bc).getPreferredBlockReplication();
    bm.blocksMap.addBlockCollection(blockInfo, bc);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestDatanodeDescriptor.java
  public void testBlocksCounter() throws Exception {
    DatanodeDescriptor dd = BlockManagerTestUtil.getLocalDatanodeDescriptor(true);
    assertEquals(0, dd.numBlocks());
    BlockInfo blk = new BlockInfoContiguous(new Block(1L), (short) 1);
    BlockInfo blk1 = new BlockInfoContiguous(new Block(2L), (short) 2);
    DatanodeStorageInfo[] storages = dd.getStorageInfos();
    assertTrue(storages.length > 0);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestPendingReplication.java

      block = new Block(1, 1, 0);
      blockInfo = new BlockInfoContiguous(block, (short) 3);

      pendingReplications.increment(block,
          DatanodeStorageInfo.toDatanodeDescriptors(

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/CreateEditsLog.java
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.Storage;

    BlockInfo[] blocks = new BlockInfo[blocksPerFile];
    for (int iB = 0; iB < blocksPerFile; ++iB) {
      blocks[iB] = 
       new BlockInfoContiguous(new Block(0, blockSize, BLOCK_GENERATION_STAMP),
           replication);
    }
    

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestCommitBlockSynchronization.java
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
        lastBlock, genStamp, length, false, false, newTargets, null);

    BlockInfo completedBlockInfo = new BlockInfoContiguous(block, (short) 1);
    completedBlockInfo.setBlockCollection(file);
    completedBlockInfo.setGenerationStamp(genStamp);
    doReturn(completedBlockInfo).when(namesystemSpy)
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, true, false, newTargets, null);

    BlockInfo completedBlockInfo = new BlockInfoContiguous(block, (short) 1);
    completedBlockInfo.setBlockCollection(file);
    completedBlockInfo.setGenerationStamp(genStamp);
    doReturn(completedBlockInfo).when(namesystemSpy)

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestINodeFile.java
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.IOUtils;
      iNodes[i] = new INodeFile(i, null, perm, 0L, 0L, null, replication,
          preferredBlockSize, (byte)0);
      iNodes[i].setLocalName(DFSUtil.string2Bytes(fileNamePrefix + i));
      BlockInfo newblock = new BlockInfoContiguous(replication);
      iNodes[i].addBlock(newblock);
    }
    

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestTruncateQuotaUpdate.java
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshotFeature;

  private BlockInfo newBlock(long size, short replication) {
    Block b = new Block(++nextMockBlockId, size, ++nextMockGenstamp);
    return new BlockInfoContiguous(b, replication);
  }

  private static void addSnapshotFeature(INodeFile file, BlockInfo[] blocks) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestFileWithSnapshotFeature.java
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
    BlockStoragePolicySuite bsps = mock(BlockStoragePolicySuite.class);
    BlockStoragePolicy bsp = mock(BlockStoragePolicy.class);
    BlockInfo[] blocks = new BlockInfo[] {
        new BlockInfoContiguous(new Block(1, BLOCK_SIZE, 1), REPL_1)
    };


