hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirAttrOp.java
    if (replication > oldBR) {
      long dsDelta = file.storagespaceConsumed(null).getStorageSpace() / oldBR;
      fsd.updateCount(iip, 0L, dsDelta, oldBR, replication, true);
    }

    final short newBR = file.getBlockReplication();
    if (newBR < oldBR) {
      long dsDelta = file.storagespaceConsumed(null).getStorageSpace() / newBR;
      fsd.updateCount(iip, 0L, dsDelta, oldBR, newBR, true);
    }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirectory.java
      INodeFile fileNode, Block block) throws IOException {
    BlockInfoContiguousUnderConstruction uc = fileNode.removeLastBlock(block);
    if (uc == null) {
      return false;
    }
    getBlockManager().removeBlockFromMap(block);
      return;
    }
    final BlockStoragePolicy policy = getBlockStoragePolicySuite()
        .getPolicy(file.getStoragePolicyID());
    file.computeQuotaDeltaForTruncate(newLength, policy, delta);
    readLock();
    try {
      verifyQuota(iip, iip.length() - 1, delta, null);
      readUnlock();
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java

      if (deleteblock) {
        Block blockToDel = ExtendedBlock.getLocalBlock(oldBlock);
        boolean remove = iFile.removeLastBlock(blockToDel) != null;
        if (remove) {
          blockManager.removeBlock(storedBlock);
        }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeFile.java
  BlockInfoContiguousUnderConstruction removeLastBlock(Block oldblock) {
    Preconditions.checkState(isUnderConstruction(),
        "file is no longer under construction");
    if (blocks == null || blocks.length == 0) {
      return null;
    }
    int size_1 = blocks.length - 1;
    if (!blocks[size_1].equals(oldblock)) {
      return null;
    }

    BlockInfoContiguousUnderConstruction uc =
        (BlockInfoContiguousUnderConstruction)blocks[size_1];
    BlockInfoContiguous[] newlist = new BlockInfoContiguous[size_1];
    System.arraycopy(blocks, 0, newlist, 0, size_1);
    setBlocks(newlist);
    return uc;
  }

    return header;
  }

  @Override
  public BlockInfoContiguous[] getBlocks() {
      QuotaCounts counts, boolean useCache,
      int lastSnapshotId) {
    long nsDelta = 1;
    counts.addNameSpace(nsDelta);

    BlockStoragePolicy bsp = null;
    if (blockStoragePolicyId != BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      bsp = bsps.getPolicy(blockStoragePolicyId);
    }

    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf == null) {
      counts.add(storagespaceConsumed(bsp));
      return counts;
    }

    FileDiffList fileDiffList = sf.getDiffs();
    int last = fileDiffList.getLastSnapshotId();

    if (lastSnapshotId == Snapshot.CURRENT_STATE_ID
        || last == Snapshot.CURRENT_STATE_ID) {
      counts.add(storagespaceConsumed(bsp));
      return counts;
    }

    final long ssDeltaNoReplication;
    short replication;
    if (last < lastSnapshotId) {
      ssDeltaNoReplication = computeFileSize(true, false);
      replication = getFileReplication();
    } else {
      int sid = fileDiffList.getSnapshotById(lastSnapshotId);
      ssDeltaNoReplication = computeFileSize(sid);
      replication = getFileReplication(sid);
    }

    counts.addStorageSpace(ssDeltaNoReplication * replication);
    if (bsp != null) {
      List<StorageType> storageTypes = bsp.chooseStorageTypes(replication);
      for (StorageType t : storageTypes) {
        if (!t.supportTypeQuota()) {
      }
    }
    counts.addContent(Content.LENGTH, fileLen);
    counts.addContent(Content.DISKSPACE, storagespaceConsumed(null)
        .getStorageSpace());

    if (getStoragePolicyID() != BLOCK_STORAGE_POLICY_ID_UNSPECIFIED){
      BlockStoragePolicy bsp = summary.getBlockStoragePolicySuite().
  public final QuotaCounts storagespaceConsumed(BlockStoragePolicy bsp) {
    QuotaCounts counts = new QuotaCounts.Builder().build();
    final Iterable<BlockInfoContiguous> blocks;
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf == null) {
      blocks = Arrays.asList(getBlocks());
    } else {
      Set<BlockInfoContiguous> allBlocks = new HashSet<>(Arrays.asList(getBlocks()));
      List<FileDiff> diffs = sf.getDiffs().asList();
      for(FileDiff diff : diffs) {
        BlockInfoContiguous[] diffBlocks = diff.getBlocks();
          allBlocks.addAll(Arrays.asList(diffBlocks));
        }
      }
      blocks = allBlocks;
    }

    final short replication = getBlockReplication();
    for (BlockInfoContiguous b : blocks) {
      long blockSize = b.isComplete() ? b.getNumBytes() :
          getPreferredBlockSize();
      counts.addStorageSpace(blockSize * replication);
      if (bsp != null) {
        List<StorageType> types = bsp.chooseStorageTypes(replication);
        for (StorageType t : types) {
          if (t.supportTypeQuota()) {
            counts.addTypeSpace(t, blockSize);
          }
        }
      }
    }
    return counts;
  }

  void computeQuotaDeltaForTruncate(
      long newLength, BlockStoragePolicy bsps,
      QuotaCounts delta) {
    final BlockInfoContiguous[] blocks = getBlocks();
    if (blocks == null || blocks.length == 0) {
      return;
    }

    long size = 0;
    for (BlockInfoContiguous b : blocks) {
      size += b.getNumBytes();
    }

    BlockInfoContiguous[] sblocks = null;
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      FileDiff diff = sf.getDiffs().getLast();
      sblocks = diff != null ? diff.getBlocks() : null;
    }

    for (int i = blocks.length - 1; i >= 0 && size > newLength;
         size -= blocks[i].getNumBytes(), --i) {
      BlockInfoContiguous bi = blocks[i];
      long truncatedBytes;
      if (size - newLength < bi.getNumBytes()) {
        truncatedBytes = bi.getNumBytes() - getPreferredBlockSize();
      } else {
        truncatedBytes = bi.getNumBytes();
      }

      if (sblocks != null && i < sblocks.length && bi.equals(sblocks[i])) {
        truncatedBytes -= bi.getNumBytes();
      }

      delta.addStorageSpace(-truncatedBytes * getBlockReplication());
      if (bsps != null) {
        List<StorageType> types = bsps.chooseStorageTypes(
            getBlockReplication());
        for (StorageType t : types) {
          if (t.supportTypeQuota()) {
            delta.addTypeSpace(t, -truncatedBytes);
          }
        }
      }
    }
  }

  void truncateBlocksTo(int n) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FileWithSnapshotFeature.java

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.INodeFileAttributes;
import org.apache.hadoop.hdfs.server.namenode.QuotaCounts;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;

  public QuotaCounts updateQuotaAndCollectBlocks(BlockStoragePolicySuite bsps, INodeFile file,
      FileDiff removed, BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes) {

    byte storagePolicyID = file.getStoragePolicyID();
    BlockStoragePolicy bsp = null;
    if (storagePolicyID != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      bsp = bsps.getPolicy(file.getStoragePolicyID());
    }


    QuotaCounts oldCounts = file.storagespaceConsumed(null);
    long oldStoragespace;
    if (removed.snapshotINode != null) {
      short replication = removed.snapshotINode.getFileReplication();
      short currentRepl = file.getBlockReplication();
      if (replication > currentRepl) {
        long oldFileSizeNoRep = currentRepl == 0
            ? file.computeFileSize(true, true)
            : oldCounts.getStorageSpace() / file.getBlockReplication();
        oldStoragespace = oldFileSizeNoRep * replication;
        oldCounts.setStorageSpace(oldStoragespace);

        if (bsp != null) {
          List<StorageType> oldTypeChosen = bsp.chooseStorageTypes(replication);
          for (StorageType t : oldTypeChosen) {
            if (t.supportTypeQuota()) {
              oldCounts.addTypeSpace(t, oldFileSizeNoRep);
            }
          }
        }
    getDiffs().combineAndCollectSnapshotBlocks(
        bsps, file, removed, collectedBlocks, removedINodes);

    QuotaCounts current = file.storagespaceConsumed(bsp);
    oldCounts.subtract(current);
    return oldCounts;
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestCommitBlockSynchronization.java
    blockInfo.setBlockCollection(file);
    blockInfo.setGenerationStamp(genStamp);
    blockInfo.initializeBlockRecovery(genStamp);
    doReturn(blockInfo).when(file).removeLastBlock(any(Block.class));
    doReturn(true).when(file).isUnderConstruction();

    doReturn(blockInfo).when(namesystemSpy).getStoredBlock(any(Block.class));
          true, newTargets, null);

    doReturn(null).when(file).removeLastBlock(any(Block.class));

    namesystemSpy.commitBlockSynchronization(

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestTruncateQuotaUpdate.java
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshotFeature;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.ArrayList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestTruncateQuotaUpdate {
  private static final int BLOCKSIZE = 1024;
  private static final short REPLICATION = 4;
  private long nextMockBlockId;
  private long nextMockGenstamp;
  private long nextMockINodeId;

  @Test
  public void testTruncateWithoutSnapshot() {
    INodeFile file = createMockFile(BLOCKSIZE * 2 + BLOCKSIZE / 2, REPLICATION);
    QuotaCounts count = new QuotaCounts.Builder().build();
    file.computeQuotaDeltaForTruncate(BLOCKSIZE + BLOCKSIZE / 2, null, count);
    Assert.assertEquals(-BLOCKSIZE / 2 * REPLICATION, count.getStorageSpace());

    count = new QuotaCounts.Builder().build();
    file.computeQuotaDeltaForTruncate(BLOCKSIZE, null, count);
    Assert.assertEquals(-(BLOCKSIZE + BLOCKSIZE / 2) * REPLICATION,
                        count.getStorageSpace());

    count = new QuotaCounts.Builder().build();
    file.computeQuotaDeltaForTruncate(0, null, count);
    Assert.assertEquals(-(BLOCKSIZE * 2 + BLOCKSIZE / 2) * REPLICATION,
                        count.getStorageSpace());
  }

  @Test
  public void testTruncateWithSnapshotNoDivergence() {
    INodeFile file = createMockFile(BLOCKSIZE * 2 + BLOCKSIZE / 2, REPLICATION);
    addSnapshotFeature(file, file.getBlocks());

    QuotaCounts count = new QuotaCounts.Builder().build();
    file.computeQuotaDeltaForTruncate(BLOCKSIZE + BLOCKSIZE / 2, null, count);
    Assert.assertEquals(BLOCKSIZE * REPLICATION, count.getStorageSpace());

    count = new QuotaCounts.Builder().build();
    file.computeQuotaDeltaForTruncate(BLOCKSIZE, null, count);
    Assert.assertEquals(0, count.getStorageSpace());

    count = new QuotaCounts.Builder().build();
    file.computeQuotaDeltaForTruncate(0, null, count);
    Assert.assertEquals(0, count.getStorageSpace());
  }

  @Test
  public void testTruncateWithSnapshotAndDivergence() {
    INodeFile file = createMockFile(BLOCKSIZE * 2 + BLOCKSIZE / 2, REPLICATION);
    BlockInfoContiguous[] blocks = new BlockInfoContiguous
        [file.getBlocks().length];
    System.arraycopy(file.getBlocks(), 0, blocks, 0, blocks.length);
    addSnapshotFeature(file, blocks);
    file.getBlocks()[1] = newBlock(BLOCKSIZE, REPLICATION);
    file.getBlocks()[2] = newBlock(BLOCKSIZE / 2, REPLICATION);

    QuotaCounts count = new QuotaCounts.Builder().build();
    file.computeQuotaDeltaForTruncate(BLOCKSIZE + BLOCKSIZE / 2, null, count);
    Assert.assertEquals(-BLOCKSIZE / 2 * REPLICATION, count.getStorageSpace());

    count = new QuotaCounts.Builder().build();
    file.computeQuotaDeltaForTruncate(BLOCKSIZE + BLOCKSIZE / 2, null, count);
    Assert.assertEquals(-BLOCKSIZE / 2 * REPLICATION, count.getStorageSpace());

    count = new QuotaCounts.Builder().build();
    file.computeQuotaDeltaForTruncate(0, null, count);
    Assert.assertEquals(-(BLOCKSIZE + BLOCKSIZE / 2) * REPLICATION, count
        .getStorageSpace());
  }

  private INodeFile createMockFile(long size, short replication) {
    ArrayList<BlockInfoContiguous> blocks = new ArrayList<>();
    long createdSize = 0;
    while (createdSize < size) {
      long blockSize = Math.min(BLOCKSIZE, size - createdSize);
      BlockInfoContiguous bi = newBlock(blockSize, replication);
      blocks.add(bi);
      createdSize += BLOCKSIZE;
    }
    PermissionStatus perm = new PermissionStatus("foo", "bar", FsPermission
        .createImmutable((short) 0x1ff));
    return new INodeFile(
        ++nextMockINodeId, new byte[0], perm, 0, 0,
        blocks.toArray(new BlockInfoContiguous[blocks.size()]), replication,
        BLOCKSIZE);
  }

  private BlockInfoContiguous newBlock(long size, short replication) {
    Block b = new Block(++nextMockBlockId, size, ++nextMockGenstamp);
    return new BlockInfoContiguous(b, replication);
  }

  private static void addSnapshotFeature(INodeFile file, BlockInfoContiguous[] blocks) {
    FileDiff diff = mock(FileDiff.class);
    when(diff.getBlocks()).thenReturn(blocks);
    FileDiffList diffList = new FileDiffList();
    @SuppressWarnings("unchecked")
    ArrayList<FileDiff> diffs = ((ArrayList<FileDiff>)Whitebox.getInternalState
        (diffList, "diffs"));
    diffs.add(diff);
    FileWithSnapshotFeature sf = new FileWithSnapshotFeature(diffList);
    file.addFeature(sf);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestFileWithSnapshotFeature.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestFileWithSnapshotFeature.java
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.QuotaCounts;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.ArrayList;

import static org.apache.hadoop.fs.StorageType.DISK;
import static org.apache.hadoop.fs.StorageType.SSD;
import static org.mockito.Mockito.anyByte;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFileWithSnapshotFeature {
  private static final int BLOCK_SIZE = 1024;
  private static final short REPL_3 = 3;
  private static final short REPL_1 = 1;

  @Test
  public void testUpdateQuotaAndCollectBlocks() {
    FileDiffList diffs = new FileDiffList();
    FileWithSnapshotFeature sf = new FileWithSnapshotFeature(diffs);
    FileDiff diff = mock(FileDiff.class);
    BlockStoragePolicySuite bsps = mock(BlockStoragePolicySuite.class);
    BlockStoragePolicy bsp = mock(BlockStoragePolicy.class);
    BlockInfoContiguous[] blocks = new BlockInfoContiguous[] {
        new BlockInfoContiguous(new Block(1, BLOCK_SIZE, 1), REPL_1)
    };

    INodeFile file = mock(INodeFile.class);
    when(file.getFileWithSnapshotFeature()).thenReturn(sf);
    when(file.getBlocks()).thenReturn(blocks);
    when(file.getStoragePolicyID()).thenReturn((byte) 1);
    when(bsps.getPolicy(anyByte())).thenReturn(bsp);
    INode.BlocksMapUpdateInfo collectedBlocks = mock(
        INode.BlocksMapUpdateInfo.class);
    ArrayList<INode> removedINodes = new ArrayList<>();
    QuotaCounts counts = sf.updateQuotaAndCollectBlocks(
        bsps, file, diff, collectedBlocks, removedINodes);
    Assert.assertEquals(0, counts.getStorageSpace());
    Assert.assertTrue(counts.getTypeSpaces().allLessOrEqual(0));

    INodeFile snapshotINode = mock(INodeFile.class);
    when(file.getBlockReplication()).thenReturn(REPL_1);
    Whitebox.setInternalState(snapshotINode, "header", (long) REPL_3 << 48);
    Whitebox.setInternalState(diff, "snapshotINode", snapshotINode);
    when(diff.getSnapshotINode()).thenReturn(snapshotINode);

    when(bsp.chooseStorageTypes(REPL_1))
        .thenReturn(Lists.newArrayList(SSD));
    when(bsp.chooseStorageTypes(REPL_3))
        .thenReturn(Lists.newArrayList(DISK));
    counts = sf.updateQuotaAndCollectBlocks(
        bsps, file, diff, collectedBlocks, removedINodes);
    Assert.assertEquals((REPL_3 - REPL_1) * BLOCK_SIZE,
                        counts.getStorageSpace());
    Assert.assertEquals(BLOCK_SIZE, counts.getTypeSpaces().get(DISK));
    Assert.assertEquals(-BLOCK_SIZE, counts.getTypeSpaces().get(SSD));
  }

}

