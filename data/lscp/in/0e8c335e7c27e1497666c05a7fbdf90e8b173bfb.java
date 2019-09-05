hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirConcatOp.java
    int count = 0;
    for (INodeFile nodeToRemove : srcList) {
      if(nodeToRemove != null) {
        nodeToRemove.clearBlocks();
        nodeToRemove.getParent().removeChild(nodeToRemove);
        fsd.getINodeMap().remove(nodeToRemove);
        count++;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeFile.java
  }

  private void setFileReplication(short replication) {
    header = HeaderFormat.REPLICATION.BITS.combine(replication, header);
  }

    setStoragePolicyID(storagePolicyId);
  }

  @Override // INodeFileAttributes
  public long getHeaderLong() {
    return header;
  }

  @Override // BlockCollection
  public BlockInfo[] getBlocks() {
    return this.blocks;
  }

  public BlockInfo[] getBlocks(int snapshot) {
    if(snapshot == CURRENT_STATE_ID || getDiffs() == null) {
      return getBlocks();
    }
    FileDiff diff = getDiffs().getDiffById(snapshot);
    BlockInfo[] snapshotBlocks = diff == null ? getBlocks() : diff.getBlocks();
    if (snapshotBlocks != null) {
      return snapshotBlocks;
    }
    snapshotBlocks = getDiffs().findLaterSnapshotBlocks(snapshot);
    return (snapshotBlocks == null) ? getBlocks() : snapshotBlocks;
  }

  private void updateBlockCollection() {
    if (blocks != null) {
      for(BlockInfo b : blocks) {
        b.setBlockCollection(this);
  }

  private void setBlocks(BlockInfo[] blocks) {
    this.blocks = blocks;
  }

  public void clearBlocks() {
    setBlocks(null);
  }

  @Override
  public void cleanSubtree(ReclaimContext reclaimContext,
      final int snapshot, int priorSnapshotId) {
        blk.setBlockCollection(null);
      }
    }
    clearBlocks();
    if (getAclFeature() != null) {
      AclStorage.removeAclFeature(getAclFeature());
    }
  public long collectBlocksBeyondMax(final long max,
      final BlocksMapUpdateInfo collectedBlocks) {
    final BlockInfo[] oldBlocks = getBlocks();
    if (oldBlocks == null) {
      return 0;
    }
    int n = 0;
    long size = 0;
    for(; n < oldBlocks.length && max > size; n++) {
      size += oldBlocks[n].getNumBytes();
    }
    if (n >= oldBlocks.length) {
      return size;
    }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestINodeFile.java
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
    f1 = inf.getXAttrFeature();
    assertEquals(f1, null);
  }

  @Test
  public void testClearBlocks() {
    INodeFile toBeCleared = createINodeFiles(1, "toBeCleared")[0];
    assertEquals(1, toBeCleared.getBlocks().length);
    toBeCleared.clearBlocks();
    assertNull(toBeCleared.getBlocks());
  }
}

