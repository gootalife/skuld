hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
            if(bc == null || (bc.isUnderConstruction() && block.equals(bc.getLastBlock()))) {
              neededReplications.remove(block, priority); // remove from neededReplications
              continue;
            }

              if ( (pendingReplications.getNumReplicas(block) > 0) ||
                   (blockHasEnoughRacks(block)) ) {
                neededReplications.remove(block, priority); // remove from neededReplications
                blockLog.info("BLOCK* Removing {} from neededReplications as" +
                        " it has enough replicas", block);
                continue;
          if(bc == null || (bc.isUnderConstruction() && block.equals(bc.getLastBlock()))) {
            neededReplications.remove(block, priority); // remove from neededReplications
            rw.targets = null;
            continue;
          }
          requiredReplication = bc.getPreferredBlockReplication();
            if ( (pendingReplications.getNumReplicas(block) > 0) ||
                 (blockHasEnoughRacks(block)) ) {
              neededReplications.remove(block, priority); // remove from neededReplications
              rw.targets = null;
              blockLog.info("BLOCK* Removing {} from neededReplications as" +
                      " it has enough replicas", block);
          if(numEffectiveReplicas + targets.length >= requiredReplication) {
            neededReplications.remove(block, priority); // remove from neededReplications
          }
        }
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/UnderReplicatedBlocks.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
  static final int QUEUE_WITH_CORRUPT_BLOCKS = 4;
  private List<LightWeightLinkedSet<Block>> priorityQueues
      = new ArrayList<LightWeightLinkedSet<Block>>(LEVEL);

  private int corruptReplOneBlocks = 0;

  UnderReplicatedBlocks() {
    for (int i = 0; i < LEVEL; i++) {
      priorityQueues.add(new LightWeightLinkedSet<Block>());
    }
  }


    for (int priority = 0; priority < LEVEL; priority++) { 
      BlockIterator neededReplicationsIterator = iterator(priority);
       neededReplicationsIterator.setToBookmark();

      blocksToProcess = Math.min(blocksToProcess, size());
      
          && neededReplicationsIterator.hasNext()) {
        Block block = neededReplicationsIterator.next();
        blocksToReplicate.get(priority).add(block);
        blockCount++;
      }
      
      if (!neededReplicationsIterator.hasNext()
          && neededReplicationsIterator.getPriority() == LEVEL - 1) {
        for (int i = 0; i < LEVEL; i++) {
          this.priorityQueues.get(i).resetBookmark();
        }
        break;
      }
    }
    return blocksToReplicate;
  }
    int getPriority() {
      return level;
    }

    private synchronized void setToBookmark() {
      if (this.isIteratorForLevel) {
        this.iterators.set(0, priorityQueues.get(this.level)
            .getBookmark());
      } else {
        for (int i = 0; i < LEVEL; i++) {
          this.iterators.set(i, priorityQueues.get(i).getBookmark());
        }
      }
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/util/LightWeightLinkedSet.java
  private DoubleLinkedElement<T> head;
  private DoubleLinkedElement<T> tail;

  private LinkedSetIterator bookmark;

    super(initCapacity, maxLoadFactor, minLoadFactor);
    head = null;
    tail = null;
    bookmark = new LinkedSetIterator();
  }

  public LightWeightLinkedSet() {
    tail = le;
    if (head == null) {
      head = le;
      bookmark.next = head;
    }

    if (bookmark.next == null) {
      bookmark.next = le;
    }
    return true;
  }
    if (tail == found) {
      tail = tail.before;
    }

    if (found == this.bookmark.next) {
      this.bookmark.next = found.after;
    }
    return found;
  }

    super.clear();
    this.head = null;
    this.tail = null;
    this.resetBookmark();
  }

  public Iterator<T> getBookmark() {
    LinkedSetIterator toRet = new LinkedSetIterator();
    toRet.next = this.bookmark.next;
    this.bookmark = toRet;
    return toRet;
  }

  public void resetBookmark() {
    this.bookmark.next = this.head;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestReplicationPolicy.java
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager.StatefulBlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
    exception.expect(IllegalArgumentException.class);
    blocksReplWorkMultiplier = DFSUtil.getReplWorkMultiplier(conf);
  }

  @Test(timeout = 60000)
  public void testUpdateDoesNotCauseSkippedReplication() {
    UnderReplicatedBlocks underReplicatedBlocks = new UnderReplicatedBlocks();

    Block block1 = new Block(ThreadLocalRandom.current().nextLong());
    Block block2 = new Block(ThreadLocalRandom.current().nextLong());
    Block block3 = new Block(ThreadLocalRandom.current().nextLong());

    final int block1CurReplicas = 2;
    final int block1ExpectedReplicas = 7;
    underReplicatedBlocks.add(block1, block1CurReplicas, 0,
        block1ExpectedReplicas);

    underReplicatedBlocks.add(block2, 2, 0, 7);

    underReplicatedBlocks.add(block3, 2, 0, 6);

    List<List<Block>> chosenBlocks;

    chosenBlocks = underReplicatedBlocks.chooseUnderReplicatedBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 0, 1, 0, 0, 0);

    underReplicatedBlocks.update(block1, block1CurReplicas+1, 0,
        block1ExpectedReplicas, 1, 0);

    chosenBlocks = underReplicatedBlocks.chooseUnderReplicatedBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 0, 1, 0, 0, 0);

    chosenBlocks = underReplicatedBlocks.chooseUnderReplicatedBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 0, 0, 1, 0, 0);
  }

  @Test(timeout = 60000)
  public void testAddStoredBlockDoesNotCauseSkippedReplication()
      throws IOException {
    Namesystem mockNS = mock(Namesystem.class);
    when(mockNS.isPopulatingReplQueues()).thenReturn(true);
    when(mockNS.hasWriteLock()).thenReturn(true);
    BlockManager bm =
        new BlockManager(mockNS, new HdfsConfiguration());
    UnderReplicatedBlocks underReplicatedBlocks = bm.neededReplications;

    Block block1 = new Block(ThreadLocalRandom.current().nextLong());
    Block block2 = new Block(ThreadLocalRandom.current().nextLong());

    underReplicatedBlocks.add(block1, 0, 1, 1);

    underReplicatedBlocks.add(block2, 0, 1, 1);

    List<List<Block>> chosenBlocks;

    chosenBlocks = underReplicatedBlocks.chooseUnderReplicatedBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 1, 0, 0, 0, 0);

    BlockInfoUnderConstruction info = new BlockInfoUnderConstructionContiguous(block1, (short)1);
    BlockCollection bc = mock(BlockCollection.class);
    when(bc.getPreferredBlockReplication()).thenReturn((short)1);
    bm.addBlockCollection(info, bc);

    StatefulBlockInfo statefulBlockInfo = new StatefulBlockInfo(info,
      block1, ReplicaState.RBW);

    bm.addStoredBlockUnderConstruction(statefulBlockInfo,
        TestReplicationPolicy.storages[0]);

    chosenBlocks = underReplicatedBlocks.chooseUnderReplicatedBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 1, 0, 0, 0, 0);
  }

  @Test(timeout = 60000)
  public void
      testConvertLastBlockToUnderConstructionDoesNotCauseSkippedReplication()
          throws IOException {
    Namesystem mockNS = mock(Namesystem.class);
    when(mockNS.isPopulatingReplQueues()).thenReturn(true);
    BlockManager bm =
        new BlockManager(mockNS, new HdfsConfiguration());
    UnderReplicatedBlocks underReplicatedBlocks = bm.neededReplications;

    Block block1 = new Block(ThreadLocalRandom.current().nextLong());
    Block block2 = new Block(ThreadLocalRandom.current().nextLong());

    underReplicatedBlocks.add(block1, 0, 1, 1);

    underReplicatedBlocks.add(block2, 0, 1, 1);

    List<List<Block>> chosenBlocks;

    chosenBlocks = underReplicatedBlocks.chooseUnderReplicatedBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 1, 0, 0, 0, 0);

    final BlockInfo info = new BlockInfoContiguous(block1, (short) 1);
    final BlockCollection mbc = mock(BlockCollection.class);
    when(mbc.getLastBlock()).thenReturn(info);
    when(mbc.getPreferredBlockSize()).thenReturn(block1.getNumBytes() + 1);
    when(mbc.getPreferredBlockReplication()).thenReturn((short)1);
    ContentSummary cs = mock(ContentSummary.class);
    when(cs.getLength()).thenReturn((long)1);
    when(mbc.computeContentSummary(bm.getStoragePolicySuite())).thenReturn(cs);
    info.setBlockCollection(mbc);
    bm.addBlockCollection(info, mbc);

    DatanodeStorageInfo[] dnAry = {storages[0]};
    final BlockInfoUnderConstruction ucBlock =
        info.convertToBlockUnderConstruction(BlockUCState.UNDER_CONSTRUCTION,
            dnAry);

    DatanodeStorageInfo storage = mock(DatanodeStorageInfo.class);
    DatanodeDescriptor dn = mock(DatanodeDescriptor.class);
    when(dn.isDecommissioned()).thenReturn(true);
    when(storage.getState()).thenReturn(DatanodeStorage.State.NORMAL);
    when(storage.getDatanodeDescriptor()).thenReturn(dn);
    when(storage.removeBlock(any(BlockInfo.class))).thenReturn(true);
    when(storage.addBlock(any(BlockInfo.class))).thenReturn
        (DatanodeStorageInfo.AddBlockResult.ADDED);
    ucBlock.addStorage(storage);

    when(mbc.setLastBlock((BlockInfo) any(), (DatanodeStorageInfo[]) any()))
    .thenReturn(ucBlock);

    bm.convertLastBlockToUnderConstruction(mbc, 0);

    chosenBlocks = underReplicatedBlocks.chooseUnderReplicatedBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 1, 0, 0, 0, 0);
  }

  @Test(timeout = 60000)
  public void testupdateNeededReplicationsDoesNotCauseSkippedReplication()
      throws IOException {
    Namesystem mockNS = mock(Namesystem.class);
    when(mockNS.isPopulatingReplQueues()).thenReturn(true);
    BlockManager bm =
        new BlockManager(mockNS, new HdfsConfiguration());
    UnderReplicatedBlocks underReplicatedBlocks = bm.neededReplications;

    Block block1 = new Block(ThreadLocalRandom.current().nextLong());
    Block block2 = new Block(ThreadLocalRandom.current().nextLong());

    underReplicatedBlocks.add(block1, 0, 1, 1);

    underReplicatedBlocks.add(block2, 0, 1, 1);

    List<List<Block>> chosenBlocks;

    chosenBlocks = underReplicatedBlocks.chooseUnderReplicatedBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 1, 0, 0, 0, 0);

    bm.setReplication((short)0, (short)1, "", block1);

    chosenBlocks = underReplicatedBlocks.chooseUnderReplicatedBlocks(1);
    assertTheChosenBlocks(chosenBlocks, 1, 0, 0, 0, 0);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/util/TestLightWeightLinkedSet.java
    assertEquals(NUM, set.size());
    assertFalse(set.isEmpty());

    Iterator<Integer> bkmrkIt = set.getBookmark();
    for (int i=0; i<set.size()/2+1; i++) {
      bkmrkIt.next();
    }
    assertTrue(bkmrkIt.hasNext());

    set.clear();
    assertEquals(0, set.size());
    assertTrue(set.isEmpty());
    bkmrkIt = set.getBookmark();
    assertFalse(bkmrkIt.hasNext());

    assertEquals(0, set.pollAll().size());
    LOG.info("Test capacity - DONE");
  }

  @Test(timeout=60000)
  public void testGetBookmarkReturnsBookmarkIterator() {
    LOG.info("Test getBookmark returns proper iterator");
    assertTrue(set.addAll(list));

    Iterator<Integer> bookmark = set.getBookmark();
    assertEquals(bookmark.next(), list.get(0));

    final int numAdvance = list.size()/2;
    for(int i=1; i<numAdvance; i++) {
      bookmark.next();
    }

    Iterator<Integer> bookmark2 = set.getBookmark();
    assertEquals(bookmark2.next(), list.get(numAdvance));
  }

  @Test(timeout=60000)
  public void testBookmarkAdvancesOnRemoveOfSameElement() {
    LOG.info("Test that the bookmark advances if we remove its element.");
    assertTrue(set.add(list.get(0)));
    assertTrue(set.add(list.get(1)));
    assertTrue(set.add(list.get(2)));

    Iterator<Integer> it = set.getBookmark();
    assertEquals(it.next(), list.get(0));
    set.remove(list.get(1));
    it = set.getBookmark();
    assertEquals(it.next(), list.get(2));
  }

  @Test(timeout=60000)
  public void testBookmarkSetToHeadOnAddToEmpty() {
    LOG.info("Test bookmark is set after adding to previously empty set.");
    Iterator<Integer> it = set.getBookmark();
    assertFalse(it.hasNext());
    set.add(list.get(0));
    set.add(list.get(1));

    it = set.getBookmark();
    assertTrue(it.hasNext());
    assertEquals(it.next(), list.get(0));
    assertEquals(it.next(), list.get(1));
    assertFalse(it.hasNext());
  }

  @Test(timeout=60000)
  public void testResetBookmarkPlacesBookmarkAtHead() {
    set.addAll(list);
    Iterator<Integer> it = set.getBookmark();
    final int numAdvance = set.size()/2;
    for (int i=0; i<numAdvance; i++) {
      it.next();
    }
    assertEquals(it.next(), list.get(numAdvance));

    set.resetBookmark();
    it = set.getBookmark();
    assertEquals(it.next(), list.get(0));
  }
}
\No newline at end of file

