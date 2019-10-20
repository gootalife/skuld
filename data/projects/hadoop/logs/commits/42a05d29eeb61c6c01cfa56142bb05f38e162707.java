hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/Checkpointer.java
    try {
      backupNode.namesystem.setImageLoaded();
      if(backupNode.namesystem.getBlocksTotal() > 0) {
        long completeBlocksTotal =
            backupNode.namesystem.getCompleteBlocksTotal();
        backupNode.namesystem.setBlockTotal(completeBlocksTotal);
      }
      bnImage.saveFSImageInAllDirs(backupNode.getNamesystem(), txid);
      if (!backupNode.namesystem.isRollingUpgrade()) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
      assert safeMode != null && !isPopulatingReplQueues();
      StartupProgress prog = NameNode.getStartupProgress();
      prog.beginPhase(Phase.SAFEMODE);
      long completeBlocksTotal = getCompleteBlocksTotal();
      prog.setTotal(Phase.SAFEMODE, STEP_AWAITING_REPORTED_BLOCKS,
          completeBlocksTotal);
      setBlockTotal(completeBlocksTotal);
      blockManager.activate(conf);
    } finally {
      writeUnlock();
  public void setBlockTotal(long completeBlocksTotal) {
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null)
      return;
    safeMode.setBlockTotal((int) completeBlocksTotal);
  }

  public long getCompleteBlocksTotal() {
    long numUCBlocks = 0;
    readLock();
    try {
      numUCBlocks = leaseManager.getNumUnderConstructionBlocks();
      return getBlocksTotal() - numUCBlocks;
    } finally {
      readUnlock();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/LeaseManager.java
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

  @VisibleForTesting
  public synchronized int countLease() {
    return sortedLeases.size();
  }

  synchronized long countPath() {
    return leasesById.size();
  }

      return holder.hashCode();
    }
    
    private Collection<Long> getFiles() {
      return Collections.unmodifiableCollection(files);
    }

    String getHolder() {
      return holder;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestLeaseManager.java
package org.apache.hadoop.hdfs.server.namenode;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class TestLeaseManager {
  @Rule
  public Timeout timeout = new Timeout(300000);

  @Test
  public void testRemoveLeases() throws Exception {
    FSNamesystem fsn = mock(FSNamesystem.class);
  @Test
  public void testCheckLeaseNotInfiniteLoop() {
    LeaseManager lm = new LeaseManager(makeMockFsNameSystem());

    lm.setLeasePeriod(0, 0);
    lm.checkLeases();
  }


  @Test
  public void testCountPath() {
    LeaseManager lm = new LeaseManager(makeMockFsNameSystem());

    lm.addLease("holder1", 1);
    assertThat(lm.countPath(), is(1L));

    lm.addLease("holder2", 2);
    assertThat(lm.countPath(), is(2L));
    lm.addLease("holder2", 2);                   // Duplicate addition
    assertThat(lm.countPath(), is(2L));

    assertThat(lm.countPath(), is(2L));

    lm.removeLease("holder2", stubInodeFile(3));
    lm.removeLease("InvalidLeaseHolder", stubInodeFile(1));
    assertThat(lm.countPath(), is(2L));

    INodeFile file = stubInodeFile(1);
    lm.reassignLease(lm.getLease(file), file, "holder2");
    assertThat(lm.countPath(), is(2L));          // Count unchanged on reassign

    lm.removeLease("holder2", stubInodeFile(2)); // Remove existing
    assertThat(lm.countPath(), is(1L));
  }

  private static FSNamesystem makeMockFsNameSystem() {
    FSDirectory dir = mock(FSDirectory.class);
    FSNamesystem fsn = mock(FSNamesystem.class);
    when(fsn.isRunning()).thenReturn(true);
    when(fsn.hasWriteLock()).thenReturn(true);
    when(fsn.getFSDirectory()).thenReturn(dir);
    return fsn;
  }

  private static INodeFile stubInodeFile(long inodeId) {
    PermissionStatus p = new PermissionStatus(
        "dummy", "dummy", new FsPermission((short) 0777));
    return new INodeFile(
        inodeId, "/foo".getBytes(), p, 0L, 0L,
        BlockInfo.EMPTY_ARRAY, (short) 1, 1L);
  }
}

