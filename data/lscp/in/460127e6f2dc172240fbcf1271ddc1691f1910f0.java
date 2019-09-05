hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
  }

  static class GetBlockLocationsResult {
    final boolean updateAccessTime;
    final LocatedBlocks blocks;
    boolean updateAccessTime() {
      return updateAccessTime;
    }
    private GetBlockLocationsResult(
        boolean updateAccessTime, LocatedBlocks blocks) {
      this.updateAccessTime = updateAccessTime;
      this.blocks = blocks;
    }
  }
  LocatedBlocks getBlockLocations(String clientMachine, String srcArg,
      long offset, long length) throws IOException {
    checkOperation(OperationCategory.READ);
    GetBlockLocationsResult res = null;
    FSPermissionChecker pc = getPermissionChecker();
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      res = getBlockLocations(pc, srcArg, offset, length, true, true);
    } catch (AccessControlException e) {
      logAuditEvent(false, "open", srcArg);
      throw e;
    } finally {
      readUnlock();
    }

    logAuditEvent(true, "open", srcArg);

    if (res.updateAccessTime()) {
      byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(
          srcArg);
      String src = srcArg;
      writeLock();
      final long now = now();
      try {
        checkOperation(OperationCategory.WRITE);
        src = dir.resolvePath(pc, srcArg, pathComponents);
        final INodesInPath iip = dir.getINodesInPath(src, true);
        INode inode = iip.getLastINode();
        boolean updateAccessTime = inode != null &&
            now > inode.getAccessTime() + getAccessTimePrecision();
        if (!isInSafeMode() && updateAccessTime) {
          boolean changed = FSDirAttrOp.setTimes(dir,
              inode, -1, now, false, iip.getLatestSnapshotId());
          if (changed) {
            getEditLog().logTimes(src, -1, now);
          }
  GetBlockLocationsResult getBlockLocations(
      FSPermissionChecker pc, String src, long offset, long length,
      boolean needBlockToken, boolean checkSafeMode) throws IOException {
    if (offset < 0) {
      throw new HadoopIllegalArgumentException(
          "Negative offset is not supported. File: " + src);
          "Negative length is not supported. File: " + src);
    }
    final GetBlockLocationsResult ret = getBlockLocationsInt(
        pc, src, offset, length, needBlockToken);

    if (checkSafeMode && isInSafeMode()) {
      for (LocatedBlock b : ret.blocks.getLocatedBlocks()) {
  }

  private GetBlockLocationsResult getBlockLocationsInt(
      FSPermissionChecker pc, final String srcArg, long offset, long length,
      boolean needBlockToken)
      throws IOException {
    String src = srcArg;
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = dir.resolvePath(pc, srcArg, pathComponents);
    final INodesInPath iip = dir.getINodesInPath(src, true);
    final INodeFile inode = INodeFile.valueOf(iip.getLastINode(), src);
    if (isPermissionEnabled) {
    boolean updateAccessTime = isAccessTimeSupported() && !isInSafeMode()
        && !iip.isSnapshot()
        && now > inode.getAccessTime() + getAccessTimePrecision();
    return new GetBlockLocationsResult(updateAccessTime, blocks);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NamenodeFsck.java
    FSNamesystem fsn = namenode.getNamesystem();
    fsn.readLock();
    try {
      blocks = fsn.getBlockLocations(
          fsn.getPermissionChecker(), path, 0, fileLen, false, false)
          .blocks;
    } catch (FileNotFoundException fnfe) {
      blocks = null;
    } finally {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFsck.java
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
    DatanodeManager dnManager = mock(DatanodeManager.class);

    when(namenode.getNamesystem()).thenReturn(fsName);
    when(fsName.getBlockLocations(any(FSPermissionChecker.class), anyString(),
                                  anyLong(), anyLong(),
                                  anyBoolean(), anyBoolean()))
        .thenThrow(new FileNotFoundException());
    when(fsName.getBlockManager()).thenReturn(blockManager);
    when(blockManager.getDatanodeManager()).thenReturn(dnManager);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestGetBlockLocations.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestGetBlockLocations.java
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.apache.hadoop.util.Time.now;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestGetBlockLocations {
  private static final String FILE_NAME = "foo";
  private static final String FILE_PATH = "/" + FILE_NAME;
  private static final long MOCK_INODE_ID = 16386;
  private static final String RESERVED_PATH =
      "/.reserved/.inodes/" + MOCK_INODE_ID;

  @Test(timeout = 30000)
  public void testResolveReservedPath() throws IOException {
    FSNamesystem fsn = setupFileSystem();
    FSEditLog editlog = fsn.getEditLog();
    fsn.getBlockLocations("dummy", RESERVED_PATH, 0, 1024);
    verify(editlog).logTimes(eq(FILE_PATH), anyLong(), anyLong());
    fsn.close();
  }

  @Test(timeout = 30000)
  public void testGetBlockLocationsRacingWithDelete() throws IOException {
    FSNamesystem fsn = spy(setupFileSystem());
    final FSDirectory fsd = fsn.getFSDirectory();
    FSEditLog editlog = fsn.getEditLog();

    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        INodesInPath iip = fsd.getINodesInPath(FILE_PATH, true);
        FSDirDeleteOp.delete(fsd, iip, new INode.BlocksMapUpdateInfo(),
                             new ArrayList<INode>(), now());
        invocation.callRealMethod();
        return null;
      }
    }).when(fsn).writeLock();
    fsn.getBlockLocations("dummy", RESERVED_PATH, 0, 1024);

    verify(editlog, never()).logTimes(anyString(), anyLong(), anyLong());
    fsn.close();
  }

  @Test(timeout = 30000)
  public void testGetBlockLocationsRacingWithRename() throws IOException {
    FSNamesystem fsn = spy(setupFileSystem());
    final FSDirectory fsd = fsn.getFSDirectory();
    FSEditLog editlog = fsn.getEditLog();
    final String DST_PATH = "/bar";
    final boolean[] renamed = new boolean[1];

    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        invocation.callRealMethod();
        if (!renamed[0]) {
          FSDirRenameOp.renameTo(fsd, fsd.getPermissionChecker(), FILE_PATH,
                                 DST_PATH, new INode.BlocksMapUpdateInfo(),
                                 false);
          renamed[0] = true;
        }
        return null;
      }
    }).when(fsn).writeLock();
    fsn.getBlockLocations("dummy", RESERVED_PATH, 0, 1024);

    verify(editlog).logTimes(eq(DST_PATH), anyLong(), anyLong());
    fsn.close();
  }

  private static FSNamesystem setupFileSystem() throws IOException {
    Configuration conf = new Configuration();
    conf.setLong(DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 1L);
    FSEditLog editlog = mock(FSEditLog.class);
    FSImage image = mock(FSImage.class);
    when(image.getEditLog()).thenReturn(editlog);
    final FSNamesystem fsn = new FSNamesystem(conf, image, true);

    final FSDirectory fsd = fsn.getFSDirectory();
    INodesInPath iip = fsd.getINodesInPath("/", true);
    PermissionStatus perm = new PermissionStatus(
        "hdfs", "supergroup",
        FsPermission.createImmutable((short) 0x1ff));
    final INodeFile file = new INodeFile(
        MOCK_INODE_ID, FILE_NAME.getBytes(Charsets.UTF_8),
        perm, 1, 1, new BlockInfoContiguous[] {}, (short) 1,
        DFS_BLOCK_SIZE_DEFAULT);
    fsn.getFSDirectory().addINode(iip, file);
    return fsn;
  }

}

