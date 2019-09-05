hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocol/LayoutVersion.java
  public static class FeatureInfo {
    private final int lv;
    private final int ancestorLV;
    private final Integer minCompatLV;
    private final String description;
    private final boolean reserved;
    private final LayoutFeature[] specialFeatures;

    public FeatureInfo(final int lv, final int ancestorLV, final String description,
        boolean reserved, LayoutFeature... specialFeatures) {
      this(lv, ancestorLV, null, description, reserved, specialFeatures);
    }

    public FeatureInfo(final int lv, final int ancestorLV, Integer minCompatLV,
        final String description, boolean reserved,
        LayoutFeature... specialFeatures) {
      this.lv = lv;
      this.ancestorLV = ancestorLV;
      this.minCompatLV = minCompatLV;
      this.description = description;
      this.reserved = reserved;
      this.specialFeatures = specialFeatures;
      return ancestorLV;
    }

    public int getMinimumCompatibleLayoutVersion() {
      return minCompatLV != null ? minCompatLV : lv;
    }

      LayoutFeature[] features) {
    SortedSet<LayoutFeature> existingFeatures = new TreeSet<LayoutFeature>(
        new LayoutFeatureComparator());
    for (SortedSet<LayoutFeature> s : map.values()) {
      existingFeatures.addAll(s);
    }
    LayoutFeature prevF = existingFeatures.isEmpty() ? null :
        existingFeatures.first();
    for (LayoutFeature f : features) {
      final FeatureInfo info = f.getInfo();
      int minCompatLV = info.getMinimumCompatibleLayoutVersion();
      if (prevF != null &&
          minCompatLV > prevF.getInfo().getMinimumCompatibleLayoutVersion()) {
        throw new AssertionError(String.format(
            "Features must be listed in order of minimum compatible layout " +
            "version.  Check features %s and %s.", prevF, f));
      }
      prevF = f;
      SortedSet<LayoutFeature> ancestorSet = map.get(info.getAncestorLayoutVersion());
      if (ancestorSet == null) {
    return getLastNonReservedFeature(features).getInfo().getLayoutVersion();
  }

  public static int getMinimumCompatibleLayoutVersion(
      LayoutFeature[] features) {
    return getLastNonReservedFeature(features).getInfo()
        .getMinimumCompatibleLayoutVersion();
  }

  static LayoutFeature getLastNonReservedFeature(LayoutFeature[] features) {
    for (int i = features.length -1; i >= 0; i--) {
      final FeatureInfo info = features[i].getInfo();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/BackupImage.java
      }
    }
    editLog.setNextTxId(txid);
    editLog.startLogSegment(txid, false,
        namesystem.getEffectiveLayoutVersion());
    if (bnState == BNState.DROP_UNTIL_NEXT_ROLL) {
      setState(BNState.JOURNAL_ONLY);
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/Checkpointer.java
        backupNode.namesystem.setBlockTotal();
      }
      bnImage.saveFSImageInAllDirs(backupNode.getNamesystem(), txid);
      if (!backupNode.namesystem.isRollingUpgrade()) {
        bnStorage.writeAll();
      }
    } finally {
      backupNode.namesystem.writeUnlock();
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLog.java
  synchronized void openForWrite(int layoutVersion) throws IOException {
    Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS,
        "Bad state: %s", state);

      throw new IllegalStateException(error);
    }
    
    startLogSegment(segmentTxId, true, layoutVersion);
    assert state == State.IN_SEGMENT : "Bad state: " + state;
  }
  
  synchronized long rollEditLog(int layoutVersion) throws IOException {
    LOG.info("Rolling edit logs");
    endCurrentLogSegment(true);
    
    long nextTxId = getLastWrittenTxId() + 1;
    startLogSegment(nextTxId, true, layoutVersion);
    
    assert curSegmentTxId == nextTxId;
    return nextTxId;
  synchronized void startLogSegment(final long segmentTxId,
      boolean writeHeaderTxn, int layoutVersion) throws IOException {
    LOG.info("Starting log segment at " + segmentTxId);
    Preconditions.checkArgument(segmentTxId > 0,
        "Bad txid: %s", segmentTxId);
    storage.attemptRestoreRemovedStorage();
    
    try {
      editLogStream = journalSet.startLogSegment(segmentTxId, layoutVersion);
    } catch (IOException ex) {
      throw new IOException("Unable to start log segment " +
          segmentTxId + ": too few journals successfully started.", ex);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImage.java
    return editLog;
  }

  void openEditLogForWrite(int layoutVersion) throws IOException {
    assert editLog != null : "editLog must be initialized";
    editLog.openForWrite(layoutVersion);
    storage.writeTransactionIdFileToStorage(editLog.getCurSegmentTxId());
  }
  
    try {
      try {
        saveFSImageInAllDirs(source, nnf, imageTxId, canceler);
        if (!source.isRollingUpgrade()) {
          storage.writeAll();
        }
      } finally {
        if (editLogWasOpen) {
          editLog.startLogSegment(imageTxId + 1, true,
              source.getEffectiveLayoutVersion());
    }
  }

  CheckpointSignature rollEditLog(int layoutVersion) throws IOException {
    getEditLog().rollEditLog(layoutVersion);
  NamenodeCommand startCheckpoint(NamenodeRegistration bnReg, // backup node
                                  NamenodeRegistration nnReg,
                                  int layoutVersion) // active name-node
  throws IOException {
    LOG.info("Start checkpoint at txid " + getEditLog().getLastWrittenTxId());
    String msg = null;
    if(storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0)
      needToReturnImg = false;
    CheckpointSignature sig = rollEditLog(layoutVersion);
    return new CheckpointCommand(sig, needToReturnImg);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageFormatProtobuf.java

      FileSummary.Builder b = FileSummary.newBuilder()
          .setOndiskVersion(FSImageUtil.FILE_VERSION)
          .setLayoutVersion(
              context.getSourceNamesystem().getEffectiveLayoutVersion());

      codec = compression.getImageCodec();
      if (codec != null) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.namenode.ha.EditLogTailer;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.ha.StandbyCheckpointer;
      if (needToSave) {
        fsImage.saveNamespace(this);
      } else {
        StartupProgress prog = NameNode.getStartupProgress();
        prog.beginPhase(Phase.SAVING_CHECKPOINT);
      if (!haEnabled || (haEnabled && startOpt == StartupOption.UPGRADE)
          || (haEnabled && startOpt == StartupOption.UPGRADEONLY)) {
        fsImage.openEditLogForWrite(getEffectiveLayoutVersion());
      }
      success = true;
    } finally {
    imageLoadComplete();
  }

  private void startSecretManager() {
    if (dtSecretManager != null) {
      try {
            nextTxId);
        editLog.setNextTxId(nextTxId);

        getFSImage().editLog.openForWrite(getEffectiveLayoutVersion());
      }

                   String clientName, String clientMachine,
                   long mtime)
      throws IOException, UnresolvedLinkException {
    requireEffectiveLayoutVersionForFeature(Feature.TRUNCATE);
    boolean ret;
    try {
      ret = truncateInt(src, newLength, clientName, clientMachine, mtime);
    }

    if (writeToEditLog) {
      if (NameNodeLayoutVersion.supports(Feature.APPEND_NEW_BLOCK,
          getEffectiveLayoutVersion())) {
        getEditLog().logAppendFile(src, file, newBlock, logRetryCache);
      } else {
        getEditLog().logOpenFile(src, file, false, logRetryCache);
      }
    }
    return ret;
  }
  LastBlockWithStatus appendFile(String src, String holder,
      String clientMachine, EnumSet<CreateFlag> flag, boolean logRetryCache)
      throws IOException {
    boolean newBlock = flag.contains(CreateFlag.NEW_BLOCK);
    if (newBlock) {
      requireEffectiveLayoutVersionForFeature(Feature.APPEND_NEW_BLOCK);
    }
    try {
      return appendFileInt(src, holder, clientMachine, newBlock, logRetryCache);
    } catch (AccessControlException e) {
      logAuditEvent(false, "append", src);
      throw e;
  void setQuota(String src, long nsQuota, long ssQuota, StorageType type)
      throws IOException {
    if (type != null) {
      requireEffectiveLayoutVersionForFeature(Feature.QUOTA_BY_STORAGE_TYPE);
    }
    checkOperation(OperationCategory.WRITE);
    writeLock();
    boolean success = false;
      if (Server.isRpcInvocation()) {
        LOG.info("Roll Edit Log from " + Server.getRemoteAddress());
      }
      return getFSImage().rollEditLog(getEffectiveLayoutVersion());
    } finally {
      writeUnlock();
    }
      
      LOG.info("Start checkpoint for " + backupNode.getAddress());
      NamenodeCommand cmd = getFSImage().startCheckpoint(backupNode,
          activeNamenode, getEffectiveLayoutVersion());
      getEditLog().logSync();
      return cmd;
    } finally {
      getEditLog().logStartRollingUpgrade(rollingUpgradeInfo.getStartTime());
      if (haEnabled) {
        getFSImage().rollEditLog(getEffectiveLayoutVersion());
      }
    } finally {
      writeUnlock();
    return rollingUpgradeInfo != null && !rollingUpgradeInfo.isFinalized();
  }

  public int getEffectiveLayoutVersion() {
    if (isRollingUpgrade()) {
      int storageLV = fsImage.getStorage().getLayoutVersion();
      if (storageLV >=
          NameNodeLayoutVersion.MINIMUM_COMPATIBLE_LAYOUT_VERSION) {
        return storageLV;
      }
    }
    return NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION;
  }

  private void requireEffectiveLayoutVersionForFeature(Feature f)
      throws HadoopIllegalArgumentException {
    int lv = getEffectiveLayoutVersion();
    if (!NameNodeLayoutVersion.supports(f, lv)) {
      throw new HadoopIllegalArgumentException(String.format(
          "Feature %s unsupported at NameNode layout version %d.  If a " +
          "rolling upgrade is in progress, then it must be finalized before " +
          "using this feature.", f, lv));
    }
  }

  void checkRollingUpgrade(String action) throws RollingUpgradeException {
    if (isRollingUpgrade()) {
      throw new RollingUpgradeException("Failed to " + action
      getEditLog().logFinalizeRollingUpgrade(rollingUpgradeInfo.getFinalizeTime());
      if (haEnabled) {
        getFSImage().rollEditLog(getEffectiveLayoutVersion());
      }
      getFSImage().updateStorageVersion();
      getFSImage().renameCheckpoint(NameNodeFile.IMAGE_ROLLBACK,

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNode.java
            LOG.trace("copying op: " + op);
          }
          if (!segmentOpen) {
            newSharedEditLog.startLogSegment(op.txid, false,
                fsns.getEffectiveLayoutVersion());
            segmentOpen = true;
          }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNodeLayoutVersion.java

  public static final int CURRENT_LAYOUT_VERSION
      = LayoutVersion.getCurrentLayoutVersion(Feature.values());
  public static final int MINIMUM_COMPATIBLE_LAYOUT_VERSION
      = LayoutVersion.getMinimumCompatibleLayoutVersion(Feature.values());

  static {
    LayoutVersion.updateMap(FEATURES, LayoutVersion.Feature.values());
  public static enum Feature implements LayoutFeature {
    ROLLING_UPGRADE(-55, -53, -55, "Support rolling upgrade", false),
    EDITLOG_LENGTH(-56, -56, "Add length field to every edit log op"),
    XATTRS(-57, -57, "Extended attributes"),
    CREATE_OVERWRITE(-58, -58, "Use single editlog record for " +
      "creating file with overwrite"),
    XATTRS_NAMESPACE_EXT(-59, -59, "Increase number of xattr namespaces"),
    BLOCK_STORAGE_POLICY(-60, -60, "Block Storage policy"),
    TRUNCATE(-61, -61, "Truncate"),
    APPEND_NEW_BLOCK(-62, -61, "Support appending to new block"),
    QUOTA_BY_STORAGE_TYPE(-63, -61, "Support quota for specific storage types");

    private final FeatureInfo info;

    Feature(final int lv, int minCompatLV, final String description) {
      this(lv, lv + 1, minCompatLV, description, false);
    }

    Feature(final int lv, final int ancestorLV, int minCompatLV,
        final String description, boolean reserved, Feature... features) {
      info = new FeatureInfo(lv, ancestorLV, minCompatLV, description, reserved,
          features);
    }
    
    @Override

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/SecondaryNameNode.java
    Checkpointer.rollForwardByApplyingLogs(manifest, dstImage, dstNamesystem);
    dstImage.saveFSImageInAllDirs(dstNamesystem, dstImage.getLastAppliedTxId());
    if (!dstNamesystem.isRollingUpgrade()) {
      dstStorage.writeAll();
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/protocol/TestLayoutVersion.java
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
        first.getInfo().getLayoutVersion());
  }

  @Test
  public void testNameNodeFeatureMinimumCompatibleLayoutVersions() {
    int baseLV = NameNodeLayoutVersion.Feature.TRUNCATE.getInfo()
        .getLayoutVersion();
    EnumSet<NameNodeLayoutVersion.Feature> compatibleFeatures = EnumSet.of(
        NameNodeLayoutVersion.Feature.TRUNCATE,
        NameNodeLayoutVersion.Feature.APPEND_NEW_BLOCK,
        NameNodeLayoutVersion.Feature.QUOTA_BY_STORAGE_TYPE);
    for (LayoutFeature f : compatibleFeatures) {
      assertEquals(String.format("Expected minimum compatible layout version " +
          "%d for feature %s.", baseLV, f), baseLV,
          f.getInfo().getMinimumCompatibleLayoutVersion());
    }
    List<LayoutFeature> features = new ArrayList<>();
    features.addAll(EnumSet.allOf(LayoutVersion.Feature.class));
    features.addAll(EnumSet.allOf(NameNodeLayoutVersion.Feature.class));
    for (LayoutFeature f : features) {
      if (!compatibleFeatures.contains(f)) {
        assertEquals(String.format("Expected feature %s to have minimum " +
            "compatible layout version set to itself.", f),
            f.getInfo().getLayoutVersion(),
            f.getInfo().getMinimumCompatibleLayoutVersion());
      }
    }
  }

  @Test
  public void testNameNodeFeatureMinimumCompatibleLayoutVersionAscending() {
    LayoutFeature prevF = null;
    for (LayoutFeature f : EnumSet.allOf(NameNodeLayoutVersion.Feature.class)) {
      if (prevF != null) {
        assertTrue(String.format("Features %s and %s not listed in order of " +
            "minimum compatible layout version.", prevF, f),
            f.getInfo().getMinimumCompatibleLayoutVersion() <=
            prevF.getInfo().getMinimumCompatibleLayoutVersion());
      } else {
        prevF = f;
      }
    }
  }

  @Test(expected=AssertionError.class)
  public void testNameNodeFeatureMinimumCompatibleLayoutVersionOutOfOrder() {
    FeatureInfo ancestorF = LayoutVersion.Feature.RESERVED_REL2_4_0.getInfo();
    LayoutFeature f = mock(LayoutFeature.class);
    when(f.getInfo()).thenReturn(new FeatureInfo(
        ancestorF.getLayoutVersion() - 1, ancestorF.getLayoutVersion(),
        ancestorF.getMinimumCompatibleLayoutVersion() + 1, "Invalid feature.",
        false));
    Map<Integer, SortedSet<LayoutFeature>> features = new HashMap<>();
    LayoutVersion.updateMap(features, LayoutVersion.Feature.values());
    LayoutVersion.updateMap(features, new LayoutFeature[] { f });
  }

  @Test
  public void testCurrentMinimumCompatibleLayoutVersion() {
    int expectedMinCompatLV = NameNodeLayoutVersion.Feature.TRUNCATE.getInfo()
        .getLayoutVersion();
    int actualMinCompatLV = LayoutVersion.getMinimumCompatibleLayoutVersion(
        NameNodeLayoutVersion.Feature.values());
    assertEquals("The minimum compatible layout version has changed.  " +
        "Downgrade to prior versions is no longer possible.  Please either " +
        "restore compatibility, or if the incompatibility is intentional, " +
        "then update this assertion.", expectedMinCompatLV, actualMinCompatLV);
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/CreateEditsLog.java

    FileNameGenerator nameGenerator = new FileNameGenerator(BASE_PATH, 100);
    FSEditLog editLog = FSImageTestUtil.createStandaloneEditLog(editsLogDir);
    editLog.openForWrite(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    addFiles(editLog, numFiles, replication, numBlocksPerFile, startingBlockId,
             blockSize, nameGenerator);
    editLog.logSync();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/FSImageTestUtil.java
      long firstTxId, long newInodeId) throws IOException {
    FSEditLog editLog = FSImageTestUtil.createStandaloneEditLog(editsLogDir);
    editLog.setNextTxId(firstTxId);
    editLog.openForWrite(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    
    PermissionStatus perms = PermissionStatus.createImmutable("fakeuser", "fakegroup",
        FsPermission.createImmutable((short)0755));

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestEditLog.java
      editLog.logSetReplication("fakefile", (short) 1);
      editLog.logSync();
      
      editLog.rollEditLog(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);

      assertExistsInStorageDirs(
          cluster, NameNodeDirType.EDITS,
      
      fsimage.rollEditLog(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    
      trans.run();

      fsimage.rollEditLog(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      
      long expectedTxns = ((NUM_THREADS+1) * 2 * NUM_TRANSACTIONS) + 2; // +2 for start/end txns
   
    FSEditLog log = FSImageTestUtil.createStandaloneEditLog(logDir);
    try {
      FileUtil.setWritable(logDir, false);
      log.openForWrite(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      fail("Did no throw exception on only having a bad dir");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
        new byte[500]);
    
    try {
      log.openForWrite(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      NameNodeMetrics mockMetrics = Mockito.mock(NameNodeMetrics.class);
      log.setMetricsForTests(mockMetrics);

    editlog.initJournalsForWrite();
    editlog.openForWrite(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    for (int i = 2; i < TXNS_PER_ROLL; i++) {
      editlog.logGenerationStampV2((long) 0);
    }
    for (int i = 0; i < numrolls; i++) {
      editlog.rollEditLog(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      
      editlog.logGenerationStampV2((long) i);
      editlog.logSync();
            cluster, NameNodeDirType.EDITS,
            NNStorage.getInProgressEditsFileName((i * 3) + 1));
        editLog.logSync();
        editLog.rollEditLog(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
        assertExistsInStorageDirs(
            cluster, NameNodeDirType.EDITS,
            NNStorage.getFinalizedEditsFileName((i * 3) + 1, (i * 3) + 3));

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFSEditLogLoader.java
      doNothing().when(spyLog).endCurrentLogSegment(true);
      spyLog.openForWrite(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      assertTrue("should exist: " + inProgressFile, inProgressFile.exists());
      
      for (int i = 0; i < numTx; i++) {

