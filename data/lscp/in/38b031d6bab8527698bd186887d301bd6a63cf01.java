hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/NamenodeProtocolServerSideTranslatorPB.java
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.VersionRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.VersionResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.EndCheckpointRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.EndCheckpointResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.ErrorReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetMostRecentCheckpointTxIdResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetTransactionIdRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetTransactionIdResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.IsUpgradeFinalizedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.IsUpgradeFinalizedResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RegisterRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RegisterResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RollEditLogRequestProto;
    return VersionResponseProto.newBuilder()
        .setInfo(PBHelper.convert(info)).build();
  }

  @Override
  public IsUpgradeFinalizedResponseProto isUpgradeFinalized(
      RpcController controller, IsUpgradeFinalizedRequestProto request)
      throws ServiceException {
    boolean isUpgradeFinalized;
    try {
      isUpgradeFinalized = impl.isUpgradeFinalized();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return IsUpgradeFinalizedResponseProto.newBuilder()
        .setIsUpgradeFinalized(isUpgradeFinalized).build();
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/NamenodeProtocolTranslatorPB.java
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamenodeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.VersionRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.EndCheckpointRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.ErrorReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlockKeysRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetMostRecentCheckpointTxIdRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetTransactionIdRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.IsUpgradeFinalizedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.IsUpgradeFinalizedResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RegisterRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RollEditLogRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.StartCheckpointRequestProto;
        RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(NamenodeProtocolPB.class), methodName);
  }

  @Override
  public boolean isUpgradeFinalized() throws IOException {
    IsUpgradeFinalizedRequestProto req = IsUpgradeFinalizedRequestProto
        .newBuilder().build();
    try {
      IsUpgradeFinalizedResponseProto response = rpcProxy.isUpgradeFinalized(
          NULL_CONTROLLER, req);
      return response.getIsUpgradeFinalized();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImage.java
    Map<StorageDirectory, StorageState> dataDirStates = 
             new HashMap<StorageDirectory, StorageState>();
    boolean isFormatted = recoverStorageDirs(startOpt, storage, dataDirStates);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Data dir states:\n  " +
  public static boolean recoverStorageDirs(StartupOption startOpt,
      NNStorage storage, Map<StorageDirectory, StorageState> dataDirStates)
      throws IOException {
    boolean isFormatted = false;
  }

  public static void checkUpgrade(NNStorage storage) throws IOException {
    for (Iterator<StorageDirectory> it = storage.dirIterator(false); it.hasNext();) {
    }
  }

  void checkUpgrade() throws IOException {
    checkUpgrade(storage);
  }

  }

  void doUpgrade(FSNamesystem target) throws IOException {
    checkUpgrade();

    this.loadFSImage(target, StartupOption.UPGRADE, null);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
  void startRollingUpgradeInternal(long startTime)
      throws IOException {
    checkRollingUpgrade("start rolling upgrade");
    getFSImage().checkUpgrade();
    setRollingUpgradeInfo(false, startTime);
  }

          + "in order to create namespace image.");
    }
    checkRollingUpgrade("start rolling upgrade");
    getFSImage().checkUpgrade();
    getFSImage().saveNamespace(this, NameNodeFile.IMAGE_ROLLBACK, null);
    LOG.info("Successfully saved namespace for preparing rolling upgrade.");

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NNStorage.java
  }

  public void setBlockPoolID(String bpid) {
    blockpoolID = bpid;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NNUpgradeUtil.java
import com.google.common.base.Preconditions;
import org.apache.hadoop.io.IOUtils;

public abstract class NNUpgradeUtil {
  
  private static final Log LOG = LogFactory.getLog(NNUpgradeUtil.class);
  
  static void doPreUpgrade(Configuration conf, StorageDirectory sd)
      throws IOException {
    LOG.info("Starting upgrade of storage directory " + sd.getRoot());

    renameCurToTmp(sd);

    final File curDir = sd.getCurrentDir();
    final File tmpDir = sd.getPreviousTmp();
    List<String> fileNameList = IOUtils.listDirectory(tmpDir, new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
    }
  }

  public static void renameCurToTmp(StorageDirectory sd) throws IOException {
    File curDir = sd.getCurrentDir();
    File prevDir = sd.getPreviousDir();
    final File tmpDir = sd.getPreviousTmp();

    Preconditions.checkState(curDir.exists(),
        "Current directory must exist for preupgrade.");
    Preconditions.checkState(!prevDir.exists(),
        "Previous directory must not exist for preupgrade.");
    Preconditions.checkState(!tmpDir.exists(),
        "Previous.tmp directory must not exist for preupgrade."
            + "Consider restarting for recovery.");

    NNStorage.rename(curDir, tmpDir);

    if (!curDir.mkdir()) {
      throw new IOException("Cannot create directory " + curDir);
    }
  }
  
  public static void doUpgrade(StorageDirectory sd, Storage storage)
      throws IOException {
    LOG.info("Performing upgrade of storage directory " + sd.getRoot());
    try {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNodeRpcServer.java
    return namesystem.getEditLog().getEditLogManifest(sinceTxId);
  }

  @Override // NamenodeProtocol
  public boolean isUpgradeFinalized() throws IOException {
    checkNNStartup();
    namesystem.checkSuperuserPrivilege();
    return namesystem.isUpgradeFinalized();
  }
    
  @Override // ClientProtocol
  public void finalizeUpgrade() throws IOException {
    checkNNStartup();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/ha/BootstrapStandby.java
import java.net.URL;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NNUpgradeUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
  }
  
  private int doRun() throws IOException {
    NamenodeProtocol proxy = createNNProtocolProxy();
    NamespaceInfo nsInfo;
    boolean isUpgradeFinalized;
    try {
      nsInfo = proxy.versionRequest();
      isUpgradeFinalized = proxy.isUpgradeFinalized();
    } catch (IOException ioe) {
      LOG.fatal("Unable to fetch namespace information from active NN at " +
          otherIpcAddr + ": " + ioe.getMessage());
        "            Block pool ID: " + nsInfo.getBlockPoolID() + "\n" +
        "               Cluster ID: " + nsInfo.getClusterID() + "\n" +
        "           Layout version: " + nsInfo.getLayoutVersion() + "\n" +
        "       isUpgradeFinalized: " + isUpgradeFinalized + "\n" +
        "=====================================================");
    
    NNStorage storage = new NNStorage(conf, dirsToFormat, editUrisToFormat);

    if (!isUpgradeFinalized) {
      LOG.info("The active NameNode is in Upgrade. " +
          "Prepare the upgrade for the standby NameNode as well.");
      if (!doPreUpgrade(storage, nsInfo)) {
        return ERR_CODE_ALREADY_FORMATTED;
      }
    } else if (!format(storage, nsInfo)) { // prompt the user to format storage
      return ERR_CODE_ALREADY_FORMATTED;
    }

    int download = downloadImage(storage, proxy);
    if (download != 0) {
      return download;
    }

    if (!isUpgradeFinalized) {
      doUpgrade(storage);
    }
    return 0;
  }

  private boolean format(NNStorage storage, NamespaceInfo nsInfo)
      throws IOException {
    if (!Storage.confirmFormat(storage.dirIterable(null), force, interactive)) {
      storage.close();
      return false;
    } else {
      storage.format(nsInfo);
      return true;
    }
  }

  private boolean doPreUpgrade(NNStorage storage, NamespaceInfo nsInfo)
      throws IOException {
    boolean isFormatted = false;
    Map<StorageDirectory, StorageState> dataDirStates =
        new HashMap<>();
    try {
      isFormatted = FSImage.recoverStorageDirs(StartupOption.UPGRADE, storage,
          dataDirStates);
      if (dataDirStates.values().contains(StorageState.NOT_FORMATTED)) {
        isFormatted = false;
        System.err.println("The original storage directory is not formatted.");
      }
    } catch (InconsistentFSStateException e) {
      LOG.warn("The storage directory is in an inconsistent state", e);
    } finally {
      storage.unlockAll();
    }

    if (!isFormatted && !format(storage, nsInfo)) {
      return false;
    }

    FSImage.checkUpgrade(storage);
    for (Iterator<StorageDirectory> it = storage.dirIterator(false);
         it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        NNUpgradeUtil.renameCurToTmp(sd);
      } catch (IOException e) {
        LOG.error("Failed to move aside pre-upgrade storage " +
            "in image directory " + sd.getRoot(), e);
        throw e;
      }
    }
    storage.setStorageInfo(nsInfo);
    storage.setBlockPoolID(nsInfo.getBlockPoolID());
    return true;
  }

  private void doUpgrade(NNStorage storage) throws IOException {
    for (Iterator<StorageDirectory> it = storage.dirIterator(false);
         it.hasNext();) {
      StorageDirectory sd = it.next();
      NNUpgradeUtil.doUpgrade(sd, storage);
    }
  }

  private int downloadImage(NNStorage storage, NamenodeProtocol proxy)
      throws IOException {
    final long imageTxId = proxy.getMostRecentCheckpointTxId();
    final long curTxId = proxy.getTransactionID();
    FSImage image = new FSImage(conf);
    try {
      image.getStorage().setStorageInfo(storage);

      if (!skipSharedEditsCheck &&
          !checkLogsAvailableForRead(image, imageTxId, curTxId)) {
        return ERR_CODE_LOGS_UNAVAILABLE;
      }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/protocol/NamenodeProtocol.java
  @Idempotent
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
    throws IOException;

  @Idempotent
  public boolean isUpgradeFinalized() throws IOException;

}


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestBootstrapStandbyWithQJM.java
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import org.mockito.internal.util.reflection.Whitebox;

public class TestBootstrapStandbyWithQJM {
  enum UpgradeState {
    NORMAL,
    RECOVER,
    FORMAT
  }

  private MiniDFSCluster cluster;
  private MiniJournalCluster jCluster;
  
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        0);

    MiniQJMHACluster miniQjmHaCluster = new MiniQJMHACluster.Builder(conf).build();
    cluster = miniQjmHaCluster.getDfsCluster();
    jCluster = miniQjmHaCluster.getJournalCluster();
    
        ImmutableList.of(0));
    FSImageTestUtil.assertNNFilesMatch(cluster);
  }

  @Test
  public void testUpgrade() throws Exception {
    testUpgrade(UpgradeState.NORMAL);
  }

  @Test
  public void testUpgradeWithRecover() throws Exception {
    testUpgrade(UpgradeState.RECOVER);
  }

  @Test
  public void testUpgradeWithFormat() throws Exception {
    testUpgrade(UpgradeState.FORMAT);
  }

  private void testUpgrade(UpgradeState state) throws Exception {
    cluster.transitionToActive(0);
    final Configuration confNN1 = cluster.getConfiguration(1);

    final File current = cluster.getNameNode(1).getFSImage().getStorage()
        .getStorageDir(0).getCurrentDir();
    final File tmp = cluster.getNameNode(1).getFSImage().getStorage()
        .getStorageDir(0).getPreviousTmp();
    cluster.shutdownNameNode(1);

    FSImage fsImage0 = cluster.getNameNode(0).getNamesystem().getFSImage();
    Whitebox.setInternalState(fsImage0, "isUpgradeFinalized", false);

    switch (state) {
      case RECOVER:
        NNStorage.rename(current, tmp);
        break;
      case FORMAT:
        final File wrongPath = new File(current.getParentFile(), "wrong");
        NNStorage.rename(current, wrongPath);
        break;
      default:
        break;
    }

    int rc = BootstrapStandby.run(new String[] { "-force" }, confNN1);
    assertEquals(0, rc);

    FSImageTestUtil.assertNNHasCheckpoints(cluster, 1,
        ImmutableList.of(0));
    FSImageTestUtil.assertNNFilesMatch(cluster);

    cluster.restartNameNode(1);
    assertFalse(cluster.getNameNode(1).getNamesystem().isUpgradeFinalized());
  }

}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestDFSUpgradeWithHA.java
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster.Builder;
import org.apache.hadoop.hdfs.qjournal.server.Journal;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
      cluster.restartNameNode(1);
      
      checkNnPreviousDirExistence(cluster, 0, true);
      checkNnPreviousDirExistence(cluster, 1, true);
      checkPreviousDirExistence(sharedDir, true);
      assertCTimesEqual(cluster);
      
      cluster.restartNameNode(1);
      
      checkNnPreviousDirExistence(cluster, 0, true);
      checkNnPreviousDirExistence(cluster, 1, true);
      checkJnPreviousDirExistence(qjCluster, true);
      assertCTimesEqual(cluster);
      

