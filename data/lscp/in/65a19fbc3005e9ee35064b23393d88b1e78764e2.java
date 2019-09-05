hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/HAUtilClient.java

import java.net.URI;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HA_DT_SERVICE_PREFIX;

@InterfaceAudience.Private
public class HAUtilClient {

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/Block.java
  public static long getGenerationStamp(String metaFile) {
    Matcher m = metaFilePattern.matcher(metaFile);
    return m.matches() ? Long.parseLong(m.group(2))
        : HdfsConstants.GRANDFATHER_GENERATION_STAMP;
  }

  }

  public Block(final long blkid) {
    this(blkid, 0, HdfsConstants.GRANDFATHER_GENERATION_STAMP);
  }

  public Block(Block blk) {

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/HdfsConstants.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/HdfsConstants.java
package org.apache.hadoop.hdfs.protocol;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

@InterfaceAudience.Private
public final class HdfsConstants {
  public static final long QUOTA_DONT_SET = Long.MAX_VALUE;
  public static final long QUOTA_RESET = -1L;
  public static final int BYTES_IN_INTEGER = Integer.SIZE / Byte.SIZE;
  public static final String HDFS_URI_SCHEME = "hdfs";
  public static final String MEMORY_STORAGE_POLICY_NAME = "LAZY_PERSIST";
  public static final String ALLSSD_STORAGE_POLICY_NAME = "ALL_SSD";
  public static final String ONESSD_STORAGE_POLICY_NAME = "ONE_SSD";
  public static final int DEFAULT_DATA_SOCKET_SIZE = 128 * 1024;
  public static final String DOT_SNAPSHOT_DIR = ".snapshot";
  public static final String SEPARATOR_DOT_SNAPSHOT_DIR
          = Path.SEPARATOR + DOT_SNAPSHOT_DIR;
  public static final String SEPARATOR_DOT_SNAPSHOT_DIR_SEPARATOR
      = Path.SEPARATOR + DOT_SNAPSHOT_DIR + Path.SEPARATOR;

  public static final long GRANDFATHER_GENERATION_STAMP = 0;
  public static final long GRANDFATHER_INODE_ID = 0;
  public static final byte BLOCK_STORAGE_POLICY_ID_UNSPECIFIED = 0;
  public static final String HA_DT_SERVICE_PREFIX = "ha-";
  public static final String SAFEMODE_EXCEPTION_CLASS_NAME =
      "org.apache.hadoop.hdfs.server.namenode.SafeModeException";
  public static final String CLIENT_NAMENODE_PROTOCOL_NAME =
      "org.apache.hadoop.hdfs.protocol.ClientProtocol";

  public enum SafeModeAction {
    SAFEMODE_LEAVE, SAFEMODE_ENTER, SAFEMODE_GET
  }

  public enum RollingUpgradeAction {
    QUERY, PREPARE, FINALIZE;

    private static final Map<String, RollingUpgradeAction> MAP
        = new HashMap<>();
    static {
      MAP.put("", QUERY);
      for(RollingUpgradeAction a : values()) {
        MAP.put(a.name(), a);
      }
    }

    public static RollingUpgradeAction fromString(String s) {
      return MAP.get(StringUtils.toUpperCase(s));
    }
  }

  public enum DatanodeReportType {
    ALL, LIVE, DEAD, DECOMMISSIONING
  }

  protected HdfsConstants() {
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/JsonUtilClient.java
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.FsPermissionExtension;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
    final long blockSize = ((Number) m.get("blockSize")).longValue();
    final short replication = ((Number) m.get("replication")).shortValue();
    final long fileId = m.containsKey("fileId") ?
        ((Number) m.get("fileId")).longValue() : HdfsConstants.GRANDFATHER_INODE_ID;
    final int childrenNum = getInt(m, "childrenNum", -1);
    final byte storagePolicy = m.containsKey("storagePolicy") ?
        (byte) ((Number) m.get("storagePolicy")).longValue() :
        HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
    return new HdfsFileStatus(len, type == WebHdfsConstants.PathType.DIRECTORY, replication,
        blockSize, mTime, aTime, permission, owner, group,
        symlink, DFSUtilClient.string2Bytes(localName),

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsFileSystem.java
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.resources.*;
              HdfsClientConfigKeys.HttpClient.RETRY_POLICY_ENABLED_DEFAULT,
              HdfsClientConfigKeys.HttpClient.RETRY_POLICY_SPEC_KEY,
              HdfsClientConfigKeys.HttpClient.RETRY_POLICY_SPEC_DEFAULT,
              HdfsConstants.SAFEMODE_EXCEPTION_CLASS_NAME);
    } else {

      int maxFailoverAttempts = conf.getInt(

hadoop-hdfs-project/hadoop-hdfs-nfs/src/main/java/org/apache/hadoop/hdfs/nfs/nfs3/RpcProgramNfs3.java
import org.apache.hadoop.hdfs.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.DNS;
      }

      return new PATHCONF3Response(Nfs3Status.NFS3_OK, attrs, 0,
          HdfsServerConstants.MAX_PATH_LENGTH, true, false, false, true);
    } catch (IOException e) {
      LOG.warn("Exception ", e);
      int status = mapErrorStatus(e);

hadoop-hdfs-project/hadoop-hdfs/src/contrib/bkjournal/src/main/java/org/apache/hadoop/contrib/bkjournal/BookKeeperJournalManager.java
package org.apache.hadoop.contrib.bkjournal;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
          return;
        }
        streams.add(elis);
        if (elis.getLastTxId() == HdfsServerConstants.INVALID_TXID) {
          return;
        }
        fromTxId = elis.getLastTxId() + 1;
      long lastTxId = l.getLastTxId();
      if (l.isInProgress()) {
        lastTxId = recoverLastTxId(l, false);
        if (lastTxId == HdfsServerConstants.INVALID_TXID) {
          break;
        }
      }
          EditLogLedgerMetadata l = EditLogLedgerMetadata.read(zkc, znode);
          try {
            long endTxId = recoverLastTxId(l, true);
            if (endTxId == HdfsServerConstants.INVALID_TXID) {
              LOG.error("Unrecoverable corruption has occurred in segment "
                  + l.toString() + " at path " + znode
                  + ". Unable to continue recovery.");

      in = new BookKeeperEditLogInputStream(lh, l, lastAddConfirmed);

      long endTxId = HdfsServerConstants.INVALID_TXID;
      FSEditLogOp op = in.readOp();
      while (op != null) {
        if (endTxId == HdfsServerConstants.INVALID_TXID
            || op.getTransactionId() == endTxId+1) {
          endTxId = op.getTransactionId();
        }
        try {
          EditLogLedgerMetadata editLogLedgerMetadata = EditLogLedgerMetadata
              .read(zkc, legderMetadataPath);
          if (editLogLedgerMetadata.getLastTxId() != HdfsServerConstants.INVALID_TXID
              && editLogLedgerMetadata.getLastTxId() < fromTxId) {

hadoop-hdfs-project/hadoop-hdfs/src/contrib/bkjournal/src/main/java/org/apache/hadoop/contrib/bkjournal/EditLogLedgerMetadata.java

import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

    this.dataLayoutVersion = dataLayoutVersion;
    this.ledgerId = ledgerId;
    this.firstTxId = firstTxId;
    this.lastTxId = HdfsServerConstants.INVALID_TXID;
    this.inprogress = true;
  }
  
  }

  void finalizeLedger(long newLastTxId) {
    assert this.lastTxId == HdfsServerConstants.INVALID_TXID;
    this.lastTxId = newLastTxId;
    this.inprogress = false;      
  }

hadoop-hdfs-project/hadoop-hdfs/src/contrib/bkjournal/src/test/java/org/apache/hadoop/contrib/bkjournal/TestBookKeeperEditLogStreams.java
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
      lh.close();

      EditLogLedgerMetadata metadata = new EditLogLedgerMetadata("/foobar",
          HdfsServerConstants.NAMENODE_LAYOUT_VERSION, lh.getId(), 0x1234);
      try {
        new BookKeeperEditLogInputStream(lh, metadata, -1);
        fail("Shouldn't get this far, should have thrown");
      }

      metadata = new EditLogLedgerMetadata("/foobar",
          HdfsServerConstants.NAMENODE_LAYOUT_VERSION, lh.getId(), 0x1234);
      try {
        new BookKeeperEditLogInputStream(lh, metadata, 0);
        fail("Shouldn't get this far, should have thrown");

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
      } catch (IOException e) {
        final long elapsed = Time.monotonicNow() - getLastLeaseRenewal();
        if (elapsed > HdfsServerConstants.LEASE_HARDLIMIT_PERIOD) {
          LOG.warn("Failed to renew lease for " + clientName + " for "
              + (elapsed/1000) + " seconds (>= hard-limit ="
              + (HdfsServerConstants.LEASE_HARDLIMIT_PERIOD/1000) + " seconds.) "
              + "Closing all files being written ...", e);
          closeAllFilesBeingWritten(true);
        } else {
          IOStreamPair pair = connectToDN(datanodes[j], timeout, lb);
          out = new DataOutputStream(new BufferedOutputStream(pair.out,
              HdfsServerConstants.SMALL_BUFFER_SIZE));
          in = new DataInputStream(pair.in);

          if (LOG.isDebugEnabled()) {

    try {
      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(pair.out,
          HdfsServerConstants.SMALL_BUFFER_SIZE));
      DataInputStream in = new DataInputStream(pair.in);
  
      new Sender(out).readBlock(lb.getBlock(), lb.getBlockToken(), clientName,

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSUtil.java
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocolPB.ClientDatanodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.annotations.VisibleForTesting;
  public static boolean isReservedPathComponent(String component) {
    for (String reserved : HdfsServerConstants.RESERVED_PATH_COMPONENTS) {
      if (component.equals(reserved)) {
        return true;
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DataStreamer.java
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
      unbufOut = saslStreams.out;
      unbufIn = saslStreams.in;
      out = new DataOutputStream(new BufferedOutputStream(unbufOut,
          HdfsServerConstants.SMALL_BUFFER_SIZE));
      in = new DataInputStream(unbufIn);

        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            HdfsServerConstants.SMALL_BUFFER_SIZE));
        blockReplyStream = new DataInputStream(unbufIn);


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/NameNodeProxies.java
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.server.namenode.ha.AbstractNNFailoverProxyProvider;

      RetryPolicy createPolicy = RetryPolicies
          .retryUpToMaximumCountWithFixedSleep(5,
              HdfsServerConstants.LEASE_SOFTLIMIT_PERIOD, TimeUnit.MILLISECONDS);
    
      Map<Class<? extends Exception>, RetryPolicy> remoteExceptionToPolicyMap 
                 = new HashMap<Class<? extends Exception>, RetryPolicy>();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/client/impl/LeaseRenewer.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
  private long emptyTime = Long.MAX_VALUE;
  private long renewal = HdfsServerConstants.LEASE_SOFTLIMIT_PERIOD/2;

  private Daemon daemon = null;

    if (renewal == dfsc.getConf().getHdfsTimeout()/2) {
      long min = HdfsServerConstants.LEASE_SOFTLIMIT_PERIOD;
      for(DFSClient c : dfsclients) {
        final int timeout = c.getConf().getHdfsTimeout();
        if (timeout > 0 && timeout < min) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocol/ClientProtocol.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocol/SnapshottableDirectoryStatus.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/ClientNamenodeProtocolPB.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/ClientNamenodeProtocolServerSideTranslatorPB.java
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
      boolean result = 
          server.complete(req.getSrc(), req.getClientName(),
          req.hasLast() ? PBHelper.convert(req.getLast()) : null,
          req.hasFileId() ? req.getFileId() : HdfsConstants.GRANDFATHER_INODE_ID);
      return CompleteResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/PBHelper.java
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdfs.protocol.FsPermissionExtension;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
        fs.getFileType().equals(FileType.IS_SYMLINK) ? 
            fs.getSymlink().toByteArray() : null,
        fs.getPath().toByteArray(),
        fs.hasFileId()? fs.getFileId(): HdfsConstants.GRANDFATHER_INODE_ID,
        fs.hasLocations() ? PBHelper.convert(fs.getLocations()) : null,
        fs.hasChildrenNum() ? fs.getChildrenNum() : -1,
        fs.hasFileEncryptionInfo() ? convert(fs.getFileEncryptionInfo()) : null,
        fs.hasStoragePolicy() ? (byte) fs.getStoragePolicy()
            : HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED);
  }

  public static SnapshottableDirectoryStatus convert(

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/qjournal/client/IPCLoggerChannel.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalOutOfSyncException;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolPB;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolTranslatorPB;
import org.apache.hadoop.hdfs.qjournal.server.GetJournalEditServlet;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
  private final ListeningExecutorService parallelExecutor;
  private long ipcSerial = 0;
  private long epoch = -1;
  private long committedTxId = HdfsServerConstants.INVALID_TXID;
  
  private final String journalId;
  private final NamespaceInfo nsInfo;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/qjournal/protocol/RequestInfo.java
package org.apache.hadoop.hdfs.qjournal.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

@InterfaceAudience.Private
public class RequestInfo {
  }

  public boolean hasCommittedTxId() {
    return (committedTxId != HdfsServerConstants.INVALID_TXID);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/qjournal/protocolPB/QJournalProtocolServerSideTranslatorPB.java
import java.net.URL;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.StartLogSegmentRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.StartLogSegmentResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
        reqInfo.getEpoch(),
        reqInfo.getIpcSerialNumber(),
        reqInfo.hasCommittedTxId() ?
          reqInfo.getCommittedTxId() : HdfsServerConstants.INVALID_TXID);
  }

  @Override

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/qjournal/server/Journal.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalNotFormattedException;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalOutOfSyncException;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.StorageInfo;

  private EditLogOutputStream curSegment;
  private long curSegmentTxId = HdfsServerConstants.INVALID_TXID;
  private long nextTxId = HdfsServerConstants.INVALID_TXID;
  private long highestWrittenTxId = 0;
  
  private final String journalId;
        new File(currentDir, LAST_WRITER_EPOCH), 0);
    this.committedTxnId = new BestEffortLongFile(
        new File(currentDir, COMMITTED_TXID_FILENAME),
        HdfsServerConstants.INVALID_TXID);
  }
  
      EditLogFile latestLog = files.remove(files.size() - 1);
      latestLog.scanLog();
      LOG.info("Latest log is " + latestLog);
      if (latestLog.getLastTxId() == HdfsServerConstants.INVALID_TXID) {
        LOG.warn("Latest log " + latestLog + " has no transactions. " +
            "moving it aside and looking for previous log");
    
    curSegment.abort();
    curSegment = null;
    curSegmentTxId = HdfsServerConstants.INVALID_TXID;
  }

      if (curSegment != null) {
        curSegment.close();
        curSegment = null;
        curSegmentTxId = HdfsServerConstants.INVALID_TXID;
      }
      
      checkSync(nextTxId == endTxId + 1,
    if (elf.isInProgress()) {
      elf.scanLog();
    }
    if (elf.getLastTxId() == HdfsServerConstants.INVALID_TXID) {
      LOG.info("Edit log file " + elf + " appears to be empty. " +
          "Moving it aside...");
      elf.moveAsideEmptyFile();
    }
    
    builder.setLastWriterEpoch(lastWriterEpoch.get());
    if (committedTxnId.get() != HdfsServerConstants.INVALID_TXID) {
      builder.setLastCommittedTxId(committedTxnId.get());
    }
    
        new File(previousDir, LAST_WRITER_EPOCH), 0);
    BestEffortLongFile prevCommittedTxnId = new BestEffortLongFile(
        new File(previousDir, COMMITTED_TXID_FILENAME),
        HdfsServerConstants.INVALID_TXID);

    lastPromisedEpoch = new PersistentLongFile(
        new File(currentDir, LAST_PROMISED_FILENAME), 0);
        new File(currentDir, LAST_WRITER_EPOCH), 0);
    committedTxnId = new BestEffortLongFile(
        new File(currentDir, COMMITTED_TXID_FILENAME),
        HdfsServerConstants.INVALID_TXID);

    try {
      lastPromisedEpoch.set(prevLastPromisedEpoch.get());

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/balancer/Dispatcher.java
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            HdfsServerConstants.IO_FILE_BUFFER_SIZE));
        in = new DataInputStream(new BufferedInputStream(unbufIn,
            HdfsServerConstants.IO_FILE_BUFFER_SIZE));

        sendRequest(out, eb, accessToken);
        receiveResponse(in);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockIdManager.java
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

import java.io.IOException;

  private final SequentialBlockIdGenerator blockIdGenerator;

  public BlockIdManager(BlockManager blockManager) {
    this.generationStampV1Limit = HdfsConstants.GRANDFATHER_GENERATION_STAMP;
    this.blockIdGenerator = new SequentialBlockIdGenerator(blockManager);
  }

    Preconditions.checkState(generationStampV2.getCurrentValue() ==
      GenerationStamp.LAST_RESERVED_STAMP);
    generationStampV2.skipTo(generationStampV1.getCurrentValue() +
      HdfsServerConstants.RESERVED_GENERATION_STAMPS_V1);

    generationStampV1Limit = generationStampV2.getCurrentValue();
    return generationStampV2.getCurrentValue();
  public void setGenerationStampV1Limit(long stamp) {
    Preconditions.checkState(generationStampV1Limit == HdfsConstants
      .GRANDFATHER_GENERATION_STAMP);
    generationStampV1Limit = stamp;
  }
    generationStampV2.setCurrentValue(GenerationStamp.LAST_RESERVED_STAMP);
    getBlockIdGenerator().setCurrentValue(SequentialBlockIdGenerator
      .LAST_RESERVED_BLOCK_ID);
    generationStampV1Limit = HdfsConstants.GRANDFATHER_GENERATION_STAMP;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockPlacementPolicyDefault.java
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
      }
    }
    
    final long requiredSize = blockSize * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE;
    final long scheduledSize = blockSize * node.getBlocksScheduled(storage.getStorageType());
    final long remaining = node.getRemaining(storage.getStorageType());
    if (requiredSize > remaining - scheduledSize) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockStoragePolicySuite.java
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
  public static BlockStoragePolicySuite createDefaultSuite() {
    final BlockStoragePolicy[] policies =
        new BlockStoragePolicy[1 << ID_BIT_LENGTH];
    final byte lazyPersistId = HdfsServerConstants.MEMORY_STORAGE_POLICY_ID;
    policies[lazyPersistId] = new BlockStoragePolicy(lazyPersistId,
        HdfsConstants.MEMORY_STORAGE_POLICY_NAME,
        new StorageType[]{StorageType.RAM_DISK, StorageType.DISK},
        new StorageType[]{StorageType.DISK},
        new StorageType[]{StorageType.DISK},
        true);    // Cannot be changed on regular files, but inherited.
    final byte allssdId = HdfsServerConstants.ALLSSD_STORAGE_POLICY_ID;
    policies[allssdId] = new BlockStoragePolicy(allssdId,
        HdfsConstants.ALLSSD_STORAGE_POLICY_NAME,
        new StorageType[]{StorageType.SSD},
        new StorageType[]{StorageType.DISK},
        new StorageType[]{StorageType.DISK});
    final byte onessdId = HdfsServerConstants.ONESSD_STORAGE_POLICY_ID;
    policies[onessdId] = new BlockStoragePolicy(onessdId,
        HdfsConstants.ONESSD_STORAGE_POLICY_NAME,
        new StorageType[]{StorageType.SSD, StorageType.DISK},
        new StorageType[]{StorageType.SSD, StorageType.DISK},
        new StorageType[]{StorageType.SSD, StorageType.DISK});
    final byte hotId = HdfsServerConstants.HOT_STORAGE_POLICY_ID;
    policies[hotId] = new BlockStoragePolicy(hotId,
        HdfsServerConstants.HOT_STORAGE_POLICY_NAME,
        new StorageType[]{StorageType.DISK}, StorageType.EMPTY_ARRAY,
        new StorageType[]{StorageType.ARCHIVE});
    final byte warmId = HdfsServerConstants.WARM_STORAGE_POLICY_ID;
    policies[warmId] = new BlockStoragePolicy(warmId,
        HdfsServerConstants.WARM_STORAGE_POLICY_NAME,
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE},
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE},
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE});
    final byte coldId = HdfsServerConstants.COLD_STORAGE_POLICY_ID;
    policies[coldId] = new BlockStoragePolicy(coldId,
        HdfsServerConstants.COLD_STORAGE_POLICY_NAME,
        new StorageType[]{StorageType.ARCHIVE}, StorageType.EMPTY_ARRAY,
        StorageType.EMPTY_ARRAY);
    return new BlockStoragePolicySuite(hotId, policies);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/common/HdfsServerConstants.java
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.MetaRecoveryContext;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.util.StringUtils;


@InterfaceAudience.Private
public interface HdfsServerConstants {
  int MIN_BLOCKS_FOR_WRITE = 1;
  long LEASE_SOFTLIMIT_PERIOD = 60 * 1000;
  long LEASE_HARDLIMIT_PERIOD = 60 * LEASE_SOFTLIMIT_PERIOD;
  long LEASE_RECOVER_PERIOD = 10 * 1000; // in ms
  int MAX_PATH_LENGTH = 8000;
  int MAX_PATH_DEPTH = 1000;
  int IO_FILE_BUFFER_SIZE = new HdfsConfiguration().getInt(
      CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
      CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
  int SMALL_BUFFER_SIZE = Math.min(IO_FILE_BUFFER_SIZE / 2,
      512);
  long INVALID_TXID = -12345;
  long RESERVED_GENERATION_STAMPS_V1 =
      1024L * 1024 * 1024 * 1024;
  int NAMENODE_LAYOUT_VERSION
      = NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION;
  int DATANODE_LAYOUT_VERSION
      = DataNodeLayoutVersion.CURRENT_LAYOUT_VERSION;
  String[] RESERVED_PATH_COMPONENTS = new String[] {
      HdfsConstants.DOT_SNAPSHOT_DIR,
      FSDirectory.DOT_RESERVED_STRING
  };
  byte[] DOT_SNAPSHOT_DIR_BYTES
              = DFSUtil.string2Bytes(HdfsConstants.DOT_SNAPSHOT_DIR);
  String HOT_STORAGE_POLICY_NAME = "HOT";
  String WARM_STORAGE_POLICY_NAME = "WARM";
  String COLD_STORAGE_POLICY_NAME = "COLD";
  byte MEMORY_STORAGE_POLICY_ID = 15;
  byte ALLSSD_STORAGE_POLICY_ID = 12;
  byte ONESSD_STORAGE_POLICY_ID = 10;
  byte HOT_STORAGE_POLICY_ID = 7;
  byte WARM_STORAGE_POLICY_ID = 5;
  byte COLD_STORAGE_POLICY_ID = 2;

  enum NodeType {
    NAME_NODE,
    DATA_NODE,
    JOURNAL_NODE
  }

  }

  enum StartupOption{
    FORMAT  ("-format"),
    CLUSTERID ("-clusterid"),
    GENCLUSTERID ("-genclusterid"),
    private int force = 0;

    StartupOption(String arg) {this.name = arg;}
    public String getName() {return name;}
    public NamenodeRole toNodeRole() {
      switch(this) {
  }

  int READ_TIMEOUT = 60 * 1000;
  int READ_TIMEOUT_EXTENSION = 5 * 1000;
  int WRITE_TIMEOUT = 8 * 60 * 1000;
  int WRITE_TIMEOUT_EXTENSION = 5 * 1000; //for write pipeline

  enum NamenodeRole {
    NAMENODE  ("NameNode"),
    BACKUP    ("Backup Node"),
    CHECKPOINT("Checkpoint Node");

    private String description = null;
    NamenodeRole(String arg) {this.description = arg;}
  
    @Override
    public String toString() {
  enum ReplicaState {
    FINALIZED(0),

    private final int value;

    ReplicaState(int v) {
      value = v;
    }

  enum BlockUCState {
    COMMITTED
  }
  
  String NAMENODE_LEASE_HOLDER = "HDFS_NameNode";
  long NAMENODE_LEASE_RECHECK_INTERVAL = 2000;

  String CRYPTO_XATTR_ENCRYPTION_ZONE =
      "raw.hdfs.crypto.encryption.zone";
  String CRYPTO_XATTR_FILE_ENCRYPTION_INFO =
      "raw.hdfs.crypto.file.encryption.info";
  String SECURITY_XATTR_UNREADABLE_BY_SUPERUSER =
      "security.hdfs.unreadable.by.superuser";
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/common/StorageInfo.java
import java.util.SortedSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.LayoutFeature;
  }

  public int getServiceLayoutVersion() {
    return storageType == NodeType.DATA_NODE ? HdfsServerConstants.DATANODE_LAYOUT_VERSION
        : HdfsServerConstants.NAMENODE_LAYOUT_VERSION;
  }

  public Map<Integer, SortedSet<LayoutFeature>> getServiceLayoutFeatureMap() {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockMetadataHeader.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;

    DataInputStream in = null;
    try {
      in = new DataInputStream(new BufferedInputStream(
        new FileInputStream(metaFile), HdfsServerConstants.IO_FILE_BUFFER_SIZE));
      return readDataChecksum(in, metaFile);
    } finally {
      IOUtils.closeStream(in);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockPoolSliceStorage.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
    LOG.info("Formatting block pool " + blockpoolID + " directory "
        + bpSdir.getCurrentDir());
    bpSdir.clearDirectory(); // create directory
    this.layoutVersion = HdfsServerConstants.DATANODE_LAYOUT_VERSION;
    this.cTime = nsInfo.getCTime();
    this.namespaceID = nsInfo.getNamespaceID();
    this.blockpoolID = nsInfo.getBlockPoolID();
    }
    readProperties(sd);
    checkVersionUpgradable(this.layoutVersion);
    assert this.layoutVersion >= HdfsServerConstants.DATANODE_LAYOUT_VERSION
       : "Future version is not allowed";
    if (getNamespaceID() != nsInfo.getNamespaceID()) {
      throw new IOException("Incompatible namespaceIDs in "
          + nsInfo.getBlockPoolID() + "; datanode blockpoolID = "
          + blockpoolID);
    }
    if (this.layoutVersion == HdfsServerConstants.DATANODE_LAYOUT_VERSION
        && this.cTime == nsInfo.getCTime()) {
      return; // regular startup
    }
    if (this.layoutVersion > HdfsServerConstants.DATANODE_LAYOUT_VERSION) {
      int restored = restoreBlockFilesFromTrash(getTrashRootDir(sd));
      LOG.info("Restored " + restored + " block files from trash " +
        "before the layout upgrade. These blocks will be moved to " +
        "the previous directory during the upgrade");
    }
    if (this.layoutVersion > HdfsServerConstants.DATANODE_LAYOUT_VERSION
        || this.cTime < nsInfo.getCTime()) {
      doUpgrade(datanode, sd, nsInfo); // upgrade
      return;
    }
    LOG.info("Upgrading block pool storage directory " + bpSd.getRoot()
        + ".\n   old LV = " + this.getLayoutVersion() + "; old CTime = "
        + this.getCTime() + ".\n   new LV = " + HdfsServerConstants.DATANODE_LAYOUT_VERSION
        + "; new CTime = " + nsInfo.getCTime());
    String dnRoot = getDataNodeStorageRoot(bpSd.getRoot().getCanonicalPath());
    
    linkAllBlocks(datanode, bpTmpDir, bpCurDir);
    this.layoutVersion = HdfsServerConstants.DATANODE_LAYOUT_VERSION;
    assert this.namespaceID == nsInfo.getNamespaceID() 
        : "Data-node and name-node layout versions must be the same.";
    this.cTime = nsInfo.getCTime();
    if (!(prevInfo.getLayoutVersion() >= HdfsServerConstants.DATANODE_LAYOUT_VERSION &&
        prevInfo.getCTime() <= nsInfo.getCTime())) { // cannot rollback
      throw new InconsistentFSStateException(bpSd.getRoot(),
          "Cannot rollback to a newer state.\nDatanode previous state: LV = "
              + prevInfo.getLayoutVersion() + " CTime = " + prevInfo.getCTime()
              + " is newer than the namespace state: LV = "
              + HdfsServerConstants.DATANODE_LAYOUT_VERSION + " CTime = " + nsInfo.getCTime());
    }
    
    LOG.info("Rolling back storage directory " + bpSd.getRoot()

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockReceiver.java
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
            out.getClass());
      }
      this.checksumOut = new DataOutputStream(new BufferedOutputStream(
          streams.getChecksumOut(), HdfsServerConstants.SMALL_BUFFER_SIZE));
      if (isCreate) {
        BlockMetadataHeader.writeHeader(checksumOut, diskChecksum);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockSender.java
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
  private static final int MIN_BUFFER_WITH_TRANSFERTO = 64*1024;
  private static final int TRANSFERTO_BUFFER_SIZE = Math.max(
      HdfsServerConstants.IO_FILE_BUFFER_SIZE, MIN_BUFFER_WITH_TRANSFERTO);
  
  private final ExtendedBlock block;
            if (metaIn.getLength() > BlockMetadataHeader.getHeaderSize()) {
              checksumIn = new DataInputStream(new BufferedInputStream(
                  metaIn, HdfsServerConstants.IO_FILE_BUFFER_SIZE));
  
              csum = BlockMetadataHeader.readDataChecksum(checksumIn, block);
              keepMetaInOpen = true;
        pktBufSize += checksumSize * maxChunksPerPacket;
      } else {
        maxChunksPerPacket = Math.max(1,
            numberOfChunks(HdfsServerConstants.IO_FILE_BUFFER_SIZE));
        pktBufSize += (chunkSize + checksumSize) * maxChunksPerPacket;
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java
        unbufIn = saslStreams.in;
        
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            HdfsServerConstants.SMALL_BUFFER_SIZE));
        in = new DataInputStream(unbufIn);
        blockSender = new BlockSender(b, 0, b.getNumBytes(), 
            false, false, true, DataNode.this, null, cachingStrategy);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataStorage.java
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
  void recoverTransitionRead(DataNode datanode, NamespaceInfo nsInfo,
      Collection<StorageLocation> dataDirs, StartupOption startOpt) throws IOException {
    if (this.initialized) {
      LOG.info("DataNode version: " + HdfsServerConstants.DATANODE_LAYOUT_VERSION
          + " and NameNode layout version: " + nsInfo.getLayoutVersion());
      this.storageDirs = new ArrayList<StorageDirectory>(dataDirs.size());
  void format(StorageDirectory sd, NamespaceInfo nsInfo,
              String datanodeUuid) throws IOException {
    sd.clearDirectory(); // create directory
    this.layoutVersion = HdfsServerConstants.DATANODE_LAYOUT_VERSION;
    this.clusterID = nsInfo.getClusterID();
    this.namespaceID = nsInfo.getNamespaceID();
    this.cTime = 0;
    }
    readProperties(sd);
    checkVersionUpgradable(this.layoutVersion);
    assert this.layoutVersion >= HdfsServerConstants.DATANODE_LAYOUT_VERSION :
      "Future version is not allowed";
    
    boolean federationSupported = 
            DatanodeStorage.isValidStorageId(sd.getStorageUuid());

    if (this.layoutVersion == HdfsServerConstants.DATANODE_LAYOUT_VERSION) {
      createStorageID(sd, !haveValidStorageId);
      return; // regular startup
    }

    if (this.layoutVersion > HdfsServerConstants.DATANODE_LAYOUT_VERSION) {
      doUpgrade(datanode, sd, nsInfo);  // upgrade
      createStorageID(sd, !haveValidStorageId);
      return;
    throw new IOException("BUG: The stored LV = " + this.getLayoutVersion()
        + " is newer than the supported LV = "
        + HdfsServerConstants.DATANODE_LAYOUT_VERSION);
  }

      LOG.info("Updating layout version from " + layoutVersion + " to "
          + HdfsServerConstants.DATANODE_LAYOUT_VERSION + " for storage "
          + sd.getRoot());
      layoutVersion = HdfsServerConstants.DATANODE_LAYOUT_VERSION;
      writeProperties(sd);
      return;
    }
    LOG.info("Upgrading storage directory " + sd.getRoot()
             + ".\n   old LV = " + this.getLayoutVersion()
             + "; old CTime = " + this.getCTime()
             + ".\n   new LV = " + HdfsServerConstants.DATANODE_LAYOUT_VERSION
             + "; new CTime = " + nsInfo.getCTime());
    
    File curDir = sd.getCurrentDir();
        STORAGE_DIR_CURRENT));
    
    layoutVersion = HdfsServerConstants.DATANODE_LAYOUT_VERSION;
    clusterID = nsInfo.getClusterID();
    writeProperties(sd);
    
    if (!prevDir.exists()) {
      if (DataNodeLayoutVersion.supports(LayoutVersion.Feature.FEDERATION,
          HdfsServerConstants.DATANODE_LAYOUT_VERSION)) {
        readProperties(sd, HdfsServerConstants.DATANODE_LAYOUT_VERSION);
        writeProperties(sd);
        LOG.info("Layout version rolled back to "
            + HdfsServerConstants.DATANODE_LAYOUT_VERSION + " for storage "
            + sd.getRoot());
      }
      return;

    if (!(prevInfo.getLayoutVersion() >= HdfsServerConstants.DATANODE_LAYOUT_VERSION
          && prevInfo.getCTime() <= nsInfo.getCTime()))  // cannot rollback
      throw new InconsistentFSStateException(sd.getRoot(),
          "Cannot rollback to a newer state.\nDatanode previous state: LV = "
              + prevInfo.getLayoutVersion() + " CTime = " + prevInfo.getCTime()
              + " is newer than the namespace state: LV = "
              + HdfsServerConstants.DATANODE_LAYOUT_VERSION + " CTime = "
              + nsInfo.getCTime());
    LOG.info("Rolling back storage directory " + sd.getRoot()
        + ".\n   target LV = " + HdfsServerConstants.DATANODE_LAYOUT_VERSION
        + "; target CTime = " + nsInfo.getCTime());
    File tmpDir = sd.getRemovedTmp();
    assert !tmpDir.exists() : "removed.tmp directory must not exist.";

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataXceiver.java
          socketIn, datanode.getXferAddress().getPort(),
          datanode.getDatanodeId());
        input = new BufferedInputStream(saslStreams.in,
          HdfsServerConstants.SMALL_BUFFER_SIZE);
        socketOut = saslStreams.out;
      } catch (InvalidMagicNumberException imne) {
        if (imne.isHandshake4Encryption()) {
    long read = 0;
    OutputStream baseStream = getOutputStream();
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
        baseStream, HdfsServerConstants.SMALL_BUFFER_SIZE));
    checkAccess(out, true, block, blockToken,
        Op.READ_BLOCK, BlockTokenIdentifier.AccessMode.READ);
  
    final DataOutputStream replyOut = new DataOutputStream(
        new BufferedOutputStream(
            getOutputStream(),
            HdfsServerConstants.SMALL_BUFFER_SIZE));
    checkAccess(replyOut, isClient, block, blockToken,
        Op.WRITE_BLOCK, BlockTokenIdentifier.AccessMode.WRITE);

          unbufMirrorOut = saslStreams.out;
          unbufMirrorIn = saslStreams.in;
          mirrorOut = new DataOutputStream(new BufferedOutputStream(unbufMirrorOut,
              HdfsServerConstants.SMALL_BUFFER_SIZE));
          mirrorIn = new DataInputStream(unbufMirrorIn);

        .getMetaDataInputStream(block);
    
    final DataInputStream checksumIn = new DataInputStream(
        new BufferedInputStream(metadataIn, HdfsServerConstants.IO_FILE_BUFFER_SIZE));
    updateCurrentThreadName("Getting checksum for block " + block);
    try {
      OutputStream baseStream = getOutputStream();
      reply = new DataOutputStream(new BufferedOutputStream(
          baseStream, HdfsServerConstants.SMALL_BUFFER_SIZE));

      writeSuccessWithChecksumInfo(blockSender, reply);
        unbufProxyIn = saslStreams.in;
        
        proxyOut = new DataOutputStream(new BufferedOutputStream(unbufProxyOut, 
            HdfsServerConstants.SMALL_BUFFER_SIZE));
        proxyReply = new DataInputStream(new BufferedInputStream(unbufProxyIn,
            HdfsServerConstants.IO_FILE_BUFFER_SIZE));
        
        IoeDuringCopyBlockOperation = true;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DirectoryScanner.java
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.util.Daemon;
    public long getGenStamp() {
      return metaSuffix != null ? Block.getGenerationStamp(
          getMetaFile().getName()) : 
            HdfsConstants.GRANDFATHER_GENERATION_STAMP;
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/BlockPoolSlice.java
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
      }
      checksumIn = new DataInputStream(
          new BufferedInputStream(new FileInputStream(metaFile),
              HdfsServerConstants.IO_FILE_BUFFER_SIZE));

      final DataChecksum checksum = BlockMetadataHeader.readDataChecksum(

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetImpl.java
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
        }
      }
      metaOut = new DataOutputStream(new BufferedOutputStream(
          new FileOutputStream(dstMeta), HdfsServerConstants.SMALL_BUFFER_SIZE));
      BlockMetadataHeader.writeHeader(metaOut, checksum);

      int offset = 0;

      final long diskGS = diskMetaFile != null && diskMetaFile.exists() ?
          Block.getGenerationStamp(diskMetaFile.getName()) :
            HdfsConstants.GRANDFATHER_GENERATION_STAMP;

      if (diskFile == null || !diskFile.exists()) {
        if (memBlockInfo == null) {
          long gs = diskMetaFile != null && diskMetaFile.exists()
              && diskMetaFile.getParent().equals(memFile.getParent()) ? diskGS
              : HdfsConstants.GRANDFATHER_GENERATION_STAMP;

          LOG.warn("Updating generation stamp for block " + blockId
              + " from " + memBlockInfo.getGenerationStamp() + " to " + gs);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetUtil.java

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;

      return Block.getGenerationStamp(listdir[j].getName());
    }
    FsDatasetImpl.LOG.warn("Block " + blockFile + " does not have a metafile!");
    return HdfsConstants.GRANDFATHER_GENERATION_STAMP;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/mover/Mover.java
    private boolean processFile(String fullPath, HdfsLocatedFileStatus status) {
      final byte policyId = status.getStoragePolicy();
      if (policyId == HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
        return false;
      }
      final BlockStoragePolicy policy = blockStoragePolicies[policyId];

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/BackupNode.java
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
    namesystem.leaseManager.setLeasePeriod(
        HdfsServerConstants.LEASE_SOFTLIMIT_PERIOD, Long.MAX_VALUE);

    registerWith(nsInfo);
      LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }
    assert HdfsServerConstants.NAMENODE_LAYOUT_VERSION == nsInfo.getLayoutVersion() :
      "Active and backup node layout versions must be the same. Expected: "
      + HdfsServerConstants.NAMENODE_LAYOUT_VERSION + " actual "+ nsInfo.getLayoutVersion();
    return nsInfo;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/EditLogBackupInputStream.java
import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;


  @Override
  public long getFirstTxId() {
    return HdfsServerConstants.INVALID_TXID;
  }

  @Override
  public long getLastTxId() {
    return HdfsServerConstants.INVALID_TXID;
  }

  @Override

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/EditLogFileInputStream.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.LayoutFlags;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.EditLogValidation;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage.HttpGetFailedException;
  EditLogFileInputStream(File name)
      throws LogHeaderCorruptException, IOException {
    this(name, HdfsServerConstants.INVALID_TXID, HdfsServerConstants.INVALID_TXID, false);
  }

      if ((op != null) && (op.hasTransactionId())) {
        long txId = op.getTransactionId();
        if ((txId >= lastTxId) &&
            (lastTxId != HdfsServerConstants.INVALID_TXID)) {
      LOG.warn("Log file " + file + " has no valid header", e);
      return new FSEditLogLoader.EditLogValidation(0,
          HdfsServerConstants.INVALID_TXID, true);
    }
    
    try {
    } catch (LogHeaderCorruptException e) {
      LOG.warn("Log file " + file + " has no valid header", e);
      return new FSEditLogLoader.EditLogValidation(0,
          HdfsServerConstants.INVALID_TXID, true);
    }

    long lastPos = 0;
    long lastTxId = HdfsServerConstants.INVALID_TXID;
    long numValid = 0;
    try {
      while (true) {
        long txid = HdfsServerConstants.INVALID_TXID;
        lastPos = in.getPosition();
        try {
          if ((txid = in.scanNextOp()) == HdfsServerConstants.INVALID_TXID) {
            break;
          }
        } catch (Throwable t) {
          FSImage.LOG.warn("After resync, position is " + in.getPosition());
          continue;
        }
        if (lastTxId == HdfsServerConstants.INVALID_TXID || txid > lastTxId) {
          lastTxId = txid;
        }
        numValid++;
          "Reached EOF when reading log header");
    }
    if (verifyLayoutVersion &&
        (logVersion < HdfsServerConstants.NAMENODE_LAYOUT_VERSION || // future version
         logVersion > Storage.LAST_UPGRADABLE_LAYOUT_VERSION)) { // unsupported
      throw new LogHeaderCorruptException(
          "Unexpected version of the file system log file: "
          + logVersion + ". Current version = "
          + HdfsServerConstants.NAMENODE_LAYOUT_VERSION + ".");
    }
    return logVersion;
  }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/EditLogInputStream.java

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

import java.io.Closeable;
import java.io.IOException;
  protected long scanNextOp() throws IOException {
    FSEditLogOp next = readOp();
    return next != null ? next.txid : HdfsServerConstants.INVALID_TXID;
  }
  

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/EditsDoubleBuffer.java
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.Writer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
    }

    public void writeOp(FSEditLogOp op) throws IOException {
      if (firstTxId == HdfsServerConstants.INVALID_TXID) {
        firstTxId = op.txid;
      } else {
        assert op.txid > firstTxId;
    @Override
    public DataOutputBuffer reset() {
      super.reset();
      firstTxId = HdfsServerConstants.INVALID_TXID;
      numTxns = 0;
      return this;
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirStatAndListingOp.java
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FsPermissionExtension;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
  }

  private static byte getStoragePolicyID(byte inodePolicy, byte parentPolicy) {
    return inodePolicy != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED ? inodePolicy :
        parentPolicy;
  }

      if (targetNode == null)
        return null;
      byte parentStoragePolicy = isSuperUser ?
          targetNode.getStoragePolicyID() : HdfsConstants
          .BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;

      if (!targetNode.isDirectory()) {
        INode cur = contents.get(startChild+i);
        byte curPolicy = isSuperUser && !cur.isSymlink()?
            cur.getLocalStoragePolicyID():
            HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
        INodeAttributes nodeAttrs = getINodeAttributes(
            fsd, src, cur.getLocalNameBytes(), cur,
            snapshot);
      listing[i] = createFileStatus(
          fsd, sRoot.getLocalNameBytes(),
          sRoot, nodeAttrs,
          HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED,
          Snapshot.CURRENT_STATE_ID, false,
          INodesInPath.fromINode(sRoot));
    }
      }

      byte policyId = includeStoragePolicy && !i.isSymlink() ?
          i.getStoragePolicyID() :
          HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      INodeAttributes nodeAttrs = getINodeAttributes(
          fsd, path, HdfsFileStatus.EMPTY_NAME, i, src.getPathSnapshotId());
      return createFileStatus(
      if (fsd.getINode4DotSnapshot(srcs) != null) {
        return new HdfsFileStatus(0, true, 0, 0, 0, 0, null, null, null, null,
            HdfsFileStatus.EMPTY_NAME, -1L, 0, null,
            HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED);
      }
      return null;
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirectory.java
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.util.ByteArray;
    EnumCounters<StorageType> typeSpaceDeltas =
        new EnumCounters<StorageType>(StorageType.class);
    if (storagePolicyID != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      BlockStoragePolicy storagePolicy = getBlockManager().getStoragePolicy(storagePolicyID);

      if (oldRep != newRep) {

  void verifyINodeName(byte[] childName) throws HadoopIllegalArgumentException {
    if (Arrays.equals(HdfsServerConstants.DOT_SNAPSHOT_DIR_BYTES, childName)) {
      String s = "\"" + HdfsConstants.DOT_SNAPSHOT_DIR + "\" is a reserved name.";
      if (!namesystem.isImageLoaded()) {
        s += "  Please rename it before upgrade.";

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLog.java
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;

  private long curSegmentTxId = HdfsServerConstants.INVALID_TXID;

  private long lastPrintTime;
      return;
    }
    
    assert curSegmentTxId == HdfsServerConstants.INVALID_TXID || // on format this is no-op
      minTxIdToKeep <= curSegmentTxId :
      "cannot purge logs older than txid " + minTxIdToKeep +
      " when current segment starts at " + curSegmentTxId;
      EditLogInputStream elis = iter.next();
      if (elis.getFirstTxId() > txId) break;
      long next = elis.getLastTxId();
      if (next == HdfsServerConstants.INVALID_TXID) {
        if (!inProgressOk) {
          throw new RuntimeException("inProgressOk = false, but " +
              "selectInputStreams returned an in-progress edit " +

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogLoader.java
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
      long lastInodeId) throws IOException {
    long inodeId = inodeIdFromOp;

    if (inodeId == HdfsConstants.GRANDFATHER_INODE_ID) {
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.ADD_INODE_ID, logVersion)) {
        throw new IOException("The layout version " + logVersion
  @SuppressWarnings("deprecation")
  private long applyEditLogOp(FSEditLogOp op, FSDirectory fsDir,
      StartupOption startOpt, int logVersion, long lastInodeId) throws IOException {
    long inodeId = HdfsConstants.GRANDFATHER_INODE_ID;
    if (LOG.isTraceEnabled()) {
      LOG.trace("replaying edit log: " + op);
    }
        if (toAddRetryCache) {
          HdfsFileStatus stat = FSDirStatAndListingOp.createFileStatusForEditLog(
              fsNamesys.dir, path, HdfsFileStatus.EMPTY_NAME, newFile,
              HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED, Snapshot.CURRENT_STATE_ID,
              false, iip);
          fsNamesys.addCacheEntryWithPayload(addCloseOp.rpcClientId,
              addCloseOp.rpcCallId, stat);
            HdfsFileStatus stat = FSDirStatAndListingOp.createFileStatusForEditLog(
                fsNamesys.dir, path,
                HdfsFileStatus.EMPTY_NAME, newFile,
                HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED,
                Snapshot.CURRENT_STATE_ID, false, iip);
            fsNamesys.addCacheEntryWithPayload(addCloseOp.rpcClientId,
                addCloseOp.rpcCallId, new LastBlockWithStatus(lb, stat));
        if (toAddRetryCache) {
          HdfsFileStatus stat = FSDirStatAndListingOp.createFileStatusForEditLog(
              fsNamesys.dir, path, HdfsFileStatus.EMPTY_NAME, file,
              HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED,
              Snapshot.CURRENT_STATE_ID, false, iip);
          fsNamesys.addCacheEntryWithPayload(appendOp.rpcClientId,
              appendOp.rpcCallId, new LastBlockWithStatus(lb, stat));
    if (Storage.is203LayoutVersion(logVersion)
        && logVersion != HdfsServerConstants.NAMENODE_LAYOUT_VERSION) {
      String msg = "During upgrade failed to load the editlog version "
          + logVersion + " from release 0.20.203. Please go back to the old "
          + " release and restart the namenode. This empties the editlog "
  static EditLogValidation validateEditLog(EditLogInputStream in) {
    long lastPos = 0;
    long lastTxId = HdfsServerConstants.INVALID_TXID;
    long numValid = 0;
    FSEditLogOp op = null;
    while (true) {
        FSImage.LOG.warn("After resync, position is " + in.getPosition());
        continue;
      }
      if (lastTxId == HdfsServerConstants.INVALID_TXID
          || op.getTransactionId() > lastTxId) {
        lastTxId = op.getTransactionId();
      }

  static EditLogValidation scanEditLog(EditLogInputStream in) {
    long lastPos = 0;
    long lastTxId = HdfsServerConstants.INVALID_TXID;
    long numValid = 0;
    FSEditLogOp op = null;
    while (true) {
        FSImage.LOG.warn("After resync, position is " + in.getPosition());
        continue;
      }
      if (lastTxId == HdfsServerConstants.INVALID_TXID
          || op.getTransactionId() > lastTxId) {
        lastTxId = op.getTransactionId();
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogOp.java
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclEditLogProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.XAttrEditLogProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.hdfs.util.XMLUtils.InvalidXmlException;
import org.apache.hadoop.hdfs.util.XMLUtils.Stanza;
  int rpcCallId;

  final void reset() {
    txid = HdfsServerConstants.INVALID_TXID;
    rpcClientId = RpcConstants.DUMMY_CLIENT_ID;
    rpcCallId = RpcConstants.INVALID_CALL_ID;
    resetSubFields();
  }

  public long getTransactionId() {
    Preconditions.checkState(txid != HdfsServerConstants.INVALID_TXID);
    return txid;
  }

  public String getTransactionIdStr() {
    return (txid == HdfsServerConstants.INVALID_TXID) ? "(none)" : "" + txid;
  }
  
  public boolean hasTransactionId() {
    return (txid != HdfsServerConstants.INVALID_TXID);
  }

  public void setTransactionId(long txid) {
    
    private AddCloseOp(FSEditLogOpCodes opCode) {
      super(opCode);
      storagePolicyId = HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      assert(opCode == OP_ADD || opCode == OP_CLOSE || opCode == OP_APPEND);
    }

        this.inodeId = in.readLong();
      } else {
        this.inodeId = HdfsConstants.GRANDFATHER_INODE_ID;
      }
      if ((-17 < logVersion && length != 4) ||
          (logVersion <= -17 && length != 5 && !NameNodeLayoutVersion.supports(
            NameNodeLayoutVersion.Feature.BLOCK_STORAGE_POLICY, logVersion)) {
          this.storagePolicyId = FSImageSerialization.readByte(in);
        } else {
          this.storagePolicyId = HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
        }
        readRpcIds(in, logVersion);
        this.inodeId = FSImageSerialization.readLong(in);
      } else {
        this.inodeId = HdfsConstants.GRANDFATHER_INODE_ID;
      }
      this.path = FSImageSerialization.readString(in);
      if (NameNodeLayoutVersion.supports(
        this.inodeId = FSImageSerialization.readLong(in);
      } else {
        this.inodeId = HdfsConstants.GRANDFATHER_INODE_ID;
      }
      this.path = FSImageSerialization.readString(in);
      this.value = FSImageSerialization.readString(in);
        op.setTransactionId(in.readLong());
      } else {
        op.setTransactionId(HdfsServerConstants.INVALID_TXID);
      }

      op.readFields(in, logVersion);
        try {
          opCodeByte = in.readByte(); // op code
        } catch (EOFException e) {
          return HdfsServerConstants.INVALID_TXID;
        }

        FSEditLogOpCodes opCode = FSEditLogOpCodes.fromByte(opCodeByte);
        if (opCode == OP_INVALID) {
          verifyTerminator();
          return HdfsServerConstants.INVALID_TXID;
        }

        int length = in.readInt(); // read the length of the op
        return txid;
      } else {
        FSEditLogOp op = decodeOp();
        return op == null ? HdfsServerConstants.INVALID_TXID : op.getTransactionId();
      }
    }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImage.java
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
    if (startOpt == StartupOption.METADATAVERSION) {
      System.out.println("HDFS Image Version: " + layoutVersion);
      System.out.println("Software format version: " +
        HdfsServerConstants.NAMENODE_LAYOUT_VERSION);
      return false;
    }

        && startOpt != StartupOption.UPGRADEONLY
        && !RollingUpgradeStartupOption.STARTED.matches(startOpt)
        && layoutVersion < Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION
        && layoutVersion != HdfsServerConstants.NAMENODE_LAYOUT_VERSION) {
      throw new IOException(
          "\nFile system image contains an old layout version " 
          + storage.getLayoutVersion() + ".\nAn upgrade to version "
          + HdfsServerConstants.NAMENODE_LAYOUT_VERSION + " is required.\n"
          + "Please restart NameNode with the \""
          + RollingUpgradeStartupOption.STARTED.getOptionString()
          + "\" option if a rolling upgrade is already started;"
    long oldCTime = storage.getCTime();
    storage.cTime = now();  // generate new cTime for the state
    int oldLV = storage.getLayoutVersion();
    storage.layoutVersion = HdfsServerConstants.NAMENODE_LAYOUT_VERSION;
    
    List<StorageDirectory> errorSDs =
      Collections.synchronizedList(new ArrayList<StorageDirectory>());
    boolean canRollback = false;
    FSImage prevState = new FSImage(conf);
    try {
      prevState.getStorage().layoutVersion = HdfsServerConstants.NAMENODE_LAYOUT_VERSION;
      for (Iterator<StorageDirectory> it = storage.dirIterator(false); it.hasNext();) {
        StorageDirectory sd = it.next();
        if (!NNUpgradeUtil.canRollBack(sd, storage, prevState.getStorage(),
            HdfsServerConstants.NAMENODE_LAYOUT_VERSION)) {
          continue;
        }
        LOG.info("Can perform rollback for " + sd);
        editLog.initJournalsForWrite();
        boolean canRollBackSharedEditLog = editLog.canRollBackSharedLog(
            prevState.getStorage(), HdfsServerConstants.NAMENODE_LAYOUT_VERSION);
        if (canRollBackSharedEditLog) {
          LOG.info("Can perform rollback for shared edit log.");
          canRollback = true;
          lastAppliedTxId = loader.getLastAppliedTxId();
        }
        if (editIn.getLastTxId() != HdfsServerConstants.INVALID_TXID) {
          lastAppliedTxId = editIn.getLastTxId();
        }
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageFormat.java
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
  @VisibleForTesting
  public static void useDefaultRenameReservedPairs() {
    renameReservedMap.clear();
    for (String key: HdfsServerConstants.RESERVED_PATH_COMPONENTS) {
      renameReservedMap.put(
          key,
          key + "." + HdfsServerConstants.NAMENODE_LAYOUT_VERSION + "."
              + "UPGRADE_RENAMED");
    }
  }
      final int layoutVersion) {
    if (!NameNodeLayoutVersion.supports(Feature.SNAPSHOT, layoutVersion)) {
      if (Arrays.equals(component, HdfsServerConstants.DOT_SNAPSHOT_DIR_BYTES)) {
        Preconditions.checkArgument(
            renameReservedMap.containsKey(HdfsConstants.DOT_SNAPSHOT_DIR),
            RESERVED_ERROR_MSG);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageFormatProtobuf.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.CacheManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.NameSystemSection;
    private long imgTxId;
    private final boolean requireSameLayoutVersion;
      }
      FileSummary summary = FSImageUtil.loadSummary(raFile);
      if (requireSameLayoutVersion && summary.getLayoutVersion() !=
          HdfsServerConstants.NAMENODE_LAYOUT_VERSION) {
        throw new IOException("Image version " + summary.getLayoutVersion() +
            " is not equal to the software version " +
            HdfsServerConstants.NAMENODE_LAYOUT_VERSION);
      }

      FileChannel channel = fin.getChannel();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImagePreTransactionalStorageInspector.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
    
    FSImageFile file = new FSImageFile(latestNameSD, 
        NNStorage.getStorageFile(latestNameSD, NameNodeFile.IMAGE),
        HdfsServerConstants.INVALID_TXID);
    LinkedList<FSImageFile> ret = new LinkedList<FSImageFile>();
    ret.add(file);
    return ret;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageStorageInspector.java
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;

    private final File file;
    
    FSImageFile(StorageDirectory sd, File file, long txId) {
      assert txId >= 0 || txId == HdfsServerConstants.INVALID_TXID
        : "Invalid txid on " + file +": " + txId;
      
      this.sd = sd;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
  private void updateStorageVersionForRollingUpgrade(final long layoutVersion,
      StartupOption startOpt) throws IOException {
    boolean rollingStarted = RollingUpgradeStartupOption.STARTED
        .matches(startOpt) && layoutVersion > HdfsServerConstants
        .NAMENODE_LAYOUT_VERSION;
    boolean rollingRollback = RollingUpgradeStartupOption.ROLLBACK
        .matches(startOpt);
    Block previousBlock = ExtendedBlock.getLocalBlock(previous);
    final INode inode;
    final INodesInPath iip;
    if (fileId == HdfsConstants.GRANDFATHER_INODE_ID) {

      final INode inode;
      if (fileId == HdfsConstants.GRANDFATHER_INODE_ID) {

      final INode inode;
      final INodesInPath iip;
      if (fileId == HdfsConstants.GRANDFATHER_INODE_ID) {
    final INodesInPath iip;
    INode inode = null;
    try {
      if (fileId == HdfsConstants.GRANDFATHER_INODE_ID) {
      checkNameNodeSafeMode("Cannot fsync file " + src);
      src = dir.resolvePath(pc, src, pathComponents);
      final INode inode;
      if (fileId == HdfsConstants.GRANDFATHER_INODE_ID) {

    finalizeINodeFileUnderConstruction(src, pendingFile,
                                       Snapshot.findLatestSnapshot(pendingFile,
                                                                   Snapshot.CURRENT_STATE_ID));

    return src;
  }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FileJournalManager.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
        try {
          long startTxId = Long.parseLong(inProgressEditsMatch.group(1));
          ret.add(
              new EditLogFile(f, startTxId, HdfsServerConstants.INVALID_TXID, true));
          continue;
        } catch (NumberFormatException nfe) {
          LOG.error("In-progress edits file " + f + " has improperly " +
        if (staleInprogressEditsMatch.matches()) {
          try {
            long startTxId = Long.parseLong(staleInprogressEditsMatch.group(1));
            ret.add(new EditLogFile(f, startTxId, HdfsServerConstants.INVALID_TXID,
                true));
            continue;
          } catch (NumberFormatException nfe) {
        }
      }
      if (elf.lastTxId < fromTxId) {
        assert elf.lastTxId != HdfsServerConstants.INVALID_TXID;
        if (LOG.isDebugEnabled()) {
          LOG.debug("passing over " + elf + " because it ends at " +
              elf.lastTxId + ", but we only care about transactions " +
          throw new CorruptionException("In-progress edit log file is corrupt: "
              + elf);
        }
        if (elf.getLastTxId() == HdfsServerConstants.INVALID_TXID) {
    EditLogFile(File file,
        long firstTxId, long lastTxId) {
      this(file, firstTxId, lastTxId, false);
      assert (lastTxId != HdfsServerConstants.INVALID_TXID)
        && (lastTxId >= firstTxId);
    }
    
    EditLogFile(File file, long firstTxId, 
                long lastTxId, boolean isInProgress) { 
      assert (lastTxId == HdfsServerConstants.INVALID_TXID && isInProgress)
        || (lastTxId != HdfsServerConstants.INVALID_TXID && lastTxId >= firstTxId);
      assert (firstTxId > 0) || (firstTxId == HdfsServerConstants.INVALID_TXID);
      assert file != null;
      
      Preconditions.checkArgument(!isInProgress ||
          lastTxId == HdfsServerConstants.INVALID_TXID);
      
      this.firstTxId = firstTxId;
      this.lastTxId = lastTxId;
    }

    public void moveAsideEmptyFile() throws IOException {
      assert lastTxId == HdfsServerConstants.INVALID_TXID;
      renameSelf(".empty");
    }
      

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INode.java
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
  public final QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps) {
    final byte storagePolicyId = isSymlink() ?
        HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED : getStoragePolicyID();
    return computeQuotaUsage(bsps, storagePolicyId,
        new QuotaCounts.Builder().build(), true, Snapshot.CURRENT_STATE_ID);
  }
  public final QuotaCounts computeQuotaUsage(
    BlockStoragePolicySuite bsps, QuotaCounts counts, boolean useCache) {
    final byte storagePolicyId = isSymlink() ?
        HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED : getStoragePolicyID();
    return computeQuotaUsage(bsps, storagePolicyId, counts,
        useCache, Snapshot.CURRENT_STATE_ID);
  }

  public abstract byte getLocalStoragePolicyID();
  public byte getStoragePolicyIDForQuota(byte parentStoragePolicyId) {
    byte localId = isSymlink() ?
        HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED : getLocalStoragePolicyID();
    return localId != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED ?
        localId : parentStoragePolicyId;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeDirectory.java
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeFile.java
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.NO_SNAPSHOT_ID;


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeId.java
import java.io.FileNotFoundException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.util.SequentialNumber;

  public static void checkId(long requestId, INode inode)
      throws FileNotFoundException {
    if (requestId != HdfsConstants.GRANDFATHER_INODE_ID && requestId != inode.getId()) {
      throw new FileNotFoundException(
          "ID mismatch. Request id and saved id: " + requestId + " , "
              + inode.getId() + " for file " + inode.getFullPathName());

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeMap.java

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;

      @Override
      public byte getStoragePolicyID(){
        return HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      }

      @Override
      public byte getLocalStoragePolicyID() {
        return HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      }
    };
      

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodesInPath.java
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

  private static boolean isDotSnapshotDir(byte[] pathComponent) {
    return pathComponent != null &&
        Arrays.equals(HdfsServerConstants.DOT_SNAPSHOT_DIR_BYTES, pathComponent);
  }

  static INodesInPath fromINode(INode inode) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/LeaseManager.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.util.Daemon;

  private final FSNamesystem fsnamesystem;

  private long softLimit = HdfsServerConstants.LEASE_SOFTLIMIT_PERIOD;
  private long hardLimit = HdfsServerConstants.LEASE_HARDLIMIT_PERIOD;


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NNStorage.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
  protected volatile long mostRecentCheckpointTxId = HdfsServerConstants.INVALID_TXID;
  
  public void format(NamespaceInfo nsInfo) throws IOException {
    Preconditions.checkArgument(nsInfo.getLayoutVersion() == 0 ||
        nsInfo.getLayoutVersion() == HdfsServerConstants.NAMENODE_LAYOUT_VERSION,
        "Bad layout version: %s", nsInfo.getLayoutVersion());
    
    this.setStorageInfo(nsInfo);
  }
  
  public void format() throws IOException {
    this.layoutVersion = HdfsServerConstants.NAMENODE_LAYOUT_VERSION;
    for (Iterator<StorageDirectory> it =
                           dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
            "storage directory " + sd.getRoot().getAbsolutePath());
      }
      props.setProperty("layoutVersion",
          Integer.toString(HdfsServerConstants.NAMENODE_LAYOUT_VERSION));
    }
    setFieldsFromProperties(props, sd);
  }
  String getDeprecatedProperty(String prop) {
    assert getLayoutVersion() > HdfsServerConstants.NAMENODE_LAYOUT_VERSION :
      "getDeprecatedProperty should only be done when loading " +
      "storage from past versions during upgrade.";
    return deprecatedProperties.get(prop);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNode.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNodeRpcServer.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.MAX_PATH_DEPTH;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.MAX_PATH_LENGTH;
import static org.apache.hadoop.util.Time.now;

import java.io.FileNotFoundException;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.FSLimitException;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
  void verifyLayoutVersion(int version) throws IOException {
    if (version != HdfsServerConstants.NAMENODE_LAYOUT_VERSION)
      throw new IncorrectVersionException(
          HdfsServerConstants.NAMENODE_LAYOUT_VERSION, version, "data node");
  }
  
  private void verifySoftwareVersion(DatanodeRegistration dnReg)

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/RedundantEditLogInputStream.java
import java.util.Comparator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.io.IOUtils;

import com.google.common.base.Preconditions;
  RedundantEditLogInputStream(Collection<EditLogInputStream> streams,
      long startTxId) {
    this.curIdx = 0;
    this.prevTxId = (startTxId == HdfsServerConstants.INVALID_TXID) ?
      HdfsServerConstants.INVALID_TXID : (startTxId - 1);
    this.state = (streams.isEmpty()) ? State.EOF : State.SKIP_UNTIL;
    this.prevException = null;
    EditLogInputStream first = null;
    for (EditLogInputStream s : streams) {
      Preconditions.checkArgument(s.getFirstTxId() !=
          HdfsServerConstants.INVALID_TXID, "invalid first txid in stream: %s", s);
      Preconditions.checkArgument(s.getLastTxId() !=
          HdfsServerConstants.INVALID_TXID, "invalid last txid in stream: %s", s);
      if (first == null) {
        first = s;
      } else {
      switch (state) {
      case SKIP_UNTIL:
       try {
          if (prevTxId != HdfsServerConstants.INVALID_TXID) {
            LOG.info("Fast-forwarding stream '" + streams[curIdx].getName() +
                "' to transaction ID " + (prevTxId + 1));
            streams[curIdx].skipUntil(prevTxId + 1);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/TransferFsImage.java
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
  private static void copyFileToStream(OutputStream out, File localfile,
      FileInputStream infile, DataTransferThrottler throttler,
      Canceler canceler) throws IOException {
    byte buf[] = new byte[HdfsServerConstants.IO_FILE_BUFFER_SIZE];
    try {
      CheckpointFaultInjector.getInstance()
          .aboutToSendFile(localfile);
            shouldSendShortFile(localfile)) {
          long len = localfile.length();
          buf = new byte[(int)Math.min(len/2, HdfsServerConstants.IO_FILE_BUFFER_SIZE)];
          infile.read(buf);
      }
      
      int num = 1;
      byte[] buf = new byte[HdfsServerConstants.IO_FILE_BUFFER_SIZE];
      while (num > 0) {
        num = stream.read(buf);
        if (num > 0) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/ha/BootstrapStandby.java
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
    if (!checkLayoutVersion(nsInfo)) {
      LOG.fatal("Layout version on remote node (" + nsInfo.getLayoutVersion()
          + ") does not match " + "this node's layout version ("
          + HdfsServerConstants.NAMENODE_LAYOUT_VERSION + ")");
      return ERR_CODE_INVALID_VERSION;
    }

  }

  private boolean checkLayoutVersion(NamespaceInfo nsInfo) throws IOException {
    return (nsInfo.getLayoutVersion() == HdfsServerConstants.NAMENODE_LAYOUT_VERSION);
  }
  
  private void parseConfAndFindOtherNN() throws IOException {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/ha/EditLogTailer.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputException;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
  private long lastRollTriggerTxId = HdfsServerConstants.INVALID_TXID;
  
  private long lastLoadedTxnId = HdfsServerConstants.INVALID_TXID;


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FileWithSnapshotFeature.java

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
    BlockStoragePolicy bsp = null;
    EnumCounters<StorageType> typeSpaces =
        new EnumCounters<StorageType>(StorageType.class);
    if (storagePolicyID != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      bsp = bsps.getPolicy(file.getStoragePolicyID());
    }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/protocol/NNHAStatusHeartbeat.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NNHAStatusHeartbeat {

  private final HAServiceState state;
  private long txid = HdfsServerConstants.INVALID_TXID;
  
  public NNHAStatusHeartbeat(HAServiceState state, long txid) {
    this.state = state;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/protocol/NamespaceInfo.java

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
  public NamespaceInfo(int nsID, String clusterID, String bpID,
      long cT, String buildVersion, String softwareVersion,
      long capabilities) {
    super(HdfsServerConstants.NAMENODE_LAYOUT_VERSION, nsID, clusterID, cT,
        NodeType.NAME_NODE);
    blockPoolID = bpID;
    this.buildVersion = buildVersion;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/protocol/RemoteEditLog.java
package org.apache.hadoop.hdfs.server.protocol;

import com.google.common.base.Function;
import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

public class RemoteEditLog implements Comparable<RemoteEditLog> {
  private long startTxId = HdfsServerConstants.INVALID_TXID;
  private long endTxId = HdfsServerConstants.INVALID_TXID;
  private boolean isInProgress = false;
  
  public RemoteEditLog() {
  public RemoteEditLog(long startTxId, long endTxId) {
    this.startTxId = startTxId;
    this.endTxId = endTxId;
    this.isInProgress = (endTxId == HdfsServerConstants.INVALID_TXID);
  }
  
  public RemoteEditLog(long startTxId, long endTxId, boolean inProgress) {
      @Override
      public Long apply(RemoteEditLog log) {
        if (null == log) {
          return HdfsServerConstants.INVALID_TXID;
        }
        return log.getStartTxId();
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/StoragePolicyAdmin.java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.util.StringUtils;
          return 2;
        }
        byte storagePolicyId = status.getStoragePolicy();
        if (storagePolicyId == HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
          System.out.println("The storage policy of " + path + " is unspecified");
          return 0;
        }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/offlineEditsViewer/OfflineEditsLoader.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;

import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
        OfflineEditsLoader loader = null;
        try {
          file = new File(inputFileName);
          elis = new EditLogFileInputStream(file, HdfsServerConstants.INVALID_TXID,
              HdfsServerConstants.INVALID_TXID, false);
          loader = new OfflineEditsBinaryLoader(visitor, elis, flags);
        } finally {
          if ((loader == null) && (elis != null)) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/ImageLoaderCurrent.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutFlags;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
    final String pathName = readINodePath(in, parentName);
    v.visit(ImageElement.INODE_PATH, pathName);

    long inodeId = HdfsConstants.GRANDFATHER_INODE_ID;
    if (supportInodeId) {
      inodeId = in.readLong();
      v.visit(ImageElement.INODE_ID, inodeId);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/fs/TestSymlinkHdfs.java
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
  public void testCreateLinkMaxPathLink() throws IOException {
    Path dir  = new Path(testBaseDir1());
    Path file = new Path(testBaseDir1(), "file");
    final int maxPathLen = HdfsServerConstants.MAX_PATH_LENGTH;
    final int dirLen     = dir.toString().length() + 1;
    int   len            = maxPathLen - dirLen;
    

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/DFSTestUtil.java
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
    final long writeTimeout = dfsClient.getDatanodeWriteTimeout(datanodes.length);
    final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
        NetUtils.getOutputStream(s, writeTimeout),
        HdfsServerConstants.SMALL_BUFFER_SIZE));
    final DataInputStream in = new DataInputStream(NetUtils.getInputStream(s));

    s2.close();
    filesystem.setStoragePolicy(pathFileCreate,
        HdfsServerConstants.HOT_STORAGE_POLICY_NAME);
    final Path pathFileMoved = new Path("/file_moved");
    filesystem.rename(pathFileCreate, pathFileMoved);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    field.setInt(null, lv);

    field = HdfsServerConstants.class.getField("DATANODE_LAYOUT_VERSION");
    field.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    field.setInt(null, lv);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockStoragePolicy.java
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;

import java.io.File;
import java.io.FileNotFoundException;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
  static final long FILE_LEN = 1024;
  static final short REPLICATION = 3;

  static final byte COLD = HdfsServerConstants.COLD_STORAGE_POLICY_ID;
  static final byte WARM = HdfsServerConstants.WARM_STORAGE_POLICY_ID;
  static final byte HOT  = HdfsServerConstants.HOT_STORAGE_POLICY_ID;
  static final byte ONESSD  = HdfsServerConstants.ONESSD_STORAGE_POLICY_ID;
  static final byte ALLSSD  = HdfsServerConstants.ALLSSD_STORAGE_POLICY_ID;
  static final byte LAZY_PERSIST  = HdfsServerConstants.MEMORY_STORAGE_POLICY_ID;

  @Test (timeout=300000)
  public void testConfigKeyEnabled() throws IOException {
    try {
      cluster.waitActive();
      cluster.getFileSystem().setStoragePolicy(new Path("/"),
          HdfsServerConstants.COLD_STORAGE_POLICY_NAME);
    } finally {
      cluster.shutdown();
    }
    try {
      cluster.waitActive();
      cluster.getFileSystem().setStoragePolicy(new Path("/"),
          HdfsServerConstants.COLD_STORAGE_POLICY_NAME);
    } finally {
      cluster.shutdown();
    }

      final Path invalidPath = new Path("/invalidPath");
      try {
        fs.setStoragePolicy(invalidPath, HdfsServerConstants.WARM_STORAGE_POLICY_NAME);
        Assert.fail("Should throw a FileNotFoundException");
      } catch (FileNotFoundException e) {
        GenericTestUtils.assertExceptionContains(invalidPath.toString(), e);
      }

      fs.setStoragePolicy(fooFile, HdfsServerConstants.COLD_STORAGE_POLICY_NAME);
      fs.setStoragePolicy(barDir, HdfsServerConstants.WARM_STORAGE_POLICY_NAME);
      fs.setStoragePolicy(barFile2, HdfsServerConstants.HOT_STORAGE_POLICY_NAME);

      dirList = fs.getClient().listPaths(dir.toString(),
          HdfsFileStatus.EMPTY_NAME).getPartialListing();
      DFSTestUtil.createFile(fs, fooFile1, FILE_LEN, REPLICATION, 0L);
      DFSTestUtil.createFile(fs, fooFile2, FILE_LEN, REPLICATION, 0L);

      fs.setStoragePolicy(fooDir, HdfsServerConstants.WARM_STORAGE_POLICY_NAME);

      HdfsFileStatus[] dirList = fs.getClient().listPaths(dir.toString(),
          HdfsFileStatus.EMPTY_NAME, true).getPartialListing();
      SnapshotTestHelper.createSnapshot(fs, dir, "s1");
      fs.setStoragePolicy(fooFile1, HdfsServerConstants.COLD_STORAGE_POLICY_NAME);

      fooList = fs.getClient().listPaths(fooDir.toString(),
          HdfsFileStatus.EMPTY_NAME).getPartialListing();
          HdfsFileStatus.EMPTY_NAME).getPartialListing(), COLD);

      fs.setStoragePolicy(fooDir, HdfsServerConstants.HOT_STORAGE_POLICY_NAME);
      dirList = fs.getClient().listPaths(dir.toString(),
          HdfsFileStatus.EMPTY_NAME, true).getPartialListing();
  @Test
  public void testChangeHotFileRep() throws Exception {
    testChangeFileRep(HdfsServerConstants.HOT_STORAGE_POLICY_NAME, HOT,
        new StorageType[]{StorageType.DISK, StorageType.DISK,
            StorageType.DISK},
        new StorageType[]{StorageType.DISK, StorageType.DISK, StorageType.DISK,
  @Test
  public void testChangeWarmRep() throws Exception {
    testChangeFileRep(HdfsServerConstants.WARM_STORAGE_POLICY_NAME, WARM,
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE,
            StorageType.ARCHIVE},
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE,
  @Test
  public void testChangeColdRep() throws Exception {
    testChangeFileRep(HdfsServerConstants.COLD_STORAGE_POLICY_NAME, COLD,
        new StorageType[]{StorageType.ARCHIVE, StorageType.ARCHIVE,
            StorageType.ARCHIVE},
        new StorageType[]{StorageType.ARCHIVE, StorageType.ARCHIVE,

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSRollback.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
          UpgradeUtilities.getCurrentBlockPoolID(cluster));
      storageInfo = new StorageInfo(
          HdfsServerConstants.DATANODE_LAYOUT_VERSION - 1,
          UpgradeUtilities.getCurrentNamespaceID(cluster),
          UpgradeUtilities.getCurrentClusterID(cluster),
          UpgradeUtilities.getCurrentFsscTime(cluster),
      
      UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "current");
      baseDirs = UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "previous");
      storageInfo = new StorageInfo(HdfsServerConstants.DATANODE_LAYOUT_VERSION,
          UpgradeUtilities.getCurrentNamespaceID(cluster),
          UpgradeUtilities.getCurrentClusterID(cluster), Long.MAX_VALUE,
          NodeType.DATA_NODE);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSStartupVersions.java
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
  private StorageData[] initializeVersions() throws Exception {
    int layoutVersionOld = Storage.LAST_UPGRADABLE_LAYOUT_VERSION;
    int layoutVersionCur = HdfsServerConstants.DATANODE_LAYOUT_VERSION;
    int layoutVersionNew = Integer.MIN_VALUE;
    int namespaceIdCur = UpgradeUtilities.getCurrentNamespaceID(null);
    int namespaceIdOld = Integer.MIN_VALUE;
      return false;
    }
    int softwareLV = HdfsServerConstants.DATANODE_LAYOUT_VERSION;
    int storedLV = datanodeVer.getLayoutVersion();
    if (softwareLV == storedLV &&  
        datanodeVer.getCTime() == namenodeVer.getCTime()) 
                                              .startupOption(StartupOption.REGULAR)
                                              .build();
    StorageData nameNodeVersion = new StorageData(
        HdfsServerConstants.NAMENODE_LAYOUT_VERSION,
        UpgradeUtilities.getCurrentNamespaceID(cluster),
        UpgradeUtilities.getCurrentClusterID(cluster),
        UpgradeUtilities.getCurrentFsscTime(cluster),

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSUpgrade.java
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      cluster = createCluster();
      baseDirs = UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "current");
      storageInfo = new StorageInfo(HdfsServerConstants.DATANODE_LAYOUT_VERSION,
          UpgradeUtilities.getCurrentNamespaceID(cluster),
          UpgradeUtilities.getCurrentClusterID(cluster), Long.MAX_VALUE,
          NodeType.DATA_NODE);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDatanodeRegistration.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.util.VersionInfo;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.security.Permission;

          .getCTime();
      StorageInfo mockStorageInfo = mock(StorageInfo.class);
      doReturn(nnCTime).when(mockStorageInfo).getCTime();
      doReturn(HdfsServerConstants.DATANODE_LAYOUT_VERSION).when(mockStorageInfo)
          .getLayoutVersion();
      DatanodeRegistration dnReg = new DatanodeRegistration(dnId,
          mockStorageInfo, null, VersionInfo.getVersion());
      doReturn(nnCTime).when(mockStorageInfo).getCTime();
      
      DatanodeRegistration mockDnReg = mock(DatanodeRegistration.class);
      doReturn(HdfsServerConstants.DATANODE_LAYOUT_VERSION).when(mockDnReg).getVersion();
      doReturn("127.0.0.1").when(mockDnReg).getIpAddr();
      doReturn(123).when(mockDnReg).getXferPort();
      doReturn("fake-storage-id").when(mockDnReg).getDatanodeUuid();
      doReturn(nnCTime).when(mockStorageInfo).getCTime();
      
      DatanodeRegistration mockDnReg = mock(DatanodeRegistration.class);
      doReturn(HdfsServerConstants.DATANODE_LAYOUT_VERSION).when(mockDnReg).getVersion();
      doReturn("fake-storage-id").when(mockDnReg).getDatanodeUuid();
      doReturn(mockStorageInfo).when(mockDnReg).getStorageInfo();
      

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestFileAppend4.java

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;

    cluster.setLeasePeriod(1000, HdfsServerConstants.LEASE_HARDLIMIT_PERIOD);

    int tries = 60;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestFileCreation.java
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;

      LocatedBlock location = client.getNamenode().addBlock(file1.toString(),
          client.clientName, null, null, HdfsConstants.GRANDFATHER_INODE_ID, null);
      System.out.println("testFileCreationError2: "
          + "Added block " + location.getBlock());

      createFile(dfs, f, 3);
      try {
        cluster.getNameNodeRpc().addBlock(f.toString(), client.clientName,
            null, null, HdfsConstants.GRANDFATHER_INODE_ID, null);
        fail();
      } catch(IOException ioe) {
        FileSystem.LOG.info("GOOD!", ioe);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestFileStatus.java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.web.HftpFileSystem;
      int fileSize, int blockSize) throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
        HdfsServerConstants.IO_FILE_BUFFER_SIZE, (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestGetBlocks.java
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;

    for (int i = 0; i < blkids.length; i++) {
      Block b = new Block(blkids[i], 0,
          HdfsConstants.GRANDFATHER_GENERATION_STAMP);
      Long v = map.get(b);
      System.out.println(b + " => " + v);
      assertEquals(blkids[i], v.longValue());

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestLease.java
package org.apache.hadoop.hdfs;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.impl.LeaseRenewer;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.EnumSetWritable;
      LeaseRenewer originalRenewer = dfs.getLeaseRenewer();
      dfs.lastLeaseRenewal = Time.monotonicNow()
      - HdfsServerConstants.LEASE_SOFTLIMIT_PERIOD - 1000;
      try {
        dfs.renewLease();
      } catch (IOException e) {}

      dfs.lastLeaseRenewal = Time.monotonicNow()
      - HdfsServerConstants.LEASE_HARDLIMIT_PERIOD - 1000;
      dfs.renewLease();


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestLeaseRecovery2.java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
    DFSTestUtil.updateConfWithFakeGroupMapping(conf, u2g_map);

    cluster.setLeasePeriod(HdfsServerConstants.LEASE_SOFTLIMIT_PERIOD,
                           HdfsServerConstants.LEASE_HARDLIMIT_PERIOD);
    String filestr = "/foo" + AppendTestUtil.nextInt();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/UpgradeUtilities.java
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
  public static int getCurrentNameNodeLayoutVersion() {
    return HdfsServerConstants.NAMENODE_LAYOUT_VERSION;
  }
  

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/qjournal/server/TestJournalNode.java
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.QJMTestUtil;
import org.apache.hadoop.hdfs.qjournal.client.IPCLoggerChannel;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
    byte[] retrievedViaHttp = DFSTestUtil.urlGetBytes(new URL(urlRoot +
        "/getJournal?segmentTxId=1&jid=" + journalId));
    byte[] expected = Bytes.concat(
            Ints.toByteArray(HdfsServerConstants.NAMENODE_LAYOUT_VERSION),
            (new byte[] { 0, 0, 0, 0 }), // layout flags section
            EDITS_DATA);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockManager.java
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
  @Before
  public void setupMockCluster() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
             "need to set a dummy value here so it assumes a multi-rack cluster");
    fsn = Mockito.mock(FSNamesystem.class);
    Mockito.doReturn(true).when(fsn).hasWriteLock();
    bm = new BlockManager(fsn, conf);
    for (DatanodeDescriptor dn : nodesToAdd) {
      cluster.add(dn);
      dn.getStorageInfos()[0].setUtilizationForTesting(
          2 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L);
      dn.updateHeartbeat(
          BlockManagerTestUtil.getStorageReportsForDatanode(dn), 0L, 0L, 0, 0,
          null);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestReplicationPolicy.java
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestBlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager.StatefulBlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
    }
    for (int i=0; i < NUM_OF_DATANODES; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }    
  }

  @Test
  public void testChooseTarget1() throws Exception {
    updateHeartbeatWithUsage(dataNodes[0],
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        0L, 0L, 4, 0); // overloaded

    DatanodeStorageInfo[] targets;
    assertFalse(isOnSameRack(targets[0], targets[2]));
    
    updateHeartbeatWithUsage(dataNodes[0],
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
  }

  private static DatanodeStorageInfo[] chooseTarget(int numOfReplicas) {
  public void testChooseTarget3() throws Exception {
    updateHeartbeatWithUsage(dataNodes[0],
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        (HdfsServerConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L,
        0L, 0L, 0, 0); // no space
        
    DatanodeStorageInfo[] targets;
    assertFalse(isOnSameRack(targets[1], targets[3]));

    updateHeartbeatWithUsage(dataNodes[0],
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
  }
  
    for(int i=0; i<2; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          (HdfsServerConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
      
    DatanodeStorageInfo[] targets;
    
    for(int i=0; i<2; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
  }

    bm.getDatanodeManager().getNetworkTopology().add(newDn);
    bm.getDatanodeManager().getHeartbeatManager().addDatanode(newDn);
    updateHeartbeatWithUsage(newDn,
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);

    excludedNodes.clear();
    for(int i=0; i<2; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          (HdfsServerConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
    
    final LogVerificationAppender appender = new LogVerificationAppender();
    
    for(int i=0; i<2; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestReplicationPolicyConsiderLoad.java
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.TestBlockStoragePolicy;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
      dnrList.add(dnr);
      dnManager.registerDatanode(dnr);
      dataNodes[i].getStorageInfos()[0].setUtilizationForTesting(
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*blockSize, 0L,
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*blockSize, 0L);
      dataNodes[i].updateHeartbeat(
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[i]),
          0L, 0L, 0, 0, null);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestReplicationPolicyWithNodeGroup.java
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.TestBlockStoragePolicy;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NetworkTopologyWithNodeGroup;
  private static void setupDataNodeCapacity() {
    for(int i=0; i<NUM_OF_DATANODES; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
  }
  
  @Test
  public void testChooseTarget1() throws Exception {
    updateHeartbeatWithUsage(dataNodes[0],
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        0L, 0L, 4, 0); // overloaded

    DatanodeStorageInfo[] targets;
    verifyNoTwoTargetsOnSameNodeGroup(targets);

    updateHeartbeatWithUsage(dataNodes[0],
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
  }

  private void verifyNoTwoTargetsOnSameNodeGroup(DatanodeStorageInfo[] targets) {
  public void testChooseTarget3() throws Exception {
    updateHeartbeatWithUsage(dataNodes[0],
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        (HdfsServerConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L,
        0L, 0L, 0, 0); // no space

    DatanodeStorageInfo[] targets;
               isOnSameRack(targets[2], targets[3]));

    updateHeartbeatWithUsage(dataNodes[0],
        2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
  }

    for(int i=0; i<3; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          (HdfsServerConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }

    DatanodeStorageInfo[] targets;
    }
    for(int i=0; i<NUM_OF_DATANODES_BOUNDARY; i++) {
      updateHeartbeatWithUsage(dataNodes[0],
                2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
                (HdfsServerConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE,
                0L, 0L, 0L, 0, 0);

      updateHeartbeatWithUsage(dataNodesInBoundaryCase[i],
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }

    DatanodeStorageInfo[] targets;
  public void testRereplicateOnBoundaryTopology() throws Exception {
    for(int i=0; i<NUM_OF_DATANODES_BOUNDARY; i++) {
      updateHeartbeatWithUsage(dataNodesInBoundaryCase[i],
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<DatanodeStorageInfo>();
    chosenNodes.add(storagesInBoundaryCase[0]);

    for(int i=0; i<NUM_OF_DATANODES_MORE_TARGETS; i++) {
      updateHeartbeatWithUsage(dataNodesInMoreTargetsCase[i],
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }

    DatanodeStorageInfo[] targets;
    for(int i=0; i<NUM_OF_DATANODES_FOR_DEPENDENCIES; i++) {
      updateHeartbeatWithUsage(dataNodesForDependencies[i],
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
    
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<DatanodeStorageInfo>();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDatanodeRegister.java

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.test.GenericTestUtils;
    doReturn(VersionInfo.getVersion()).when(fakeNsInfo).getSoftwareVersion();
    doReturn(HdfsServerConstants.NAMENODE_LAYOUT_VERSION).when(fakeNsInfo)
        .getLayoutVersion();
    
    DatanodeProtocolClientSideTranslatorPB fakeDnProt = 
  @Test
  public void testDifferentLayoutVersions() throws Exception {
    assertEquals(HdfsServerConstants.NAMENODE_LAYOUT_VERSION,
        actor.retrieveNamespaceInfo().getLayoutVersion());
    
    doReturn(HdfsServerConstants.NAMENODE_LAYOUT_VERSION * 1000).when(fakeNsInfo)
        .getLayoutVersion();
    try {
      actor.retrieveNamespaceInfo();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDirectoryScanner.java
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
      long blockId = deleteMetaFile();
      scan(totalBlocks, 1, 1, 0, 0, 1);
      verifyGenStamp(blockId, HdfsConstants.GRANDFATHER_GENERATION_STAMP);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      blockId = createBlockFile();
      totalBlocks++;
      scan(totalBlocks, 1, 1, 0, 1, 0);
      verifyAddition(blockId, HdfsConstants.GRANDFATHER_GENERATION_STAMP, 0);
      scan(totalBlocks, 0, 0, 0, 0, 0);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/mover/TestStorageMover.java
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
    DEFAULT_CONF.setLong(DFSConfigKeys.DFS_MOVER_MOVEDWINWIDTH_KEY, 2000L);

    DEFAULT_POLICIES = BlockStoragePolicySuite.createDefaultSuite();
    HOT = DEFAULT_POLICIES.getPolicy(HdfsServerConstants.HOT_STORAGE_POLICY_NAME);
    WARM = DEFAULT_POLICIES.getPolicy(HdfsServerConstants.WARM_STORAGE_POLICY_NAME);
    COLD = DEFAULT_POLICIES.getPolicy(HdfsServerConstants.COLD_STORAGE_POLICY_NAME);
    TestBalancer.initTestSetup();
    Dispatcher.setDelayAfterErrors(1000L);
  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/NNThroughputBenchmark.java
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
      long end = Time.now();
      for(boolean written = !closeUponCreate; !written; 
        written = nameNodeProto.complete(fileNames[daemonId][inputIdx],
                                    clientName, null, HdfsConstants.GRANDFATHER_INODE_ID));
      return end-start;
    }

            new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)), true, replication,
            BLOCK_SIZE, null);
        ExtendedBlock lastBlock = addBlocks(fileName, clientName);
        nameNodeProto.complete(fileName, clientName, lastBlock, HdfsConstants.GRANDFATHER_INODE_ID);
      }
      for(int idx=0; idx < nrDatanodes; idx++) {
      ExtendedBlock prevBlock = null;
      for(int jdx = 0; jdx < blocksPerFile; jdx++) {
        LocatedBlock loc = nameNodeProto.addBlock(fileName, clientName,
            prevBlock, null, HdfsConstants.GRANDFATHER_INODE_ID, null);
        prevBlock = loc.getBlock();
        for(DatanodeInfo dnInfo : loc.getLocations()) {
          int dnIdx = Arrays.binarySearch(datanodes, dnInfo.getXferAddr());

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestAddBlockRetry.java
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
    LOG.info("Starting first addBlock for " + src);
    LocatedBlock[] onRetryBlock = new LocatedBlock[1];
    DatanodeStorageInfo targets[] = ns.getNewBlockTargets(
        src, HdfsConstants.GRANDFATHER_INODE_ID, "clientName",
        null, null, null, onRetryBlock);
    assertNotNull("Targets must be generated", targets);

    LOG.info("Starting second addBlock for " + src);
    nn.addBlock(src, "clientName", null, null,
        HdfsConstants.GRANDFATHER_INODE_ID, null);
    assertTrue("Penultimate block must be complete",
        checkFileProgress(src, false));
    LocatedBlocks lbs = nn.getBlockLocations(src, 0, Long.MAX_VALUE);

    LocatedBlock newBlock = ns.storeAllocatedBlock(
        src, HdfsConstants.GRANDFATHER_INODE_ID, "clientName", null, targets);
    assertEquals("Blocks are not equal", lb2.getBlock(), newBlock.getBlock());

    LOG.info("Starting first addBlock for " + src);
    LocatedBlock lb1 = nameNodeRpc.addBlock(src, "clientName", null, null,
        HdfsConstants.GRANDFATHER_INODE_ID, null);
    assertTrue("Block locations should be present",
        lb1.getLocations().length > 0);

    cluster.restartNameNode();
    nameNodeRpc = cluster.getNameNodeRpc();
    LocatedBlock lb2 = nameNodeRpc.addBlock(src, "clientName", null, null,
        HdfsConstants.GRANDFATHER_INODE_ID, null);
    assertEquals("Blocks are not equal", lb1.getBlock(), lb2.getBlock());
    assertTrue("Wrong locations with retry", lb2.getLocations().length > 0);
  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestEditLog.java
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
  
    @Override
    public long getFirstTxId() {
      return HdfsServerConstants.INVALID_TXID;
    }
    
    @Override
    public long getLastTxId() {
      return HdfsServerConstants.INVALID_TXID;
    }
  
    @Override

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestEditLogFileInputStream.java
import java.net.URL;
import java.util.EnumMap;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.util.Holder;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.junit.Test;

    URL url = new URL("http://localhost/fakeLog");
    EditLogInputStream elis = EditLogFileInputStream.fromUrl(factory, url,
        HdfsServerConstants.INVALID_TXID, HdfsServerConstants.INVALID_TXID, false);
    EnumMap<FSEditLogOpCodes, Holder<Integer>> counts = FSImageTestUtil
        .countEditLogOpTypes(elis);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFSEditLogLoader.java
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.EditLogValidation;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
      truncateFile(logFile, txOffset);
      validation = EditLogFileInputStream.validateEditLog(logFile);
      long expectedEndTxId = (txId == 0) ?
          HdfsServerConstants.INVALID_TXID : (txId - 1);
      assertEquals("Failed when corrupting txid " + txId + " txn opcode " +
        "at " + txOffset, expectedEndTxId, validation.getEndTxId());
      assertTrue(!validation.hasCorruptHeader());
    EditLogValidation validation =
        EditLogFileInputStream.validateEditLog(logFile);
    assertTrue(!validation.hasCorruptHeader());
    assertEquals(HdfsServerConstants.INVALID_TXID, validation.getEndTxId());
  }

  private static final Map<Byte, FSEditLogOpCodes> byteToEnum =

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFSPermissionChecker.java
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
    PermissionStatus permStatus = PermissionStatus.createImmutable(owner, group,
      FsPermission.createImmutable(perm));
    INodeDirectory inodeDirectory = new INodeDirectory(
      HdfsConstants.GRANDFATHER_INODE_ID, name.getBytes("UTF-8"), permStatus, 0L);
    parent.addChild(inodeDirectory);
    return inodeDirectory;
  }
      String owner, String group, short perm) throws IOException {
    PermissionStatus permStatus = PermissionStatus.createImmutable(owner, group,
      FsPermission.createImmutable(perm));
    INodeFile inodeFile = new INodeFile(HdfsConstants.GRANDFATHER_INODE_ID,
      name.getBytes("UTF-8"), permStatus, 0L, 0L, null, REPLICATION,
      PREFERRED_BLOCK_SIZE, (byte)0);
    parent.addChild(inodeFile);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFileTruncate.java
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
    checkBlockRecovery(p);

    NameNodeAdapter.getLeaseManager(cluster.getNamesystem())
        .setLeasePeriod(HdfsServerConstants.LEASE_SOFTLIMIT_PERIOD,
            HdfsServerConstants.LEASE_HARDLIMIT_PERIOD);

    checkFullFile(p, newLength, contents);
    fs.delete(p, false);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestINodeFile.java
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
  private long preferredBlockSize = 1024;

  INodeFile createINodeFile(short replication, long preferredBlockSize) {
    return new INodeFile(HdfsConstants.GRANDFATHER_INODE_ID, null, perm, 0L, 0L,
        null, replication, preferredBlockSize, (byte)0);
  }

  private static INodeFile createINodeFile(byte storagePolicyID) {
    return new INodeFile(HdfsConstants.GRANDFATHER_INODE_ID, null, perm, 0L, 0L,
        null, (short)3, 1024L, storagePolicyID);
  }

    INodeFile inf = createINodeFile(replication, preferredBlockSize);
    inf.setLocalName(DFSUtil.string2Bytes("f"));

    INodeDirectory root = new INodeDirectory(HdfsConstants.GRANDFATHER_INODE_ID,
        INodeDirectory.ROOT_NAME, perm, 0L);
    INodeDirectory dir = new INodeDirectory(HdfsConstants.GRANDFATHER_INODE_ID,
        DFSUtil.string2Bytes("d"), perm, 0L);

    assertEquals("f", inf.getFullPathName());

    {//cast from INodeFileUnderConstruction
      final INode from = new INodeFile(
          HdfsConstants.GRANDFATHER_INODE_ID, null, perm, 0L, 0L, null, replication,
          1024L, (byte)0);
      from.asFile().toUnderConstruction("client", "machine");
    
    }

    {//cast from INodeDirectory
      final INode from = new INodeDirectory(HdfsConstants.GRANDFATHER_INODE_ID, null,
          perm, 0L);

  @Test
  public void testFileUnderConstruction() {
    replication = 3;
    final INodeFile file = new INodeFile(HdfsConstants.GRANDFATHER_INODE_ID, null,
        perm, 0L, 0L, null, replication, 1024L, (byte)0);
    assertFalse(file.isUnderConstruction());


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestMetadataVersionOutput.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.junit.After;
import org.junit.Test;

      assertExceptionContains("ExitException", e);
    }
    final String verNumStr = HdfsServerConstants.NAMENODE_LAYOUT_VERSION + "";
    assertTrue(baos.toString("UTF-8").
      contains("HDFS Image Version: " + verNumStr));
    assertTrue(baos.toString("UTF-8").

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestNameNodeOptionParsing.java
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.junit.Assert;
    opt = NameNode.parseArguments(new String[] { "-upgrade", "-renameReserved"});
    assertEquals(StartupOption.UPGRADE, opt);
    assertEquals(
        ".snapshot." + HdfsServerConstants.NAMENODE_LAYOUT_VERSION
            + ".UPGRADE_RENAMED",
        FSImageFormat.renameReservedMap.get(".snapshot"));
    assertEquals(
        ".reserved." + HdfsServerConstants.NAMENODE_LAYOUT_VERSION
            + ".UPGRADE_RENAMED",
        FSImageFormat.renameReservedMap.get(".reserved"));


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestTruncateQuotaUpdate.java
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
import org.junit.After;
import org.junit.Assert;
    dfs.mkdirs(dir);
    dfs.setQuota(dir, Long.MAX_VALUE - 1, DISKQUOTA);
    dfs.setQuotaByStorageType(dir, StorageType.DISK, DISKQUOTA);
    dfs.setStoragePolicy(dir, HdfsServerConstants.HOT_STORAGE_POLICY_NAME);
  }

  @After

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestDFSUpgradeWithHA.java
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster.Builder;
import org.apache.hadoop.hdfs.qjournal.server.Journal;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
    BestEffortLongFile committedTxnId = (BestEffortLongFile) Whitebox
        .getInternalState(journal1, "committedTxnId");
    return committedTxnId != null ? committedTxnId.get() :
        HdfsServerConstants.INVALID_TXID;
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestOpenFilesWithSnapshot.java
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
    String clientName = fs.getClient().getClientName();
    nameNodeRpc.addBlock(fileWithEmptyBlock.toString(), clientName, null, null,
        HdfsConstants.GRANDFATHER_INODE_ID, null);
    fs.createSnapshot(path, "s2");

    fs.rename(new Path("/test/test"), new Path("/test/test-renamed"));

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestSnapshot.java
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper.TestDirectoryTree;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper.TestDirectoryTree.Node;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestJsonUtil.java
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.util.Time;
import org.codehaus.jackson.map.ObjectMapper;
    final HdfsFileStatus status = new HdfsFileStatus(1001L, false, 3, 1L << 26,
        now, now + 10, new FsPermission((short) 0644), "user", "group",
        DFSUtil.string2Bytes("bar"), DFSUtil.string2Bytes("foo"),
        HdfsConstants.GRANDFATHER_INODE_ID, 0, null, (byte) 0);
    final FileStatus fstatus = toFileStatus(status, parent);
    System.out.println("status  = " + status);
    System.out.println("fstatus = " + fstatus);

