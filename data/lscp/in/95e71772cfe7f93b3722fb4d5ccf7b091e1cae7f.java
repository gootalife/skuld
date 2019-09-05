hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  public static final Class<RamDiskReplicaLruTracker>  DFS_DATANODE_RAM_DISK_REPLICA_TRACKER_DEFAULT = RamDiskReplicaLruTracker.class;
  public static final String  DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_KEY = "dfs.datanode.network.counts.cache.max.size";
  public static final int     DFS_DATANODE_NETWORK_COUNTS_CACHE_MAX_SIZE_DEFAULT = Integer.MAX_VALUE;
  public static final String DFS_DATANODE_NON_LOCAL_LAZY_PERSIST =
      "dfs.datanode.non.local.lazy.persist";
  public static final boolean DFS_DATANODE_NON_LOCAL_LAZY_PERSIST_DEFAULT =
      false;

  public static final String  DFS_DATANODE_DUPLICATE_REPLICA_DELETION = "dfs.datanode.duplicate.replica.deletion";

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DNConf.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_NON_LOCAL_LAZY_PERSIST;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_NON_LOCAL_LAZY_PERSIST_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY;

  final long maxLockedMemory;

  private final boolean allowNonLocalLazyPersist;

  public DNConf(Configuration conf) {
    this.conf = conf;
    socketTimeout = conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY,
    this.restartReplicaExpiry = conf.getLong(
        DFS_DATANODE_RESTART_REPLICA_EXPIRY_KEY,
        DFS_DATANODE_RESTART_REPLICA_EXPIRY_DEFAULT) * 1000L;

    this.allowNonLocalLazyPersist = conf.getBoolean(
        DFS_DATANODE_NON_LOCAL_LAZY_PERSIST,
        DFS_DATANODE_NON_LOCAL_LAZY_PERSIST_DEFAULT);
  }

  public boolean getIgnoreSecurePortsForTesting() {
    return ignoreSecurePortsForTesting;
  }

  public boolean getAllowNonLocalLazyPersist() {
    return allowNonLocalLazyPersist;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataXceiver.java
import java.security.MessageDigest;
import java.util.Arrays;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
      final long latestGenerationStamp,
      DataChecksum requestedChecksum,
      CachingStrategy cachingStrategy,
      boolean allowLazyPersist,
      final boolean pinning,
      final boolean[] targetPinnings) throws IOException {
    previousOpClientName = clientname;
    final boolean isClient = !isDatanode;
    final boolean isTransfer = stage == BlockConstructionStage.TRANSFER_RBW
        || stage == BlockConstructionStage.TRANSFER_FINALIZED;
    allowLazyPersist = allowLazyPersist &&
        (dnConf.getAllowNonLocalLazyPersist() || peer.isLocal());
    long size = 0;
    if (isTransfer && targets.length > 0) {
        + localAddress);

    final DataOutputStream replyOut = getBufferedOutputStream();
    checkAccess(replyOut, isClient, block, blockToken,
        Op.WRITE_BLOCK, BlockTokenIdentifier.AccessMode.WRITE);

      if (isDatanode || 
          stage != BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
        blockReceiver = getBlockReceiver(block, storageType, in,
            peer.getRemoteAddressString(),
            peer.getLocalAddressString(),
            stage, latestGenerationStamp, minBytesRcvd, maxBytesRcvd,
              smallBufferSize));
          mirrorIn = new DataInputStream(unbufMirrorIn);

          if (targetPinnings != null && targetPinnings.length > 0) {
            new Sender(mirrorOut).writeBlock(originalBlock, targetStorageTypes[0],
              blockToken, clientname, targets, targetStorageTypes, srcDataNode,
              stage, pipelineSize, minBytesRcvd, maxBytesRcvd,
              latestGenerationStamp, requestedChecksum, cachingStrategy,
                allowLazyPersist, targetPinnings[0], targetPinnings);
          } else {
            new Sender(mirrorOut).writeBlock(originalBlock, targetStorageTypes[0],
              blockToken, clientname, targets, targetStorageTypes, srcDataNode,
              stage, pipelineSize, minBytesRcvd, maxBytesRcvd,
              latestGenerationStamp, requestedChecksum, cachingStrategy,
                allowLazyPersist, false, targetPinnings);
          }

          mirrorOut.flush();
    }

    datanode.getMetrics().addWriteBlockOp(elapsed());
    datanode.getMetrics().incrWritesFromClient(peer.isLocal(), size);
  }

  @Override
        DataChecksum remoteChecksum = DataTransferProtoUtil.fromProto(
            checksumInfo.getChecksum());
        blockReceiver = getBlockReceiver(block, storageType,
            proxyReply, proxySock.getRemoteSocketAddress().toString(),
            proxySock.getLocalSocketAddress().toString(),
            null, 0, 0, 0, "", null, datanode, remoteChecksum,
    datanode.metrics.addReplaceBlockOp(elapsed());
  }


  @VisibleForTesting
  BlockReceiver getBlockReceiver(
      final ExtendedBlock block, final StorageType storageType,
      final DataInputStream in,
      final String inAddr, final String myAddr,
      final BlockConstructionStage stage,
      final long newGs, final long minBytesRcvd, final long maxBytesRcvd,
      final String clientname, final DatanodeInfo srcDataNode,
      final DataNode dn, DataChecksum requestedChecksum,
      CachingStrategy cachingStrategy,
      final boolean allowLazyPersist,
      final boolean pinning) throws IOException {
    return new BlockReceiver(block, storageType, in,
        inAddr, myAddr, stage, newGs, minBytesRcvd, maxBytesRcvd,
        clientname, srcDataNode, dn, requestedChecksum,
        cachingStrategy, allowLazyPersist, pinning);
  }

  @VisibleForTesting
  DataOutputStream getBufferedOutputStream() {
    return new DataOutputStream(
        new BufferedOutputStream(getOutputStream(), smallBufferSize));
  }


  private long elapsed() {
    return monotonicNow() - opStartTime;
  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDataXceiverLazyPersistHint.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDataXceiverLazyPersistHint.java

package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.net.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.*;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.util.DataChecksum;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.ArgumentCaptor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;


public class TestDataXceiverLazyPersistHint {
  @Rule
  public Timeout timeout = new Timeout(300000);

  private enum PeerLocality {
    LOCAL,
    REMOTE
  }

  private enum NonLocalLazyPersist {
    ALLOWED,
    NOT_ALLOWED
  }

  @Test
  public void testWithLocalClient() throws IOException {
    ArgumentCaptor<Boolean> captor = ArgumentCaptor.forClass(Boolean.class);
    DataXceiver xceiver = makeStubDataXceiver(
        PeerLocality.LOCAL, NonLocalLazyPersist.NOT_ALLOWED, captor);

    for (Boolean lazyPersistSetting : Arrays.asList(true, false)) {
      issueWriteBlockCall(xceiver, lazyPersistSetting);
      assertThat(captor.getValue(), is(lazyPersistSetting));
    }
  }

  @Test
  public void testWithRemoteClient() throws IOException {
    ArgumentCaptor<Boolean> captor = ArgumentCaptor.forClass(Boolean.class);
    DataXceiver xceiver = makeStubDataXceiver(
        PeerLocality.REMOTE, NonLocalLazyPersist.NOT_ALLOWED, captor);

    for (Boolean lazyPersistSetting : Arrays.asList(true, false)) {
      issueWriteBlockCall(xceiver, lazyPersistSetting);
      assertThat(captor.getValue(), is(false));
    }
  }

  @Test
  public void testOverrideWithRemoteClient() throws IOException {
    ArgumentCaptor<Boolean> captor = ArgumentCaptor.forClass(Boolean.class);
    DataXceiver xceiver = makeStubDataXceiver(
        PeerLocality.REMOTE, NonLocalLazyPersist.ALLOWED, captor);

    for (Boolean lazyPersistSetting : Arrays.asList(true, false)) {
      issueWriteBlockCall(xceiver, lazyPersistSetting);
      assertThat(captor.getValue(), is(lazyPersistSetting));
    }
  }

  private void issueWriteBlockCall(DataXceiver xceiver, boolean lazyPersist)
      throws IOException {
    xceiver.writeBlock(
        new ExtendedBlock("Dummy-pool", 0L),
        StorageType.RAM_DISK,
        null,
        "Dummy-Client",
        new DatanodeInfo[0],
        new StorageType[0],
        mock(DatanodeInfo.class),
        BlockConstructionStage.PIPELINE_SETUP_CREATE,
        0, 0, 0, 0,
        DataChecksum.newDataChecksum(DataChecksum.Type.NULL, 0),
        CachingStrategy.newDefaultStrategy(),
        lazyPersist,
        false, null);
  }


  private static DataXceiver makeStubDataXceiver(
      PeerLocality locality,
      NonLocalLazyPersist nonLocalLazyPersist,
      final ArgumentCaptor<Boolean> captor) throws IOException {
    DataXceiver xceiverSpy = spy(DataXceiver.create(
            getMockPeer(locality),
            getMockDn(nonLocalLazyPersist),
            mock(DataXceiverServer.class)));

    doReturn(mock(BlockReceiver.class)).when(xceiverSpy).getBlockReceiver(
        any(ExtendedBlock.class), any(StorageType.class),
        any(DataInputStream.class), anyString(), anyString(),
        any(BlockConstructionStage.class), anyLong(), anyLong(), anyLong(),
        anyString(), any(DatanodeInfo.class), any(DataNode.class),
        any(DataChecksum.class), any(CachingStrategy.class),
        captor.capture(), anyBoolean());
    doReturn(mock(DataOutputStream.class)).when(xceiverSpy)
        .getBufferedOutputStream();
    return xceiverSpy;
  }

  private static Peer getMockPeer(PeerLocality locality) {
    Peer peer = mock(Peer.class);
    when(peer.isLocal()).thenReturn(locality == PeerLocality.LOCAL);
    when(peer.getRemoteAddressString()).thenReturn("1.1.1.1:1000");
    when(peer.getLocalAddressString()).thenReturn("2.2.2.2:2000");
    return peer;
  }

  private static DataNode getMockDn(NonLocalLazyPersist nonLocalLazyPersist) {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(
        DFS_DATANODE_NON_LOCAL_LAZY_PERSIST,
        nonLocalLazyPersist == NonLocalLazyPersist.ALLOWED);
    DNConf dnConf = new DNConf(conf);
    DataNodeMetrics mockMetrics = mock(DataNodeMetrics.class);
    DataNode mockDn = mock(DataNode.class);
    when(mockDn.getDnConf()).thenReturn(dnConf);
    when(mockDn.getConf()).thenReturn(conf);
    when(mockDn.getMetrics()).thenReturn(mockMetrics);
    return mockDn;
  }
}

