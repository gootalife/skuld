hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/MultipleIOException.java
package org.apache.hadoop.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
    }
    return new MultipleIOException(exceptions);
  }

  public static class Builder {
    private List<IOException> exceptions;
    
    public void add(Throwable t) {
      if (exceptions == null) {
        exceptions = new ArrayList<>();
      }
      exceptions.add(t instanceof IOException? (IOException)t
          : new IOException(t));
    }

    public IOException build() {
      return createIOException(exceptions);
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSOutputStream.java
  @Override
  protected void checkClosed() throws IOException {
    if (isClosed()) {
      streamer.getLastException().throwException4Close();
    }
  }

    computePacketChunkSize(dfsClient.getConf().getWritePacketSize(), bytesPerChecksum);

    streamer = new DataStreamer(stat, null, dfsClient, src, progress, checksum,
        cachingStrategy, byteArrayManager, favoredNodes);
  }

  static DFSOutputStream newStreamForCreate(DFSClient dfsClient, String src,
  private DFSOutputStream(DFSClient dfsClient, String src,
      EnumSet<CreateFlag> flags, Progressable progress, LocatedBlock lastBlock,
      HdfsFileStatus stat, DataChecksum checksum, String[] favoredNodes)
          throws IOException {
    this(dfsClient, src, progress, stat, checksum);
    initialFileSize = stat.getLen(); // length of file when opened
    this.shouldSyncBlock = flags.contains(CreateFlag.SYNC_BLOCK);
      computePacketChunkSize(dfsClient.getConf().getWritePacketSize(),
          bytesPerChecksum);
      streamer = new DataStreamer(stat, lastBlock != null ? lastBlock.getBlock() : null,
          dfsClient, src, progress, checksum, cachingStrategy, byteArrayManager,
          favoredNodes);
    }
  }

        dfsClient.getPathTraceScope("newStreamForAppend", src);
    try {
      final DFSOutputStream out = new DFSOutputStream(dfsClient, src, flags,
          progress, lastBlock, stat, checksum, favoredNodes);
      out.start();
      return out;
    } finally {
      DFSClient.LOG.warn("Error while syncing", e);
      synchronized (this) {
        if (!isClosed()) {
          streamer.getLastException().set(e);
          closeThreads(true);
        }
      }
    if (isClosed()) {
      return;
    }
    streamer.getLastException().set(new IOException("Lease timeout of "
        + (dfsClient.getConf().getHdfsTimeout()/1000) + " seconds expired."));
    closeThreads(true);
    dfsClient.endFileLease(fileId);

  protected synchronized void closeImpl() throws IOException {
    if (isClosed()) {
      streamer.getLastException().check();
      return;
    }

    try {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DataStreamer.java
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.htrace.TraceInfo;
import org.apache.htrace.TraceScope;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

@InterfaceAudience.Private
class DataStreamer extends Daemon {
  static final Log LOG = LogFactory.getLog(DataStreamer.class);
      final int length, final DFSClient client) throws IOException {
    final DfsClientConf conf = client.getConf();
    final String dnAddr = first.getXferAddr(conf.isConnectToDnViaHostname());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to datanode " + dnAddr);
    }
    final InetSocketAddress isa = NetUtils.createSocketAddr(dnAddr);
    final Socket sock = client.socketFactory.createSocket();
    NetUtils.connect(sock, isa, client.getRandomLocalInterfaceAddr(), conf.getSocketTimeout());
    sock.setSoTimeout(timeout);
    sock.setSendBufferSize(HdfsConstants.DEFAULT_DATA_SOCKET_SIZE);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Send buf size " + sock.getSendBufferSize());
    }
    return sock;
  }
    packets.clear();
  }
  
  static class LastException {
    private Throwable thrown;

    synchronized void set(Throwable t) {
      Preconditions.checkNotNull(t);
      Preconditions.checkState(thrown == null);
      this.thrown = t;
    }

    synchronized void clear() {
      thrown = null;
    }

    synchronized void check() throws IOException {
      if (thrown != null) {
        throw new IOException(thrown);
      }
    }

    synchronized void throwException4Close() throws IOException {
      check();
      final IOException ioe = new ClosedChannelException();
      thrown = ioe;
      throw ioe;
    }
  }

  private volatile boolean streamerClosed = false;
  private ExtendedBlock block; // its length is number of bytes acked
  private Token<BlockTokenIdentifier> accessToken;
  private volatile DatanodeInfo[] nodes = null; // list of targets for current block
  private volatile StorageType[] storageTypes = null;
  private volatile String[] storageIDs = null;
  volatile boolean hasError = false;
  volatile int errorIndex = -1;
  private boolean isHflushed = false;
  private final boolean isAppend;

  private long currentSeqno = 0;
  private long lastQueuedSeqno = -1;
  private long lastAckedSeqno = -1;
  private long bytesCurBlock = 0; // bytes written in current block
  private final LastException lastException = new LastException();
  private Socket s;

  private final DFSClient dfsClient;
  private long artificialSlowdown = 0;
  private final List<DatanodeInfo> congestedNodes = new ArrayList<>();
  private static final int CONGESTION_BACKOFF_MEAN_TIME_IN_MS = 5000;
  private static final int CONGESTION_BACK_OFF_MAX_TIME_IN_MS =
      CONGESTION_BACKOFF_MEAN_TIME_IN_MS * 10;
  private int lastCongestionBackoffTime;

  private final LoadingCache<DatanodeInfo, DatanodeInfo> excludedNodes;
  private final String[] favoredNodes;

  private DataStreamer(HdfsFileStatus stat, DFSClient dfsClient, String src,
                       Progressable progress, DataChecksum checksum,
                       AtomicReference<CachingStrategy> cachingStrategy,
                       ByteArrayManager byteArrayManage,
                       boolean isAppend, String[] favoredNodes) {
    this.dfsClient = dfsClient;
    this.src = src;
    this.progress = progress;
    this.checksum4WriteBlock = checksum;
    this.cachingStrategy = cachingStrategy;
    this.byteArrayManager = byteArrayManage;
    this.isLazyPersistFile = isLazyPersist(stat);
    this.dfsclientSlowLogThresholdMs =
        dfsClient.getConf().getSlowIoWarningThresholdMs();
    this.excludedNodes = initExcludedNodes();
    this.isAppend = isAppend;
    this.favoredNodes = favoredNodes;
  }

  DataStreamer(HdfsFileStatus stat, ExtendedBlock block, DFSClient dfsClient,
               String src, Progressable progress, DataChecksum checksum,
               AtomicReference<CachingStrategy> cachingStrategy,
               ByteArrayManager byteArrayManage, String[] favoredNodes) {
    this(stat, dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage, false, favoredNodes);
    this.block = block;
    stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
  }
               AtomicReference<CachingStrategy> cachingStrategy,
               ByteArrayManager byteArrayManage) throws IOException {
    this(stat, dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage, true, null);
    stage = BlockConstructionStage.PIPELINE_SETUP_APPEND;
    block = lastBlock.getBlock();
    bytesSent = block.getNumBytes();
    this.storageIDs = storageIDs;
  }

  }

  private void endBlock() {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Closing old block " + block);
    }
    this.setName("DataStreamer for file " + src);
    closeResponder();
          response.join();
          response = null;
        } catch (InterruptedException  e) {
          LOG.warn("Caught exception", e);
        }
      }

            try {
              dataQueue.wait(timeout);
            } catch (InterruptedException  e) {
              LOG.warn("Caught exception", e);
            }
            doSleep = false;
            now = Time.monotonicNow();
            try {
              backOffIfNecessary();
            } catch (InterruptedException e) {
              LOG.warn("Caught exception", e);
            }
            one = dataQueue.getFirst(); // regular data packet
            long parents[] = one.getTraceParents();

        if (stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("Allocating new block");
          }
          setPipeline(nextBlockOutputStream());
          initDataStreaming();
        } else if (stage == BlockConstructionStage.PIPELINE_SETUP_APPEND) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("Append to block " + block);
          }
          setupPipelineForAppendOrRecovery();
          initDataStreaming();
                dataQueue.wait(1000);
              } catch (InterruptedException  e) {
                LOG.warn("Caught exception", e);
              }
            }
          }
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("DataStreamer block " + block +
              " sending packet " + one);
        }

          if (e instanceof QuotaExceededException) {
            LOG.debug("DataStreamer Quota Exception", e);
          } else {
            LOG.warn("DataStreamer Exception", e);
          }
        }
        lastException.set(e);
        hasError = true;
        if (errorIndex == -1 && restartingNodeIndex.get() == -1) {
  void waitForAckedSeqno(long seqno) throws IOException {
    TraceScope scope = Trace.startSpan("waitForAckedSeqno", Sampler.NEVER);
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Waiting for ack for: " + seqno);
      }
      long begin = Time.monotonicNow();
      try {
      }
      long duration = Time.monotonicNow() - begin;
      if (duration > dfsclientSlowLogThresholdMs) {
        LOG.warn("Slow waitForAckedSeqno took " + duration
            + "ms (threshold=" + dfsclientSlowLogThresholdMs + "ms)");
      }
    } finally {

  private void checkClosed() throws IOException {
    if (streamerClosed) {
      lastException.throwException4Close();
    }
  }

        response.close();
        response.join();
      } catch (InterruptedException  e) {
        LOG.warn("Caught exception", e);
      } finally {
        response = null;
      }
  }

  private void closeStream() {
    final MultipleIOException.Builder b = new MultipleIOException.Builder();

    if (blockStream != null) {
      try {
        blockStream.close();
      } catch (IOException e) {
        b.add(e);
      } finally {
        blockStream = null;
      }
      try {
        blockReplyStream.close();
      } catch (IOException e) {
        b.add(e);
      } finally {
        blockReplyStream = null;
      }
      try {
        s.close();
      } catch (IOException e) {
        b.add(e);
      } finally {
        s = null;
      }
    }

    final IOException ioe = b.build();
    if (ioe != null) {
      lastException.set(ioe);
    }
  }

          long duration = Time.monotonicNow() - begin;
          if (duration > dfsclientSlowLogThresholdMs
              && ack.getSeqno() != DFSPacket.HEART_BEAT_SEQNO) {
            LOG.warn("Slow ReadProcessor read fields took " + duration
                + "ms (threshold=" + dfsclientSlowLogThresholdMs + "ms); ack: "
                + ack + ", targets: " + Arrays.asList(targets));
          } else if (LOG.isDebugEnabled()) {
            LOG.debug("DFSClient " + ack);
          }

          long seqno = ack.getSeqno();
                  + Time.monotonicNow();
              setRestartingNodeIndex(i);
              String message = "A datanode is restarting: " + targets[i];
              LOG.info(message);
              throw new IOException(message);
            }
          }
        } catch (Exception e) {
          if (!responderClosed) {
            lastException.set(e);
            hasError = true;
              dataQueue.notifyAll();
            }
            if (restartingNodeIndex.get() == -1) {
              LOG.warn("Exception for " + block, e);
            }
            responderClosed = true;
          }
  private boolean processDatanodeError() throws IOException {
    if (response != null) {
      LOG.info("Error Recovery for " + block +
          " waiting for responder to exit. ");
      return true;
    }
      if (++pipelineRecoveryCount > 5) {
        LOG.warn("Error recovering pipeline for writing " +
            block + ". Already retried 5 times for the same packet.");
        lastException.set(new IOException("Failing write. Tried pipeline " +
            "recovery 5 times without success."));
    if (nodes == null || nodes.length == 0) {
      String msg = "Could not get block locations. " + "Source file \""
          + src + "\" - Aborting...";
      LOG.warn(msg);
      lastException.set(new IOException(msg));
      streamerClosed = true;
      return false;
    }
          streamerClosed = true;
          return false;
        }
        LOG.warn("Error Recovery for block " + block +
            " in pipeline " + pipelineMsg +
            ": bad datanode " + nodes[errorIndex]);
        failed.add(nodes[errorIndex]);
        if (restartingNodeIndex.get() == -1) {
          hasError = false;
        }
        lastException.clear();
        errorIndex = -1;
      }

          if (!dfsClient.dtpReplaceDatanodeOnFailure.isBestEffort()) {
            throw ioe;
          }
          LOG.warn("Failed to replace datanode."
              + " Continue with the remaining datanodes since "
              + HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY
              + " is set to true.", ioe);
        restartDeadline = 0;
        int expiredNodeIndex = restartingNodeIndex.get();
        restartingNodeIndex.set(-1);
        LOG.warn("Datanode did not restart in time: " +
            nodes[expiredNodeIndex]);
    ExtendedBlock oldBlock = block;
    do {
      hasError = false;
      lastException.clear();
      errorIndex = -1;
      success = false;

      success = createBlockOutputStream(nodes, storageTypes, 0L, false);

      if (!success) {
        LOG.info("Abandoning " + block);
        dfsClient.namenode.abandonBlock(block, stat.getFileId(), src,
            dfsClient.clientName);
        block = null;
        LOG.info("Excluding datanode " + nodes[errorIndex]);
        excludedNodes.put(nodes[errorIndex], nodes[errorIndex]);
      }
    } while (!success && --count >= 0);
  private boolean createBlockOutputStream(DatanodeInfo[] nodes,
      StorageType[] nodeStorageTypes, long newGS, boolean recoveryFlag) {
    if (nodes.length == 0) {
      LOG.info("nodes are empty for write pipeline of " + block);
      return false;
    }
    Status pipelineStatus = SUCCESS;
    String firstBadLink = "";
    boolean checkRestart = false;
    if (LOG.isDebugEnabled()) {
      LOG.debug("pipeline = " + Arrays.asList(nodes));
    }

        hasError = false;
      } catch (IOException ie) {
        if (restartingNodeIndex.get() == -1) {
          LOG.info("Exception in createBlockOutputStream", ie);
        }
        if (ie instanceof InvalidEncryptionKeyException && refetchEncryptionKey > 0) {
          LOG.info("Will fetch a new encryption key and retry, "
              + "encryption key was invalid when connecting to "
              + nodes[0] + " : " + ie);
              + Time.monotonicNow();
          restartingNodeIndex.set(errorIndex);
          errorIndex = -1;
          LOG.info("Waiting for the datanode to be restarted: " +
              nodes[restartingNodeIndex.get()]);
        }
        hasError = true;
        lastException.set(ie);
        result =  false;  // error
      } finally {
        if (!result) {
          new HashSet<String>(Arrays.asList(favoredNodes));
      for (int i = 0; i < nodes.length; i++) {
        pinnings[i] = favoredSet.remove(nodes[i].getXferAddrWithHostname());
        if (LOG.isDebugEnabled()) {
          LOG.debug(nodes[i].getXferAddrWithHostname() +
              " was chosen by name node (favored=" + pinnings[i] + ").");
        }
      }
      if (shouldLog && !favoredSet.isEmpty()) {
        LOG.warn("These favored nodes were specified but not chosen: "
            + favoredSet + " Specified favored nodes: "
            + Arrays.toString(favoredNodes));

      }
      return pinnings;
              throw e;
            } else {
              --retries;
              LOG.info("Exception while adding a block", e);
              long elapsed = Time.monotonicNow() - localstart;
              if (elapsed > 5000) {
                LOG.info("Waiting for replication for "
                    + (elapsed / 1000) + " seconds");
              }
              try {
                LOG.warn("NotReplicatedYetException sleeping " + src
                    + " retries left " + retries);
                Thread.sleep(sleeptime);
                sleeptime *= 2;
              } catch (InterruptedException ie) {
                LOG.warn("Caught exception", ie);
              }
            }
          } else {
                     (int)(base + Math.random() * range));
        lastCongestionBackoffTime = t;
        sb.append(" are congested. Backing off for ").append(t).append(" ms");
        LOG.info(sb.toString());
        congestedNodes.clear();
      }
    }
    return accessToken;
  }

      packet.addTraceParent(Trace.currentSpan());
      dataQueue.addLast(packet);
      lastQueuedSeqno = packet.getSeqno();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Queued packet " + packet.getSeqno());
      }
      dataQueue.notifyAll();
    }
          @Override
          public void onRemoval(
              RemovalNotification<DatanodeInfo, DatanodeInfo> notification) {
            LOG.info("Removing node " + notification.getKey()
                + " from the excluded nodes list");
          }
        }).build(new CacheLoader<DatanodeInfo, DatanodeInfo>() {
  }

  LastException getLastException(){
    return lastException;
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSOutputStream.java
    FSDataOutputStream os = fs.create(new Path("/test"));
    DFSOutputStream dos = (DFSOutputStream) Whitebox.getInternalState(os,
        "wrappedStream");
    DataStreamer streamer = (DataStreamer) Whitebox
        .getInternalState(dos, "streamer");
    @SuppressWarnings("unchecked")
        mock(HdfsFileStatus.class),
        mock(ExtendedBlock.class),
        client,
        "foo", null, null, null, null, null);

    DataOutputStream blockStream = mock(DataOutputStream.class);
    doThrow(new IOException()).when(blockStream).flush();

