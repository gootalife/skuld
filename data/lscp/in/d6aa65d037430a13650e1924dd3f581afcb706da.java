hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DataStreamer.java
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.BlockWrite;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
    }
  }

  static class ErrorState {
    private boolean error = false;
    private int badNodeIndex = -1;
    private int restartingNodeIndex = -1;
    private long restartingNodeDeadline = 0;
    private final long datanodeRestartTimeout;

    ErrorState(long datanodeRestartTimeout) {
      this.datanodeRestartTimeout = datanodeRestartTimeout;
    }

    synchronized void reset() {
      error = false;
      badNodeIndex = -1;
      restartingNodeIndex = -1;
      restartingNodeDeadline = 0;
    }

    synchronized boolean hasError() {
      return error;
    }

    synchronized boolean hasDatanodeError() {
      return error && isNodeMarked();
    }

    synchronized void setError(boolean err) {
      this.error = err;
    }

    synchronized void setBadNodeIndex(int index) {
      this.badNodeIndex = index;
    }

    synchronized int getBadNodeIndex() {
      return badNodeIndex;
    }

    synchronized int getRestartingNodeIndex() {
      return restartingNodeIndex;
    }

    synchronized void initRestartingNode(int i, String message) {
      restartingNodeIndex = i;
      restartingNodeDeadline =  Time.monotonicNow() + datanodeRestartTimeout;
      badNodeIndex = -1;
      LOG.info(message);
    }

    synchronized boolean isRestartingNode() {
      return restartingNodeIndex >= 0;
    }

    synchronized boolean isNodeMarked() {
      return badNodeIndex >= 0 || isRestartingNode();
    }

    synchronized void markFirstNodeIfNotMarked() {
      if (!isNodeMarked()) {
        badNodeIndex = 0;
      }
    }

    synchronized void adjustState4RestartingNode() {
      if (restartingNodeIndex >= 0) {
        if (badNodeIndex > restartingNodeIndex) {
          restartingNodeIndex = -1;
        } else if (badNodeIndex < restartingNodeIndex) {
          restartingNodeIndex--;
        } else {
          throw new IllegalStateException("badNodeIndex = " + badNodeIndex
              + " = restartingNodeIndex = " + restartingNodeIndex);
        }
      }

      if (!isRestartingNode()) {
        error = false;
      }
      badNodeIndex = -1;
    }

    synchronized void checkRestartingNodeDeadline(DatanodeInfo[] nodes) {
      if (restartingNodeIndex >= 0) {
        if (!error) {
          throw new IllegalStateException("error=false while checking" +
              " restarting node deadline");
        }

        if (badNodeIndex == restartingNodeIndex) {
          badNodeIndex = -1;
        }
        if (Time.monotonicNow() >= restartingNodeDeadline) {
          restartingNodeDeadline = 0;
          final int i = restartingNodeIndex;
          restartingNodeIndex = -1;
          LOG.warn("Datanode " + i + " did not restart within "
              + datanodeRestartTimeout + "ms: " + nodes[i]);
          if (badNodeIndex == -1) {
            badNodeIndex = i;
          }
        }
      }
    }
  }

  private volatile boolean streamerClosed = false;
  private ExtendedBlock block; // its length is number of bytes acked
  private Token<BlockTokenIdentifier> accessToken;
  private volatile DatanodeInfo[] nodes = null; // list of targets for current block
  private volatile StorageType[] storageTypes = null;
  private volatile String[] storageIDs = null;
  private final ErrorState errorState;

  private BlockConstructionStage stage;  // block construction stage
  private long bytesSent = 0; // number of bytes that've been sent
  private final boolean isLazyPersistFile;
    this.cachingStrategy = cachingStrategy;
    this.byteArrayManager = byteArrayManage;
    this.isLazyPersistFile = isLazyPersist(stat);
    this.isAppend = isAppend;
    this.favoredNodes = favoredNodes;

    final DfsClientConf conf = dfsClient.getConf();
    this.dfsclientSlowLogThresholdMs = conf.getSlowIoWarningThresholdMs();
    this.excludedNodes = initExcludedNodes(conf.getExcludedNodesCacheExpiry());
    this.errorState = new ErrorState(conf.getDatanodeRestartTimeout());
  }

  void setPipelineInConstruction(LocatedBlock lastBlock) throws IOException{
    setPipeline(lastBlock);
    if (nodes.length < 1) {
      throw new IOException("Unable to retrieve blocks locations " +
          " for last block " + block +
    stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
  }

  private boolean shouldStop() {
    return streamerClosed || errorState.hasError() || !dfsClient.clientRunning;
  }

    TraceScope scope = NullScope.INSTANCE;
    while (!streamerClosed && dfsClient.clientRunning) {
      if (errorState.hasError() && response != null) {
        try {
          response.close();
          response.join();
      DFSPacket one;
      try {
        boolean doSleep = processDatanodeError();

        final int halfSocketTimeout = dfsClient.getConf().getSocketTimeout()/2; 
        synchronized (dataQueue) {
          long now = Time.monotonicNow();
          while ((!shouldStop() && dataQueue.size() == 0 &&
              (stage != BlockConstructionStage.DATA_STREAMING ||
                  stage == BlockConstructionStage.DATA_STREAMING &&
                      now - lastPacket < halfSocketTimeout)) || doSleep ) {
            doSleep = false;
            now = Time.monotonicNow();
          }
          if (shouldStop()) {
            continue;
          }
          if (dataQueue.isEmpty()) {
            one = createHeartbeatPacket();
          } else {
            try {
              backOffIfNecessary();
            LOG.debug("Append to block " + block);
          }
          setupPipelineForAppendOrRecovery();
          if (streamerClosed) {
            continue;
          }
          initDataStreaming();
        if (one.isLastPacketInBlock()) {
          synchronized (dataQueue) {
            while (!shouldStop() && ackQueue.size() != 0) {
              try {
                dataQueue.wait(1000);
              }
            }
          }
          if (shouldStop()) {
            continue;
          }
          stage = BlockConstructionStage.PIPELINE_CLOSE;
          errorState.markFirstNodeIfNotMarked();
          throw e;
        } finally {
          writeScope.close();
          bytesSent = tmpBytesSent;
        }

        if (shouldStop()) {
          continue;
        }

        if (one.isLastPacketInBlock()) {
          synchronized (dataQueue) {
            while (!shouldStop() && ackQueue.size() != 0) {
              dataQueue.wait(1000);// wait for acks to arrive from datanodes
            }
          }
          if (shouldStop()) {
            continue;
          }

        }
      } catch (Throwable e) {
        if (!errorState.isRestartingNode()) {
          if (e instanceof QuotaExceededException) {
        }
        lastException.set(e);
        assert !(e instanceof NullPointerException);
        errorState.setError(true);
        if (!errorState.isNodeMarked()) {
          streamerClosed = true;
        }
    }
  }

            if (PipelineAck.isRestartOOBStatus(reply) &&
                shouldWaitForRestart(i)) {
              final String message = "Datanode " + i + " is restarting: "
                  + targets[i];
              errorState.initRestartingNode(i, message);
              throw new IOException(message);
            }
            if (reply != SUCCESS) {
              errorState.setBadNodeIndex(i); // mark bad datanode
              throw new IOException("Bad response " + reply +
                  " for " + block + " from datanode " + targets[i]);
            }
          }

        } catch (Exception e) {
          if (!responderClosed) {
            lastException.set(e);
            errorState.setError(true);
            errorState.markFirstNodeIfNotMarked();
            synchronized (dataQueue) {
              dataQueue.notifyAll();
            }
            if (!errorState.isRestartingNode()) {
              LOG.warn("Exception for " + block, e);
            }
            responderClosed = true;
    }
  }

  private boolean processDatanodeError() throws IOException {
    if (!errorState.hasDatanodeError()) {
      return false;
    }
    if (response != null) {
      LOG.info("Error Recovery for " + block +
          " waiting for responder to exit. ");
              .append("The current failed datanode replacement policy is ")
              .append(dfsClient.dtpReplaceDatanodeOnFailure).append(", and ")
              .append("a client may configure this via '")
              .append(BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY)
              .append("' in its configuration.")
              .toString());
    }
    boolean success = false;
    long newGS = 0L;
    while (!success && !streamerClosed && dfsClient.clientRunning) {
      if (!handleRestartingDatanode()) {
        return false;
      }

      final boolean isRecovery = errorState.hasError();
      if (!handleBadDatanode()) {
        return false;
      }

      handleDatanodeReplacement();

      final LocatedBlock lb = updateBlockForPipeline();
      newGS = lb.getBlock().getGenerationStamp();
      accessToken = lb.getBlockToken();

      success = createBlockOutputStream(nodes, storageTypes, newGS, isRecovery);

      failPacket4Testing();

      errorState.checkRestartingNodeDeadline(nodes);
    } // while

    if (success) {
      block = updatePipeline(newGS);
    }
    return false; // do not sleep, continue processing
  }

  private boolean handleRestartingDatanode() {
    if (errorState.isRestartingNode()) {
      final long delay = Math.min(errorState.datanodeRestartTimeout, 4000L);
      try {
        Thread.sleep(delay);
      } catch (InterruptedException ie) {
        lastException.set(new IOException(
            "Interrupted while waiting for restarting "
            + nodes[errorState.getRestartingNodeIndex()]));
        streamerClosed = true;
        return false;
      }
    }
    return true;
  }

  private boolean handleBadDatanode() {
    final int badNodeIndex = errorState.getBadNodeIndex();
    if (badNodeIndex >= 0) {
      if (nodes.length <= 1) {
        lastException.set(new IOException("All datanodes "
            + Arrays.toString(nodes) + " are bad. Aborting..."));
        streamerClosed = true;
        return false;
      }

      LOG.warn("Error Recovery for " + block + " in pipeline "
          + Arrays.toString(nodes) + ": datanode " + badNodeIndex
          + "("+ nodes[badNodeIndex] + ") is bad.");
      failed.add(nodes[badNodeIndex]);

      DatanodeInfo[] newnodes = new DatanodeInfo[nodes.length-1];
      arraycopy(nodes, newnodes, badNodeIndex);

      final StorageType[] newStorageTypes = new StorageType[newnodes.length];
      arraycopy(storageTypes, newStorageTypes, badNodeIndex);

      final String[] newStorageIDs = new String[newnodes.length];
      arraycopy(storageIDs, newStorageIDs, badNodeIndex);

      setPipeline(newnodes, newStorageTypes, newStorageIDs);

      errorState.adjustState4RestartingNode();
      lastException.clear();
    }
    return true;
  }

  private void handleDatanodeReplacement() throws IOException {
    if (dfsClient.dtpReplaceDatanodeOnFailure.satisfy(stat.getReplication(),
        nodes, isAppend, isHflushed)) {
      try {
        }
        LOG.warn("Failed to replace datanode."
            + " Continue with the remaining datanodes since "
            + BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY
            + " is set to true.", ioe);
      }
    }
  }

  private void failPacket4Testing() {
    if (failPacket) { // for testing
      failPacket = false;
      try {
        Thread.sleep(2000);
      } catch (InterruptedException ie) {}
    }
  }

  LocatedBlock updateBlockForPipeline() throws IOException {
    return dfsClient.namenode.updateBlockForPipeline(
        block, dfsClient.clientName);
  }

  ExtendedBlock updatePipeline(long newGS) throws IOException {
    final ExtendedBlock newBlock = new ExtendedBlock(
        block.getBlockPoolId(), block.getBlockId(), block.getNumBytes(), newGS);
    dfsClient.namenode.updatePipeline(dfsClient.clientName, block, newBlock,
        nodes, storageIDs);
    return newBlock;
  }

    boolean success = false;
    ExtendedBlock oldBlock = block;
    do {
      errorState.reset();
      lastException.clear();
      success = false;

      DatanodeInfo[] excluded =
        dfsClient.namenode.abandonBlock(block, stat.getFileId(), src,
            dfsClient.clientName);
        block = null;
        final DatanodeInfo badNode = nodes[errorState.getBadNodeIndex()];
        LOG.info("Excluding datanode " + badNode);
        excludedNodes.put(badNode, badNode);
      }
    } while (!success && --count >= 0);

        if (PipelineAck.isRestartOOBStatus(pipelineStatus) &&
            !errorState.isRestartingNode()) {
          checkRestart = true;
          throw new IOException("A datanode is restarting.");
        }
        assert null == blockStream : "Previous blockStream unclosed";
        blockStream = out;
        result =  true; // success
        errorState.reset();
      } catch (IOException ie) {
        if (!errorState.isRestartingNode()) {
          LOG.info("Exception in createBlockOutputStream", ie);
        }
        if (ie instanceof InvalidEncryptionKeyException && refetchEncryptionKey > 0) {
          for (int i = 0; i < nodes.length; i++) {
            if (firstBadLink.equals(nodes[i].getXferAddr())) {
              errorState.setBadNodeIndex(i);
              break;
            }
          }
        } else {
          assert checkRestart == false;
          errorState.setBadNodeIndex(0);
        }

        final int i = errorState.getBadNodeIndex();
        if (checkRestart && shouldWaitForRestart(i)) {
          errorState.initRestartingNode(i, "Datanode " + i + " is restarting: " + nodes[i]);
        }
        errorState.setError(true);
        lastException.set(ie);
        result =  false;  // error
      } finally {
    return new DFSPacket(buf, 0, 0, DFSPacket.HEART_BEAT_SEQNO, 0, false);
  }

  private static LoadingCache<DatanodeInfo, DatanodeInfo> initExcludedNodes(
      long excludedNodesCacheExpiry) {
    return CacheBuilder.newBuilder()
        .expireAfterWrite(excludedNodesCacheExpiry, TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener<DatanodeInfo, DatanodeInfo>() {
          @Override
          public void onRemoval(

