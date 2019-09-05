hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSOutputStream.java
  @Override
  protected void checkClosed() throws IOException {
    if (isClosed()) {
      getStreamer().getLastException().throwException4Close();
    }
  }

  @VisibleForTesting
  public synchronized DatanodeInfo[] getPipeline() {
    if (getStreamer().streamerClosed()) {
      return null;
    }
    DatanodeInfo[] currentNodes = getStreamer().getNodes();
    if (currentNodes == null) {
      return null;
    }
      streamer = new DataStreamer(lastBlock, stat, dfsClient, src, progress, checksum,
          cachingStrategy, byteArrayManager);
      getStreamer().setBytesCurBlock(lastBlock.getBlockSize());
      adjustPacketChunkSize(stat);
      getStreamer().setPipelineInConstruction(lastBlock);
    } else {
      computePacketChunkSize(dfsClient.getConf().getWritePacketSize(),
          bytesPerChecksum);
      computePacketChunkSize(0, freeInCksum);
      setChecksumBufSize(freeInCksum);
      getStreamer().setAppendChunk(true);
    } else {
    }

    if (currentPacket == null) {
      currentPacket = createPacket(packetSize, chunksPerPacket, getStreamer()
          .getBytesCurBlock(), getStreamer().getAndIncCurrentSeqno(), false);
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("DFSClient writeChunk allocating new packet seqno=" + 
            currentPacket.getSeqno() +
            ", src=" + src +
            ", packetSize=" + packetSize +
            ", chunksPerPacket=" + chunksPerPacket +
            ", bytesCurBlock=" + getStreamer().getBytesCurBlock());
      }
    }

    currentPacket.writeChecksum(checksum, ckoff, cklen);
    currentPacket.writeData(b, offset, len);
    currentPacket.incNumChunks();
    getStreamer().incBytesCurBlock(len);

    if (currentPacket.getNumChunks() == currentPacket.getMaxChunks() ||
        getStreamer().getBytesCurBlock() == blockSize) {
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("DFSClient writeChunk packet full seqno=" +
            currentPacket.getSeqno() +
            ", src=" + src +
            ", bytesCurBlock=" + getStreamer().getBytesCurBlock() +
            ", blockSize=" + blockSize +
            ", appendChunk=" + getStreamer().getAppendChunk());
      }
      getStreamer().waitAndQueuePacket(currentPacket);
      currentPacket = null;

      adjustChunkBoundary();
  protected void adjustChunkBoundary() {
    if (getStreamer().getAppendChunk() &&
        getStreamer().getBytesCurBlock() % bytesPerChecksum == 0) {
      getStreamer().setAppendChunk(false);
      resetChecksumBufSize();
    }

    if (!getStreamer().getAppendChunk()) {
      int psize = Math.min((int)(blockSize- getStreamer().getBytesCurBlock()),
          dfsClient.getConf().getWritePacketSize());
      computePacketChunkSize(psize, bytesPerChecksum);
    }
  protected void endBlock() throws IOException {
    if (getStreamer().getBytesCurBlock() == blockSize) {
      currentPacket = createPacket(0, 0, getStreamer().getBytesCurBlock(),
          getStreamer().getAndIncCurrentSeqno(), true);
      currentPacket.setSyncBlock(shouldSyncBlock);
      getStreamer().waitAndQueuePacket(currentPacket);
      currentPacket = null;
      getStreamer().setBytesCurBlock(0);
      lastFlushOffset = 0;
    }
  }

        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("DFSClient flush(): "
              + " bytesCurBlock=" + getStreamer().getBytesCurBlock()
              + " lastFlushOffset=" + lastFlushOffset
              + " createNewBlock=" + endBlock);
        }
        if (lastFlushOffset != getStreamer().getBytesCurBlock()) {
          assert getStreamer().getBytesCurBlock() > lastFlushOffset;
          lastFlushOffset = getStreamer().getBytesCurBlock();
          if (isSync && currentPacket == null && !endBlock) {
            currentPacket = createPacket(packetSize, chunksPerPacket,
                getStreamer().getBytesCurBlock(), getStreamer()
                    .getAndIncCurrentSeqno(), false);
          }
        } else {
          if (isSync && getStreamer().getBytesCurBlock() > 0 && !endBlock) {
            currentPacket = createPacket(packetSize, chunksPerPacket,
                getStreamer().getBytesCurBlock(), getStreamer()
                    .getAndIncCurrentSeqno(), false);
          } else if (currentPacket != null) {
            currentPacket.releaseBuffer(byteArrayManager);
        }
        if (currentPacket != null) {
          currentPacket.setSyncBlock(isSync);
          getStreamer().waitAndQueuePacket(currentPacket);
          currentPacket = null;
        }
        if (endBlock && getStreamer().getBytesCurBlock() > 0) {
          currentPacket = createPacket(0, 0, getStreamer().getBytesCurBlock(),
              getStreamer().getAndIncCurrentSeqno(), true);
          currentPacket.setSyncBlock(shouldSyncBlock || isSync);
          getStreamer().waitAndQueuePacket(currentPacket);
          currentPacket = null;
          getStreamer().setBytesCurBlock(0);
          lastFlushOffset = 0;
        } else {
          getStreamer().setBytesCurBlock(
              getStreamer().getBytesCurBlock() - numKept);
        }

        toWaitFor = getStreamer().getLastQueuedSeqno();
      } // end synchronized

      getStreamer().waitForAckedSeqno(toWaitFor);

      if (updateLength || getStreamer().getPersistBlocks().get()) {
        synchronized (this) {
          if (!getStreamer().streamerClosed()
              && getStreamer().getBlock() != null) {
            lastBlockLength = getStreamer().getBlock().getNumBytes();
          }
        }
      }
      if (getStreamer().getPersistBlocks().getAndSet(false) || updateLength) {
        try {
          dfsClient.namenode.fsync(src, fileId, dfsClient.clientName,
              lastBlockLength);
      }

      synchronized(this) {
        if (!getStreamer().streamerClosed()) {
          getStreamer().setHflush();
        }
      }
    } catch (InterruptedIOException interrupt) {
      DFSClient.LOG.warn("Error while syncing", e);
      synchronized (this) {
        if (!isClosed()) {
          getStreamer().getLastException().set(e);
          closeThreads(true);
        }
      }
  public synchronized int getCurrentBlockReplication() throws IOException {
    dfsClient.checkOpen();
    checkClosed();
    if (getStreamer().streamerClosed()) {
      return blockReplication; // no pipeline, return repl factor of file
    }
    DatanodeInfo[] currentNodes = getStreamer().getNodes();
    if (currentNodes == null) {
      return blockReplication; // no pipeline, return repl factor of file
    }
      getStreamer().queuePacket(currentPacket);
      currentPacket = null;
      toWaitFor = getStreamer().getLastQueuedSeqno();
    }

    getStreamer().waitForAckedSeqno(toWaitFor);
  }

  protected synchronized void start() {
    getStreamer().start();
  }
  
    if (isClosed()) {
      return;
    }
    getStreamer().getLastException().set(new IOException("Lease timeout of "
        + (dfsClient.getConf().getHdfsTimeout()/1000) + " seconds expired."));
    closeThreads(true);
    dfsClient.endFileLease(fileId);
  }

  boolean isClosed() {
    return closed || getStreamer().streamerClosed();
  }

  void setClosed() {
    closed = true;
    getStreamer().release();
  }

  protected void closeThreads(boolean force) throws IOException {
    try {
      getStreamer().close(force);
      getStreamer().join();
      getStreamer().closeSocket();
    } catch (InterruptedException e) {
      throw new IOException("Failed to shutdown streamer");
    } finally {
      getStreamer().setSocketToNull();
      setClosed();
    }
  }

  protected synchronized void closeImpl() throws IOException {
    if (isClosed()) {
      getStreamer().getLastException().check(true);
      return;
    }

      flushBuffer();       // flush from all upper layers

      if (currentPacket != null) {
        getStreamer().waitAndQueuePacket(currentPacket);
        currentPacket = null;
      }

      if (getStreamer().getBytesCurBlock() != 0) {
        currentPacket = createPacket(0, 0, getStreamer().getBytesCurBlock(),
            getStreamer().getAndIncCurrentSeqno(), true);
        currentPacket.setSyncBlock(shouldSyncBlock);
      }

      flushInternal();             // flush all data to Datanodes
      ExtendedBlock lastBlock = getStreamer().getBlock();
      closeThreads(false);
      TraceScope scope = Trace.startSpan("completeFile", Sampler.NEVER);
      try {

  @VisibleForTesting
  public void setArtificialSlowdown(long period) {
    getStreamer().setArtificialSlowdown(period);
  }

  @VisibleForTesting
  synchronized Token<BlockTokenIdentifier> getBlockToken() {
    return getStreamer().getBlockToken();
  }

  @Override

  @VisibleForTesting
  ExtendedBlock getBlock() {
    return getStreamer().getBlock();
  }

  @VisibleForTesting
  public long getFileId() {
    return fileId;
  }

  protected synchronized void setStreamer(DataStreamer streamer) {
    this.streamer = streamer;
  }

  protected synchronized DataStreamer getStreamer() {
    return streamer;
  }
}

