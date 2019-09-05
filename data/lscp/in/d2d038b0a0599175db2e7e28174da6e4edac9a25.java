hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockReceiver.java
      try {
        long onDiskLen = replicaInfo.getBytesOnDisk();
        if (onDiskLen<offsetInBlock) {
          long partialChunkSizeOnDisk = onDiskLen % bytesPerChecksum;
          boolean alignedOnDisk = partialChunkSizeOnDisk == 0;
          boolean alignedInPacket = firstByteInBlock % bytesPerChecksum == 0;

          boolean doPartialCrc = !alignedOnDisk && !shouldNotWriteChecksum;

          if (!alignedInPacket && len > bytesPerChecksum) {
            throw new IOException("Unexpected packet data length for "
                +  block + " from " + inAddr + ": a partial chunk must be "
                + " sent in an individual packet (data length = " + len
                +  " > bytesPerChecksum = " + bytesPerChecksum + ")");
          }

          Checksum partialCrc = null;
          if (doPartialCrc) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("receivePacket for " + block 
                  + ": previous write did not end at the chunk boundary."
                  + " onDiskLen=" + onDiskLen);
            }
            long offsetInChecksum = BlockMetadataHeader.getHeaderSize() +
                onDiskLen / bytesPerChecksum * checksumSize;
            partialCrc = computePartialChunkCrc(onDiskLen, offsetInChecksum);
          }

          int startByteToDisk = (int)(onDiskLen-firstByteInBlock) 
              + dataBuf.arrayOffset() + dataBuf.position();

          int numBytesToDisk = (int)(offsetInBlock-onDiskLen);
          
          final byte[] lastCrc;
          if (shouldNotWriteChecksum) {
            lastCrc = null;
          } else {
            int skip = 0;
            byte[] crcBytes = null;

            if (doPartialCrc) { // not chunk-aligned on disk
              int bytesToReadForRecalc =
                  (int)(bytesPerChecksum - partialChunkSizeOnDisk);
              if (numBytesToDisk < bytesToReadForRecalc) {
                bytesToReadForRecalc = numBytesToDisk;
              }

              partialCrc.update(dataBuf.array(), startByteToDisk,
                  bytesToReadForRecalc);
              byte[] buf = FSOutputSummer.convertToByteStream(partialCrc,
                  checksumSize);
              crcBytes = copyLastChunkChecksum(buf, checksumSize, buf.length);
              adjustCrcFilePosition();
              checksumOut.write(buf);
              if(LOG.isDebugEnabled()) {
                LOG.debug("Writing out partial crc for data len " + len +
                    ", skip=" + skip);
              }
              skip++; //  For the partial chunk that was just read.
            }

            long lastChunkBoundary = onDiskLen - (onDiskLen%bytesPerChecksum);
            long skippedDataBytes = lastChunkBoundary - firstByteInBlock;

            if (skippedDataBytes > 0) {
              skip += (int)(skippedDataBytes / bytesPerChecksum) +
                  ((skippedDataBytes % bytesPerChecksum == 0) ? 0 : 1);
            }
            skip *= checksumSize; // Convert to number of bytes

            final int offset = checksumBuf.arrayOffset() +
                checksumBuf.position() + skip;
            final int end = offset + checksumLen - skip;
            if (offset > end) {
              assert crcBytes != null;
              lastCrc = crcBytes;
            } else {
              final int remainingBytes = checksumLen - skip;
              lastCrc = copyLastChunkChecksum(checksumBuf.array(),
                  checksumSize, end);
              checksumOut.write(checksumBuf.array(), offset, remainingBytes);
            }
          }


