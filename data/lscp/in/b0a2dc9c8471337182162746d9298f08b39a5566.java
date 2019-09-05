hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockReceiver.java
          long partialChunkSizeOnDisk = onDiskLen % bytesPerChecksum;
          long lastChunkBoundary = onDiskLen - partialChunkSizeOnDisk;
          boolean alignedOnDisk = partialChunkSizeOnDisk == 0;
          boolean alignedInPacket = firstByteInBlock % bytesPerChecksum == 0;

          boolean overwriteLastCrc = !alignedOnDisk && !shouldNotWriteChecksum;
          boolean doCrcRecalc = overwriteLastCrc &&
              (lastChunkBoundary != firstByteInBlock);

          Checksum partialCrc = null;
          if (doCrcRecalc) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("receivePacket for " + block 
                  + ": previous write did not end at the chunk boundary."
            int skip = 0;
            byte[] crcBytes = null;

            if (overwriteLastCrc) { // not chunk-aligned on disk
              adjustCrcFilePosition();
            }

            if (doCrcRecalc) {
              int bytesToReadForRecalc =
                  (int)(bytesPerChecksum - partialChunkSizeOnDisk);
              byte[] buf = FSOutputSummer.convertToByteStream(partialCrc,
                  checksumSize);
              crcBytes = copyLastChunkChecksum(buf, checksumSize, buf.length);
              checksumOut.write(buf);
              if(LOG.isDebugEnabled()) {
                LOG.debug("Writing out partial crc for data len " + len +
            long skippedDataBytes = lastChunkBoundary - firstByteInBlock;

            if (skippedDataBytes > 0) {

