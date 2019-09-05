hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/VolumeScanner.java
          return 0;
        }
      }
      if (curBlockIter != null) {
        long saveDelta = monotonicMs - curBlockIter.getLastSavedMs();
        if (saveDelta >= conf.cursorSaveMs) {
          LOG.debug("{}: saving block iterator {} after {} ms.",
              this, curBlockIter, saveDelta);
          saveBlockIterator(curBlockIter);
        }
      }
      bytesScanned = scanBlock(block, conf.targetBytesPerSec);
      if (bytesScanned >= 0) {
        scannedBytesSum += bytesScanned;

