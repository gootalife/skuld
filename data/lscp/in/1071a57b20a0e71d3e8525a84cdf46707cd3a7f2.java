hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/OfflineImageViewer.java
      done = true;
    } finally {
      if (!done) {
        if (tracker != null) {
          LOG.error("image loading failed at offset " + tracker.getPos());
        } else {
          LOG.error("Failed to load image file.");
        }
      }
      IOUtils.cleanup(LOG, in, tracker);
    }

