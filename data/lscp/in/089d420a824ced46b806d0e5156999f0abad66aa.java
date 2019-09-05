hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/ha/BootstrapStandby.java
      image.saveDigestAndRenameCheckpointImage(NameNodeFile.IMAGE, imageTxId,
          hash);
    } catch (IOException ioe) {
      throw ioe;
    } finally {
      image.close();
    }
    return 0;
  }

