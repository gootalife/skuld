hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDatanodeDeath.java

      dfstream.setChunksPerPacket(5);

      final long myseed = AppendTestUtil.nextLong();
      byte[] buffer = AppendTestUtil.randomBytes(myseed, fileSize);

