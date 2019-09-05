hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSOutputStream.java
  protected final int bytesPerChecksum;

  protected DFSPacket currentPacket = null;
  private DataStreamer streamer;
  protected int packetSize = 0; // write packet size, not including the header.
  protected int chunksPerPacket = 0;
  protected long lastFlushOffset = 0; // offset when flush was invoked

