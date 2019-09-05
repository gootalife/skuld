hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/compress/snappy/SnappyDecompressor.java
public class SnappyDecompressor implements Decompressor {
  private static final Log LOG =
      LogFactory.getLog(SnappyDecompressor.class.getName());
  private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;

  private int directBufferSize;

