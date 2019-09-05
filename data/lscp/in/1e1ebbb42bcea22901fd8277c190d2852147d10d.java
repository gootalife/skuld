hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/compress/SnappyCodec.java
  public static void checkNativeCodeLoaded() {
      if (!NativeCodeLoader.isNativeCodeLoaded() ||
          !NativeCodeLoader.buildSupportsSnappy()) {
        throw new RuntimeException("native snappy library not available: " +
            "this version of libhadoop was built without " +
            "snappy support.");

