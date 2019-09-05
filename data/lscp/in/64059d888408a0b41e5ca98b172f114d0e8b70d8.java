hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/compress/bzip2/Bzip2Factory.java
  public static synchronized boolean isNativeBzip2Loaded(Configuration conf) {
    String libname = conf.get("io.compression.codec.bzip2.library", 
                              "system-native");
    if (!bzip2LibraryName.equals(libname)) {

