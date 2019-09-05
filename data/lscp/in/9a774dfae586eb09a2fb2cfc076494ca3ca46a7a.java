hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/JsonUtilClient.java
    return null;
  }

  static byte[] getXAttr(final Map<?, ?> json) throws IOException {
    if (json == null) {
      return null;
    }

    Map<String, byte[]> xAttrs = toXAttrs(json);
    if (xAttrs != null && !xAttrs.values().isEmpty()) {
      return xAttrs.values().iterator().next();
    }

    return null;
  }

  static Map<String, byte[]> toXAttrs(final Map<?, ?> json)
      throws IOException {
    if (json == null) {

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsFileSystem.java
        new XAttrEncodingParam(XAttrCodec.HEX)) {
      @Override
      byte[] decodeResponse(Map<?, ?> json) throws IOException {
        return JsonUtilClient.getXAttr(json);
      }
    }.run();
  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/FSXAttrBaseTest.java
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));

    final byte[] theValue = fs.getXAttr(path, "USER.a2");
    Assert.assertArrayEquals(value2, theValue);

    try {
      final byte[] value = fs.getXAttr(path, name3);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/TestOfflineImageViewerForXAttr.java
          "user.attr1"));
      assertEquals("value1", value);

      value = new String(webhdfs.getXAttr(new Path("/dir1"), "USER.attr1"));
      assertEquals("value1", value);

      Map<String, byte[]> contentMap = webhdfs.getXAttrs(new Path("/dir1"),
          names);


