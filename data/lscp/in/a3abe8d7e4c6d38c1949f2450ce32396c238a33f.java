hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/FsActionParam.java
  public static final String DEFAULT = NULL;

  private static String FS_ACTION_PATTERN = "[r-][w-][x-]";

  private static final Domain DOMAIN = new Domain(NAME,
      Pattern.compile(FS_ACTION_PATTERN));

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/resources/TestParam.java
    Assert.assertEquals("s1", s1.getValue());
    Assert.assertEquals("s2", s2.getValue());
  }

  @Test
  public void testFsActionParam() {
    new FsActionParam("rwx");
    new FsActionParam("rw-");
    new FsActionParam("r-x");
    new FsActionParam("-wx");
    new FsActionParam("r--");
    new FsActionParam("-w-");
    new FsActionParam("--x");
    new FsActionParam("---");

    try {
      new FsActionParam("rw");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }

    try {
      new FsActionParam("qwx");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }

    try {
      new FsActionParam("qrwx");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }

    try {
      new FsActionParam("rwxx");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }

    try {
      new FsActionParam("xwr");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }

    try {
      new FsActionParam("r-w");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }
  }
}

