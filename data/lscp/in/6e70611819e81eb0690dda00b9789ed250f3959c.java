hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/TestShellBasedIdMapping.java
  @Test
  public void testStaticMapUpdate() throws IOException {
    assumeTrue(!Shell.WINDOWS);
    File tempStaticMapFile = File.createTempFile("nfs-", ".map");
    tempStaticMapFile.delete();
    Configuration conf = new Configuration();

