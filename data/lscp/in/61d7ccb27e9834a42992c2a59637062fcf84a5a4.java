hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
    }
  }

  public boolean isHDFSEncryptionEnabled() {
    return DFSUtil.isHDFSEncryptionEnabled(this.conf);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSUtil.java
  public static KeyProvider createKeyProvider(
      final Configuration conf) throws IOException {
    final String providerUriStr =
        conf.getTrimmed(DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI, "");
    if (providerUriStr.isEmpty()) {
      return null;
    }
    final URI providerUri;
  public static int getSmallBufferSize(Configuration conf) {
    return Math.min(getIoFileBufferSize(conf) / 2, 512);
  }

  public static boolean isHDFSEncryptionEnabled(Configuration conf) {
    return !conf.getTrimmed(
        DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI, "").isEmpty();
  }

}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/KeyProviderCache.java

  private URI createKeyProviderURI(Configuration conf) {
    final String providerUriStr =
        conf.getTrimmed(DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI, "");
    if (providerUriStr.isEmpty()) {
      LOG.error("Could not find uri with key ["
          + DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI
          + "] to create a keyProvider !!");

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSUtil.java
    } catch (IOException ignored) {
    }
  }

  @Test
  public void testEncryptionProbe() throws Throwable {
    Configuration conf = new Configuration(false);
    conf.unset(DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI);
    assertFalse("encryption enabled on no provider key",
        DFSUtil.isHDFSEncryptionEnabled(conf));
    conf.set(DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI, "");
    assertFalse("encryption enabled on empty provider key",
        DFSUtil.isHDFSEncryptionEnabled(conf));
    conf.set(DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI, "\n\t\n");
    assertFalse("encryption enabled on whitespace provider key",
        DFSUtil.isHDFSEncryptionEnabled(conf));
    conf.set(DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI, "http://hadoop.apache.org");
    assertTrue("encryption disabled on valid provider key",
        DFSUtil.isHDFSEncryptionEnabled(conf));

  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestEncryptionZones.java
    cluster.getNamesystem().getProvider().flush();
    KeyProvider provider = KeyProviderFactory
        .get(new URI(conf.getTrimmed(DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI)),
            conf);
    List<String> keys = provider.getKeys();
    assertEquals("Expected NN to have created one key per zone", 1,

