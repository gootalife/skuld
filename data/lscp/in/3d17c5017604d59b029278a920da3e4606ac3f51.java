hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
    }
  }

  public boolean isHDFSEncryptionEnabled() {
    return conf.get(
        DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI, null) != null;
  }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DistributedFileSystem.java
  public Token<?>[] addDelegationTokens(
      final String renewer, Credentials credentials) throws IOException {
    Token<?>[] tokens = super.addDelegationTokens(renewer, credentials);
    if (dfs.isHDFSEncryptionEnabled()) {
      KeyProviderDelegationTokenExtension keyProviderDelegationTokenExtension =
          KeyProviderDelegationTokenExtension.
              createKeyProviderDelegationTokenExtension(dfs.getKeyProvider());

