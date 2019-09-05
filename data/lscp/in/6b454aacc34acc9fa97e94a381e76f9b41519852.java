hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/api/impl/ContainerManagementProtocolProxy.java

  public ContainerManagementProtocolProxy(Configuration conf,
      NMTokenCache nmTokenCache) {
    this.conf = new Configuration(conf);
    this.nmTokenCache = nmTokenCache;

    maxConnectedNMs =
      cmProxy = Collections.emptyMap();
      this.conf.setInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
          0);
    }

