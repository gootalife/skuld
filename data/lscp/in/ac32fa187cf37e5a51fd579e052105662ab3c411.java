hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeManager.java

  private void stopRecoveryStore() throws IOException {
    nmStore.stop();
    if (null != context) {
      if (context.getDecommissioned() && nmStore.canRecover()) {
        LOG.info("Removing state store due to decommission");
        Configuration conf = getConfig();
        Path recoveryRoot =
            new Path(conf.get(YarnConfiguration.NM_RECOVERY_DIR));
        LOG.info("Removing state store at " + recoveryRoot
            + " due to decommission");
        FileSystem recoveryFs = FileSystem.getLocal(conf);
        }
      }
    }
  }

  private void recoverTokens(NMTokenSecretManagerInNM nmTokenSecretManager,
      NMContainerTokenSecretManager containerTokenSecretManager)

