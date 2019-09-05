hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeManager.java
  }

  private void stopRecoveryStore() throws IOException {
    if (null != nmStore) {
      nmStore.stop();
      if (null != context) {
        if (context.getDecommissioned() && nmStore.canRecover()) {
        }
      }
    }
  }

  private void recoverTokens(NMTokenSecretManagerInNM nmTokenSecretManager,
      NMContainerTokenSecretManager containerTokenSecretManager)

