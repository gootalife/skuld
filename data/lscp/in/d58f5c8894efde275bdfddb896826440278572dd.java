hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/security/DelegationTokenRenewer.java
            DelegationTokenToRenew t = iter.next();
            if (t.token.getKind().equals(new Text("HDFS_DELEGATION_TOKEN"))) {
              iter.remove();
              allTokens.remove(t.token);
              t.cancelTimer();
              LOG.info("Removed expiring token " + t);
            }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/security/TestDelegationTokenRenewer.java
          new HashMap<ApplicationAccessType, String>(), false, "default", 1,
          credentials);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        return
            rm.getRMContext().getDelegationTokenRenewer().getAllTokens()
            .get(token1) == null;
      }
    }, 1000, 20000);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        return !rm.getRMContext().getDelegationTokenRenewer()

