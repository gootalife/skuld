hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ResourceManager.java
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxy;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
      GenericOptionsParser hParser = new GenericOptionsParser(conf, argv);
      argv = hParser.getRemainingArgs();
      if (argv.length >= 1) {
        if (argv[0].equals("-format-state-store")) {
          deleteRMStateStore(conf);
        } else if (argv[0].equals("-remove-application-from-state-store")
            && argv.length == 2) {
          removeApplication(conf, argv[1]);
        } else {
          printUsage(System.err);
        }
      } else {
        ResourceManager resourceManager = new ResourceManager();
        ShutdownHookManager.get().addShutdownHook(
      rmStore.stop();
    }
  }

  private static void removeApplication(Configuration conf, String applicationId)
      throws Exception {
    RMStateStore rmStore = RMStateStoreFactory.getStore(conf);
    rmStore.init(conf);
    rmStore.start();
    try {
      ApplicationId removeAppId = ConverterUtils.toApplicationId(applicationId);
      LOG.info("Deleting application " + removeAppId + " from state store");
      rmStore.removeApplication(removeAppId);
      LOG.info("Application is deleted from state store");
    } finally {
      rmStore.stop();
    }
  }

  private static void printUsage(PrintStream out) {
    out.println("Usage: java ResourceManager [-format-state-store]");
    out.println("                            "
        + "[-remove-application-from-state-store <appId>]" + "\n");
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/FileSystemRMStateStore.java
    }
  }

  @Override
  public synchronized void removeApplication(ApplicationId removeAppId)
      throws Exception {
    Path nodeRemovePath = getAppDir(rmAppRoot, removeAppId);
    if (existsWithRetries(nodeRemovePath)) {
      deleteFileWithRetries(nodeRemovePath);
    }
  }

  private Path getAppDir(Path root, ApplicationId appId) {
    return getNodePath(root, appId.toString());
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/LeveldbRMStateStore.java
    fs.delete(root, true);
  }

  @Override
  public synchronized void removeApplication(ApplicationId removeAppId)
      throws IOException {
    String appKey = getApplicationNodeKey(removeAppId);
    LOG.info("Removing state for app " + removeAppId);
    try {
      db.delete(bytes(appKey));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  int getNumEntriesInDatabase() throws IOException {
    int numEntries = 0;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/MemoryRMStateStore.java
  public void deleteStore() throws Exception {
  }

  @Override
  public void removeApplication(ApplicationId removeAppId) throws Exception {
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/NullRMStateStore.java
  }

  @Override
  public void removeApplication(ApplicationId removeAppId) throws Exception {
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/RMStateStore.java
  public abstract void deleteStore() throws Exception;

  public abstract void removeApplication(ApplicationId removeAppId)
      throws Exception;

  public void setResourceManager(ResourceManager rm) {
    this.resourceManager = rm;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/ZKRMStateStore.java
    }
  }

  @Override
  public synchronized void removeApplication(ApplicationId removeAppId)
      throws Exception {
    String appIdRemovePath = getNodePath(rmAppRoot, removeAppId.toString());
    if (existsWithRetries(appIdRemovePath, true) != null) {
      deleteWithRetries(appIdRemovePath, true);
    }
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/RMStateStoreTestBase.java
    }
  }

  public void testRemoveApplication(RMStateStoreHelper stateStoreHelper)
      throws Exception {
    RMStateStore store = stateStoreHelper.getRMStateStore();
    int noOfApps = 2;
    ArrayList<RMApp> appList =
        createAndStoreApps(stateStoreHelper, store, noOfApps);

    RMApp rmApp1 = appList.get(0);
    store.removeApplication(rmApp1.getApplicationId());
    Assert.assertFalse(stateStoreHelper.appExists(rmApp1));

    RMApp rmApp2 = appList.get(1);
    Assert.assertTrue(stateStoreHelper.appExists(rmApp2));
  }

  protected void modifyAppState() throws Exception {

  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/TestFSRMStateStore.java
      testEpoch(fsTester);
      testAppDeletion(fsTester);
      testDeleteStore(fsTester);
      testRemoveApplication(fsTester);
      testAMRMTokenSecretManagerStateStore(fsTester);
    } finally {
      cluster.shutdown();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/TestLeveldbRMStateStore.java
    testDeleteStore(tester);
  }

  @Test(timeout = 60000)
  public void testRemoveApplication() throws Exception {
    LeveldbStateStoreTester tester = new LeveldbStateStoreTester();
    testRemoveApplication(tester);
  }

  @Test(timeout = 60000)
  public void testAMTokens() throws Exception {
    LeveldbStateStoreTester tester = new LeveldbStateStoreTester();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/TestZKRMStateStore.java
    testEpoch(zkTester);
    testAppDeletion(zkTester);
    testDeleteStore(zkTester);
    testRemoveApplication(zkTester);
    testAMRMTokenSecretManagerStateStore(zkTester);
  }


