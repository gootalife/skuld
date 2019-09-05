hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/FileSystemRMStateStore.java
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
  protected static final String AMRMTOKEN_SECRET_MANAGER_NODE =
      "AMRMTokenSecretManagerNode";

  private static final String UNREADABLE_BY_SUPERUSER_XATTRIB =
          "security.hdfs.unreadable.by.superuser";
  protected FileSystem fs;
  @VisibleForTesting
  protected Configuration fsConf;
  private Path dtSequenceNumberPath = null;
  private int fsNumRetries;
  private long fsRetryInterval;
  private boolean isHDFS;

  @VisibleForTesting
  Path fsWorkingPath;
    }

    fs = fsWorkingPath.getFileSystem(fsConf);
    isHDFS = fs.getScheme().toLowerCase().contains("hdfs");
    mkdirsWithRetries(rmDTSecretManagerRoot);
    mkdirsWithRetries(rmAppRoot);
    mkdirsWithRetries(amrmTokenSecretManagerRoot);
  }

  @VisibleForTesting
  void setIsHDFS(boolean isHDFS) {
    this.isHDFS = isHDFS;
  }

  @Override
  protected synchronized void closeInternal() throws Exception {
    closeWithRetries();
    byte[] data =
        ((VersionPBImpl) CURRENT_VERSION_INFO).getProto().toByteArray();
    if (existsWithRetries(versionNodePath)) {
      updateFile(versionNodePath, data, false);
    } else {
      writeFileWithRetries(versionNodePath, data, false);
    }
  }
  
      byte[] storeData = Epoch.newInstance(currentEpoch + 1).getProto()
          .toByteArray();
      updateFile(epochNodePath, storeData, false);
    } else {
      byte[] storeData = Epoch.newInstance(currentEpoch + 1).getProto()
          .toByteArray();
      writeFileWithRetries(epochNodePath, storeData, false);
    }
    return currentEpoch;
  }
          }
          byte[] childData = readFileWithRetries(childNodeStatus.getPath(),
                  childNodeStatus.getLen());
          setUnreadableBySuperuserXattrib(childNodeStatus.getPath());
          if (childNodeName.startsWith(ApplicationId.appIdStrPrefix)) {
            if (LOG.isDebugEnabled()) {
    try {
      writeFileWithRetries(nodeCreatePath, appStateData, true);
    } catch (Exception e) {
      LOG.info("Error storing info for app: " + appId, e);
      throw e;
    try {
      updateFile(nodeCreatePath, appStateData, true);
    } catch (Exception e) {
      LOG.info("Error updating info for app: " + appId, e);
      throw e;
    try {
      writeFileWithRetries(nodeCreatePath, attemptStateData, true);
    } catch (Exception e) {
      LOG.info("Error storing info for attempt: " + appAttemptId, e);
      throw e;
    try {
      updateFile(nodeCreatePath, attemptStateData, true);
    } catch (Exception e) {
      LOG.info("Error updating info for attempt: " + appAttemptId, e);
      throw e;
        new RMDelegationTokenIdentifierData(identifier, renewDate);
    if (isUpdate) {
      LOG.info("Updating RMDelegationToken_" + identifier.getSequenceNumber());
      updateFile(nodeCreatePath, identifierData.toByteArray(), true);
    } else {
      LOG.info("Storing RMDelegationToken_" + identifier.getSequenceNumber());
      writeFileWithRetries(nodeCreatePath, identifierData.toByteArray(), true);

      Path latestSequenceNumberPath = getNodePath(rmDTSecretManagerRoot,
    try (DataOutputStream fsOut = new DataOutputStream(os)) {
      LOG.info("Storing RMDelegationKey_" + masterKey.getKeyId());
      masterKey.write(fsOut);
      writeFileWithRetries(nodeCreatePath, os.toByteArray(), true);
    }
  }

    return getNodePath(root, appId.toString());
  }

  @VisibleForTesting
  protected Path getAppDir(ApplicationId appId) {
    return getAppDir(rmAppRoot, appId);
  }

  @VisibleForTesting
  protected Path getAppAttemptDir(ApplicationAttemptId appAttId) {
    return getNodePath(getAppDir(appAttId.getApplicationId()), appAttId
            .toString());
  }

  private boolean checkAndRemovePartialRecordWithRetries(final Path record)
    }.runWithRetries();
  }

  private void writeFileWithRetries(final Path outputPath, final byte[] data,
                                    final boolean makeUnreadableByAdmin)
          throws Exception {
    new FSAction<Void>() {
      @Override
      public Void run() throws Exception {
        writeFile(outputPath, data, makeUnreadableByAdmin);
        return null;
      }
    }.runWithRetries();
  protected void writeFile(Path outputPath, byte[] data, boolean
          makeUnradableByAdmin) throws Exception {
    Path tempPath =
        new Path(outputPath.getParent(), outputPath.getName() + ".tmp");
    FSDataOutputStream fsOut = null;
    try {
      fsOut = fs.create(tempPath, true);
      if (makeUnradableByAdmin) {
        setUnreadableBySuperuserXattrib(tempPath);
      }
      fsOut.write(data);
      fsOut.close();
      fsOut = null;
  protected void updateFile(Path outputPath, byte[] data, boolean
          makeUnradableByAdmin) throws Exception {
    Path newPath = new Path(outputPath.getParent(), outputPath.getName() + ".new");
    writeFileWithRetries(newPath, data, makeUnradableByAdmin);
    replaceFile(newPath, outputPath);
  }

        AMRMTokenSecretManagerState.newInstance(amrmTokenSecretManagerState);
    byte[] stateData = data.getProto().toByteArray();
    if (isUpdate) {
      updateFile(nodeCreatePath, stateData, true);
    } else {
      writeFileWithRetries(nodeCreatePath, stateData, true);
    }
  }

  public long getRetryInterval() {
    return fsRetryInterval;
  }

  private void setUnreadableBySuperuserXattrib(Path p)
          throws IOException {
    if (isHDFS &&
            !fs.getXAttrs(p).containsKey(UNREADABLE_BY_SUPERUSER_XATTRIB)) {
      fs.setXAttr(p, UNREADABLE_BY_SUPERUSER_XATTRIB, null,
              EnumSet.of(XAttrSetFlag.CREATE));
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/RMStateStoreTestBase.java

  }

  public static class StoreStateVerifier {
    void afterStoreApp(RMStateStore store, ApplicationId appId) {}
    void afterStoreAppAttempt(RMStateStore store, ApplicationAttemptId
            appAttId) {}
  }

  interface RMStateStoreHelper {
    RMStateStore getRMStateStore() throws Exception;
    boolean isFinalStateValid() throws Exception;

  void testRMAppStateStore(RMStateStoreHelper stateStoreHelper)
          throws Exception {
    testRMAppStateStore(stateStoreHelper, new StoreStateVerifier());
  }

  void testRMAppStateStore(RMStateStoreHelper stateStoreHelper,
                           StoreStateVerifier verifier)
      throws Exception {
    long submitTime = System.currentTimeMillis();
    long startTime = System.currentTimeMillis() + 1234;
    Configuration conf = new YarnConfiguration();
        .toApplicationAttemptId("appattempt_1352994193343_0001_000001");
    ApplicationId appId1 = attemptId1.getApplicationId();
    storeApp(store, appId1, submitTime, startTime);
    verifier.afterStoreApp(store, appId1);

    Token<AMRMTokenIdentifier> appAttemptToken1 =
    storeApp(store, appIdRemoved, submitTime, startTime);
    storeAttempt(store, attemptIdRemoved,
        "container_1352994193343_0002_01_000001", null, null, dispatcher);
    verifier.afterStoreAppAttempt(store, attemptIdRemoved);

    RMApp mockRemovedApp = mock(RMApp.class);
    RMAppAttemptMetrics mockRmAppAttemptMetrics = 

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/TestFSRMStateStore.java
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
    Path workingDirPathURI;
    TestFileSystemRMStore store;
    MiniDFSCluster cluster;
    boolean adminCheckEnable;

    class TestFileSystemRMStore extends FileSystemRMStateStore {

      }
    }

    public TestFSRMStateStoreTester(MiniDFSCluster cluster, boolean adminCheckEnable) throws Exception {
      Path workingDirPath = new Path("/yarn/Test");
      this.adminCheckEnable = adminCheckEnable;
      this.cluster = cluster;
      FileSystem fs = cluster.getFileSystem();
      fs.mkdirs(workingDirPath);
      store.startInternal();
      Assert.assertTrue(store.fs != previousFs);
      Assert.assertTrue(store.fs.getConf() == store.fsConf);
      if (adminCheckEnable) {
        store.setIsHDFS(true);
      } else {
        store.setIsHDFS(false);
      }
      return store;
    }


    @Override
    public void writeVersion(Version version) throws Exception {
      store.updateFile(store.getVersionNode(), ((VersionPBImpl)
              version)
              .getProto().toByteArray(), false);
    }

    @Override
    MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
      fsTester = new TestFSRMStateStoreTester(cluster, false);
      FSDataOutputStream fsOut = null;
    }
  }

  @Test(timeout = 60000)
  public void testHDFSRMStateStore() throws Exception {
    final HdfsConfiguration conf = new HdfsConfiguration();
    UserGroupInformation yarnAdmin =
            UserGroupInformation.createUserForTesting("yarn",
                    new String[]{"admin"});
    final MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.getFileSystem().mkdir(new Path("/yarn"),
            FsPermission.valueOf("-rwxrwxrwx"));
    cluster.getFileSystem().setOwner(new Path("/yarn"), "yarn", "admin");
    final UserGroupInformation hdfsAdmin = UserGroupInformation.getCurrentUser();
    final StoreStateVerifier verifier = new StoreStateVerifier() {
      @Override
      void afterStoreApp(final RMStateStore store, final ApplicationId appId) {
        try {
          Thread.sleep(5000);
          hdfsAdmin.doAs(
                  new PrivilegedExceptionAction<Void>() {
                    @Override
                    public Void run() throws Exception {
                      verifyFilesUnreadablebyHDFS(cluster,
                              ((FileSystemRMStateStore) store).getAppDir
                                      (appId));
                      return null;
                    }
                  });
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      void afterStoreAppAttempt(final RMStateStore store,
                                final ApplicationAttemptId appAttId) {
        try {
          Thread.sleep(5000);
          hdfsAdmin.doAs(
                  new PrivilegedExceptionAction<Void>() {
                    @Override
                    public Void run() throws Exception {
                      verifyFilesUnreadablebyHDFS(cluster,
                              ((FileSystemRMStateStore) store)
                                      .getAppAttemptDir(appAttId));
                      return null;
                    }
                  });
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
    try {
      yarnAdmin.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          fsTester = new TestFSRMStateStoreTester(cluster, true);
          testRMAppStateStore(fsTester, verifier);
          return null;
        }
      });
    } finally {
      cluster.shutdown();
    }
  }

  private void verifyFilesUnreadablebyHDFS(MiniDFSCluster cluster,
                                                     Path root) throws Exception{
    DistributedFileSystem fs = cluster.getFileSystem();
    Queue<Path> paths = new LinkedList<>();
    paths.add(root);
    while (!paths.isEmpty()) {
      Path p = paths.poll();
      FileStatus stat = fs.getFileStatus(p);
      if (!stat.isDirectory()) {
        try {
          LOG.warn("\n\n ##Testing path [" + p + "]\n\n");
          fs.open(p);
          Assert.fail("Super user should not be able to read ["+ UserGroupInformation.getCurrentUser() + "] [" + p.getName() + "]");
        } catch (AccessControlException e) {
          Assert.assertTrue(e.getMessage().contains("superuser is not allowed to perform this operation"));
        } catch (Exception e) {
          Assert.fail("Should get an AccessControlException here");
        }
      }
      if (stat.isDirectory()) {
        FileStatus[] ls = fs.listStatus(p);
        for (FileStatus f : ls) {
          paths.add(f.getPath());
        }
      }
    }

  }

  @Test(timeout = 60000)
  public void testCheckMajorVersionChange() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
      fsTester = new TestFSRMStateStoreTester(cluster, false) {
        Version VERSION_INFO = Version.newInstance(Integer.MAX_VALUE, 0);

        @Override
        new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();
    try {
      TestFSRMStateStoreTester fsTester = new TestFSRMStateStoreTester(cluster, false);
      final RMStateStore store = fsTester.getRMStateStore();
      store.setRMDispatcher(new TestDispatcher());
      final AtomicBoolean assertionFailedInThread = new AtomicBoolean(false);

