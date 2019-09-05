hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
  public static final String DEFAULT_YARN_APP_ACL = " ";

  @Private
  public static final String YARN_INTERMEDIATE_DATA_ENCRYPTION = YARN_PREFIX
      + "intermediate-data-encryption.enable";

  @Private
  public static final Boolean DEFAULT_YARN_INTERMEDIATE_DATA_ENCRYPTION = false;

  public static final String RM_ADMIN_ADDRESS = 
    RM_PREFIX + "admin.address";

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/FileSystemRMStateStore.java
  private Path dtSequenceNumberPath = null;
  private int fsNumRetries;
  private long fsRetryInterval;
  private boolean intermediateEncryptionEnabled =
      YarnConfiguration.DEFAULT_YARN_INTERMEDIATE_DATA_ENCRYPTION;

  @VisibleForTesting
  Path fsWorkingPath;
    fsRetryInterval =
        conf.getLong(YarnConfiguration.FS_RM_STATE_STORE_RETRY_INTERVAL_MS,
                YarnConfiguration.DEFAULT_FS_RM_STATE_STORE_RETRY_INTERVAL_MS);
    intermediateEncryptionEnabled =
        conf.getBoolean(YarnConfiguration.YARN_INTERMEDIATE_DATA_ENCRYPTION,
          YarnConfiguration.DEFAULT_YARN_INTERMEDIATE_DATA_ENCRYPTION);
  }

  @Override
    }

    fs = fsWorkingPath.getFileSystem(fsConf);
    mkdirsWithRetries(rmDTSecretManagerRoot);
    mkdirsWithRetries(rmAppRoot);
    mkdirsWithRetries(amrmTokenSecretManagerRoot);
  }

  @Override
  protected synchronized void closeInternal() throws Exception {
    closeWithRetries();

  private void setUnreadableBySuperuserXattrib(Path p)
          throws IOException {
    if (fs.getScheme().toLowerCase().contains("hdfs")
        && intermediateEncryptionEnabled
        && !fs.getXAttrs(p).containsKey(UNREADABLE_BY_SUPERUSER_XATTRIB)) {
      fs.setXAttr(p, UNREADABLE_BY_SUPERUSER_XATTRIB, null,
        EnumSet.of(XAttrSetFlag.CREATE));
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/TestFSRMStateStore.java
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestFSRMStateStore extends RMStateStoreTestBase {
      conf.setInt(YarnConfiguration.FS_RM_STATE_STORE_NUM_RETRIES, 8);
      conf.setLong(YarnConfiguration.FS_RM_STATE_STORE_RETRY_INTERVAL_MS,
              900L);
      if (adminCheckEnable) {
        conf.setBoolean(
          YarnConfiguration.YARN_INTERMEDIATE_DATA_ENCRYPTION, true);
      }
      this.store = new TestFileSystemRMStore(conf);
      Assert.assertEquals(store.getNumRetries(), 8);
      Assert.assertEquals(store.getRetryInterval(), 900L);
      store.startInternal();
      Assert.assertTrue(store.fs != previousFs);
      Assert.assertTrue(store.fs.getConf() == store.fsConf);
      return store;
    }


