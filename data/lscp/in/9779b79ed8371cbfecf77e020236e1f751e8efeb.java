hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
  }

  private static BlockTokenSecretManager createBlockTokenSecretManager(
      final Configuration conf) throws IOException {
    final boolean isEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, 
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT);

    if (!isEnabled) {
      if (UserGroupInformation.isSecurityEnabled()) {
        String errMessage = "Security is enabled but block access tokens " +
            "(via " + DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY + ") " +
            "aren't enabled. This may cause issues " +
            "when clients attempt to connect to a DataNode. Aborting NameNode";
        throw new IOException(errMessage);
      }
      return null;
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }

    boolean isEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY,
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT);
    if (!isEnabled) {
      String errMessage = "Security is enabled but block access tokens " +
          "(via " + DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY + ") " +
          "aren't enabled. This may cause issues " +
          "when clients attempt to connect to a DataNode. Aborting DataNode";
      throw new RuntimeException(errMessage);
    }

    SaslPropertiesResolver saslPropsResolver = dnConf.getSaslPropsResolver();
    if (resources != null && saslPropsResolver == null) {
      return;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
        cacheManager.stopMonitorThread();
        cacheManager.clearDirectiveStats();
      }
      if (blockManager != null) {
        blockManager.getDatanodeManager().clearPendingCachingCommands();
        blockManager.getDatanodeManager().setShouldSendCachingCommands(false);
        blockManager.clearQueues();
      }
      initializedReplQueues = false;
    } finally {
      writeUnlock();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/protocol/datatransfer/sasl/SaslDataTransferTestCase.java
import java.io.File;
import java.util.Properties;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.http.HttpConfig;

  private static File baseDir;
  private static String hdfsPrincipal;
  private static String userPrincipal;
  private static MiniKdc kdc;
  private static String hdfsKeytab;
  private static String userKeyTab;
  private static String spnegoPrincipal;

  public static String getUserKeyTab() {
    return userKeyTab;
  }

  public static String getUserPrincipal() {
    return userPrincipal;
  }

  public static String getHdfsPrincipal() {
    return hdfsPrincipal;
  }

  public static String getHdfsKeytab() {
    return hdfsKeytab;
  }

  @BeforeClass
  public static void initKdc() throws Exception {
    baseDir = new File(System.getProperty("test.build.dir", "target/test-dir"),
    kdc = new MiniKdc(kdcConf, baseDir);
    kdc.start();

    String userName = RandomStringUtils.randomAlphabetic(8);
    File userKeytabFile = new File(baseDir, userName + ".keytab");
    userKeyTab = userKeytabFile.getAbsolutePath();
    kdc.createPrincipal(userKeytabFile, userName + "/localhost");
    userPrincipal = userName + "/localhost@" + kdc.getRealm();

    String superUserName = "hdfs";
    File hdfsKeytabFile = new File(baseDir, superUserName + ".keytab");
    hdfsKeytab = hdfsKeytabFile.getAbsolutePath();
    kdc.createPrincipal(hdfsKeytabFile, superUserName + "/localhost", "HTTP/localhost");
    hdfsPrincipal = superUserName + "/localhost@" + kdc.getRealm();
    spnegoPrincipal = "HTTP/localhost@" + kdc.getRealm();
  }

    HdfsConfiguration conf = new HdfsConfiguration();
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, hdfsKeytab);
    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, hdfsKeytab);
    conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, dataTransferProtection);

