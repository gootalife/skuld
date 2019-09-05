hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/FileContextMainOperationsBaseTest.java
    byte[] bb = new byte[(int)len];
    FSDataInputStream fsdis = fc.open(path);
    try {
      fsdis.readFully(bb);
    } finally {
      fsdis.close();
    }
    byte[] bb = new byte[data.length];
    FSDataInputStream fsdis = fc.open(path);
    try {
      fsdis.readFully(bb);
    } finally {
      fsdis.close();
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/fs/SWebHdfs.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/fs/SWebHdfs.java

package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.SWebHdfsFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SWebHdfs extends DelegateToFileSystem {

  public static final String SCHEME = "swebhdfs";

  SWebHdfs(URI theUri, Configuration conf)
      throws IOException, URISyntaxException {
    super(theUri, new SWebHdfsFileSystem(), conf, SCHEME, false);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/fs/WebHdfs.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/fs/WebHdfs.java

package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class WebHdfs extends DelegateToFileSystem {

  public static final String SCHEME = "webhdfs";

  WebHdfs(URI theUri, Configuration conf)
      throws IOException, URISyntaxException {
    super(theUri, new WebHdfsFileSystem(), conf, SCHEME, false);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/fs/TestSWebHdfsFileContextMainOperations.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/fs/TestSWebHdfsFileContextMainOperations.java
package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.fs.FileContextTestHelper.getDefaultBlockSize;
import static org.apache.hadoop.fs.FileContextTestHelper.getFileData;

public class TestSWebHdfsFileContextMainOperations
    extends TestWebHdfsFileContextMainOperations {

  private static MiniDFSCluster cluster;
  private static Path defaultWorkingDirectory;
  private static String keystoresDir;
  private static String sslConfDir;
  protected static URI webhdfsUrl;

  private static final HdfsConfiguration CONF = new HdfsConfiguration();

  private static final String BASEDIR =
      System.getProperty("test.build.dir", "target/test-dir") + "/"
          + TestSWebHdfsFileContextMainOperations.class.getSimpleName();
  protected static int numBlocks = 2;
  protected static final byte[] data = getFileData(numBlocks,
      getDefaultBlockSize());

  private static Configuration sslConf;

  @BeforeClass
  public static void clusterSetupAtBeginning()
      throws IOException, LoginException, URISyntaxException {

    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    keystoresDir = new File(BASEDIR).getAbsolutePath();
    sslConf = new Configuration();

    try {
      sslConfDir = KeyStoreTestUtil
          .getClasspathDir(TestSWebHdfsFileContextMainOperations.class);
      KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, sslConf, false);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    CONF.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, "HTTPS_ONLY");
    CONF.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    CONF.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    CONF.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "DEFAULT_AND_LOCALHOST");
    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(2).build();

    cluster.waitClusterUp();
    webhdfsUrl = new URI(SWebHdfs.SCHEME + "://" + cluster.getConfiguration(0)
        .get(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY));

    fc = FileContext.getFileContext(webhdfsUrl, CONF);
    defaultWorkingDirectory = fc.makeQualified(new Path(
        "/user/" + UserGroupInformation.getCurrentUser().getShortUserName()));
    fc.mkdir(defaultWorkingDirectory, FileContext.DEFAULT_PERM, true);

  }

  @Override
  public URI getWebhdfsUrl() {
    return webhdfsUrl;
  }

  @AfterClass
  public static void ClusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/fs/TestWebHdfsFileContextMainOperations.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/fs/TestWebHdfsFileContextMainOperations.java

package org.apache.hadoop.fs;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.FileContextTestHelper.getDefaultBlockSize;
import static org.apache.hadoop.fs.FileContextTestHelper.getFileData;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class TestWebHdfsFileContextMainOperations
    extends FileContextMainOperationsBaseTest {

  protected static MiniDFSCluster cluster;
  private static Path defaultWorkingDirectory;
  protected static URI webhdfsUrl;

  protected static int numBlocks = 2;

  protected static final byte[] data = getFileData(numBlocks,
      getDefaultBlockSize());
  protected static final HdfsConfiguration CONF = new HdfsConfiguration();

  @Override
  public Path getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }

  public URI getWebhdfsUrl() {
    return webhdfsUrl;
  }

  @BeforeClass
  public static void clusterSetupAtBeginning()
      throws IOException, LoginException, URISyntaxException {

    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(2).build();
    cluster.waitClusterUp();
    webhdfsUrl = new URI(WebHdfs.SCHEME + "://" + cluster.getConfiguration(0)
        .get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY));
    fc = FileContext.getFileContext(webhdfsUrl, CONF);
    defaultWorkingDirectory = fc.makeQualified(new Path(
        "/user/" + UserGroupInformation.getCurrentUser().getShortUserName()));
    fc.mkdir(defaultWorkingDirectory, FileContext.DEFAULT_PERM, true);
  }

  @Before
  public void setUp() throws Exception {
    URI webhdfsUrlReal = getWebhdfsUrl();
    Path testBuildData = new Path(
        webhdfsUrlReal + "/build/test/data/" + RandomStringUtils
            .randomAlphanumeric(10));
    Path rootPath = new Path(testBuildData, "root-uri");

    localFsRootPath = rootPath.makeQualified(webhdfsUrlReal, null);
    fc.mkdir(getTestRootPath(fc, "test"), FileContext.DEFAULT_PERM, true);
  }

  private Path getTestRootPath(FileContext fc, String path) {
    return fileContextTestHelper.getTestRootPath(fc, path);
  }

  @Override
  protected boolean listCorruptedBlocksSupported() {
    return false;
  }

  @Test
  public void testUnsupportedSymlink() throws IOException {
  }

  public void testSetVerifyChecksum() throws IOException {
    final Path rootPath = getTestRootPath(fc, "test");
    final Path path = new Path(rootPath, "zoo");

    FSDataOutputStream out = fc
        .create(path, EnumSet.of(CREATE), Options.CreateOpts.createParent());
    try {
      out.write(data, 0, data.length);
    } finally {
      out.close();
    }

    fc.setVerifyChecksum(true, path);

    FileStatus fileStatus = fc.getFileStatus(path);
    final long len = fileStatus.getLen();
    assertTrue(len == data.length);
    byte[] bb = new byte[(int) len];
    FSDataInputStream fsdis = fc.open(path);
    try {
      fsdis.readFully(bb);
    } finally {
      fsdis.close();
    }
    assertArrayEquals(data, bb);
  }

  @AfterClass
  public static void ClusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
}

