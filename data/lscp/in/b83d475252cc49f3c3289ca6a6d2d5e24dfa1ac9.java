hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/DelegateToFileSystem.java
      Configuration conf, String supportedScheme, boolean authorityRequired)
      throws IOException, URISyntaxException {
    super(theUri, supportedScheme, authorityRequired, 
        getDefaultPortIfDefined(theFsImpl));
    fsImpl = theFsImpl;
    fsImpl.initialize(theUri, conf);
    fsImpl.statistics = getStatistics();
  }

  private static int getDefaultPortIfDefined(FileSystem theFsImpl) {
    int defaultPort = theFsImpl.getDefaultPort();
    return defaultPort != 0 ? defaultPort : -1;
  }

  @Override
  public Path getInitialWorkingDirectory() {
    return fsImpl.getInitialWorkingDirectory();

hadoop-tools/hadoop-azure/src/test/java/org/apache/hadoop/fs/azure/TestWasbUriAndConfiguration.java

package org.apache.hadoop.fs.azure;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount.CreateOptions;
        Configuration conf = testAccount.getFileSystem().getConf();
        String authority = testAccount.getFileSystem().getUri().getAuthority();
        URI defaultUri = new URI(defaultScheme, authority, null, null, null);
        conf.set(FS_DEFAULT_NAME_KEY, defaultUri.toString());
        
        conf.addResource("azure-test.xml");
    testAccount = AzureBlobStorageTestAccount.createMock();
    Configuration conf = testAccount.getFileSystem().getConf();
    conf.set(FS_DEFAULT_NAME_KEY, "file:///");
    try {
      FileSystem.get(new URI("wasb:///random/path"), conf);
      fail("Should've thrown.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testWasbAsDefaultFileSystemHasNoPort() throws Exception {
    try {
      testAccount = AzureBlobStorageTestAccount.createMock();
      Configuration conf = testAccount.getFileSystem().getConf();
      String authority = testAccount.getFileSystem().getUri().getAuthority();
      URI defaultUri = new URI("wasb", authority, null, null, null);
      conf.set(FS_DEFAULT_NAME_KEY, defaultUri.toString());
      conf.addResource("azure-test.xml");

      FileSystem fs = FileSystem.get(conf);
      assertTrue(fs instanceof NativeAzureFileSystem);
      assertEquals(-1, fs.getUri().getPort());

      AbstractFileSystem afs = FileContext.getFileContext(conf)
          .getDefaultFileSystem();
      assertTrue(afs instanceof Wasb);
      assertEquals(-1, afs.getUri().getPort());
    } finally {
      FileSystem.closeAll();
    }
  }
}

