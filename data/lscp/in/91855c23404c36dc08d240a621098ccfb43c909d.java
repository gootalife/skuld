hadoop-tools/hadoop-azure/src/main/java/org/apache/hadoop/fs/azure/AzureNativeFileSystemStore.java
  private static final String KEY_SELF_THROTTLE_READ_FACTOR = "fs.azure.selfthrottling.read.factor";
  private static final String KEY_SELF_THROTTLE_WRITE_FACTOR = "fs.azure.selfthrottling.write.factor";

  private static final String KEY_ENABLE_STORAGE_CLIENT_LOGGING = "fs.azure.storage.client.logging";

  private static final String PERMISSION_METADATA_KEY = "hdi_permission";
  private static final String OLD_PERMISSION_METADATA_KEY = "asv_permission";
  private static final String IS_FOLDER_METADATA_KEY = "hdi_isfolder";
    selfThrottlingWriteFactor = sessionConfiguration.getFloat(
        KEY_SELF_THROTTLE_WRITE_FACTOR, DEFAULT_SELF_THROTTLE_WRITE_FACTOR);

    OperationContext.setLoggingEnabledByDefault(sessionConfiguration.
        getBoolean(KEY_ENABLE_STORAGE_CLIENT_LOGGING, false));

    if (LOG.isDebugEnabled()) {
      LOG.debug(String
          .format(

hadoop-tools/hadoop-azure/src/test/java/org/apache/hadoop/fs/azure/TestNativeAzureFileSystemClientLogging.java
++ b/hadoop-tools/hadoop-azure/src/test/java/org/apache/hadoop/fs/azure/TestNativeAzureFileSystemClientLogging.java

package org.apache.hadoop.fs.azure;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.StringTokenizer;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.log4j.Logger;
import org.junit.Test;

public class TestNativeAzureFileSystemClientLogging extends
    NativeAzureFileSystemBaseTest {

  private AzureBlobStorageTestAccount testAccount;

  private static final String KEY_LOGGING_CONF_STRING = "fs.azure.storage.client.logging";

  private static final String TEMP_DIR = "tempDir";

  private boolean verifyStorageClientLogs(String capturedLogs, String entity)
      throws Exception {

    URI uri = testAccount.getRealAccount().getBlobEndpoint();
    String container = testAccount.getRealContainer().getName();
    String validateString = uri + Path.SEPARATOR + container + Path.SEPARATOR
        + entity;
    boolean entityFound = false;

    StringTokenizer tokenizer = new StringTokenizer(capturedLogs, "\n");

    while (tokenizer.hasMoreTokens()) {
      String token = tokenizer.nextToken();
      if (token.contains(validateString)) {
        entityFound = true;
        break;
      }
    }
    return entityFound;
  }

  private void updateFileSystemConfiguration(Boolean loggingFlag)
      throws Exception {

    Configuration conf = fs.getConf();
    conf.set(KEY_LOGGING_CONF_STRING, loggingFlag.toString());
    URI uri = fs.getUri();
    fs.initialize(uri, conf);
  }

  private void performWASBOperations() throws Exception {

    Path tempDir = new Path(Path.SEPARATOR + TEMP_DIR);
    fs.mkdirs(tempDir);
    fs.delete(tempDir, true);
  }

  @Test
  public void testLoggingEnabled() throws Exception {

    LogCapturer logs = LogCapturer.captureLogs(new Log4JLogger(Logger
        .getRootLogger()));

    updateFileSystemConfiguration(true);

    performWASBOperations();

    assertTrue(verifyStorageClientLogs(logs.getOutput(), TEMP_DIR));
  }

  @Test
  public void testLoggingDisabled() throws Exception {

    LogCapturer logs = LogCapturer.captureLogs(new Log4JLogger(Logger
        .getRootLogger()));

    updateFileSystemConfiguration(false);

    performWASBOperations();

    assertFalse(verifyStorageClientLogs(logs.getOutput(), TEMP_DIR));
  }

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create();
    return testAccount;
  }
}
\No newline at end of file

