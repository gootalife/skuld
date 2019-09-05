hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/ProviderUtils.java
package org.apache.hadoop.security;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.apache.hadoop.security.alias.LocalJavaKeyStoreProvider;

public class ProviderUtils {
    }
    return new Path(result.toString());
  }

  public static URI nestURIForLocalJavaKeyStoreProvider(final URI localFile)
      throws URISyntaxException {
    if (!("file".equals(localFile.getScheme()))) {
      throw new IllegalArgumentException("passed URI had a scheme other than " +
          "file.");
    }
    if (localFile.getAuthority() != null) {
      throw new IllegalArgumentException("passed URI must not have an " +
          "authority component. For non-local keystores, please use " +
          JavaKeyStoreProvider.class.getName());
    }
    return new URI(LocalJavaKeyStoreProvider.SCHEME_NAME,
        "//file" + localFile.getSchemeSpecificPart(), localFile.getFragment());
  }

}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/alias/AbstractJavaKeyStoreProvider.java

package org.apache.hadoop.security.alias;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
@InterfaceAudience.Private
public abstract class AbstractJavaKeyStoreProvider extends CredentialProvider {
  public static final Log LOG = LogFactory.getLog(
      AbstractJavaKeyStoreProvider.class);
  public static final String CREDENTIAL_PASSWORD_NAME =
      "HADOOP_CREDSTORE_PASSWORD";
  public static final String KEYSTORE_PASSWORD_FILE_KEY =
  protected void initFileSystem(URI keystoreUri, Configuration conf)
      throws IOException {
    path = ProviderUtils.unnestUri(keystoreUri);
    if (LOG.isDebugEnabled()) {
      LOG.debug("backing jks path initialized to " + path);
    }
  }

  @Override
    writeLock.lock();
    try {
      if (!changed) {
        LOG.debug("Keystore hasn't changed, returning.");
        return;
      }
      LOG.debug("Writing out keystore.");
      try (OutputStream out = getOutputStreamForKeystore()) {
        keyStore.store(out, password);
      } catch (KeyStoreException e) {

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/alias/LocalJavaKeyStoreProvider.java

  @Override
  protected OutputStream getOutputStreamForKeystore() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("using '" + file + "' for output stream.");
    }
    FileOutputStream out = new FileOutputStream(file);
    return out;
  }

  @Override
  protected boolean keystoreExists() throws IOException {
    return file.exists() && (file.length() > 0);
  }

  @Override
    super.initFileSystem(uri, conf);
    try {
      file = new File(new URI(getPath().toString()));
      if (LOG.isDebugEnabled()) {
        LOG.debug("initialized local file as '" + file + "'.");
        if (file.exists()) {
          LOG.debug("the local file exists and is size " + file.length());
          if (LOG.isTraceEnabled()) {
            if (file.canRead()) {
              LOG.trace("we can read the local file.");
            }
            if (file.canWrite()) {
              LOG.trace("we can write the local file.");
            }
          }
        } else {
          LOG.debug("the local file does not exist.");
        }
      }
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  @Override
  public void flush() throws IOException {
    super.flush();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Reseting permissions to '" + permissions + "'");
    }
    if (!Shell.WINDOWS) {
      Files.setPosixFilePermissions(Paths.get(file.getCanonicalPath()),
          permissions);

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/alias/TestCredentialProviderFactory.java
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestCredentialProviderFactory {
  public static final Log LOG = LogFactory.getLog(TestCredentialProviderFactory.class);

  @Rule
  public final TestName test = new TestName();

  @Before
  public void announce() {
    LOG.info("Running test " + test.getMethodName());
  }

  private static char[] chars = { 'a', 'b', 'c', 'd', 'e', 'f', 'g',
  'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w',

hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3/S3Credentials.java

package org.apache.hadoop.fs.s3;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
  public void initialize(URI uri, Configuration conf) throws IOException {
    if (uri.getHost() == null) {
      throw new IllegalArgumentException("Invalid hostname in URI " + uri);
    }
      accessKey = conf.getTrimmed(accessKeyProperty);
    }
    if (secretAccessKey == null) {
      final char[] pass = conf.getPassword(secretAccessKeyProperty);
      if (pass != null) {
        secretAccessKey = (new String(pass)).trim();
      }
    }
    if (accessKey == null && secretAccessKey == null) {
      throw new IllegalArgumentException("AWS " +

hadoop-tools/hadoop-aws/src/test/java/org/apache/hadoop/fs/s3/TestS3Credentials.java
package org.apache.hadoop.fs.s3;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

import java.io.File;
import java.net.URI;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestS3Credentials {
  public static final Log LOG = LogFactory.getLog(TestS3Credentials.class);

  @Rule
  public final TestName test = new TestName();

  @Before
  public void announce() {
    LOG.info("Running test " + test.getMethodName());
  }

  private static final String EXAMPLE_ID = "AKASOMEACCESSKEY";
  private static final String EXAMPLE_KEY =
      "RGV0cm9pdCBSZ/WQgY2xl/YW5lZCB1cAEXAMPLE";

  @Test
  public void testInvalidHostnameWithUnderscores() throws Exception {
    S3Credentials s3Credentials = new S3Credentials();
    try {
      assertEquals("Invalid hostname in URI s3://a:b@c_d", e.getMessage());
    }
  }

  @Test
  public void testPlaintextConfigPassword() throws Exception {
    S3Credentials s3Credentials = new S3Credentials();
    Configuration conf = new Configuration();
    conf.set("fs.s3.awsAccessKeyId", EXAMPLE_ID);
    conf.set("fs.s3.awsSecretAccessKey", EXAMPLE_KEY);
    s3Credentials.initialize(new URI("s3://foobar"), conf);
    assertEquals("Could not retrieve proper access key", EXAMPLE_ID,
        s3Credentials.getAccessKey());
    assertEquals("Could not retrieve proper secret", EXAMPLE_KEY,
        s3Credentials.getSecretAccessKey());
  }

  @Test
  public void testPlaintextConfigPasswordWithWhitespace() throws Exception {
    S3Credentials s3Credentials = new S3Credentials();
    Configuration conf = new Configuration();
    conf.set("fs.s3.awsAccessKeyId", "\r\n " + EXAMPLE_ID +
        " \r\n");
    conf.set("fs.s3.awsSecretAccessKey", "\r\n " + EXAMPLE_KEY +
        " \r\n");
    s3Credentials.initialize(new URI("s3://foobar"), conf);
    assertEquals("Could not retrieve proper access key", EXAMPLE_ID,
        s3Credentials.getAccessKey());
    assertEquals("Could not retrieve proper secret", EXAMPLE_KEY,
        s3Credentials.getSecretAccessKey());
  }

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testCredentialProvider() throws Exception {
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    final CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    provider.createCredentialEntry("fs.s3.awsSecretAccessKey",
        EXAMPLE_KEY.toCharArray());
    provider.flush();

    S3Credentials s3Credentials = new S3Credentials();
    conf.set("fs.s3.awsAccessKeyId", EXAMPLE_ID);
    s3Credentials.initialize(new URI("s3://foobar"), conf);
    assertEquals("Could not retrieve proper access key", EXAMPLE_ID,
        s3Credentials.getAccessKey());
    assertEquals("Could not retrieve proper secret", EXAMPLE_KEY,
        s3Credentials.getSecretAccessKey());
  }

  @Test(expected=IllegalArgumentException.class)
  public void noSecretShouldThrow() throws Exception {
    S3Credentials s3Credentials = new S3Credentials();
    Configuration conf = new Configuration();
    conf.set("fs.s3.awsAccessKeyId", EXAMPLE_ID);
    s3Credentials.initialize(new URI("s3://foobar"), conf);
  }

  @Test(expected=IllegalArgumentException.class)
  public void noAccessIdShouldThrow() throws Exception {
    S3Credentials s3Credentials = new S3Credentials();
    Configuration conf = new Configuration();
    conf.set("fs.s3.awsSecretAccessKey", EXAMPLE_KEY);
    s3Credentials.initialize(new URI("s3://foobar"), conf);
  }
}

