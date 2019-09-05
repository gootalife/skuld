hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/alias/AbstractJavaKeyStoreProvider.java
++ b/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/alias/AbstractJavaKeyStoreProvider.java

package org.apache.hadoop.security.alias;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.ProviderUtils;

import com.google.common.base.Charsets;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@InterfaceAudience.Private
public abstract class AbstractJavaKeyStoreProvider extends CredentialProvider {
  public static final String CREDENTIAL_PASSWORD_NAME =
      "HADOOP_CREDSTORE_PASSWORD";
  public static final String KEYSTORE_PASSWORD_FILE_KEY =
      "hadoop.security.credstore.java-keystore-provider.password-file";
  public static final String KEYSTORE_PASSWORD_DEFAULT = "none";

  private Path path;
  private final URI uri;
  private final KeyStore keyStore;
  private char[] password = null;
  private boolean changed = false;
  private Lock readLock;
  private Lock writeLock;

  protected AbstractJavaKeyStoreProvider(URI uri, Configuration conf)
      throws IOException {
    this.uri = uri;
    initFileSystem(uri, conf);
    if (System.getenv().containsKey(CREDENTIAL_PASSWORD_NAME)) {
      password = System.getenv(CREDENTIAL_PASSWORD_NAME).toCharArray();
    }
    if (password == null) {
      String pwFile = conf.get(KEYSTORE_PASSWORD_FILE_KEY);
      if (pwFile != null) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        URL pwdFile = cl.getResource(pwFile);
        if (pwdFile != null) {
          try (InputStream is = pwdFile.openStream()) {
            password = IOUtils.toString(is).trim().toCharArray();
          }
        }
      }
    }
    if (password == null) {
      password = KEYSTORE_PASSWORD_DEFAULT.toCharArray();
    }
    try {
      keyStore = KeyStore.getInstance("jceks");
      if (keystoreExists()) {
        stashOriginalFilePermissions();
        try (InputStream in = getInputStreamForFile()) {
          keyStore.load(in, password);
        }
      } else {
        createPermissions("700");
        keyStore.load(null, password);
      }
    } catch (KeyStoreException e) {
      throw new IOException("Can't create keystore", e);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("Can't load keystore " + getPathAsString(), e);
    } catch (CertificateException e) {
      throw new IOException("Can't load keystore " + getPathAsString(), e);
    }
    ReadWriteLock lock = new ReentrantReadWriteLock(true);
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  public Path getPath() {
    return path;
  }

  public void setPath(Path p) {
    this.path = p;
  }

  public char[] getPassword() {
    return password;
  }

  public void setPassword(char[] pass) {
    this.password = pass;
  }

  public boolean isChanged() {
    return changed;
  }

  public void setChanged(boolean chg) {
    this.changed = chg;
  }

  public Lock getReadLock() {
    return readLock;
  }

  public void setReadLock(Lock rl) {
    this.readLock = rl;
  }

  public Lock getWriteLock() {
    return writeLock;
  }

  public void setWriteLock(Lock wl) {
    this.writeLock = wl;
  }

  public URI getUri() {
    return uri;
  }

  public KeyStore getKeyStore() {
    return keyStore;
  }

  public Map<String, CredentialEntry> getCache() {
    return cache;
  }

  private final Map<String, CredentialEntry> cache =
      new HashMap<String, CredentialEntry>();

  protected final String getPathAsString() {
    return getPath().toString();
  }

  protected abstract String getSchemeName();

  protected abstract OutputStream getOutputStreamForKeystore()
      throws IOException;

  protected abstract boolean keystoreExists() throws IOException;

  protected abstract InputStream getInputStreamForFile() throws IOException;

  protected abstract void createPermissions(String perms) throws IOException;

  protected abstract void stashOriginalFilePermissions() throws IOException;

  protected void initFileSystem(URI keystoreUri, Configuration conf)
      throws IOException {
    path = ProviderUtils.unnestUri(keystoreUri);
  }

  @Override
  public CredentialEntry getCredentialEntry(String alias)
      throws IOException {
    readLock.lock();
    try {
      SecretKeySpec key = null;
      try {
        if (cache.containsKey(alias)) {
          return cache.get(alias);
        }
        if (!keyStore.containsAlias(alias)) {
          return null;
        }
        key = (SecretKeySpec) keyStore.getKey(alias, password);
      } catch (KeyStoreException e) {
        throw new IOException("Can't get credential " + alias + " from "
            + getPathAsString(), e);
      } catch (NoSuchAlgorithmException e) {
        throw new IOException("Can't get algorithm for credential " + alias
            + " from " + getPathAsString(), e);
      } catch (UnrecoverableKeyException e) {
        throw new IOException("Can't recover credential " + alias + " from "
            + getPathAsString(), e);
      }
      return new CredentialEntry(alias, bytesToChars(key.getEncoded()));
    } finally {
      readLock.unlock();
    }
  }

  public static char[] bytesToChars(byte[] bytes) throws IOException {
    String pass;
    pass = new String(bytes, Charsets.UTF_8);
    return pass.toCharArray();
  }

  @Override
  public List<String> getAliases() throws IOException {
    readLock.lock();
    try {
      ArrayList<String> list = new ArrayList<String>();
      String alias = null;
      try {
        Enumeration<String> e = keyStore.aliases();
        while (e.hasMoreElements()) {
          alias = e.nextElement();
          list.add(alias);
        }
      } catch (KeyStoreException e) {
        throw new IOException("Can't get alias " + alias + " from "
            + getPathAsString(), e);
      }
      return list;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public CredentialEntry createCredentialEntry(String alias, char[] credential)
      throws IOException {
    writeLock.lock();
    try {
      if (keyStore.containsAlias(alias) || cache.containsKey(alias)) {
        throw new IOException("Credential " + alias + " already exists in "
            + this);
      }
      return innerSetCredential(alias, credential);
    } catch (KeyStoreException e) {
      throw new IOException("Problem looking up credential " + alias + " in "
          + this, e);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void deleteCredentialEntry(String name) throws IOException {
    writeLock.lock();
    try {
      try {
        if (keyStore.containsAlias(name)) {
          keyStore.deleteEntry(name);
        } else {
          throw new IOException("Credential " + name + " does not exist in "
              + this);
        }
      } catch (KeyStoreException e) {
        throw new IOException("Problem removing " + name + " from " + this, e);
      }
      cache.remove(name);
      changed = true;
    } finally {
      writeLock.unlock();
    }
  }

  CredentialEntry innerSetCredential(String alias, char[] material)
      throws IOException {
    writeLock.lock();
    try {
      keyStore.setKeyEntry(alias,
          new SecretKeySpec(new String(material).getBytes("UTF-8"), "AES"),
          password, null);
    } catch (KeyStoreException e) {
      throw new IOException("Can't store credential " + alias + " in " + this,
          e);
    } finally {
      writeLock.unlock();
    }
    changed = true;
    return new CredentialEntry(alias, material);
  }

  @Override
  public void flush() throws IOException {
    writeLock.lock();
    try {
      if (!changed) {
        return;
      }
      try (OutputStream out = getOutputStreamForKeystore()) {
        keyStore.store(out, password);
      } catch (KeyStoreException e) {
        throw new IOException("Can't store keystore " + this, e);
      } catch (NoSuchAlgorithmException e) {
        throw new IOException("No such algorithm storing keystore " + this, e);
      } catch (CertificateException e) {
        throw new IOException("Certificate exception storing keystore " + this,
            e);
      }
      changed = false;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public String toString() {
    return uri.toString();
  }
}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/alias/JavaKeyStoreProvider.java

package org.apache.hadoop.security.alias;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

@InterfaceAudience.Private
public class JavaKeyStoreProvider extends AbstractJavaKeyStoreProvider {
  public static final String SCHEME_NAME = "jceks";

  private FileSystem fs;
  private FsPermission permissions;

  private JavaKeyStoreProvider(URI uri, Configuration conf)
      throws IOException {
    super(uri, conf);
  }

  @Override
  protected String getSchemeName() {
    return SCHEME_NAME;
  }

  @Override
  protected OutputStream getOutputStreamForKeystore() throws IOException {
    FSDataOutputStream out = FileSystem.create(fs, getPath(), permissions);
    return out;
  }

  @Override
  protected boolean keystoreExists() throws IOException {
    return fs.exists(getPath());
  }

  @Override
  protected InputStream getInputStreamForFile() throws IOException {
    return fs.open(getPath());
  }

  @Override
  protected void createPermissions(String perms) {
    permissions = new FsPermission(perms);
  }

  @Override
  protected void stashOriginalFilePermissions() throws IOException {
    FileStatus s = fs.getFileStatus(getPath());
    permissions = s.getPermission();
  }

  protected void initFileSystem(URI uri, Configuration conf)
      throws IOException {
    super.initFileSystem(uri, conf);
    fs = getPath().getFileSystem(conf);
  }


hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/alias/LocalJavaKeyStoreProvider.java
++ b/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/alias/LocalJavaKeyStoreProvider.java

package org.apache.hadoop.security.alias;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.EnumSet;

@InterfaceAudience.Private
public final class LocalJavaKeyStoreProvider extends
    AbstractJavaKeyStoreProvider {
  public static final String SCHEME_NAME = "localjceks";
  private File file;
  private Set<PosixFilePermission> permissions;

  private LocalJavaKeyStoreProvider(URI uri, Configuration conf)
      throws IOException {
    super(uri, conf);
  }

  @Override
  protected String getSchemeName() {
    return SCHEME_NAME;
  }

  @Override
  protected OutputStream getOutputStreamForKeystore() throws IOException {
    FileOutputStream out = new FileOutputStream(file);
    return out;
  }

  @Override
  protected boolean keystoreExists() throws IOException {
    return file.exists();
  }

  @Override
  protected InputStream getInputStreamForFile() throws IOException {
    FileInputStream is = new FileInputStream(file);
    return is;
  }

  @Override
  protected void createPermissions(String perms) throws IOException {
    int mode = 700;
    try {
      mode = Integer.parseInt(perms, 8);
    } catch (NumberFormatException nfe) {
      throw new IOException("Invalid permissions mode provided while "
          + "trying to createPermissions", nfe);
    }
    permissions = modeToPosixFilePermission(mode);
  }

  @Override
  protected void stashOriginalFilePermissions() throws IOException {
    if (!Shell.WINDOWS) {
      Path path = Paths.get(file.getCanonicalPath());
      permissions = Files.getPosixFilePermissions(path);
    } else {
      String[] cmd = Shell.getGetPermissionCommand();
      String[] args = new String[cmd.length + 1];
      System.arraycopy(cmd, 0, args, 0, cmd.length);
      args[cmd.length] = file.getCanonicalPath();
      String out = Shell.execCommand(args);
      StringTokenizer t = new StringTokenizer(out, Shell.TOKEN_SEPARATOR_REGEX);
      String permString = t.nextToken().substring(1);
      permissions = PosixFilePermissions.fromString(permString);
    }
  }

  @Override
  protected void initFileSystem(URI uri, Configuration conf)
      throws IOException {
    super.initFileSystem(uri, conf);
    try {
      file = new File(new URI(getPath().toString()));
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void flush() throws IOException {
    super.flush();
    if (!Shell.WINDOWS) {
      Files.setPosixFilePermissions(Paths.get(file.getCanonicalPath()),
          permissions);
    } else {
      FsPermission fsPermission = FsPermission.valueOf(
          "-" + PosixFilePermissions.toString(permissions));
      FileUtil.setPermission(file, fsPermission);
    }
  }

  public static class Factory extends CredentialProviderFactory {
    @Override
    public CredentialProvider createProvider(URI providerName,
        Configuration conf) throws IOException {
      if (SCHEME_NAME.equals(providerName.getScheme())) {
        return new LocalJavaKeyStoreProvider(providerName, conf);
      }
      return null;
    }
  }

  private static Set<PosixFilePermission> modeToPosixFilePermission(
      int mode) {
    Set<PosixFilePermission> perms = EnumSet.noneOf(PosixFilePermission.class);
    if ((mode & 0001) != 0) {
      perms.add(PosixFilePermission.OTHERS_EXECUTE);
    }
    if ((mode & 0002) != 0) {
      perms.add(PosixFilePermission.OTHERS_WRITE);
    }
    if ((mode & 0004) != 0) {
      perms.add(PosixFilePermission.OTHERS_READ);
    }
    if ((mode & 0010) != 0) {
      perms.add(PosixFilePermission.GROUP_EXECUTE);
    }
    if ((mode & 0020) != 0) {
      perms.add(PosixFilePermission.GROUP_WRITE);
    }
    if ((mode & 0040) != 0) {
      perms.add(PosixFilePermission.GROUP_READ);
    }
    if ((mode & 0100) != 0) {
      perms.add(PosixFilePermission.OWNER_EXECUTE);
    }
    if ((mode & 0200) != 0) {
      perms.add(PosixFilePermission.OWNER_WRITE);
    }
    if ((mode & 0400) != 0) {
      perms.add(PosixFilePermission.OWNER_READ);
    }
    return perms;
  }
}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/alias/TestCredentialProviderFactory.java
    checkPermissionRetention(conf, ourUrl, path);
  }

  @Test
  public void testLocalJksProvider() throws Exception {
    Configuration conf = new Configuration();
    final Path jksPath = new Path(tmpDir.toString(), "test.jks");
    final String ourUrl =
        LocalJavaKeyStoreProvider.SCHEME_NAME + "://file" + jksPath.toUri();

    File file = new File(tmpDir, "test.jks");
    file.delete();
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, ourUrl);
    checkSpecificProvider(conf, ourUrl);
    Path path = ProviderUtils.unnestUri(new URI(ourUrl));
    FileSystem fs = path.getFileSystem(conf);
    FileStatus s = fs.getFileStatus(path);
    assertTrue("Unexpected permissions: " + s.getPermission().toString(), s.getPermission().toString().equals("rwx------"));
    assertTrue(file + " should exist", file.isFile());

    fs.setPermission(path, new FsPermission("777"));
    checkPermissionRetention(conf, ourUrl, path);
  }

  public void checkPermissionRetention(Configuration conf, String ourUrl,
      Path path) throws Exception {
    CredentialProvider provider = CredentialProviderFactory.getProviders(conf).get(0);

