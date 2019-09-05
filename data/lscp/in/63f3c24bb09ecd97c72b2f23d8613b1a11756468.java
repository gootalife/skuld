hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/alias/AbstractJavaKeyStoreProvider.java
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
    return keyStore;
  }

  protected final String getPathAsString() {
    return getPath().toString();
  }
    try {
      SecretKeySpec key = null;
      try {
        if (!keyStore.containsAlias(alias)) {
          return null;
        }
      throws IOException {
    writeLock.lock();
    try {
      if (keyStore.containsAlias(alias)) {
        throw new IOException("Credential " + alias + " already exists in "
            + this);
      }
      } catch (KeyStoreException e) {
        throw new IOException("Problem removing " + name + " from " + this, e);
      }
      changed = true;
    } finally {
      writeLock.unlock();

