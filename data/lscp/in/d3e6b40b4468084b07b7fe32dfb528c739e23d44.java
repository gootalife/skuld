hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/ClientProtocol.java
package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;
  @Idempotent
  @Idempotent
  @Idempotent
  @Idempotent
  @Idempotent
  @Idempotent
  @AtMostOnce
  @Idempotent
  boolean mkdirs(String src, FsPermission masked, boolean createParent)
  @Idempotent
  @Idempotent
  @Idempotent
  long getPreferredBlockSize(String filename)
  @Idempotent
  boolean restoreFailedStorage(String arg) throws IOException;
  @Idempotent
  @Idempotent
  @Idempotent
  @Idempotent
  @Idempotent
  @Idempotent

