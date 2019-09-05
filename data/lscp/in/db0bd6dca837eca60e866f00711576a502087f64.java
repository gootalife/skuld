hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/DFSUtilClient.java

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.primitives.SignedBytes;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_NAMESERVICES;

public class DFSUtilClient {
  public static final byte[] EMPTY_BYTES = {};
  private static final Logger LOG = LoggerFactory.getLogger(
      DFSUtilClient.class);
    return blkLocations;
  }

  public static int compareBytes(byte[] left, byte[] right) {
    if (left == null) {
      left = EMPTY_BYTES;
    }
    if (right == null) {
      right = EMPTY_BYTES;
    }
    return SignedBytes.lexicographicalComparator().compare(left, right);
  }

  public static byte[] byteArray2bytes(byte[][] pathComponents) {
    if (pathComponents.length == 0) {
      return EMPTY_BYTES;
    } else if (pathComponents.length == 1
        && (pathComponents[0] == null || pathComponents[0].length == 0)) {
      return new byte[]{(byte) Path.SEPARATOR_CHAR};
    }
    int length = 0;
    for (int i = 0; i < pathComponents.length; i++) {
      length += pathComponents[i].length;
      if (i < pathComponents.length - 1) {
        length++; // for SEPARATOR
      }
    }
    byte[] path = new byte[length];
    int index = 0;
    for (int i = 0; i < pathComponents.length; i++) {
      System.arraycopy(pathComponents[i], 0, path, index,
          pathComponents[i].length);
      index += pathComponents[i].length;
      if (i < pathComponents.length - 1) {
        path[index] = (byte) Path.SEPARATOR_CHAR;
        index++;
      }
    }
    return path;
  }

    }
    return true;
  }

  public static String durationToString(long durationMs) {
    boolean negative = false;
    if (durationMs < 0) {
      negative = true;
      durationMs = -durationMs;
    }
    long durationSec = durationMs / 1000;
    final int secondsPerMinute = 60;
    final int secondsPerHour = 60*60;
    final int secondsPerDay = 60*60*24;
    final long days = durationSec / secondsPerDay;
    durationSec -= days * secondsPerDay;
    final long hours = durationSec / secondsPerHour;
    durationSec -= hours * secondsPerHour;
    final long minutes = durationSec / secondsPerMinute;
    durationSec -= minutes * secondsPerMinute;
    final long seconds = durationSec;
    final long milliseconds = durationMs % 1000;
    String format = "%03d:%02d:%02d:%02d.%03d";
    if (negative)  {
      format = "-" + format;
    }
    return String.format(format, days, hours, minutes, seconds, milliseconds);
  }

  public static String dateToIso8601String(Date date) {
    SimpleDateFormat df =
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.ENGLISH);
    return df.format(date);
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsClientConfigKeys.java
  boolean DFS_WEBHDFS_ENABLED_DEFAULT = true;
  String  DFS_NAMENODE_HTTP_PORT_KEY = "dfs.http.port";
  String  DFS_NAMENODE_HTTPS_PORT_KEY = "dfs.https.port";
  int DFS_NAMENODE_RPC_PORT_DEFAULT = 8020;

  interface Retry {

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/CacheDirectiveEntry.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/CacheDirectiveEntry.java
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceStability.Evolving
@InterfaceAudience.Public
public class CacheDirectiveEntry {
  private final CacheDirectiveInfo info;
  private final CacheDirectiveStats stats;

  public CacheDirectiveEntry(CacheDirectiveInfo info,
      CacheDirectiveStats stats) {
    this.info = info;
    this.stats = stats;
  }

  public CacheDirectiveInfo getInfo() {
    return info;
  }

  public CacheDirectiveStats getStats() {
    return stats;
  }
};

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/CacheDirectiveInfo.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/CacheDirectiveInfo.java
package org.apache.hadoop.hdfs.protocol;

import java.util.Date;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.DFSUtilClient;

@InterfaceStability.Evolving
@InterfaceAudience.Public
public class CacheDirectiveInfo {
  public static class Builder {
    private Long id;
    private Path path;
    private Short replication;
    private String pool;
    private Expiration expiration;

    public CacheDirectiveInfo build() {
      return new CacheDirectiveInfo(id, path, replication, pool, expiration);
    }

    public Builder() {
    }

    public Builder(CacheDirectiveInfo directive) {
      this.id = directive.getId();
      this.path = directive.getPath();
      this.replication = directive.getReplication();
      this.pool = directive.getPool();
      this.expiration = directive.getExpiration();
    }

    public Builder setId(Long id) {
      this.id = id;
      return this;
    }

    public Builder setPath(Path path) {
      this.path = path;
      return this;
    }

    public Builder setReplication(Short replication) {
      this.replication = replication;
      return this;
    }

    public Builder setPool(String pool) {
      this.pool = pool;
      return this;
    }

    public Builder setExpiration(Expiration expiration) {
      this.expiration = expiration;
      return this;
    }
  }

  public static class Expiration {

    public static final long MAX_RELATIVE_EXPIRY_MS =
        Long.MAX_VALUE / 4; // This helps prevent weird overflow bugs

    public static final Expiration NEVER = newRelative(MAX_RELATIVE_EXPIRY_MS);

    public static Expiration newRelative(long ms) {
      return new Expiration(ms, true);
    }

    public static Expiration newAbsolute(Date date) {
      return new Expiration(date.getTime(), false);
    }

    public static Expiration newAbsolute(long ms) {
      return new Expiration(ms, false);
    }

    private final long ms;
    private final boolean isRelative;

    private Expiration(long ms, boolean isRelative) {
      if (isRelative) {
        Preconditions.checkArgument(ms <= MAX_RELATIVE_EXPIRY_MS,
            "Expiration time is too far in the future!");
      }
      this.ms = ms;
      this.isRelative = isRelative;
    }

    public boolean isRelative() {
      return isRelative;
    }

    public long getMillis() {
      return ms;
    }

    public Date getAbsoluteDate() {
      return new Date(getAbsoluteMillis());
    }

    public long getAbsoluteMillis() {
      if (!isRelative) {
        return ms;
      } else {
        return new Date().getTime() + ms;
      }
    }

    @Override
    public String toString() {
      if (isRelative) {
        return DFSUtilClient.durationToString(ms);
      }
      return DFSUtilClient.dateToIso8601String(new Date(ms));
    }
  }

  private final Long id;
  private final Path path;
  private final Short replication;
  private final String pool;
  private final Expiration expiration;

  CacheDirectiveInfo(Long id, Path path, Short replication, String pool,
      Expiration expiration) {
    this.id = id;
    this.path = path;
    this.replication = replication;
    this.pool = pool;
    this.expiration = expiration;
  }

  public Long getId() {
    return id;
  }

  public Path getPath() {
    return path;
  }

  public Short getReplication() {
    return replication;
  }

  public String getPool() {
    return pool;
  }

  public Expiration getExpiration() {
    return expiration;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    CacheDirectiveInfo other = (CacheDirectiveInfo)o;
    return new EqualsBuilder().append(getId(), other.getId()).
        append(getPath(), other.getPath()).
        append(getReplication(), other.getReplication()).
        append(getPool(), other.getPool()).
        append(getExpiration(), other.getExpiration()).
        isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(id).
        append(path).
        append(replication).
        append(pool).
        append(expiration).
        hashCode();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    String prefix = "";
    if (id != null) {
      builder.append(prefix).append("id: ").append(id);
      prefix = ", ";
    }
    if (path != null) {
      builder.append(prefix).append("path: ").append(path);
      prefix = ", ";
    }
    if (replication != null) {
      builder.append(prefix).append("replication: ").append(replication);
      prefix = ", ";
    }
    if (pool != null) {
      builder.append(prefix).append("pool: ").append(pool);
      prefix = ", ";
    }
    if (expiration != null) {
      builder.append(prefix).append("expiration: ").append(expiration);
      prefix = ", ";
    }
    builder.append("}");
    return builder.toString();
  }
};

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/CacheDirectiveStats.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/CacheDirectiveStats.java
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceStability.Evolving
@InterfaceAudience.Public
public class CacheDirectiveStats {
  public static class Builder {
    private long bytesNeeded;
    private long bytesCached;
    private long filesNeeded;
    private long filesCached;
    private boolean hasExpired;

    public CacheDirectiveStats build() {
      return new CacheDirectiveStats(bytesNeeded, bytesCached, filesNeeded,
          filesCached, hasExpired);
    }

    public Builder() {
    }

    public Builder setBytesNeeded(long bytesNeeded) {
      this.bytesNeeded = bytesNeeded;
      return this;
    }

    public Builder setBytesCached(long bytesCached) {
      this.bytesCached = bytesCached;
      return this;
    }

    public Builder setFilesNeeded(long filesNeeded) {
      this.filesNeeded = filesNeeded;
      return this;
    }

    public Builder setFilesCached(long filesCached) {
      this.filesCached = filesCached;
      return this;
    }

    public Builder setHasExpired(boolean hasExpired) {
      this.hasExpired = hasExpired;
      return this;
    }
  }

  private final long bytesNeeded;
  private final long bytesCached;
  private final long filesNeeded;
  private final long filesCached;
  private final boolean hasExpired;

  private CacheDirectiveStats(long bytesNeeded, long bytesCached,
      long filesNeeded, long filesCached, boolean hasExpired) {
    this.bytesNeeded = bytesNeeded;
    this.bytesCached = bytesCached;
    this.filesNeeded = filesNeeded;
    this.filesCached = filesCached;
    this.hasExpired = hasExpired;
  }

  public long getBytesNeeded() {
    return bytesNeeded;
  }

  public long getBytesCached() {
    return bytesCached;
  }

  public long getFilesNeeded() {
    return filesNeeded;
  }

  public long getFilesCached() {
    return filesCached;
  }

  public boolean hasExpired() {
    return hasExpired;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    builder.append("bytesNeeded: ").append(bytesNeeded);
    builder.append(", ").append("bytesCached: ").append(bytesCached);
    builder.append(", ").append("filesNeeded: ").append(filesNeeded);
    builder.append(", ").append("filesCached: ").append(filesCached);
    builder.append(", ").append("hasExpired: ").append(hasExpired);
    builder.append("}");
    return builder.toString();
  }
};

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/CachePoolEntry.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/CachePoolEntry.java

package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CachePoolEntry {
  private final CachePoolInfo info;
  private final CachePoolStats stats;

  public CachePoolEntry(CachePoolInfo info, CachePoolStats stats) {
    this.info = info;
    this.stats = stats;
  }

  public CachePoolInfo getInfo() {
    return info;
  }

  public CachePoolStats getStats() {
    return stats;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/CachePoolInfo.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/CachePoolInfo.java

package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo.Expiration;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CachePoolInfo {
  public static final Log LOG = LogFactory.getLog(CachePoolInfo.class);

  public static final long RELATIVE_EXPIRY_NEVER =
      Expiration.MAX_RELATIVE_EXPIRY_MS;
  public static final long DEFAULT_MAX_RELATIVE_EXPIRY =
      RELATIVE_EXPIRY_NEVER;

  public static final long LIMIT_UNLIMITED = Long.MAX_VALUE;
  public static final long DEFAULT_LIMIT = LIMIT_UNLIMITED;

  final String poolName;

  @Nullable
  String ownerName;

  @Nullable
  String groupName;

  @Nullable
  FsPermission mode;

  @Nullable
  Long limit;

  @Nullable
  Long maxRelativeExpiryMs;

  public CachePoolInfo(String poolName) {
    this.poolName = poolName;
  }

  public String getPoolName() {
    return poolName;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public CachePoolInfo setOwnerName(String ownerName) {
    this.ownerName = ownerName;
    return this;
  }

  public String getGroupName() {
    return groupName;
  }

  public CachePoolInfo setGroupName(String groupName) {
    this.groupName = groupName;
    return this;
  }

  public FsPermission getMode() {
    return mode;
  }

  public CachePoolInfo setMode(FsPermission mode) {
    this.mode = mode;
    return this;
  }

  public Long getLimit() {
    return limit;
  }

  public CachePoolInfo setLimit(Long bytes) {
    this.limit = bytes;
    return this;
  }

  public Long getMaxRelativeExpiryMs() {
    return maxRelativeExpiryMs;
  }

  public CachePoolInfo setMaxRelativeExpiryMs(Long ms) {
    this.maxRelativeExpiryMs = ms;
    return this;
  }

  public String toString() {
    return new StringBuilder().append("{").
      append("poolName:").append(poolName).
      append(", ownerName:").append(ownerName).
      append(", groupName:").append(groupName).
      append(", mode:").append((mode == null) ? "null" :
          String.format("0%03o", mode.toShort())).
      append(", limit:").append(limit).
      append(", maxRelativeExpiryMs:").append(maxRelativeExpiryMs).
      append("}").toString();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o == null) { return false; }
    if (o == this) { return true; }
    if (o.getClass() != getClass()) {
      return false;
    }
    CachePoolInfo other = (CachePoolInfo)o;
    return new EqualsBuilder().
        append(poolName, other.poolName).
        append(ownerName, other.ownerName).
        append(groupName, other.groupName).
        append(mode, other.mode).
        append(limit, other.limit).
        append(maxRelativeExpiryMs, other.maxRelativeExpiryMs).
        isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().
        append(poolName).
        append(ownerName).
        append(groupName).
        append(mode).
        append(limit).
        append(maxRelativeExpiryMs).
        hashCode();
  }

  public static void validate(CachePoolInfo info) throws IOException {
    if (info == null) {
      throw new InvalidRequestException("CachePoolInfo is null");
    }
    if ((info.getLimit() != null) && (info.getLimit() < 0)) {
      throw new InvalidRequestException("Limit is negative.");
    }
    if (info.getMaxRelativeExpiryMs() != null) {
      long maxRelativeExpiryMs = info.getMaxRelativeExpiryMs();
      if (maxRelativeExpiryMs < 0l) {
        throw new InvalidRequestException("Max relative expiry is negative.");
      }
      if (maxRelativeExpiryMs > Expiration.MAX_RELATIVE_EXPIRY_MS) {
        throw new InvalidRequestException("Max relative expiry is too big.");
      }
    }
    validateName(info.poolName);
  }

  public static void validateName(String poolName) throws IOException {
    if (poolName == null || poolName.isEmpty()) {
      throw new IOException("invalid empty cache pool name");
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/CachePoolStats.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/CachePoolStats.java

package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CachePoolStats {
  public static class Builder {
    private long bytesNeeded;
    private long bytesCached;
    private long bytesOverlimit;
    private long filesNeeded;
    private long filesCached;

    public Builder() {
    }

    public Builder setBytesNeeded(long bytesNeeded) {
      this.bytesNeeded = bytesNeeded;
      return this;
    }

    public Builder setBytesCached(long bytesCached) {
      this.bytesCached = bytesCached;
      return this;
    }

    public Builder setBytesOverlimit(long bytesOverlimit) {
      this.bytesOverlimit = bytesOverlimit;
      return this;
    }

    public Builder setFilesNeeded(long filesNeeded) {
      this.filesNeeded = filesNeeded;
      return this;
    }

    public Builder setFilesCached(long filesCached) {
      this.filesCached = filesCached;
      return this;
    }

    public CachePoolStats build() {
      return new CachePoolStats(bytesNeeded, bytesCached, bytesOverlimit,
          filesNeeded, filesCached);
    }
  };

  private final long bytesNeeded;
  private final long bytesCached;
  private final long bytesOverlimit;
  private final long filesNeeded;
  private final long filesCached;

  private CachePoolStats(long bytesNeeded, long bytesCached,
      long bytesOverlimit, long filesNeeded, long filesCached) {
    this.bytesNeeded = bytesNeeded;
    this.bytesCached = bytesCached;
    this.bytesOverlimit = bytesOverlimit;
    this.filesNeeded = filesNeeded;
    this.filesCached = filesCached;
  }

  public long getBytesNeeded() {
    return bytesNeeded;
  }

  public long getBytesCached() {
    return bytesCached;
  }

  public long getBytesOverlimit() {
    return bytesOverlimit;
  }

  public long getFilesNeeded() {
    return filesNeeded;
  }

  public long getFilesCached() {
    return filesCached;
  }

  public String toString() {
    return new StringBuilder().append("{").
      append("bytesNeeded:").append(bytesNeeded).
      append(", bytesCached:").append(bytesCached).
      append(", bytesOverlimit:").append(bytesOverlimit).
      append(", filesNeeded:").append(filesNeeded).
      append(", filesCached:").append(filesCached).
      append("}").toString();
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/SnapshotDiffReport.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/SnapshotDiffReport.java
package org.apache.hadoop.hdfs.protocol;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Objects;
import org.apache.hadoop.hdfs.DFSUtilClient;

public class SnapshotDiffReport {
  private final static String LINE_SEPARATOR = System.getProperty(
      "line.separator", "\n");

  public enum DiffType {
    CREATE("+"),     
    MODIFY("M"),    
    DELETE("-"), 
    RENAME("R");
    
    private final String label;
    
    private DiffType(String label) {
      this.label = label;
    }
    
    public String getLabel() {
      return label;
    }
    
    public static DiffType getTypeFromLabel(String label) {
      if (label.equals(CREATE.getLabel())) {
        return CREATE;
      } else if (label.equals(MODIFY.getLabel())) {
        return MODIFY;
      } else if (label.equals(DELETE.getLabel())) {
        return DELETE;
      } else if (label.equals(RENAME.getLabel())) {
        return RENAME;
      }
      return null;
    }
  };
  
  public static class DiffReportEntry {
    private final DiffType type;
    private final byte[] sourcePath;
    private final byte[] targetPath;

    public DiffReportEntry(DiffType type, byte[] sourcePath) {
      this(type, sourcePath, null);
    }

    public DiffReportEntry(DiffType type, byte[][] sourcePathComponents) {
      this(type, sourcePathComponents, null);
    }

    public DiffReportEntry(DiffType type, byte[] sourcePath, byte[] targetPath) {
      this.type = type;
      this.sourcePath = sourcePath;
      this.targetPath = targetPath;
    }
    
    public DiffReportEntry(DiffType type, byte[][] sourcePathComponents,
        byte[][] targetPathComponents) {
      this.type = type;
      this.sourcePath = DFSUtilClient.byteArray2bytes(sourcePathComponents);
      this.targetPath = targetPathComponents == null ? null : DFSUtilClient
          .byteArray2bytes(targetPathComponents);
    }
    
    @Override
    public String toString() {
      String str = type.getLabel() + "\t" + getPathString(sourcePath);
      if (type == DiffType.RENAME) {
        str += " -> " + getPathString(targetPath);
      }
      return str;
    }
    
    public DiffType getType() {
      return type;
    }

    static String getPathString(byte[] path) {
      String pathStr = DFSUtilClient.bytes2String(path);
      if (pathStr.isEmpty()) {
        return Path.CUR_DIR;
      } else {
        return Path.CUR_DIR + Path.SEPARATOR + pathStr;
      }
    }

    public byte[] getSourcePath() {
      return sourcePath;
    }

    public byte[] getTargetPath() {
      return targetPath;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } 
      if (other != null && other instanceof DiffReportEntry) {
        DiffReportEntry entry = (DiffReportEntry) other;
        return type.equals(entry.getType())
            && Arrays.equals(sourcePath, entry.getSourcePath())
            && Arrays.equals(targetPath, entry.getTargetPath());
      }
      return false;
    }
    
    @Override
    public int hashCode() {
      return Objects.hashCode(getSourcePath(), getTargetPath());
    }
  }
  
  private final String snapshotRoot;

  private final String fromSnapshot;
  
  private final String toSnapshot;
  
  private final List<DiffReportEntry> diffList;
  
  public SnapshotDiffReport(String snapshotRoot, String fromSnapshot,
      String toSnapshot, List<DiffReportEntry> entryList) {
    this.snapshotRoot = snapshotRoot;
    this.fromSnapshot = fromSnapshot;
    this.toSnapshot = toSnapshot;
    this.diffList = entryList != null ? entryList : Collections
        .<DiffReportEntry> emptyList();
  }
  
  public String getSnapshotRoot() {
    return snapshotRoot;
  }

  public String getFromSnapshot() {
    return fromSnapshot;
  }

  public String getLaterSnapshotName() {
    return toSnapshot;
  }
  
  public List<DiffReportEntry> getDiffList() {
    return diffList;
  }
  
  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    String from = fromSnapshot == null || fromSnapshot.isEmpty() ? 
        "current directory" : "snapshot " + fromSnapshot;
    String to = toSnapshot == null || toSnapshot.isEmpty() ? "current directory"
        : "snapshot " + toSnapshot;
    str.append("Difference between " + from + " and " + to
        + " under directory " + snapshotRoot + ":" + LINE_SEPARATOR);
    for (DiffReportEntry entry : diffList) {
      str.append(entry.toString() + LINE_SEPARATOR);
    }
    return str.toString();
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/SnapshottableDirectoryStatus.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/SnapshottableDirectoryStatus.java
package org.apache.hadoop.hdfs.protocol;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtilClient;

public class SnapshottableDirectoryStatus {
  public static final Comparator<SnapshottableDirectoryStatus> COMPARATOR
      = new Comparator<SnapshottableDirectoryStatus>() {
    @Override
    public int compare(SnapshottableDirectoryStatus left,
                       SnapshottableDirectoryStatus right) {
      int d = DFSUtilClient.compareBytes(left.parentFullPath, right.parentFullPath);
      return d != 0? d
          : DFSUtilClient.compareBytes(left.dirStatus.getLocalNameInBytes(),
              right.dirStatus.getLocalNameInBytes());
    }
  };

  private final HdfsFileStatus dirStatus;
  
  private final int snapshotNumber;
  
  private final int snapshotQuota;
  
  private final byte[] parentFullPath;
  
  public SnapshottableDirectoryStatus(long modification_time, long access_time,
      FsPermission permission, String owner, String group, byte[] localName,
      long inodeId, int childrenNum,
      int snapshotNumber, int snapshotQuota, byte[] parentFullPath) {
    this.dirStatus = new HdfsFileStatus(0, true, 0, 0, modification_time,
        access_time, permission, owner, group, null, localName, inodeId,
        childrenNum, null, HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED);
    this.snapshotNumber = snapshotNumber;
    this.snapshotQuota = snapshotQuota;
    this.parentFullPath = parentFullPath;
  }

  public int getSnapshotNumber() {
    return snapshotNumber;
  }

  public int getSnapshotQuota() {
    return snapshotQuota;
  }
  
  public byte[] getParentFullPath() {
    return parentFullPath;
  }

  public HdfsFileStatus getDirStatus() {
    return dirStatus;
  }
  
  public Path getFullPath() {
    String parentFullPathStr = 
        (parentFullPath == null || parentFullPath.length == 0) ? 
            null : DFSUtilClient.bytes2String(parentFullPath);
    if (parentFullPathStr == null
        && dirStatus.getLocalNameInBytes().length == 0) {
      return new Path("/");
    } else {
      return parentFullPathStr == null ? new Path(dirStatus.getLocalName())
          : new Path(parentFullPathStr, dirStatus.getLocalName());
    }
  }
  
  public static void print(SnapshottableDirectoryStatus[] stats, 
      PrintStream out) {
    if (stats == null || stats.length == 0) {
      out.println();
      return;
    }
    int maxRepl = 0, maxLen = 0, maxOwner = 0, maxGroup = 0;
    int maxSnapshotNum = 0, maxSnapshotQuota = 0;
    for (SnapshottableDirectoryStatus status : stats) {
      maxRepl = maxLength(maxRepl, status.dirStatus.getReplication());
      maxLen = maxLength(maxLen, status.dirStatus.getLen());
      maxOwner = maxLength(maxOwner, status.dirStatus.getOwner());
      maxGroup = maxLength(maxGroup, status.dirStatus.getGroup());
      maxSnapshotNum = maxLength(maxSnapshotNum, status.snapshotNumber);
      maxSnapshotQuota = maxLength(maxSnapshotQuota, status.snapshotQuota);
    }
    
    StringBuilder fmt = new StringBuilder();
    fmt.append("%s%s "); // permission string
    fmt.append("%"  + maxRepl  + "s ");
    fmt.append((maxOwner > 0) ? "%-" + maxOwner + "s " : "%s");
    fmt.append((maxGroup > 0) ? "%-" + maxGroup + "s " : "%s");
    fmt.append("%"  + maxLen   + "s ");
    fmt.append("%s "); // mod time
    fmt.append("%"  + maxSnapshotNum  + "s ");
    fmt.append("%"  + maxSnapshotQuota  + "s ");
    fmt.append("%s"); // path
    
    String lineFormat = fmt.toString();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
         
    for (SnapshottableDirectoryStatus status : stats) {
      String line = String.format(lineFormat, "d", 
          status.dirStatus.getPermission(),
          status.dirStatus.getReplication(),
          status.dirStatus.getOwner(),
          status.dirStatus.getGroup(),
          String.valueOf(status.dirStatus.getLen()),
          dateFormat.format(new Date(status.dirStatus.getModificationTime())),
          status.snapshotNumber, status.snapshotQuota, 
          status.getFullPath().toString()
      );
      out.println(line);
    }
  }

  private static int maxLength(int n, Object value) {
    return Math.max(n, String.valueOf(value).length());
  }

  public static class Bean {
    private final String path;
    private final int snapshotNumber;
    private final int snapshotQuota;
    private final long modificationTime;
    private final short permission;
    private final String owner;
    private final String group;

    public Bean(String path, int snapshotNumber, int snapshotQuota,
        long modificationTime, short permission, String owner, String group) {
      this.path = path;
      this.snapshotNumber = snapshotNumber;
      this.snapshotQuota = snapshotQuota;
      this.modificationTime = modificationTime;
      this.permission = permission;
      this.owner = owner;
      this.group = group;
    }

    public String getPath() {
      return path;
    }

    public int getSnapshotNumber() {
      return snapshotNumber;
    }

    public int getSnapshotQuota() {
      return snapshotQuota;
    }

    public long getModificationTime() {
      return modificationTime;
    }

    public short getPermission() {
      return permission;
    }

    public String getOwner() {
      return owner;
    }

    public String getGroup() {
      return group;
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/security/token/delegation/DelegationTokenSelector.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/security/token/delegation/DelegationTokenSelector.java
package org.apache.hadoop.hdfs.security.token.delegation;

import java.net.URI;
import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;

@InterfaceAudience.Private
public class DelegationTokenSelector
    extends AbstractDelegationTokenSelector<DelegationTokenIdentifier>{
  public static final String SERVICE_NAME_KEY = "hdfs.service.host_";

  public Token<DelegationTokenIdentifier> selectToken(
      final URI nnUri, Collection<Token<?>> tokens,
      final Configuration conf) {
    Text serviceName = SecurityUtil.buildTokenService(nnUri);
    final String nnServiceName = conf.get(SERVICE_NAME_KEY + serviceName);
    
    int nnRpcPort = HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT;
    if (nnServiceName != null) {
      nnRpcPort = NetUtils.createSocketAddr(nnServiceName, nnRpcPort).getPort(); 
    }
    serviceName = SecurityUtil.buildTokenService(
    		NetUtils.createSocketAddrForHost(nnUri.getHost(), nnRpcPort));
    
    return selectToken(serviceName, tokens);
  }

  public DelegationTokenSelector() {
    super(DelegationTokenIdentifier.HDFS_DELEGATION_KIND);
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/server/namenode/NotReplicatedYetException.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/server/namenode/NotReplicatedYetException.java

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NotReplicatedYetException extends IOException {
  private static final long serialVersionUID = 1L;

  public NotReplicatedYetException(String msg) {
    super(msg);
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/server/protocol/DatanodeStorage.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/server/protocol/DatanodeStorage.java
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.fs.StorageType;

import java.util.UUID;

public class DatanodeStorage {
  public enum State {
    NORMAL,
    
    READ_ONLY_SHARED,

    FAILED;
  }
  
  private final String storageID;
  private final State state;
  private final StorageType storageType;
  private static final String STORAGE_ID_PREFIX = "DS-";

  public DatanodeStorage(String storageID) {
    this(storageID, State.NORMAL, StorageType.DEFAULT);
  }

  public DatanodeStorage(String sid, State s, StorageType sm) {
    this.storageID = sid;
    this.state = s;
    this.storageType = sm;
  }

  public String getStorageID() {
    return storageID;
  }

  public State getState() {
    return state;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  public static String generateUuid() {
    return STORAGE_ID_PREFIX + UUID.randomUUID();
  }

  public static boolean isValidStorageId(final String storageID) {
    try {
      if (storageID != null && storageID.indexOf(STORAGE_ID_PREFIX) == 0) {
        UUID.fromString(storageID.substring(STORAGE_ID_PREFIX.length()));
        return true;
      }
    } catch (IllegalArgumentException iae) {
    }

    return false;
  }

  @Override
  public String toString() {
    return "DatanodeStorage["+ storageID + "," + storageType + "," + state +"]";
  }
  
  @Override
  public boolean equals(Object other){
    if (other == this) {
      return true;
    }

    if ((other == null) ||
        !(other instanceof DatanodeStorage)) {
      return false;
    }
    DatanodeStorage otherStorage = (DatanodeStorage) other;
    return otherStorage.getStorageID().compareTo(getStorageID()) == 0;
  }

  @Override
  public int hashCode() {
    return getStorageID().hashCode();
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/server/protocol/DatanodeStorageReport.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/server/protocol/DatanodeStorageReport.java
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class DatanodeStorageReport {
  final DatanodeInfo datanodeInfo;
  final StorageReport[] storageReports;

  public DatanodeStorageReport(DatanodeInfo datanodeInfo,
      StorageReport[] storageReports) {
    this.datanodeInfo = datanodeInfo;
    this.storageReports = storageReports;
  }

  public DatanodeInfo getDatanodeInfo() {
    return datanodeInfo;
  }

  public StorageReport[] getStorageReports() {
    return storageReports;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/server/protocol/StorageReport.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/server/protocol/StorageReport.java
package org.apache.hadoop.hdfs.server.protocol;

public class StorageReport {
  private final DatanodeStorage storage;
  private final boolean failed;
  private final long capacity;
  private final long dfsUsed;
  private final long remaining;
  private final long blockPoolUsed;

  public static final StorageReport[] EMPTY_ARRAY = {};
  
  public StorageReport(DatanodeStorage storage, boolean failed,
      long capacity, long dfsUsed, long remaining, long bpUsed) {
    this.storage = storage;
    this.failed = failed;
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.blockPoolUsed = bpUsed;
  }

  public DatanodeStorage getStorage() {
    return storage;
  }

  public boolean isFailed() {
    return failed;
  }

  public long getCapacity() {
    return capacity;
  }

  public long getDfsUsed() {
    return dfsUsed;
  }

  public long getRemaining() {
    return remaining;
  }

  public long getBlockPoolUsed() {
    return blockPoolUsed;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSUtil.java
import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.BlockingService;

@InterfaceAudience.Private
public class DFSUtil {
  public static final Log LOG = LogFactory.getLog(DFSUtil.class.getName());
  
  private DFSUtil() { /* Hidden constructor */ }
  private static final ThreadLocal<Random> RANDOM = new ThreadLocal<Random>() {
    @Override
    return Joiner.on(Path.SEPARATOR).join(components);
  }

  public static String path2String(final Object path) {
    return path == null? null
  public static String dateToIso8601String(Date date) {
    return DFSUtilClient.dateToIso8601String(date);
  }

  public static String durationToString(long durationMs) {
    return DFSUtilClient.durationToString(durationMs);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/PBHelper.java
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.protocol.Block;
  public static BlockKeyProto convert(BlockKey key) {
    byte[] encodedKey = key.getEncodedKey();
    ByteString keyBytes = ByteString.copyFrom(encodedKey == null ? 
        DFSUtilClient.EMPTY_BYTES : encodedKey);
    return BlockKeyProto.newBuilder().setKeyId(key.getKeyId())
        .setKeyBytes(keyBytes).setExpiryDate(key.getExpiryDate()).build();
  }
    int snapshotQuota = status.getSnapshotQuota();
    byte[] parentFullPath = status.getParentFullPath();
    ByteString parentFullPathBytes = ByteString.copyFrom(
        parentFullPath == null ? DFSUtilClient.EMPTY_BYTES : parentFullPath);
    HdfsFileStatusProto fs = convert(status.getDirStatus());
    SnapshottableDirectoryStatusProto.Builder builder = 
        SnapshottableDirectoryStatusProto
      return null;
    }
    ByteString sourcePath = ByteString
        .copyFrom(entry.getSourcePath() == null ? DFSUtilClient.EMPTY_BYTES : entry
            .getSourcePath());
    String modification = entry.getType().getLabel();
    SnapshotDiffReportEntryProto.Builder builder = SnapshotDiffReportEntryProto
        .setModificationLabel(modification);
    if (entry.getType() == DiffType.RENAME) {
      ByteString targetPath = ByteString
          .copyFrom(entry.getTargetPath() == null ? DFSUtilClient.EMPTY_BYTES : entry
              .getTargetPath());
      builder.setTargetPath(targetPath);
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INode.java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.DFSUtil;

  @Override
  public final int compareTo(byte[] bytes) {
    return DFSUtilClient.compareBytes(getLocalNameBytes(), bytes);
  }

  @Override

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/SnapshotManager.java
import javax.management.ObjectName;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.protocol.SnapshotInfo;
            dir.getChildrenNum(Snapshot.CURRENT_STATE_ID),
            dir.getDirectorySnapshottableFeature().getNumSnapshots(),
            dir.getDirectorySnapshottableFeature().getSnapshotQuota(),
            dir.getParent() == null ? DFSUtilClient.EMPTY_BYTES :
                DFSUtil.string2Bytes(dir.getParent().getFullPathName()));
        statusList.add(status);
      }

