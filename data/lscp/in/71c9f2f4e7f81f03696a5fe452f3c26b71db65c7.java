hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/fs/CacheFlag.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/fs/CacheFlag.java
package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum CacheFlag {

  FORCE((short) 0x01);
  private final short mode;

  private CacheFlag(short mode) {
    this.mode = mode;
  }

  short getMode() {
    return mode;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/fs/XAttr.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/fs/XAttr.java
package org.apache.hadoop.fs;

import java.util.Arrays;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class XAttr {

  public static enum NameSpace {
    USER,
    TRUSTED,
    SECURITY,
    SYSTEM,
    RAW;
  }

  private final NameSpace ns;
  private final String name;
  private final byte[] value;

  public static class Builder {
    private NameSpace ns = NameSpace.USER;
    private String name;
    private byte[] value;

    public Builder setNameSpace(NameSpace ns) {
      this.ns = ns;
      return this;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setValue(byte[] value) {
      this.value = value;
      return this;
    }

    public XAttr build() {
      return new XAttr(ns, name, value);
    }
  }

  private XAttr(NameSpace ns, String name, byte[] value) {
    this.ns = ns;
    this.name = name;
    this.value = value;
  }

  public NameSpace getNameSpace() {
    return ns;
  }

  public String getName() {
    return name;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(811, 67)
        .append(name)
        .append(ns)
        .append(value)
        .toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    XAttr rhs = (XAttr) obj;
    return new EqualsBuilder()
        .append(ns, rhs.ns)
        .append(name, rhs.name)
        .append(value, rhs.value)
        .isEquals();
  }

  public boolean equalsIgnoreValue(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    XAttr rhs = (XAttr) obj;
    return new EqualsBuilder()
        .append(ns, rhs.ns)
        .append(name, rhs.name)
        .isEquals();
  }

  @Override
  public String toString() {
    return "XAttr [ns=" + ns + ", name=" + name + ", value="
        + Arrays.toString(value) + "]";
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/inotify/Event.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/inotify/Event.java

package org.apache.hadoop.hdfs.inotify;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;

import java.util.List;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class Event {
  public static enum EventType {
    CREATE, CLOSE, APPEND, RENAME, METADATA, UNLINK
  }

  private EventType eventType;

  public EventType getEventType() {
    return eventType;
  }

  public Event(EventType eventType) {
    this.eventType = eventType;
  }

  public static class CloseEvent extends Event {
    private String path;
    private long fileSize;
    private long timestamp;

    public CloseEvent(String path, long fileSize, long timestamp) {
      super(EventType.CLOSE);
      this.path = path;
      this.fileSize = fileSize;
      this.timestamp = timestamp;
    }

    public String getPath() {
      return path;
    }

    public long getFileSize() {
      return fileSize;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }

  public static class CreateEvent extends Event {

    public static enum INodeType {
      FILE, DIRECTORY, SYMLINK;
    }

    private INodeType iNodeType;
    private String path;
    private long ctime;
    private int replication;
    private String ownerName;
    private String groupName;
    private FsPermission perms;
    private String symlinkTarget;
    private boolean overwrite;
    private long defaultBlockSize;

    public static class Builder {
      private INodeType iNodeType;
      private String path;
      private long ctime;
      private int replication;
      private String ownerName;
      private String groupName;
      private FsPermission perms;
      private String symlinkTarget;
      private boolean overwrite;
      private long defaultBlockSize = 0;

      public Builder iNodeType(INodeType type) {
        this.iNodeType = type;
        return this;
      }

      public Builder path(String path) {
        this.path = path;
        return this;
      }

      public Builder ctime(long ctime) {
        this.ctime = ctime;
        return this;
      }

      public Builder replication(int replication) {
        this.replication = replication;
        return this;
      }

      public Builder ownerName(String ownerName) {
        this.ownerName = ownerName;
        return this;
      }

      public Builder groupName(String groupName) {
        this.groupName = groupName;
        return this;
      }

      public Builder perms(FsPermission perms) {
        this.perms = perms;
        return this;
      }

      public Builder symlinkTarget(String symlinkTarget) {
        this.symlinkTarget = symlinkTarget;
        return this;
      }

      public Builder overwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
      }

      public Builder defaultBlockSize(long defaultBlockSize) {
        this.defaultBlockSize = defaultBlockSize;
        return this;
      }

      public CreateEvent build() {
        return new CreateEvent(this);
      }
    }

    private CreateEvent(Builder b) {
      super(EventType.CREATE);
      this.iNodeType = b.iNodeType;
      this.path = b.path;
      this.ctime = b.ctime;
      this.replication = b.replication;
      this.ownerName = b.ownerName;
      this.groupName = b.groupName;
      this.perms = b.perms;
      this.symlinkTarget = b.symlinkTarget;
      this.overwrite = b.overwrite;
      this.defaultBlockSize = b.defaultBlockSize;
    }

    public INodeType getiNodeType() {
      return iNodeType;
    }

    public String getPath() {
      return path;
    }

    public long getCtime() {
      return ctime;
    }

    public int getReplication() {
      return replication;
    }

    public String getOwnerName() {
      return ownerName;
    }

    public String getGroupName() {
      return groupName;
    }

    public FsPermission getPerms() {
      return perms;
    }

    public String getSymlinkTarget() {
      return symlinkTarget;
    }

    public boolean getOverwrite() {
      return overwrite;
    }

    public long getDefaultBlockSize() {
      return defaultBlockSize;
    }
  }

  public static class MetadataUpdateEvent extends Event {

    public static enum MetadataType {
      TIMES, REPLICATION, OWNER, PERMS, ACLS, XATTRS;
    }

    private String path;
    private MetadataType metadataType;
    private long mtime;
    private long atime;
    private int replication;
    private String ownerName;
    private String groupName;
    private FsPermission perms;
    private List<AclEntry> acls;
    private List<XAttr> xAttrs;
    private boolean xAttrsRemoved;

    public static class Builder {
      private String path;
      private MetadataType metadataType;
      private long mtime;
      private long atime;
      private int replication;
      private String ownerName;
      private String groupName;
      private FsPermission perms;
      private List<AclEntry> acls;
      private List<XAttr> xAttrs;
      private boolean xAttrsRemoved;

      public Builder path(String path) {
        this.path = path;
        return this;
      }

      public Builder metadataType(MetadataType type) {
        this.metadataType = type;
        return this;
      }

      public Builder mtime(long mtime) {
        this.mtime = mtime;
        return this;
      }

      public Builder atime(long atime) {
        this.atime = atime;
        return this;
      }

      public Builder replication(int replication) {
        this.replication = replication;
        return this;
      }

      public Builder ownerName(String ownerName) {
        this.ownerName = ownerName;
        return this;
      }

      public Builder groupName(String groupName) {
        this.groupName = groupName;
        return this;
      }

      public Builder perms(FsPermission perms) {
        this.perms = perms;
        return this;
      }

      public Builder acls(List<AclEntry> acls) {
        this.acls = acls;
        return this;
      }

      public Builder xAttrs(List<XAttr> xAttrs) {
        this.xAttrs = xAttrs;
        return this;
      }

      public Builder xAttrsRemoved(boolean xAttrsRemoved) {
        this.xAttrsRemoved = xAttrsRemoved;
        return this;
      }

      public MetadataUpdateEvent build() {
        return new MetadataUpdateEvent(this);
      }
    }

    private MetadataUpdateEvent(Builder b) {
      super(EventType.METADATA);
      this.path = b.path;
      this.metadataType = b.metadataType;
      this.mtime = b.mtime;
      this.atime = b.atime;
      this.replication = b.replication;
      this.ownerName = b.ownerName;
      this.groupName = b.groupName;
      this.perms = b.perms;
      this.acls = b.acls;
      this.xAttrs = b.xAttrs;
      this.xAttrsRemoved = b.xAttrsRemoved;
    }

    public String getPath() {
      return path;
    }

    public MetadataType getMetadataType() {
      return metadataType;
    }

    public long getMtime() {
      return mtime;
    }

    public long getAtime() {
      return atime;
    }

    public int getReplication() {
      return replication;
    }

    public String getOwnerName() {
      return ownerName;
    }

    public String getGroupName() {
      return groupName;
    }

    public FsPermission getPerms() {
      return perms;
    }

    public List<AclEntry> getAcls() {
      return acls;
    }

    public List<XAttr> getxAttrs() {
      return xAttrs;
    }

    public boolean isxAttrsRemoved() {
      return xAttrsRemoved;
    }

  }

  public static class RenameEvent extends Event {
    private String srcPath;
    private String dstPath;
    private long timestamp;

    public static class Builder {
      private String srcPath;
      private String dstPath;
      private long timestamp;

      public Builder srcPath(String srcPath) {
        this.srcPath = srcPath;
        return this;
      }

      public Builder dstPath(String dstPath) {
        this.dstPath = dstPath;
        return this;
      }

      public Builder timestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
      }

      public RenameEvent build() {
        return new RenameEvent(this);
      }
    }

    private RenameEvent(Builder builder) {
      super(EventType.RENAME);
      this.srcPath = builder.srcPath;
      this.dstPath = builder.dstPath;
      this.timestamp = builder.timestamp;
    }

    public String getSrcPath() {
      return srcPath;
    }

    public String getDstPath() {
      return dstPath;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }

  public static class AppendEvent extends Event {
    private String path;
    private boolean newBlock;

    public static class Builder {
      private String path;
      private boolean newBlock;

      public Builder path(String path) {
        this.path = path;
        return this;
      }

      public Builder newBlock(boolean newBlock) {
        this.newBlock = newBlock;
        return this;
      }

      public AppendEvent build() {
        return new AppendEvent(this);
      }
    }

    private AppendEvent(Builder b) {
      super(EventType.APPEND);
      this.path = b.path;
      this.newBlock = b.newBlock;
    }

    public String getPath() {
      return path;
    }

    public boolean toNewBlock() {
      return newBlock;
    }
  }

  public static class UnlinkEvent extends Event {
    private String path;
    private long timestamp;

    public static class Builder {
      private String path;
      private long timestamp;

      public Builder path(String path) {
        this.path = path;
        return this;
      }

      public Builder timestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
      }

      public UnlinkEvent build() {
        return new UnlinkEvent(this);
      }
    }

    private UnlinkEvent(Builder builder) {
      super(EventType.UNLINK);
      this.path = builder.path;
      this.timestamp = builder.timestamp;
    }

    public String getPath() {
      return path;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/inotify/EventBatch.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/inotify/EventBatch.java

package org.apache.hadoop.hdfs.inotify;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Public
public class EventBatch {
  private final long txid;
  private final Event[] events;

  public EventBatch(long txid, Event[] events) {
    this.txid = txid;
    this.events = events;
  }

  public long getTxid() {
    return txid;
  }

  public Event[] getEvents() { return events; }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/inotify/EventBatchList.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/inotify/EventBatchList.java

package org.apache.hadoop.hdfs.inotify;

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.List;

@InterfaceAudience.Private
public class EventBatchList {
  private List<EventBatch> batches;
  private long firstTxid;
  private long lastTxid;
  private long syncTxid;

  public EventBatchList(List<EventBatch> batches, long firstTxid,
                         long lastTxid, long syncTxid) {
    this.batches = batches;
    this.firstTxid = firstTxid;
    this.lastTxid = lastTxid;
    this.syncTxid = syncTxid;
  }

  public List<EventBatch> getBatches() {
    return batches;
  }

  public long getFirstTxid() {
    return firstTxid;
  }

  public long getLastTxid() {
    return lastTxid;
  }

  public long getSyncTxid() {
    return syncTxid;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/AlreadyBeingCreatedException.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/AlreadyBeingCreatedException.java

package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AlreadyBeingCreatedException extends IOException {
  static final long serialVersionUID = 0x12308AD009L;
  public AlreadyBeingCreatedException(String msg) {
    super(msg);
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/BlockStoragePolicy.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/BlockStoragePolicy.java

package org.apache.hadoop.hdfs.protocol;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class BlockStoragePolicy {
  public static final Logger LOG = LoggerFactory.getLogger(BlockStoragePolicy
      .class);

  private final byte id;
  private final String name;

  private final StorageType[] storageTypes;
  private final StorageType[] creationFallbacks;
  private final StorageType[] replicationFallbacks;
  private boolean copyOnCreateFile;

  @VisibleForTesting
  public BlockStoragePolicy(byte id, String name, StorageType[] storageTypes,
      StorageType[] creationFallbacks, StorageType[] replicationFallbacks) {
    this(id, name, storageTypes, creationFallbacks, replicationFallbacks,
         false);
  }

  @VisibleForTesting
  public BlockStoragePolicy(byte id, String name, StorageType[] storageTypes,
      StorageType[] creationFallbacks, StorageType[] replicationFallbacks,
      boolean copyOnCreateFile) {
    this.id = id;
    this.name = name;
    this.storageTypes = storageTypes;
    this.creationFallbacks = creationFallbacks;
    this.replicationFallbacks = replicationFallbacks;
    this.copyOnCreateFile = copyOnCreateFile;
  }

  public List<StorageType> chooseStorageTypes(final short replication) {
    final List<StorageType> types = new LinkedList<StorageType>();
    int i = 0, j = 0;

    for (;i < replication && j < storageTypes.length; ++j) {
      if (!storageTypes[j].isTransient()) {
        types.add(storageTypes[j]);
        ++i;
      }
    }

    final StorageType last = storageTypes[storageTypes.length - 1];
    if (!last.isTransient()) {
      for (; i < replication; i++) {
        types.add(last);
      }
    }
    return types;
  }

  public List<StorageType> chooseStorageTypes(final short replication,
      final Iterable<StorageType> chosen) {
    return chooseStorageTypes(replication, chosen, null);
  }

  private List<StorageType> chooseStorageTypes(final short replication,
      final Iterable<StorageType> chosen, final List<StorageType> excess) {
    final List<StorageType> types = chooseStorageTypes(replication);
    diff(types, chosen, excess);
    return types;
  }

  public List<StorageType> chooseStorageTypes(final short replication,
      final Iterable<StorageType> chosen,
      final EnumSet<StorageType> unavailables,
      final boolean isNewBlock) {
    final List<StorageType> excess = new LinkedList<StorageType>();
    final List<StorageType> storageTypes = chooseStorageTypes(
        replication, chosen, excess);
    final int expectedSize = storageTypes.size() - excess.size();
    final List<StorageType> removed = new LinkedList<StorageType>();
    for(int i = storageTypes.size() - 1; i >= 0; i--) {
      final StorageType t = storageTypes.get(i);
      if (unavailables.contains(t)) {
        final StorageType fallback = isNewBlock?
            getCreationFallback(unavailables)
            : getReplicationFallback(unavailables);
        if (fallback == null) {
          removed.add(storageTypes.remove(i));
        } else {
          storageTypes.set(i, fallback);
        }
      }
    }
    diff(storageTypes, excess, null);
    if (storageTypes.size() < expectedSize) {
      LOG.warn("Failed to place enough replicas: expected size is " + expectedSize
          + " but only " + storageTypes.size() + " storage types can be selected "
          + "(replication=" + replication
          + ", selected=" + storageTypes
          + ", unavailable=" + unavailables
          + ", removed=" + removed
          + ", policy=" + this + ")");
    }
    return storageTypes;
  }

  private static void diff(List<StorageType> t, Iterable<StorageType> c,
      List<StorageType> e) {
    for(StorageType storagetype : c) {
      final int i = t.indexOf(storagetype);
      if (i >= 0) {
        t.remove(i);
      } else if (e != null) {
        e.add(storagetype);
      }
    }
  }

  public List<StorageType> chooseExcess(final short replication,
      final Iterable<StorageType> chosen) {
    final List<StorageType> types = chooseStorageTypes(replication);
    final List<StorageType> excess = new LinkedList<StorageType>();
    diff(types, chosen, excess);
    return excess;
  }

  public StorageType getCreationFallback(EnumSet<StorageType> unavailables) {
    return getFallback(unavailables, creationFallbacks);
  }

  public StorageType getReplicationFallback(EnumSet<StorageType> unavailables) {
    return getFallback(unavailables, replicationFallbacks);
  }

  @Override
  public int hashCode() {
    return Byte.valueOf(id).hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj == null || !(obj instanceof BlockStoragePolicy)) {
      return false;
    }
    final BlockStoragePolicy that = (BlockStoragePolicy)obj;
    return this.id == that.id;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + name + ":" + id
        + ", storageTypes=" + Arrays.asList(storageTypes)
        + ", creationFallbacks=" + Arrays.asList(creationFallbacks)
        + ", replicationFallbacks=" + Arrays.asList(replicationFallbacks) + "}";
  }

  public byte getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public StorageType[] getStorageTypes() {
    return this.storageTypes;
  }

  public StorageType[] getCreationFallbacks() {
    return this.creationFallbacks;
  }

  public StorageType[] getReplicationFallbacks() {
    return this.replicationFallbacks;
  }

  private static StorageType getFallback(EnumSet<StorageType> unavailables,
      StorageType[] fallbacks) {
    for(StorageType fb : fallbacks) {
      if (!unavailables.contains(fb)) {
        return fb;
      }
    }
    return null;
  }

  public boolean isCopyOnCreateFile() {
    return copyOnCreateFile;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/CorruptFileBlocks.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/CorruptFileBlocks.java
package org.apache.hadoop.hdfs.protocol;

import java.util.Arrays;

public class CorruptFileBlocks {
  private static final int PRIME = 16777619;

  private final String[] files;
  private final String cookie;

  public CorruptFileBlocks() {
    this(new String[0], "");
  }

  public CorruptFileBlocks(String[] files, String cookie) {
    this.files = files;
    this.cookie = cookie;
  }

  public String[] getFiles() {
    return files;
  }

  public String getCookie() {
    return cookie;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof CorruptFileBlocks)) {
      return false;
    }
    CorruptFileBlocks other = (CorruptFileBlocks) obj;
    return cookie.equals(other.cookie) &&
      Arrays.equals(files, other.files);
  }


  @Override
  public int hashCode() {
    int result = cookie.hashCode();

    for (String file : files) {
      result = PRIME * result + file.hashCode();
    }

    return result;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/DSQuotaExceededException.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/DSQuotaExceededException.java

package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import static org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix.long2String;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DSQuotaExceededException extends QuotaExceededException {
  protected static final long serialVersionUID = 1L;

  public DSQuotaExceededException() {}

  public DSQuotaExceededException(String msg) {
    super(msg);
  }

  public DSQuotaExceededException(long quota, long count) {
    super(quota, count);
  }

  @Override
  public String getMessage() {
    String msg = super.getMessage();
    if (msg == null) {
      return "The DiskSpace quota" + (pathName==null?"": " of " + pathName)
          + " is exceeded: quota = " + quota + " B = " + long2String(quota, "B", 2)
          + " but diskspace consumed = " + count + " B = " + long2String(count, "B", 2);
    } else {
      return msg;
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/DatanodeLocalInfo.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/DatanodeLocalInfo.java
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeLocalInfo {
  private final String softwareVersion;
  private final String configVersion;
  private final long uptime; // datanode uptime in seconds.

  public DatanodeLocalInfo(String softwareVersion,
      String configVersion, long uptime) {
    this.softwareVersion = softwareVersion;
    this.configVersion = configVersion;
    this.uptime = uptime;
  }

  public String getSoftwareVersion() {
    return this.softwareVersion;
  }

  public String getConfigVersion() {
    return this.configVersion;
  }

  public long getUptime() {
    return this.uptime;
  }

  public String getDatanodeLocalReport() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("Uptime: " + getUptime());
    buffer.append(", Software version: " + getSoftwareVersion());
    buffer.append(", Config version: " + getConfigVersion());
    return buffer.toString();
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/DirectoryListing.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/DirectoryListing.java
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DirectoryListing {
  private HdfsFileStatus[] partialListing;
  private int remainingEntries;

  public DirectoryListing(HdfsFileStatus[] partialListing,
      int remainingEntries) {
    if (partialListing == null) {
      throw new IllegalArgumentException("partial listing should not be null");
    }
    if (partialListing.length == 0 && remainingEntries != 0) {
      throw new IllegalArgumentException("Partial listing is empty but " +
          "the number of remaining entries is not zero");
    }
    this.partialListing = partialListing;
    this.remainingEntries = remainingEntries;
  }

  public HdfsFileStatus[] getPartialListing() {
    return partialListing;
  }

  public int getRemainingEntries() {
    return remainingEntries;
  }

  public boolean hasMore() {
    return remainingEntries != 0;
  }

  public byte[] getLastName() {
    if (partialListing.length == 0) {
      return null;
    }
    return partialListing[partialListing.length-1].getLocalNameInBytes();
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/EncryptionZone.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/EncryptionZone.java
package org.apache.hadoop.hdfs.protocol;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class EncryptionZone {

  private final long id;
  private final String path;
  private final CipherSuite suite;
  private final CryptoProtocolVersion version;
  private final String keyName;

  public EncryptionZone(long id, String path, CipherSuite suite,
      CryptoProtocolVersion version, String keyName) {
    this.id = id;
    this.path = path;
    this.suite = suite;
    this.version = version;
    this.keyName = keyName;
  }

  public long getId() {
    return id;
  }

  public String getPath() {
    return path;
  }

  public CipherSuite getSuite() {
    return suite;
  }

  public CryptoProtocolVersion getVersion() { return version; }

  public String getKeyName() {
    return keyName;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(13, 31)
        .append(id)
        .append(path)
        .append(suite)
        .append(version)
        .append(keyName).
      toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }

    EncryptionZone rhs = (EncryptionZone) obj;
    return new EqualsBuilder().
      append(id, rhs.id).
      append(path, rhs.path).
      append(suite, rhs.suite).
      append(version, rhs.version).
      append(keyName, rhs.keyName).
      isEquals();
  }

  @Override
  public String toString() {
    return "EncryptionZone [id=" + id +
        ", path=" + path +
        ", suite=" + suite +
        ", version=" + version +
        ", keyName=" + keyName + "]";
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/LastBlockWithStatus.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/LastBlockWithStatus.java
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LastBlockWithStatus {

  private final LocatedBlock lastBlock;

  private final HdfsFileStatus fileStatus;

  public LastBlockWithStatus(LocatedBlock lastBlock, HdfsFileStatus fileStatus) {
    this.lastBlock = lastBlock;
    this.fileStatus = fileStatus;
  }

  public LocatedBlock getLastBlock() {
    return lastBlock;
  }

  public HdfsFileStatus getFileStatus() {
    return fileStatus;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/NSQuotaExceededException.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/NSQuotaExceededException.java

package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class NSQuotaExceededException extends QuotaExceededException {
  protected static final long serialVersionUID = 1L;

  private String prefix;

  public NSQuotaExceededException() {}

  public NSQuotaExceededException(String msg) {
    super(msg);
  }

  public NSQuotaExceededException(long quota, long count) {
    super(quota, count);
  }

  @Override
  public String getMessage() {
    String msg = super.getMessage();
    if (msg == null) {
      msg = "The NameSpace quota (directories and files)" +
      (pathName==null?"":(" of directory " + pathName)) +
          " is exceeded: quota=" + quota + " file count=" + count;

      if (prefix != null) {
        msg = prefix + ": " + msg;
      }
    }
    return msg;
  }

  public void setMessagePrefix(final String prefix) {
    this.prefix = prefix;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/QuotaExceededException.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/QuotaExceededException.java

package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class QuotaExceededException extends IOException {
  protected static final long serialVersionUID = 1L;
  protected String pathName=null;
  protected long quota; // quota
  protected long count; // actual value

  protected QuotaExceededException() {}

  protected QuotaExceededException(String msg) {
    super(msg);
  }

  protected QuotaExceededException(long quota, long count) {
    this.quota = quota;
    this.count = count;
  }

  public void setPathName(String path) {
    this.pathName = path;
  }

  @Override
  public String getMessage() {
    return super.getMessage();
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/RollingUpgradeInfo.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/RollingUpgradeInfo.java
package org.apache.hadoop.hdfs.protocol;

import java.util.Date;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RollingUpgradeInfo extends RollingUpgradeStatus {
  private final long startTime;
  private long finalizeTime;
  private boolean createdRollbackImages;

  public RollingUpgradeInfo(String blockPoolId, boolean createdRollbackImages,
      long startTime, long finalizeTime) {
    super(blockPoolId, finalizeTime != 0);
    this.createdRollbackImages = createdRollbackImages;
    this.startTime = startTime;
    this.finalizeTime = finalizeTime;
  }

  public boolean createdRollbackImages() {
    return createdRollbackImages;
  }

  public void setCreatedRollbackImages(boolean created) {
    this.createdRollbackImages = created;
  }

  public boolean isStarted() {
    return startTime != 0;
  }

  public long getStartTime() {
    return startTime;
  }

  @Override
  public boolean isFinalized() {
    return finalizeTime != 0;
  }

  public void finalize(long finalizeTime) {
    if (finalizeTime != 0) {
      this.finalizeTime = finalizeTime;
      createdRollbackImages = false;
    }
  }

  public long getFinalizeTime() {
    return finalizeTime;
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ (int)startTime ^ (int)finalizeTime;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj == null || !(obj instanceof RollingUpgradeInfo)) {
      return false;
    }
    final RollingUpgradeInfo that = (RollingUpgradeInfo)obj;
    return super.equals(that)
        && this.startTime == that.startTime
        && this.finalizeTime == that.finalizeTime;
  }

  @Override
  public String toString() {
    return super.toString()
      +  "\n     Start Time: " + (startTime == 0? "<NOT STARTED>": timestamp2String(startTime))
      +  "\n  Finalize Time: " + (finalizeTime == 0? "<NOT FINALIZED>": timestamp2String(finalizeTime));
  }

  private static String timestamp2String(long timestamp) {
    return new Date(timestamp) + " (=" + timestamp + ")";
  }

  public static class Bean {
    private final String blockPoolId;
    private final long startTime;
    private final long finalizeTime;
    private final boolean createdRollbackImages;

    public Bean(RollingUpgradeInfo f) {
      this.blockPoolId = f.getBlockPoolId();
      this.startTime = f.startTime;
      this.finalizeTime = f.finalizeTime;
      this.createdRollbackImages = f.createdRollbackImages();
    }

    public String getBlockPoolId() {
      return blockPoolId;
    }

    public long getStartTime() {
      return startTime;
    }

    public long getFinalizeTime() {
      return finalizeTime;
    }

    public boolean isCreatedRollbackImages() {
      return createdRollbackImages;
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/RollingUpgradeStatus.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/RollingUpgradeStatus.java
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RollingUpgradeStatus {
  private final String blockPoolId;
  private final boolean finalized;

  public RollingUpgradeStatus(String blockPoolId, boolean finalized) {
    this.blockPoolId = blockPoolId;
    this.finalized = finalized;
  }

  public String getBlockPoolId() {
    return blockPoolId;
  }

  public boolean isFinalized() {
    return finalized;
  }

  @Override
  public int hashCode() {
    return blockPoolId.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj == null || !(obj instanceof RollingUpgradeStatus)) {
      return false;
    }
    final RollingUpgradeStatus that = (RollingUpgradeStatus) obj;
    return this.blockPoolId.equals(that.blockPoolId)
        && this.isFinalized() == that.isFinalized();
  }

  @Override
  public String toString() {
    return "  Block Pool ID: " + blockPoolId;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/SnapshotAccessControlException.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/SnapshotAccessControlException.java
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.security.AccessControlException;

public class SnapshotAccessControlException extends AccessControlException {
  private static final long serialVersionUID = 1L;

  public SnapshotAccessControlException(final String message) {
    super(message);
  }

  public SnapshotAccessControlException(final Throwable cause) {
    super(cause);
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/security/token/block/DataEncryptionKey.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/security/token/block/DataEncryptionKey.java
package org.apache.hadoop.hdfs.security.token.block;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class DataEncryptionKey {
  public final int keyId;
  public final String blockPoolId;
  public final byte[] nonce;
  public final byte[] encryptionKey;
  public final long expiryDate;
  public final String encryptionAlgorithm;

  public DataEncryptionKey(int keyId, String blockPoolId, byte[] nonce,
      byte[] encryptionKey, long expiryDate, String encryptionAlgorithm) {
    this.keyId = keyId;
    this.blockPoolId = blockPoolId;
    this.nonce = nonce;
    this.encryptionKey = encryptionKey;
    this.expiryDate = expiryDate;
    this.encryptionAlgorithm = encryptionAlgorithm;
  }

  @Override
  public String toString() {
    return keyId + "/" + blockPoolId + "/" + nonce.length + "/" +
        encryptionKey.length;
  }
}

