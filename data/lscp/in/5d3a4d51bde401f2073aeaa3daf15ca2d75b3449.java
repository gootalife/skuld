hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/DFSUtilClient.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/DFSUtilClient.java
package org.apache.hadoop.hdfs;

import org.apache.hadoop.util.StringUtils;

import java.io.UnsupportedEncodingException;

public class DFSUtilClient {
  public static String bytes2String(byte[] bytes) {
    return bytes2String(bytes, 0, bytes.length);
  }

  public static float getPercentUsed(long used, long capacity) {
    return capacity <= 0 ? 100 : (used * 100.0f)/capacity;
  }

  public static float getPercentRemaining(long remaining, long capacity) {
    return capacity <= 0 ? 0 : (remaining * 100.0f)/capacity;
  }

  public static String percent2String(double percentage) {
    return StringUtils.format("%.2f%%", percentage);
  }

  private static String bytes2String(byte[] bytes, int offset, int length) {
    try {
      return new String(bytes, offset, length, "UTF8");
    } catch(UnsupportedEncodingException e) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }

}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/Block.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/Block.java
package org.apache.hadoop.hdfs.protocol;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.*;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Block implements Writable, Comparable<Block> {
  public static final String BLOCK_FILE_PREFIX = "blk_";
  public static final String METADATA_EXTENSION = ".meta";
  static {                                      // register a ctor
    WritableFactories.setFactory
      (Block.class,
       new WritableFactory() {
         @Override
         public Writable newInstance() { return new Block(); }
       });
  }

  public static final Pattern blockFilePattern = Pattern
      .compile(BLOCK_FILE_PREFIX + "(-??\\d++)$");
  public static final Pattern metaFilePattern = Pattern
      .compile(BLOCK_FILE_PREFIX + "(-??\\d++)_(\\d++)\\" + METADATA_EXTENSION
          + "$");
  public static final Pattern metaOrBlockFilePattern = Pattern
      .compile(BLOCK_FILE_PREFIX + "(-??\\d++)(_(\\d++)\\" + METADATA_EXTENSION
          + ")?$");

  public static boolean isBlockFilename(File f) {
    String name = f.getName();
    return blockFilePattern.matcher(name).matches();
  }

  public static long filename2id(String name) {
    Matcher m = blockFilePattern.matcher(name);
    return m.matches() ? Long.parseLong(m.group(1)) : 0;
  }

  public static boolean isMetaFilename(String name) {
    return metaFilePattern.matcher(name).matches();
  }

  public static File metaToBlockFile(File metaFile) {
    return new File(metaFile.getParent(), metaFile.getName().substring(
        0, metaFile.getName().lastIndexOf('_')));
  }

  public static long getGenerationStamp(String metaFile) {
    Matcher m = metaFilePattern.matcher(metaFile);
    return m.matches() ? Long.parseLong(m.group(2))
        : HdfsConstantsClient.GRANDFATHER_GENERATION_STAMP;
  }

  public static long getBlockId(String metaOrBlockFile) {
    Matcher m = metaOrBlockFilePattern.matcher(metaOrBlockFile);
    return m.matches() ? Long.parseLong(m.group(1)) : 0;
  }

  private long blockId;
  private long numBytes;
  private long generationStamp;

  public Block() {this(0, 0, 0);}

  public Block(final long blkid, final long len, final long generationStamp) {
    set(blkid, len, generationStamp);
  }

  public Block(final long blkid) {
    this(blkid, 0, HdfsConstantsClient.GRANDFATHER_GENERATION_STAMP);
  }

  public Block(Block blk) {
    this(blk.blockId, blk.numBytes, blk.generationStamp);
  }

  public Block(File f, long len, long genstamp) {
    this(filename2id(f.getName()), len, genstamp);
  }

  public void set(long blkid, long len, long genStamp) {
    this.blockId = blkid;
    this.numBytes = len;
    this.generationStamp = genStamp;
  }
  public long getBlockId() {
    return blockId;
  }

  public void setBlockId(long bid) {
    blockId = bid;
  }

  public String getBlockName() {
    return BLOCK_FILE_PREFIX + String.valueOf(blockId);
  }

  public long getNumBytes() {
    return numBytes;
  }
  public void setNumBytes(long len) {
    this.numBytes = len;
  }

  public long getGenerationStamp() {
    return generationStamp;
  }

  public void setGenerationStamp(long stamp) {
    generationStamp = stamp;
  }

  @Override
  public String toString() {
    return getBlockName() + "_" + getGenerationStamp();
  }

  public void appendStringTo(StringBuilder sb) {
    sb.append(BLOCK_FILE_PREFIX)
      .append(blockId)
      .append("_")
      .append(getGenerationStamp());
  }


  @Override // Writable
  public void write(DataOutput out) throws IOException {
    writeHelper(out);
  }

  @Override // Writable
  public void readFields(DataInput in) throws IOException {
    readHelper(in);
  }

  final void writeHelper(DataOutput out) throws IOException {
    out.writeLong(blockId);
    out.writeLong(numBytes);
    out.writeLong(generationStamp);
  }

  final void readHelper(DataInput in) throws IOException {
    this.blockId = in.readLong();
    this.numBytes = in.readLong();
    this.generationStamp = in.readLong();
    if (numBytes < 0) {
      throw new IOException("Unexpected block size: " + numBytes);
    }
  }

  public void writeId(DataOutput out) throws IOException {
    out.writeLong(blockId);
    out.writeLong(generationStamp);
  }

  public void readId(DataInput in) throws IOException {
    this.blockId = in.readLong();
    this.generationStamp = in.readLong();
  }

  @Override // Comparable
  public int compareTo(Block b) {
    return blockId < b.blockId ? -1 :
           blockId > b.blockId ? 1 : 0;
  }

  @Override // Object
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Block)) {
      return false;
    }
    return compareTo((Block)o) == 0;
  }

  public static boolean matchingIdAndGenStamp(Block a, Block b) {
    if (a == b) return true; // same block, or both null
    if (a == null || b == null) return false; // only one null
    return a.blockId == b.blockId &&
           a.generationStamp == b.generationStamp;
  }

  @Override // Object
  public int hashCode() {
    return (int)(blockId^(blockId>>>32));
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/DatanodeID.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/DatanodeID.java

package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeID implements Comparable<DatanodeID> {
  public static final DatanodeID[] EMPTY_ARRAY = {};

  private String ipAddr;     // IP address
  private String hostName;   // hostname claimed by datanode
  private String peerHostName; // hostname from the actual connection
  private int xferPort;      // data streaming port
  private int infoPort;      // info server port
  private int infoSecurePort; // info server port
  private int ipcPort;       // IPC server port
  private String xferAddr;

  private final String datanodeUuid;

  public DatanodeID(DatanodeID from) {
    this(from.getDatanodeUuid(), from);
  }

  @VisibleForTesting
  public DatanodeID(String datanodeUuid, DatanodeID from) {
    this(from.getIpAddr(),
        from.getHostName(),
        datanodeUuid,
        from.getXferPort(),
        from.getInfoPort(),
        from.getInfoSecurePort(),
        from.getIpcPort());
    this.peerHostName = from.getPeerHostName();
  }

  public DatanodeID(String ipAddr, String hostName, String datanodeUuid,
      int xferPort, int infoPort, int infoSecurePort, int ipcPort) {
    setIpAndXferPort(ipAddr, xferPort);
    this.hostName = hostName;
    this.datanodeUuid = checkDatanodeUuid(datanodeUuid);
    this.infoPort = infoPort;
    this.infoSecurePort = infoSecurePort;
    this.ipcPort = ipcPort;
  }

  public void setIpAddr(String ipAddr) {
    setIpAndXferPort(ipAddr, xferPort);
  }

  private void setIpAndXferPort(String ipAddr, int xferPort) {
    this.ipAddr = ipAddr;
    this.xferPort = xferPort;
    this.xferAddr = ipAddr + ":" + xferPort;
  }

  public void setPeerHostName(String peerHostName) {
    this.peerHostName = peerHostName;
  }

  public String getDatanodeUuid() {
    return datanodeUuid;
  }

  private String checkDatanodeUuid(String uuid) {
    if (uuid == null || uuid.isEmpty()) {
      return null;
    } else {
      return uuid;
    }
  }

  public String getIpAddr() {
    return ipAddr;
  }

  public String getHostName() {
    return hostName;
  }

  public String getPeerHostName() {
    return peerHostName;
  }

  public String getXferAddr() {
    return xferAddr;
  }

  private String getIpcAddr() {
    return ipAddr + ":" + ipcPort;
  }

  public String getInfoAddr() {
    return ipAddr + ":" + infoPort;
  }

  public String getInfoSecureAddr() {
    return ipAddr + ":" + infoSecurePort;
  }

  public String getXferAddrWithHostname() {
    return hostName + ":" + xferPort;
  }

  private String getIpcAddrWithHostname() {
    return hostName + ":" + ipcPort;
  }

  public String getXferAddr(boolean useHostname) {
    return useHostname ? getXferAddrWithHostname() : getXferAddr();
  }

  public String getIpcAddr(boolean useHostname) {
    return useHostname ? getIpcAddrWithHostname() : getIpcAddr();
  }

  public int getXferPort() {
    return xferPort;
  }

  public int getInfoPort() {
    return infoPort;
  }

  public int getInfoSecurePort() {
    return infoSecurePort;
  }

  public int getIpcPort() {
    return ipcPort;
  }

  @Override
  public boolean equals(Object to) {
    if (this == to) {
      return true;
    }
    if (!(to instanceof DatanodeID)) {
      return false;
    }
    return (getXferAddr().equals(((DatanodeID)to).getXferAddr()) &&
        datanodeUuid.equals(((DatanodeID)to).getDatanodeUuid()));
  }

  @Override
  public int hashCode() {
    return datanodeUuid.hashCode();
  }

  @Override
  public String toString() {
    return getXferAddr();
  }

  public void updateRegInfo(DatanodeID nodeReg) {
    setIpAndXferPort(nodeReg.getIpAddr(), nodeReg.getXferPort());
    hostName = nodeReg.getHostName();
    peerHostName = nodeReg.getPeerHostName();
    infoPort = nodeReg.getInfoPort();
    infoSecurePort = nodeReg.getInfoSecurePort();
    ipcPort = nodeReg.getIpcPort();
  }

  @Override
  public int compareTo(DatanodeID that) {
    return getXferAddr().compareTo(that.getXferAddr());
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/DatanodeInfo.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/DatanodeInfo.java
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSUtilClient.percent2String;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeInfo extends DatanodeID implements Node {
  private long capacity;
  private long dfsUsed;
  private long remaining;
  private long blockPoolUsed;
  private long cacheCapacity;
  private long cacheUsed;
  private long lastUpdate;
  private long lastUpdateMonotonic;
  private int xceiverCount;
  private String location = NetworkTopology.DEFAULT_RACK;
  private String softwareVersion;
  private List<String> dependentHostNames = new LinkedList<String>();


  public enum AdminStates {
    NORMAL("In Service"),
    DECOMMISSION_INPROGRESS("Decommission In Progress"),
    DECOMMISSIONED("Decommissioned");

    final String value;

    AdminStates(final String v) {
      this.value = v;
    }

    @Override
    public String toString() {
      return value;
    }

    public static AdminStates fromValue(final String value) {
      for (AdminStates as : AdminStates.values()) {
        if (as.value.equals(value)) return as;
      }
      return NORMAL;
    }
  }

  protected AdminStates adminState;

  public DatanodeInfo(DatanodeInfo from) {
    super(from);
    this.capacity = from.getCapacity();
    this.dfsUsed = from.getDfsUsed();
    this.remaining = from.getRemaining();
    this.blockPoolUsed = from.getBlockPoolUsed();
    this.cacheCapacity = from.getCacheCapacity();
    this.cacheUsed = from.getCacheUsed();
    this.lastUpdate = from.getLastUpdate();
    this.lastUpdateMonotonic = from.getLastUpdateMonotonic();
    this.xceiverCount = from.getXceiverCount();
    this.location = from.getNetworkLocation();
    this.adminState = from.getAdminState();
  }

  public DatanodeInfo(DatanodeID nodeID) {
    super(nodeID);
    this.capacity = 0L;
    this.dfsUsed = 0L;
    this.remaining = 0L;
    this.blockPoolUsed = 0L;
    this.cacheCapacity = 0L;
    this.cacheUsed = 0L;
    this.lastUpdate = 0L;
    this.lastUpdateMonotonic = 0L;
    this.xceiverCount = 0;
    this.adminState = null;
  }

  public DatanodeInfo(DatanodeID nodeID, String location) {
    this(nodeID);
    this.location = location;
  }

  public DatanodeInfo(DatanodeID nodeID, String location,
      final long capacity, final long dfsUsed, final long remaining,
      final long blockPoolUsed, final long cacheCapacity, final long cacheUsed,
      final long lastUpdate, final long lastUpdateMonotonic,
      final int xceiverCount, final AdminStates adminState) {
    this(nodeID.getIpAddr(), nodeID.getHostName(), nodeID.getDatanodeUuid(),
        nodeID.getXferPort(), nodeID.getInfoPort(), nodeID.getInfoSecurePort(),
        nodeID.getIpcPort(), capacity, dfsUsed, remaining, blockPoolUsed,
        cacheCapacity, cacheUsed, lastUpdate, lastUpdateMonotonic,
        xceiverCount, location, adminState);
  }

  public DatanodeInfo(final String ipAddr, final String hostName,
      final String datanodeUuid, final int xferPort, final int infoPort,
      final int infoSecurePort, final int ipcPort,
      final long capacity, final long dfsUsed, final long remaining,
      final long blockPoolUsed, final long cacheCapacity, final long cacheUsed,
      final long lastUpdate, final long lastUpdateMonotonic,
      final int xceiverCount, final String networkLocation,
      final AdminStates adminState) {
    super(ipAddr, hostName, datanodeUuid, xferPort, infoPort,
            infoSecurePort, ipcPort);
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.blockPoolUsed = blockPoolUsed;
    this.cacheCapacity = cacheCapacity;
    this.cacheUsed = cacheUsed;
    this.lastUpdate = lastUpdate;
    this.lastUpdateMonotonic = lastUpdateMonotonic;
    this.xceiverCount = xceiverCount;
    this.location = networkLocation;
    this.adminState = adminState;
  }

  @Override
  public String getName() {
    return getXferAddr();
  }

  public long getCapacity() { return capacity; }

  public long getDfsUsed() { return dfsUsed; }

  public long getBlockPoolUsed() { return blockPoolUsed; }

  public long getNonDfsUsed() {
    long nonDFSUsed = capacity - dfsUsed - remaining;
    return nonDFSUsed < 0 ? 0 : nonDFSUsed;
  }

  public float getDfsUsedPercent() {
    return DFSUtilClient.getPercentUsed(dfsUsed, capacity);
  }

  public long getRemaining() { return remaining; }

  public float getBlockPoolUsedPercent() {
    return DFSUtilClient.getPercentUsed(blockPoolUsed, capacity);
  }

  public float getRemainingPercent() {
    return DFSUtilClient.getPercentRemaining(remaining, capacity);
  }

  public long getCacheCapacity() {
    return cacheCapacity;
  }

  public long getCacheUsed() {
    return cacheUsed;
  }

  public float getCacheUsedPercent() {
    return DFSUtilClient.getPercentUsed(cacheUsed, cacheCapacity);
  }

  public long getCacheRemaining() {
    return cacheCapacity - cacheUsed;
  }

  public float getCacheRemainingPercent() {
    return DFSUtilClient.getPercentRemaining(getCacheRemaining(), cacheCapacity);
  }

  public long getLastUpdate() { return lastUpdate; }

  public long getLastUpdateMonotonic() { return lastUpdateMonotonic;}

  public void setLastUpdateMonotonic(long lastUpdateMonotonic) {
    this.lastUpdateMonotonic = lastUpdateMonotonic;
  }

  public int getXceiverCount() { return xceiverCount; }

  public void setCapacity(long capacity) {
    this.capacity = capacity;
  }

  public void setDfsUsed(long dfsUsed) {
    this.dfsUsed = dfsUsed;
  }

  public void setRemaining(long remaining) {
    this.remaining = remaining;
  }

  public void setBlockPoolUsed(long bpUsed) {
    this.blockPoolUsed = bpUsed;
  }

  public void setCacheCapacity(long cacheCapacity) {
    this.cacheCapacity = cacheCapacity;
  }

  public void setCacheUsed(long cacheUsed) {
    this.cacheUsed = cacheUsed;
  }

  public void setLastUpdate(long lastUpdate) {
    this.lastUpdate = lastUpdate;
  }

  public void setXceiverCount(int xceiverCount) {
    this.xceiverCount = xceiverCount;
  }

  public synchronized String getNetworkLocation() {return location;}

  public synchronized void setNetworkLocation(String location) {
    this.location = NodeBase.normalize(location);
  }

  public void addDependentHostName(String hostname) {
    dependentHostNames.add(hostname);
  }

  public List<String> getDependentHostNames() {
    return dependentHostNames;
  }

  public void setDependentHostNames(List<String> dependencyList) {
    dependentHostNames = dependencyList;
  }

  public String getDatanodeReport() {
    StringBuilder buffer = new StringBuilder();
    long c = getCapacity();
    long r = getRemaining();
    long u = getDfsUsed();
    long nonDFSUsed = getNonDfsUsed();
    float usedPercent = getDfsUsedPercent();
    float remainingPercent = getRemainingPercent();
    long cc = getCacheCapacity();
    long cr = getCacheRemaining();
    long cu = getCacheUsed();
    float cacheUsedPercent = getCacheUsedPercent();
    float cacheRemainingPercent = getCacheRemainingPercent();
    String lookupName = NetUtils.getHostNameOfIP(getName());

    buffer.append("Name: "+ getName());
    if (lookupName != null) {
      buffer.append(" (" + lookupName + ")");
    }
    buffer.append("\n");
    buffer.append("Hostname: " + getHostName() + "\n");

    if (!NetworkTopology.DEFAULT_RACK.equals(location)) {
      buffer.append("Rack: "+location+"\n");
    }
    buffer.append("Decommission Status : ");
    if (isDecommissioned()) {
      buffer.append("Decommissioned\n");
    } else if (isDecommissionInProgress()) {
      buffer.append("Decommission in progress\n");
    } else {
      buffer.append("Normal\n");
    }
    buffer.append("Configured Capacity: "+c+" ("+StringUtils.byteDesc(c)+")"+"\n");
    buffer.append("DFS Used: "+u+" ("+StringUtils.byteDesc(u)+")"+"\n");
    buffer.append("Non DFS Used: "+nonDFSUsed+" ("+StringUtils.byteDesc(nonDFSUsed)+")"+"\n");
    buffer.append("DFS Remaining: " +r+ " ("+StringUtils.byteDesc(r)+")"+"\n");
    buffer.append("DFS Used%: "+percent2String(usedPercent) + "\n");
    buffer.append("DFS Remaining%: "+percent2String(remainingPercent) + "\n");
    buffer.append("Configured Cache Capacity: "+cc+" ("+StringUtils.byteDesc(cc)+")"+"\n");
    buffer.append("Cache Used: "+cu+" ("+StringUtils.byteDesc(cu)+")"+"\n");
    buffer.append("Cache Remaining: " +cr+ " ("+StringUtils.byteDesc(cr)+")"+"\n");
    buffer.append("Cache Used%: "+percent2String(cacheUsedPercent) + "\n");
    buffer.append("Cache Remaining%: "+percent2String(cacheRemainingPercent) + "\n");
    buffer.append("Xceivers: "+getXceiverCount()+"\n");
    buffer.append("Last contact: "+new Date(lastUpdate)+"\n");
    return buffer.toString();
  }

  public String dumpDatanode() {
    StringBuilder buffer = new StringBuilder();
    long c = getCapacity();
    long r = getRemaining();
    long u = getDfsUsed();
    long cc = getCacheCapacity();
    long cr = getCacheRemaining();
    long cu = getCacheUsed();
    buffer.append(getName());
    if (!NetworkTopology.DEFAULT_RACK.equals(location)) {
      buffer.append(" "+location);
    }
    if (isDecommissioned()) {
      buffer.append(" DD");
    } else if (isDecommissionInProgress()) {
      buffer.append(" DP");
    } else {
      buffer.append(" IN");
    }
    buffer.append(" " + c + "(" + StringUtils.byteDesc(c)+")");
    buffer.append(" " + u + "(" + StringUtils.byteDesc(u)+")");
    buffer.append(" " + percent2String(u/(double)c));
    buffer.append(" " + r + "(" + StringUtils.byteDesc(r)+")");
    buffer.append(" " + cc + "(" + StringUtils.byteDesc(cc)+")");
    buffer.append(" " + cu + "(" + StringUtils.byteDesc(cu)+")");
    buffer.append(" " + percent2String(cu/(double)cc));
    buffer.append(" " + cr + "(" + StringUtils.byteDesc(cr)+")");
    buffer.append(" " + new Date(lastUpdate));
    return buffer.toString();
  }

  public void startDecommission() {
    adminState = AdminStates.DECOMMISSION_INPROGRESS;
  }

  public void stopDecommission() {
    adminState = null;
  }

  public boolean isDecommissionInProgress() {
    return adminState == AdminStates.DECOMMISSION_INPROGRESS;
  }

  public boolean isDecommissioned() {
    return adminState == AdminStates.DECOMMISSIONED;
  }

  public void setDecommissioned() {
    adminState = AdminStates.DECOMMISSIONED;
  }

  public AdminStates getAdminState() {
    if (adminState == null) {
      return AdminStates.NORMAL;
    }
    return adminState;
  }

  public boolean isStale(long staleInterval) {
    return (Time.monotonicNow() - lastUpdateMonotonic) >= staleInterval;
  }

  protected void setAdminState(AdminStates newState) {
    if (newState == AdminStates.NORMAL) {
      adminState = null;
    }
    else {
      adminState = newState;
    }
  }

  private transient int level; //which level of the tree the node resides
  private transient Node parent; //its parent

  @Override
  public Node getParent() { return parent; }
  @Override
  public void setParent(Node parent) {this.parent = parent;}

  @Override
  public int getLevel() { return level; }
  @Override
  public void setLevel(int level) {this.level = level;}

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return (this == obj) || super.equals(obj);
  }

  public String getSoftwareVersion() {
    return softwareVersion;
  }

  public void setSoftwareVersion(String softwareVersion) {
    this.softwareVersion = softwareVersion;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/DatanodeInfoWithStorage.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/DatanodeInfoWithStorage.java
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeInfoWithStorage extends DatanodeInfo {
  private final String storageID;
  private final StorageType storageType;

  public DatanodeInfoWithStorage(DatanodeInfo from, String storageID,
                                 StorageType storageType) {
    super(from);
    this.storageID = storageID;
    this.storageType = storageType;
    setSoftwareVersion(from.getSoftwareVersion());
    setDependentHostNames(from.getDependentHostNames());
    setLevel(from.getLevel());
    setParent(from.getParent());
  }

  public String getStorageID() {
    return storageID;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    return "DatanodeInfoWithStorage[" + super.toString() + "," + storageID +
        "," + storageType + "]";
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/ExtendedBlock.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/ExtendedBlock.java
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ExtendedBlock {
  private String poolId;
  private Block block;

  public ExtendedBlock() {
    this(null, 0, 0, 0);
  }

  public ExtendedBlock(final ExtendedBlock b) {
    this(b.poolId, new Block(b.block));
  }

  public ExtendedBlock(final String poolId, final long blockId) {
    this(poolId, blockId, 0, 0);
  }

  public ExtendedBlock(String poolId, Block b) {
    this.poolId = poolId;
    this.block = b;
  }

  public ExtendedBlock(final String poolId, final long blkid, final long len,
      final long genstamp) {
    this.poolId = poolId;
    block = new Block(blkid, len, genstamp);
  }

  public String getBlockPoolId() {
    return poolId;
  }

  public String getBlockName() {
    return block.getBlockName();
  }

  public long getNumBytes() {
    return block.getNumBytes();
  }

  public long getBlockId() {
    return block.getBlockId();
  }

  public long getGenerationStamp() {
    return block.getGenerationStamp();
  }

  public void setBlockId(final long bid) {
    block.setBlockId(bid);
  }

  public void setGenerationStamp(final long genStamp) {
    block.setGenerationStamp(genStamp);
  }

  public void setNumBytes(final long len) {
    block.setNumBytes(len);
  }

  public void set(String poolId, Block blk) {
    this.poolId = poolId;
    this.block = blk;
  }

  public static Block getLocalBlock(final ExtendedBlock b) {
    return b == null ? null : b.getLocalBlock();
  }

  public Block getLocalBlock() {
    return block;
  }

  @Override // Object
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExtendedBlock)) {
      return false;
    }
    ExtendedBlock b = (ExtendedBlock)o;
    return b.block.equals(block) && b.poolId.equals(poolId);
  }

  @Override // Object
  public int hashCode() {
    int result = 31 + poolId.hashCode();
    return (31 * result + block.hashCode());
  }

  @Override // Object
  public String toString() {
    return poolId + ":" + block;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/FsPermissionExtension.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/FsPermissionExtension.java
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.FsPermission;

@InterfaceAudience.Private
public class FsPermissionExtension extends FsPermission {
  private final static short ACL_BIT = 1 << 12;
  private final static short ENCRYPTED_BIT = 1 << 13;
  private final boolean aclBit;
  private final boolean encryptedBit;

  public FsPermissionExtension(FsPermission perm, boolean hasAcl,
      boolean isEncrypted) {
    super(perm.toShort());
    aclBit = hasAcl;
    encryptedBit = isEncrypted;
  }

  public FsPermissionExtension(short perm) {
    super(perm);
    aclBit = (perm & ACL_BIT) != 0;
    encryptedBit = (perm & ENCRYPTED_BIT) != 0;
  }

  @Override
  public short toExtendedShort() {
    return (short)(toShort() |
        (aclBit ? ACL_BIT : 0) | (encryptedBit ? ENCRYPTED_BIT : 0));
  }

  @Override
  public boolean getAclBit() {
    return aclBit;
  }

  @Override
  public boolean getEncryptedBit() {
    return encryptedBit;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/HdfsConstantsClient.java
hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/HdfsFileStatus.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/HdfsFileStatus.java
package org.apache.hadoop.hdfs.protocol;

import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtilClient;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HdfsFileStatus {

  private final byte[] path;  // local name of the inode that's encoded in java UTF8
  private final byte[] symlink; // symlink target encoded in java UTF8 or null
  private final long length;
  private final boolean isdir;
  private final short block_replication;
  private final long blocksize;
  private final long modification_time;
  private final long access_time;
  private final FsPermission permission;
  private final String owner;
  private final String group;
  private final long fileId;

  private final FileEncryptionInfo feInfo;

  private final int childrenNum;
  private final byte storagePolicy;

  public static final byte[] EMPTY_NAME = new byte[0];

  public HdfsFileStatus(long length, boolean isdir, int block_replication,
      long blocksize, long modification_time, long access_time,
      FsPermission permission, String owner, String group, byte[] symlink,
      byte[] path, long fileId, int childrenNum, FileEncryptionInfo feInfo,
      byte storagePolicy) {
    this.length = length;
    this.isdir = isdir;
    this.block_replication = (short)block_replication;
    this.blocksize = blocksize;
    this.modification_time = modification_time;
    this.access_time = access_time;
    this.permission = (permission == null) ?
        ((isdir || symlink!=null) ?
            FsPermission.getDefault() :
            FsPermission.getFileDefault()) :
        permission;
    this.owner = (owner == null) ? "" : owner;
    this.group = (group == null) ? "" : group;
    this.symlink = symlink;
    this.path = path;
    this.fileId = fileId;
    this.childrenNum = childrenNum;
    this.feInfo = feInfo;
    this.storagePolicy = storagePolicy;
  }

  public final long getLen() {
    return length;
  }

  public final boolean isDir() {
    return isdir;
  }

  public boolean isSymlink() {
    return symlink != null;
  }

  public final long getBlockSize() {
    return blocksize;
  }

  public final short getReplication() {
    return block_replication;
  }

  public final long getModificationTime() {
    return modification_time;
  }

  public final long getAccessTime() {
    return access_time;
  }

  public final FsPermission getPermission() {
    return permission;
  }

  public final String getOwner() {
    return owner;
  }

  public final String getGroup() {
    return group;
  }

  public final boolean isEmptyLocalName() {
    return path.length == 0;
  }

  public final String getLocalName() {
    return DFSUtilClient.bytes2String(path);
  }

  public final byte[] getLocalNameInBytes() {
    return path;
  }

  public final String getFullName(final String parent) {
    if (isEmptyLocalName()) {
      return parent;
    }

    StringBuilder fullName = new StringBuilder(parent);
    if (!parent.endsWith(Path.SEPARATOR)) {
      fullName.append(Path.SEPARATOR);
    }
    fullName.append(getLocalName());
    return fullName.toString();
  }

  public final Path getFullPath(final Path parent) {
    if (isEmptyLocalName()) {
      return parent;
    }

    return new Path(parent, getLocalName());
  }

  public final String getSymlink() {
    return DFSUtilClient.bytes2String(symlink);
  }

  public final byte[] getSymlinkInBytes() {
    return symlink;
  }

  public final long getFileId() {
    return fileId;
  }

  public final FileEncryptionInfo getFileEncryptionInfo() {
    return feInfo;
  }

  public final int getChildrenNum() {
    return childrenNum;
  }

  public final byte getStoragePolicy() {
    return storagePolicy;
  }

  public final FileStatus makeQualified(URI defaultUri, Path path) {
    return new FileStatus(getLen(), isDir(), getReplication(),
        getBlockSize(), getModificationTime(),
        getAccessTime(),
        getPermission(), getOwner(), getGroup(),
        isSymlink() ? new Path(getSymlink()) : null,
        (getFullPath(path)).makeQualified(
            defaultUri, null)); // fully-qualify path
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/LocatedBlock.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/LocatedBlock.java
package org.apache.hadoop.hdfs.protocol;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import com.google.common.collect.Lists;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LocatedBlock {

  private final ExtendedBlock b;
  private long offset;  // offset of the first byte of the block in the file
  private final DatanodeInfoWithStorage[] locs;
  private final String[] storageIDs;
  private final StorageType[] storageTypes;
  private boolean corrupt;
  private Token<BlockTokenIdentifier> blockToken = new Token<BlockTokenIdentifier>();
  private DatanodeInfo[] cachedLocs;

  private static final DatanodeInfoWithStorage[] EMPTY_LOCS =
      new DatanodeInfoWithStorage[0];

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs) {
    this(b, locs, null, null, -1, false, EMPTY_LOCS);
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs,
                      String[] storageIDs, StorageType[] storageTypes) {
    this(b, locs, storageIDs, storageTypes, -1, false, EMPTY_LOCS);
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, String[] storageIDs,
                      StorageType[] storageTypes, long startOffset,
                      boolean corrupt, DatanodeInfo[] cachedLocs) {
    this.b = b;
    this.offset = startOffset;
    this.corrupt = corrupt;
    if (locs==null) {
      this.locs = EMPTY_LOCS;
    } else {
      this.locs = new DatanodeInfoWithStorage[locs.length];
      for(int i = 0; i < locs.length; i++) {
        DatanodeInfo di = locs[i];
        DatanodeInfoWithStorage storage = new DatanodeInfoWithStorage(di,
            storageIDs != null ? storageIDs[i] : null,
            storageTypes != null ? storageTypes[i] : null);
        this.locs[i] = storage;
      }
    }
    this.storageIDs = storageIDs;
    this.storageTypes = storageTypes;

    if (cachedLocs == null || cachedLocs.length == 0) {
      this.cachedLocs = EMPTY_LOCS;
    } else {
      this.cachedLocs = cachedLocs;
    }
  }

  public Token<BlockTokenIdentifier> getBlockToken() {
    return blockToken;
  }

  public void setBlockToken(Token<BlockTokenIdentifier> token) {
    this.blockToken = token;
  }

  public ExtendedBlock getBlock() {
    return b;
  }

  public DatanodeInfo[] getLocations() {
    return locs;
  }

  public StorageType[] getStorageTypes() {
    return storageTypes;
  }

  public String[] getStorageIDs() {
    return storageIDs;
  }

  public void updateCachedStorageInfo() {
    if (storageIDs != null) {
      for(int i = 0; i < locs.length; i++) {
        storageIDs[i] = locs[i].getStorageID();
      }
    }
    if (storageTypes != null) {
      for(int i = 0; i < locs.length; i++) {
        storageTypes[i] = locs[i].getStorageType();
      }
    }
  }

  public long getStartOffset() {
    return offset;
  }

  public long getBlockSize() {
    return b.getNumBytes();
  }

  public void setStartOffset(long value) {
    this.offset = value;
  }

  public void setCorrupt(boolean corrupt) {
    this.corrupt = corrupt;
  }

  public boolean isCorrupt() {
    return this.corrupt;
  }

  public void addCachedLoc(DatanodeInfo loc) {
    List<DatanodeInfo> cachedList = Lists.newArrayList(cachedLocs);
    if (cachedList.contains(loc)) {
      return;
    }
    for (DatanodeInfoWithStorage di : locs) {
      if (loc.equals(di)) {
        cachedList.add(di);
        cachedLocs = cachedList.toArray(cachedLocs);
        return;
      }
    }
    cachedList.add(loc);
    cachedLocs = cachedList.toArray(cachedLocs);
  }

  public DatanodeInfo[] getCachedLocations() {
    return cachedLocs;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + b
        + "; getBlockSize()=" + getBlockSize()
        + "; corrupt=" + corrupt
        + "; offset=" + offset
        + "; locs=" + Arrays.asList(locs)
        + "}";
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/LocatedBlocks.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/LocatedBlocks.java
package org.apache.hadoop.hdfs.protocol;

import java.util.List;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileEncryptionInfo;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LocatedBlocks {
  private final long fileLength;
  private final List<LocatedBlock> blocks; // array of blocks with prioritized locations
  private final boolean underConstruction;
  private final LocatedBlock lastLocatedBlock;
  private final boolean isLastBlockComplete;
  private final FileEncryptionInfo fileEncryptionInfo;

  public LocatedBlocks() {
    fileLength = 0;
    blocks = null;
    underConstruction = false;
    lastLocatedBlock = null;
    isLastBlockComplete = false;
    fileEncryptionInfo = null;
  }

  public LocatedBlocks(long flength, boolean isUnderConstuction,
    List<LocatedBlock> blks, LocatedBlock lastBlock,
    boolean isLastBlockCompleted, FileEncryptionInfo feInfo) {
    fileLength = flength;
    blocks = blks;
    underConstruction = isUnderConstuction;
    this.lastLocatedBlock = lastBlock;
    this.isLastBlockComplete = isLastBlockCompleted;
    this.fileEncryptionInfo = feInfo;
  }

  public List<LocatedBlock> getLocatedBlocks() {
    return blocks;
  }

  public LocatedBlock getLastLocatedBlock() {
    return lastLocatedBlock;
  }

  public boolean isLastBlockComplete() {
    return isLastBlockComplete;
  }

  public LocatedBlock get(int index) {
    return blocks.get(index);
  }

  public int locatedBlockCount() {
    return blocks == null ? 0 : blocks.size();
  }

  public long getFileLength() {
    return this.fileLength;
  }

  public boolean isUnderConstruction() {
    return underConstruction;
  }

  public FileEncryptionInfo getFileEncryptionInfo() {
    return fileEncryptionInfo;
  }

  public int findBlock(long offset) {
    LocatedBlock key = new LocatedBlock(
        new ExtendedBlock(), new DatanodeInfo[0]);
    key.setStartOffset(offset);
    key.getBlock().setNumBytes(1);
    Comparator<LocatedBlock> comp =
      new Comparator<LocatedBlock>() {
        @Override
        public int compare(LocatedBlock a, LocatedBlock b) {
          long aBeg = a.getStartOffset();
          long bBeg = b.getStartOffset();
          long aEnd = aBeg + a.getBlockSize();
          long bEnd = bBeg + b.getBlockSize();
          if(aBeg <= bBeg && bEnd <= aEnd
              || bBeg <= aBeg && aEnd <= bEnd)
            return 0; // one of the blocks is inside the other
          if(aBeg < bBeg)
            return -1; // a's left bound is to the left of the b's
          return 1;
        }
      };
    return Collections.binarySearch(blocks, key, comp);
  }

  public void insertRange(int blockIdx, List<LocatedBlock> newBlocks) {
    int oldIdx = blockIdx;
    int insStart = 0, insEnd = 0;
    for(int newIdx = 0; newIdx < newBlocks.size() && oldIdx < blocks.size();
                                                        newIdx++) {
      long newOff = newBlocks.get(newIdx).getStartOffset();
      long oldOff = blocks.get(oldIdx).getStartOffset();
      if(newOff < oldOff) {
        insEnd++;
      } else if(newOff == oldOff) {
        blocks.set(oldIdx, newBlocks.get(newIdx));
        if(insStart < insEnd) { // insert new blocks
          blocks.addAll(oldIdx, newBlocks.subList(insStart, insEnd));
          oldIdx += insEnd - insStart;
        }
        insStart = insEnd = newIdx+1;
        oldIdx++;
      } else {  // newOff > oldOff
        assert false : "List of LocatedBlock must be sorted by startOffset";
      }
    }
    insEnd = newBlocks.size();
    if(insStart < insEnd) { // insert new blocks
      blocks.addAll(oldIdx, newBlocks.subList(insStart, insEnd));
    }
  }

  public static int getInsertIndex(int binSearchResult) {
    return binSearchResult >= 0 ? binSearchResult : -(binSearchResult+1);
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(getClass().getSimpleName());
    b.append("{")
     .append("\n  fileLength=").append(fileLength)
     .append("\n  underConstruction=").append(underConstruction)
     .append("\n  blocks=").append(blocks)
     .append("\n  lastLocatedBlock=").append(lastLocatedBlock)
     .append("\n  isLastBlockComplete=").append(isLastBlockComplete)
     .append("}");
    return b.toString();
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/security/token/block/BlockTokenIdentifier.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/security/token/block/BlockTokenIdentifier.java

package org.apache.hadoop.hdfs.security.token.block;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

@InterfaceAudience.Private
public class BlockTokenIdentifier extends TokenIdentifier {
  static final Text KIND_NAME = new Text("HDFS_BLOCK_TOKEN");

  public enum AccessMode {
    READ, WRITE, COPY, REPLACE
  }

  private long expiryDate;
  private int keyId;
  private String userId;
  private String blockPoolId;
  private long blockId;
  private final EnumSet<AccessMode> modes;

  private byte [] cache;

  public BlockTokenIdentifier() {
    this(null, null, 0, EnumSet.noneOf(AccessMode.class));
  }

  public BlockTokenIdentifier(String userId, String bpid, long blockId,
      EnumSet<AccessMode> modes) {
    this.cache = null;
    this.userId = userId;
    this.blockPoolId = bpid;
    this.blockId = blockId;
    this.modes = modes == null ? EnumSet.noneOf(AccessMode.class) : modes;
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  @Override
  public UserGroupInformation getUser() {
    if (userId == null || "".equals(userId)) {
      String user = blockPoolId + ":" + Long.toString(blockId);
      return UserGroupInformation.createRemoteUser(user);
    }
    return UserGroupInformation.createRemoteUser(userId);
  }

  public long getExpiryDate() {
    return expiryDate;
  }

  public void setExpiryDate(long expiryDate) {
    this.cache = null;
    this.expiryDate = expiryDate;
  }

  public int getKeyId() {
    return this.keyId;
  }

  public void setKeyId(int keyId) {
    this.cache = null;
    this.keyId = keyId;
  }

  public String getUserId() {
    return userId;
  }

  public String getBlockPoolId() {
    return blockPoolId;
  }

  public long getBlockId() {
    return blockId;
  }

  public EnumSet<AccessMode> getAccessModes() {
    return modes;
  }

  @Override
  public String toString() {
    return "block_token_identifier (expiryDate=" + this.getExpiryDate()
        + ", keyId=" + this.getKeyId() + ", userId=" + this.getUserId()
        + ", blockPoolId=" + this.getBlockPoolId()
        + ", blockId=" + this.getBlockId() + ", access modes="
        + this.getAccessModes() + ")";
  }

  static boolean isEqual(Object a, Object b) {
    return a == null ? b == null : a.equals(b);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof BlockTokenIdentifier) {
      BlockTokenIdentifier that = (BlockTokenIdentifier) obj;
      return this.expiryDate == that.expiryDate && this.keyId == that.keyId
          && isEqual(this.userId, that.userId)
          && isEqual(this.blockPoolId, that.blockPoolId)
          && this.blockId == that.blockId
          && isEqual(this.modes, that.modes);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return (int) expiryDate ^ keyId ^ (int) blockId ^ modes.hashCode()
        ^ (userId == null ? 0 : userId.hashCode())
        ^ (blockPoolId == null ? 0 : blockPoolId.hashCode());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.cache = null;
    expiryDate = WritableUtils.readVLong(in);
    keyId = WritableUtils.readVInt(in);
    userId = WritableUtils.readString(in);
    blockPoolId = WritableUtils.readString(in);
    blockId = WritableUtils.readVLong(in);
    int length = WritableUtils.readVIntInRange(in, 0,
        AccessMode.class.getEnumConstants().length);
    for (int i = 0; i < length; i++) {
      modes.add(WritableUtils.readEnum(in, AccessMode.class));
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVLong(out, expiryDate);
    WritableUtils.writeVInt(out, keyId);
    WritableUtils.writeString(out, userId);
    WritableUtils.writeString(out, blockPoolId);
    WritableUtils.writeVLong(out, blockId);
    WritableUtils.writeVInt(out, modes.size());
    for (AccessMode aMode : modes) {
      WritableUtils.writeEnum(out, aMode);
    }
  }

  @Override
  public byte[] getBytes() {
    if(cache == null) cache = super.getBytes();

    return cache;
  }

  @InterfaceAudience.Private
  public static class Renewer extends Token.TrivialRenewer {
    @Override
    protected Text getKind() {
      return KIND_NAME;
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/security/token/delegation/DelegationTokenIdentifier.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/security/token/delegation/DelegationTokenIdentifier.java

package org.apache.hadoop.hdfs.security.token.delegation;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

@InterfaceAudience.Private
public class DelegationTokenIdentifier
    extends AbstractDelegationTokenIdentifier {
  public static final Text HDFS_DELEGATION_KIND = new Text("HDFS_DELEGATION_TOKEN");

  public DelegationTokenIdentifier() {
  }

  public DelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
    super(owner, renewer, realUser);
  }

  @Override
  public Text getKind() {
    return HDFS_DELEGATION_KIND;
  }

  @Override
  public String toString() {
    return getKind() + " token " + getSequenceNumber()
        + " for " + getUser().getShortUserName();
  }

  public static String stringifyToken(final Token<?> token) throws IOException {
    DelegationTokenIdentifier ident = new DelegationTokenIdentifier();
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    ident.readFields(in);

    if (token.getService().getLength() > 0) {
      return ident + " on " + token.getService();
    } else {
      return ident.toString();
    }
  }

  public static class WebHdfsDelegationTokenIdentifier
      extends DelegationTokenIdentifier {
    public WebHdfsDelegationTokenIdentifier() {
      super();
    }
    @Override
    public Text getKind() {
      return WebHdfsConstants.WEBHDFS_TOKEN_KIND;
    }
  }

  public static class SWebHdfsDelegationTokenIdentifier
      extends WebHdfsDelegationTokenIdentifier {
    public SWebHdfsDelegationTokenIdentifier() {
      super();
    }
    @Override
    public Text getKind() {
      return WebHdfsConstants.SWEBHDFS_TOKEN_KIND;
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsConstants.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsConstants.java
package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.Text;

@InterfaceAudience.Private
public class WebHdfsConstants {
  public static final Text WEBHDFS_TOKEN_KIND = new Text("WEBHDFS delegation");
  public static final Text SWEBHDFS_TOKEN_KIND = new Text("SWEBHDFS delegation");

  enum PathType {
    FILE, DIRECTORY, SYMLINK;

    static PathType valueOf(HdfsFileStatus status) {
      return status.isDir()? DIRECTORY: status.isSymlink()? SYMLINK: FILE;
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSUtil.java
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocol/SnapshottableDirectoryStatus.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/PBHelper.java
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
        fs.hasChildrenNum() ? fs.getChildrenNum() : -1,
        fs.hasFileEncryptionInfo() ? convert(fs.getFileEncryptionInfo()) : null,
        fs.hasStoragePolicy() ? (byte) fs.getStoragePolicy()
            : HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED);
  }

  public static SnapshottableDirectoryStatus convert(

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
    postponedMisreplicatedBlocksCount.set(0);
  };

  public static LocatedBlock newLocatedBlock(
      ExtendedBlock b, DatanodeStorageInfo[] storages,
      long startOffset, boolean corrupt) {
    return new LocatedBlock(
        b, DatanodeStorageInfo.toDatanodeInfos(storages),
        DatanodeStorageInfo.toStorageIDs(storages),
        DatanodeStorageInfo.toStorageTypes(storages),
        startOffset, corrupt,
        null);
  }

  private static class ReplicationWork {


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockStoragePolicySuite.java
  public static final XAttr.NameSpace XAttrNS = XAttr.NameSpace.SYSTEM;

  public static final int ID_BIT_LENGTH = 4;

  @VisibleForTesting
  public static BlockStoragePolicySuite createDefaultSuite() {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DatanodeStorageInfo.java

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/HeartbeatManager.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;

  @Override
  public synchronized float getCapacityUsedPercent() {
    return DFSUtilClient.getPercentUsed(stats.capacityUsed, stats.capacityTotal);
  }

  @Override

  @Override
  public synchronized float getCapacityRemainingPercent() {
    return DFSUtilClient.getPercentRemaining(stats.capacityRemaining,
                                             stats.capacityTotal);
  }

  @Override

  @Override
  public synchronized float getPercentBlockPoolUsed() {
    return DFSUtilClient.getPercentUsed(stats.blockPoolUsed,
                                        stats.capacityTotal);
  }

  @Override

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/mover/Mover.java

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
    private boolean processFile(String fullPath, HdfsLocatedFileStatus status) {
      final byte policyId = status.getStoragePolicy();
      if (policyId == HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
        return false;
      }
      final BlockStoragePolicy policy = blockStoragePolicies[policyId];

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirStatAndListingOp.java
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FsPermissionExtension;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
  }

  private static byte getStoragePolicyID(byte inodePolicy, byte parentPolicy) {
    return inodePolicy != HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED ? inodePolicy :
        parentPolicy;
  }

      if (targetNode == null)
        return null;
      byte parentStoragePolicy = isSuperUser ?
          targetNode.getStoragePolicyID() : HdfsConstantsClient
          .BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;

      if (!targetNode.isDirectory()) {
        return new DirectoryListing(
        INode cur = contents.get(startChild+i);
        byte curPolicy = isSuperUser && !cur.isSymlink()?
            cur.getLocalStoragePolicyID():
            HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
        listing[i] = createFileStatus(fsd, src, cur.getLocalNameBytes(), cur,
            needLocation, getStoragePolicyID(curPolicy,
                parentStoragePolicy), snapshot, isRawPath, iip);
    for (int i = 0; i < numOfListing; i++) {
      Snapshot.Root sRoot = snapshots.get(i + skipSize).getRoot();
      listing[i] = createFileStatus(fsd, src, sRoot.getLocalNameBytes(), sRoot,
          HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED, Snapshot.CURRENT_STATE_ID,
          false, INodesInPath.fromINode(sRoot));
    }
    return new DirectoryListing(
    try {
      final INode i = src.getLastINode();
      byte policyId = includeStoragePolicy && i != null && !i.isSymlink() ?
          i.getStoragePolicyID() : HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      return i == null ? null : createFileStatus(
          fsd, path, HdfsFileStatus.EMPTY_NAME, i, policyId,
          src.getPathSnapshotId(), isRawPath, src);
      if (fsd.getINode4DotSnapshot(srcs) != null) {
        return new HdfsFileStatus(0, true, 0, 0, 0, 0, null, null, null, null,
            HdfsFileStatus.EMPTY_NAME, -1L, 0, null,
            HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED);
      }
      return null;
    }
    if (fsd.getINode4DotSnapshot(src) != null) {
      return new HdfsFileStatus(0, true, 0, 0, 0, 0, null, null, null, null,
          HdfsFileStatus.EMPTY_NAME, -1L, 0, null,
          HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED);
    }
    return null;
  }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirectory.java
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.security.AccessControlException;
    EnumCounters<StorageType> typeSpaceDeltas =
        new EnumCounters<StorageType>(StorageType.class);
    if (storagePolicyID != HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      BlockStoragePolicy storagePolicy = getBlockManager().getStoragePolicy(storagePolicyID);

      if (oldRep != newRep) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogLoader.java
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
        if (toAddRetryCache) {
          HdfsFileStatus stat = FSDirStatAndListingOp.createFileStatus(
              fsNamesys.dir, path, HdfsFileStatus.EMPTY_NAME, newFile,
              HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED, Snapshot.CURRENT_STATE_ID,
              false, iip);
          fsNamesys.addCacheEntryWithPayload(addCloseOp.rpcClientId,
              addCloseOp.rpcCallId, stat);
            HdfsFileStatus stat = FSDirStatAndListingOp.createFileStatus(
                fsNamesys.dir, path,
                HdfsFileStatus.EMPTY_NAME, newFile,
                HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED,
                Snapshot.CURRENT_STATE_ID, false, iip);
            fsNamesys.addCacheEntryWithPayload(addCloseOp.rpcClientId,
                addCloseOp.rpcCallId, new LastBlockWithStatus(lb, stat));
        if (toAddRetryCache) {
          HdfsFileStatus stat = FSDirStatAndListingOp.createFileStatus(
              fsNamesys.dir, path, HdfsFileStatus.EMPTY_NAME, file,
              HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED,
              Snapshot.CURRENT_STATE_ID, false, iip);
          fsNamesys.addCacheEntryWithPayload(appendOp.rpcClientId,
              appendOp.rpcCallId, new LastBlockWithStatus(lb, stat));

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLogOp.java
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.XAttrEditLogProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.hdfs.util.XMLUtils.InvalidXmlException;
import org.apache.hadoop.hdfs.util.XMLUtils.Stanza;
    
    private AddCloseOp(FSEditLogOpCodes opCode) {
      super(opCode);
      storagePolicyId = HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      assert(opCode == OP_ADD || opCode == OP_CLOSE || opCode == OP_APPEND);
    }

            NameNodeLayoutVersion.Feature.BLOCK_STORAGE_POLICY, logVersion)) {
          this.storagePolicyId = FSImageSerialization.readByte(in);
        } else {
          this.storagePolicyId = HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
        }
        readRpcIds(in, logVersion);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java

  LocatedBlock makeLocatedBlock(Block blk, DatanodeStorageInfo[] locs,
                                        long offset) throws IOException {
    LocatedBlock lBlk = BlockManager.newLocatedBlock(
        getExtendedBlock(blk), locs, offset, false);
    getBlockManager().setBlockToken(
        lBlk, BlockTokenIdentifier.AccessMode.WRITE);
    final DatanodeStorageInfo[] targets = blockManager.chooseTarget4AdditionalDatanode(
        src, numAdditionalNodes, clientnode, chosen, 
        excludes, preferredblocksize, storagePolicyID);
    final LocatedBlock lb = BlockManager.newLocatedBlock(
        blk, targets, -1, false);
    blockManager.setBlockToken(lb, BlockTokenIdentifier.AccessMode.COPY);
    return lb;
  }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INode.java
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
  public final QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps) {
    final byte storagePolicyId = isSymlink() ?
        HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED : getStoragePolicyID();
    return computeQuotaUsage(bsps, storagePolicyId,
        new QuotaCounts.Builder().build(), true, Snapshot.CURRENT_STATE_ID);
  }
  public final QuotaCounts computeQuotaUsage(
    BlockStoragePolicySuite bsps, QuotaCounts counts, boolean useCache) {
    final byte storagePolicyId = isSymlink() ?
        HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED : getStoragePolicyID();
    return computeQuotaUsage(bsps, storagePolicyId, counts,
        useCache, Snapshot.CURRENT_STATE_ID);
  }

  public abstract byte getLocalStoragePolicyID();
  public byte getStoragePolicyIDForQuota(byte parentStoragePolicyId) {
    byte localId = isSymlink() ?
        HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED : getLocalStoragePolicyID();
    return localId != HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED ?
        localId : parentStoragePolicyId;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeDirectory.java
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import static org.apache.hadoop.hdfs.protocol.HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;

        return (xattr.getValue())[0];
      }
    }
    return BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
  }

  @Override
  public byte getStoragePolicyID() {
    byte id = getLocalStoragePolicyID();
    if (id != BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      return id;
    }
    return getParent() != null ? getParent().getStoragePolicyID() : BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
  }

  void setQuota(BlockStoragePolicySuite bsps, long nsQuota, long ssQuota, StorageType type) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeFile.java
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.protocol.HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.NO_SNAPSHOT_ID;

  @Override
  public byte getStoragePolicyID() {
    byte id = getLocalStoragePolicyID();
    if (id == BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      return this.getParent() != null ?
          this.getParent().getStoragePolicyID() : id;
    }
    counts.addNameSpace(nsDelta);
    counts.addStorageSpace(ssDeltaNoReplication * replication);

    if (blockStoragePolicyId != BLOCK_STORAGE_POLICY_ID_UNSPECIFIED){
      BlockStoragePolicy bsp = bsps.getPolicy(blockStoragePolicyId);
      List<StorageType> storageTypes = bsp.chooseStorageTypes(replication);
      for (StorageType t : storageTypes) {
    counts.addContent(Content.LENGTH, fileLen);
    counts.addContent(Content.DISKSPACE, storagespaceConsumed());

    if (getStoragePolicyID() != BLOCK_STORAGE_POLICY_ID_UNSPECIFIED){
      BlockStoragePolicy bsp = summary.getBlockStoragePolicySuite().
          getPolicy(getStoragePolicyID());
      List<StorageType> storageTypes = bsp.chooseStorageTypes(getFileReplication());

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeMap.java

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;

      @Override
      public byte getStoragePolicyID(){
        return HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      }

      @Override
      public byte getLocalStoragePolicyID() {
        return HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      }
    };
      

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/FileWithSnapshotFeature.java

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
    BlockStoragePolicy bsp = null;
    EnumCounters<StorageType> typeSpaces =
        new EnumCounters<StorageType>(StorageType.class);
    if (storagePolicyID != HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      bsp = bsps.getPolicy(file.getStoragePolicyID());
    }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/web/resources/NamenodeWebHdfsMethods.java
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.ParamFilter;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.*;
import org.apache.hadoop.io.Text;
      return null;
    }
    final Token<? extends TokenIdentifier> t = c.getAllTokens().iterator().next();
    Text kind = request.getScheme().equals("http") ? WebHdfsConstants.WEBHDFS_TOKEN_KIND
        : WebHdfsConstants.SWEBHDFS_TOKEN_KIND;
    t.setKind(kind);
    return t;
  }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/StoragePolicyAdmin.java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
          return 2;
        }
        byte storagePolicyId = status.getStoragePolicy();
        if (storagePolicyId == HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
          System.out.println("The storage policy of " + path + " is unspecified");
          return 0;
        }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/JsonUtilClient.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/SWebHdfsFileSystem.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/TokenAspect.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsFileSystem.java
hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockStoragePolicy.java
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.protocol.HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;

import java.io.File;
import java.io.FileNotFoundException;
          HdfsFileStatus.EMPTY_NAME, true).getPartialListing();
      HdfsFileStatus[] barList = fs.getClient().listPaths(barDir.toString(),
          HdfsFileStatus.EMPTY_NAME, true).getPartialListing();
      checkDirectoryListing(dirList, BLOCK_STORAGE_POLICY_ID_UNSPECIFIED,
                            BLOCK_STORAGE_POLICY_ID_UNSPECIFIED);
      checkDirectoryListing(barList, BLOCK_STORAGE_POLICY_ID_UNSPECIFIED,
                            BLOCK_STORAGE_POLICY_ID_UNSPECIFIED);

      final Path invalidPath = new Path("/invalidPath");
      try {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestNamenodeCapacityReport.java
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
            + " percentRemaining " + percentRemaining);
        
        assertTrue(configCapacity == (used + remaining + nonDFSUsed));
        assertTrue(percentUsed == DFSUtilClient.getPercentUsed(used,
                                                               configCapacity));
        assertTrue(percentRemaining == DFSUtilClient.getPercentRemaining(
            remaining, configCapacity));
        assertTrue(percentBpUsed == DFSUtilClient.getPercentUsed(bpUsed,
                                                                 configCapacity));
      }   
      
      assertTrue(configCapacity == (used + remaining + nonDFSUsed));

      assertTrue(percentUsed == DFSUtilClient.getPercentUsed(used,
                                                             configCapacity));

      assertTrue(percentBpUsed == DFSUtilClient.getPercentUsed(bpUsed,
                                                               configCapacity));

      assertTrue(percentRemaining == ((float)remaining * 100.0f)/(float)configCapacity);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHdfsUrl.java
          dtId, dtSecretManager);
      SecurityUtil.setTokenService(
          token, NetUtils.createSocketAddr(uri.getAuthority()));
      token.setKind(WebHdfsConstants.WEBHDFS_TOKEN_KIND);
      ugi.addToken(token);
    }
    return (WebHdfsFileSystem) FileSystem.get(uri, conf);

