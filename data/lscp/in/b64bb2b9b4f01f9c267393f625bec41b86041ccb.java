hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/retry/RetryUtils.java
      boolean defaultRetryPolicyEnabled,
      String retryPolicySpecKey,
      String defaultRetryPolicySpec,
      final String remoteExceptionToRetry
      ) {
    
    final RetryPolicy multipleLinearRandomRetry = 
          final RetryPolicy p;
          if (e instanceof RemoteException) {
            final RemoteException re = (RemoteException)e;
            p = remoteExceptionToRetry.equals(re.getClassName())?
                multipleLinearRandomRetry: RetryPolicies.TRY_ONCE_THEN_FAIL;
          } else if (e instanceof IOException || e instanceof ServiceException) {
            p = multipleLinearRandomRetry;

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/DFSUtilClient.java

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
public class DFSUtilClient {
  private static final Logger LOG = LoggerFactory.getLogger(
      DFSUtilClient.class);
  public static byte[] string2Bytes(String str) {
    return str.getBytes(Charsets.UTF_8);
  }

    }
  }

  public static BlockLocation[] locatedBlocks2Locations(LocatedBlocks blocks) {
    if (blocks == null) {
      return new BlockLocation[0];
    }
    return locatedBlocks2Locations(blocks.getLocatedBlocks());
  }

  public static BlockLocation[] locatedBlocks2Locations(
      List<LocatedBlock> blocks) {
    if (blocks == null) {
      return new BlockLocation[0];
    }
    int nrBlocks = blocks.size();
    BlockLocation[] blkLocations = new BlockLocation[nrBlocks];
    if (nrBlocks == 0) {
      return blkLocations;
    }
    int idx = 0;
    for (LocatedBlock blk : blocks) {
      assert idx < nrBlocks : "Incorrect index";
      DatanodeInfo[] locations = blk.getLocations();
      String[] hosts = new String[locations.length];
      String[] xferAddrs = new String[locations.length];
      String[] racks = new String[locations.length];
      for (int hCnt = 0; hCnt < locations.length; hCnt++) {
        hosts[hCnt] = locations[hCnt].getHostName();
        xferAddrs[hCnt] = locations[hCnt].getXferAddr();
        NodeBase node = new NodeBase(xferAddrs[hCnt],
                                     locations[hCnt].getNetworkLocation());
        racks[hCnt] = node.toString();
      }
      DatanodeInfo[] cachedLocations = blk.getCachedLocations();
      String[] cachedHosts = new String[cachedLocations.length];
      for (int i=0; i<cachedLocations.length; i++) {
        cachedHosts[i] = cachedLocations[i].getHostName();
      }
      blkLocations[idx] = new BlockLocation(xferAddrs, hosts, cachedHosts,
                                            racks,
                                            blk.getStartOffset(),
                                            blk.getBlockSize(),
                                            blk.isCorrupt());
      idx++;
    }
    return blkLocations;
  }

    }
    return value;
  }

  public static boolean isValidName(String src) {
    if (!src.startsWith(Path.SEPARATOR)) {
      return false;
    }

    String[] components = StringUtils.split(src, '/');
    for (int i = 0; i < components.length; i++) {
      String element = components[i];
      if (element.equals(".")  ||
          (element.contains(":"))  ||
          (element.contains("/"))) {
        return false;
      }
      if (element.equals("..")) {
        if (components.length > 4
            && components[1].equals(".reserved")
            && components[2].equals(".inodes")) {
          continue;
        }
        return false;
      }
      if (element.isEmpty() && i != components.length - 1 &&
          i != 0) {
        return false;
      }
    }
    return true;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsClientConfigKeys.java
  long    DFS_BLOCK_SIZE_DEFAULT = 128*1024*1024;
  String  DFS_REPLICATION_KEY = "dfs.replication";
  short   DFS_REPLICATION_DEFAULT = 3;
  String  DFS_WEBHDFS_USER_PATTERN_KEY = "dfs.webhdfs.user.provider.user.pattern";
  String  DFS_WEBHDFS_USER_PATTERN_DEFAULT = "^[A-Za-z_][A-Za-z0-9._-]*[$]?$";
  String DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT =
      "^(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?(,(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?)*$";
  int     DFS_NAMENODE_HTTPS_PORT_DEFAULT = 50470;
  String  DFS_NAMENODE_HTTPS_ADDRESS_KEY = "dfs.namenode.https-address";
  String DFS_HA_NAMENODES_KEY_PREFIX = "dfs.ha.namenodes";
  String  DFS_WEBHDFS_ENABLED_KEY = "dfs.webhdfs.enabled";
  boolean DFS_WEBHDFS_ENABLED_DEFAULT = true;
  String  DFS_NAMENODE_HTTP_PORT_KEY = "dfs.http.port";
  String  DFS_NAMENODE_HTTPS_PORT_KEY = "dfs.https.port";

  interface Retry {

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/HdfsConstantsClient.java
hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/ByteRangeInputStream.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/ByteRangeInputStream.java

package org.apache.hadoop.hdfs.web;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.http.HttpStatus;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;

public abstract class ByteRangeInputStream extends FSInputStream {

  public static abstract class URLOpener {
    protected URL url;

    public URLOpener(URL u) {
      url = u;
    }

    public void setURL(URL u) {
      url = u;
    }

    public URL getURL() {
      return url;
    }

    protected abstract HttpURLConnection connect(final long offset,
        final boolean resolved) throws IOException;
  }

  enum StreamStatus {
    NORMAL, SEEK, CLOSED
  }
  protected InputStream in;
  protected final URLOpener originalURL;
  protected final URLOpener resolvedURL;
  protected long startPos = 0;
  protected long currentPos = 0;
  protected Long fileLength = null;

  StreamStatus status = StreamStatus.SEEK;

  public ByteRangeInputStream(URLOpener o, URLOpener r) throws IOException {
    this.originalURL = o;
    this.resolvedURL = r;
    getInputStream();
  }

  protected abstract URL getResolvedUrl(final HttpURLConnection connection
      ) throws IOException;

  @VisibleForTesting
  protected InputStream getInputStream() throws IOException {
    switch (status) {
      case NORMAL:
        break;
      case SEEK:
        if (in != null) {
          in.close();
        }
        in = openInputStream();
        status = StreamStatus.NORMAL;
        break;
      case CLOSED:
        throw new IOException("Stream closed");
    }
    return in;
  }

  @VisibleForTesting
  protected InputStream openInputStream() throws IOException {
    final boolean resolved = resolvedURL.getURL() != null;
    final URLOpener opener = resolved? resolvedURL: originalURL;

    final HttpURLConnection connection = opener.connect(startPos, resolved);
    resolvedURL.setURL(getResolvedUrl(connection));

    InputStream in = connection.getInputStream();
    final Map<String, List<String>> headers = connection.getHeaderFields();
    if (isChunkedTransferEncoding(headers)) {
      fileLength = null;
    } else {
      long streamlength = getStreamLength(connection, headers);
      fileLength = startPos + streamlength;

      in = new BoundedInputStream(in, streamlength);
    }

    return in;
  }

  private static long getStreamLength(HttpURLConnection connection,
      Map<String, List<String>> headers) throws IOException {
    String cl = connection.getHeaderField(HttpHeaders.CONTENT_LENGTH);
    if (cl == null) {
      if (connection.getResponseCode() == HttpStatus.SC_PARTIAL_CONTENT) {
        cl = connection.getHeaderField(HttpHeaders.CONTENT_RANGE);
        return getLengthFromRange(cl);
      } else {
        throw new IOException(HttpHeaders.CONTENT_LENGTH + " is missing: "
            + headers);
      }
    }
    return Long.parseLong(cl);
  }

  private static long getLengthFromRange(String cl) throws IOException {
    try {

      String[] str = cl.substring(6).split("[-/]");
      return Long.parseLong(str[1]) - Long.parseLong(str[0]) + 1;
    } catch (Exception e) {
      throw new IOException(
          "failed to get content length by parsing the content range: " + cl
              + " " + e.getMessage());
    }
  }

  private static boolean isChunkedTransferEncoding(
      final Map<String, List<String>> headers) {
    return contains(headers, HttpHeaders.TRANSFER_ENCODING, "chunked")
        || contains(headers, HttpHeaders.TE, "chunked");
  }

  private static boolean contains(final Map<String, List<String>> headers,
      final String key, final String value) {
    final List<String> values = headers.get(key);
    if (values != null) {
      for(String v : values) {
        for(final StringTokenizer t = new StringTokenizer(v, ",");
            t.hasMoreTokens(); ) {
          if (value.equalsIgnoreCase(t.nextToken())) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private int update(final int n) throws IOException {
    if (n != -1) {
      currentPos += n;
    } else if (fileLength != null && currentPos < fileLength) {
      throw new IOException("Got EOF but currentPos = " + currentPos
          + " < filelength = " + fileLength);
    }
    return n;
  }

  @Override
  public int read() throws IOException {
    final int b = getInputStream().read();
    update((b == -1) ? -1 : 1);
    return b;
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    return update(getInputStream().read(b, off, len));
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos != currentPos) {
      startPos = pos;
      currentPos = pos;
      if (status != StreamStatus.CLOSED) {
        status = StreamStatus.SEEK;
      }
    }
  }

  @Override
  public long getPos() throws IOException {
    return currentPos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
      in = null;
    }
    status = StreamStatus.CLOSED;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/JsonUtilClient.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/JsonUtilClient.java
package org.apache.hadoop.hdfs.web;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.MD5MD5CRC32CastagnoliFileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.FsPermissionExtension;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class JsonUtilClient {
  static final DatanodeInfo[] EMPTY_DATANODE_INFO_ARRAY = {};

  static RemoteException toRemoteException(final Map<?, ?> json) {
    final Map<?, ?> m = (Map<?, ?>)json.get(RemoteException.class.getSimpleName());
    final String message = (String)m.get("message");
    final String javaClassName = (String)m.get("javaClassName");
    return new RemoteException(javaClassName, message);
  }

  static Token<? extends TokenIdentifier> toToken(
      final Map<?, ?> m) throws IOException {
    if (m == null) {
      return null;
    }

    final Token<DelegationTokenIdentifier> token
        = new Token<>();
    token.decodeFromUrlString((String)m.get("urlString"));
    return token;
  }

  @SuppressWarnings("unchecked")
  static Token<BlockTokenIdentifier> toBlockToken(
      final Map<?, ?> m) throws IOException {
    return (Token<BlockTokenIdentifier>)toToken(m);
  }

  static FsPermission toFsPermission(
      final String s, Boolean aclBit, Boolean encBit) {
    FsPermission perm = new FsPermission(Short.parseShort(s, 8));
    final boolean aBit = (aclBit != null) ? aclBit : false;
    final boolean eBit = (encBit != null) ? encBit : false;
    if (aBit || eBit) {
      return new FsPermissionExtension(perm, aBit, eBit);
    } else {
      return perm;
    }
  }

  static HdfsFileStatus toFileStatus(final Map<?, ?> json, boolean includesType) {
    if (json == null) {
      return null;
    }

    final Map<?, ?> m = includesType ?
        (Map<?, ?>)json.get(FileStatus.class.getSimpleName()) : json;
    final String localName = (String) m.get("pathSuffix");
    final WebHdfsConstants.PathType type = WebHdfsConstants.PathType.valueOf((String) m.get("type"));
    final byte[] symlink = type != WebHdfsConstants.PathType.SYMLINK? null
        : DFSUtilClient.string2Bytes((String) m.get("symlink"));

    final long len = ((Number) m.get("length")).longValue();
    final String owner = (String) m.get("owner");
    final String group = (String) m.get("group");
    final FsPermission permission = toFsPermission((String) m.get("permission"),
                                                   (Boolean) m.get("aclBit"),
                                                   (Boolean) m.get("encBit"));
    final long aTime = ((Number) m.get("accessTime")).longValue();
    final long mTime = ((Number) m.get("modificationTime")).longValue();
    final long blockSize = ((Number) m.get("blockSize")).longValue();
    final short replication = ((Number) m.get("replication")).shortValue();
    final long fileId = m.containsKey("fileId") ?
        ((Number) m.get("fileId")).longValue() : HdfsConstantsClient.GRANDFATHER_INODE_ID;
    final int childrenNum = getInt(m, "childrenNum", -1);
    final byte storagePolicy = m.containsKey("storagePolicy") ?
        (byte) ((Number) m.get("storagePolicy")).longValue() :
        HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
    return new HdfsFileStatus(len, type == WebHdfsConstants.PathType.DIRECTORY, replication,
        blockSize, mTime, aTime, permission, owner, group,
        symlink, DFSUtilClient.string2Bytes(localName),
        fileId, childrenNum, null,
        storagePolicy);
  }

  static ExtendedBlock toExtendedBlock(final Map<?, ?> m) {
    if (m == null) {
      return null;
    }

    final String blockPoolId = (String)m.get("blockPoolId");
    final long blockId = ((Number) m.get("blockId")).longValue();
    final long numBytes = ((Number) m.get("numBytes")).longValue();
    final long generationStamp =
        ((Number) m.get("generationStamp")).longValue();
    return new ExtendedBlock(blockPoolId, blockId, numBytes, generationStamp);
  }

  static int getInt(Map<?, ?> m, String key, final int defaultValue) {
    Object value = m.get(key);
    if (value == null) {
      return defaultValue;
    }
    return ((Number) value).intValue();
  }

  static long getLong(Map<?, ?> m, String key, final long defaultValue) {
    Object value = m.get(key);
    if (value == null) {
      return defaultValue;
    }
    return ((Number) value).longValue();
  }

  static String getString(
      Map<?, ?> m, String key, final String defaultValue) {
    Object value = m.get(key);
    if (value == null) {
      return defaultValue;
    }
    return (String) value;
  }

  static List<?> getList(Map<?, ?> m, String key) {
    Object list = m.get(key);
    if (list instanceof List<?>) {
      return (List<?>) list;
    } else {
      return null;
    }
  }

  static DatanodeInfo toDatanodeInfo(final Map<?, ?> m)
    throws IOException {
    if (m == null) {
      return null;
    }


    String ipAddr = getString(m, "ipAddr", null);
    int xferPort = getInt(m, "xferPort", -1);
    if (ipAddr == null) {
      String name = getString(m, "name", null);
      if (name != null) {
        int colonIdx = name.indexOf(':');
        if (colonIdx > 0) {
          ipAddr = name.substring(0, colonIdx);
          xferPort = Integer.parseInt(name.substring(colonIdx +1));
        } else {
          throw new IOException(
              "Invalid value in server response: name=[" + name + "]");
        }
      } else {
        throw new IOException(
            "Missing both 'ipAddr' and 'name' in server response.");
      }
    }

    if (xferPort == -1) {
      throw new IOException(
          "Invalid or missing 'xferPort' in server response.");
    }

    return new DatanodeInfo(
        ipAddr,
        (String)m.get("hostName"),
        (String)m.get("storageID"),
        xferPort,
        ((Number) m.get("infoPort")).intValue(),
        getInt(m, "infoSecurePort", 0),
        ((Number) m.get("ipcPort")).intValue(),

        getLong(m, "capacity", 0l),
        getLong(m, "dfsUsed", 0l),
        getLong(m, "remaining", 0l),
        getLong(m, "blockPoolUsed", 0l),
        getLong(m, "cacheCapacity", 0l),
        getLong(m, "cacheUsed", 0l),
        getLong(m, "lastUpdate", 0l),
        getLong(m, "lastUpdateMonotonic", 0l),
        getInt(m, "xceiverCount", 0),
        getString(m, "networkLocation", ""),
        DatanodeInfo.AdminStates.valueOf(getString(m, "adminState", "NORMAL")));
  }

  static DatanodeInfo[] toDatanodeInfoArray(final List<?> objects)
      throws IOException {
    if (objects == null) {
      return null;
    } else if (objects.isEmpty()) {
      return EMPTY_DATANODE_INFO_ARRAY;
    } else {
      final DatanodeInfo[] array = new DatanodeInfo[objects.size()];
      int i = 0;
      for (Object object : objects) {
        array[i++] = toDatanodeInfo((Map<?, ?>) object);
      }
      return array;
    }
  }

  static LocatedBlock toLocatedBlock(final Map<?, ?> m) throws IOException {
    if (m == null) {
      return null;
    }

    final ExtendedBlock b = toExtendedBlock((Map<?, ?>)m.get("block"));
    final DatanodeInfo[] locations = toDatanodeInfoArray(
        getList(m, "locations"));
    final long startOffset = ((Number) m.get("startOffset")).longValue();
    final boolean isCorrupt = (Boolean)m.get("isCorrupt");
    final DatanodeInfo[] cachedLocations = toDatanodeInfoArray(
        getList(m, "cachedLocations"));

    final LocatedBlock locatedblock = new LocatedBlock(b, locations,
        null, null, startOffset, isCorrupt, cachedLocations);
    locatedblock.setBlockToken(toBlockToken((Map<?, ?>)m.get("blockToken")));
    return locatedblock;
  }

  static List<LocatedBlock> toLocatedBlockList(
      final List<?> objects) throws IOException {
    if (objects == null) {
      return null;
    } else if (objects.isEmpty()) {
      return Collections.emptyList();
    } else {
      final List<LocatedBlock> list = new ArrayList<>(objects.size());
      for (Object object : objects) {
        list.add(toLocatedBlock((Map<?, ?>) object));
      }
      return list;
    }
  }

  static ContentSummary toContentSummary(final Map<?, ?> json) {
    if (json == null) {
      return null;
    }

    final Map<?, ?> m = (Map<?, ?>)json.get(ContentSummary.class.getSimpleName());
    final long length = ((Number) m.get("length")).longValue();
    final long fileCount = ((Number) m.get("fileCount")).longValue();
    final long directoryCount = ((Number) m.get("directoryCount")).longValue();
    final long quota = ((Number) m.get("quota")).longValue();
    final long spaceConsumed = ((Number) m.get("spaceConsumed")).longValue();
    final long spaceQuota = ((Number) m.get("spaceQuota")).longValue();

    return new ContentSummary.Builder().length(length).fileCount(fileCount).
        directoryCount(directoryCount).quota(quota).spaceConsumed(spaceConsumed).
        spaceQuota(spaceQuota).build();
  }

  static MD5MD5CRC32FileChecksum toMD5MD5CRC32FileChecksum(
      final Map<?, ?> json) throws IOException {
    if (json == null) {
      return null;
    }

    final Map<?, ?> m = (Map<?, ?>)json.get(FileChecksum.class.getSimpleName());
    final String algorithm = (String)m.get("algorithm");
    final int length = ((Number) m.get("length")).intValue();
    final byte[] bytes = StringUtils.hexStringToByte((String) m.get("bytes"));

    final DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    final DataChecksum.Type crcType =
        MD5MD5CRC32FileChecksum.getCrcTypeFromAlgorithmName(algorithm);
    final MD5MD5CRC32FileChecksum checksum;

    switch(crcType) {
      case CRC32:
        checksum = new MD5MD5CRC32GzipFileChecksum();
        break;
      case CRC32C:
        checksum = new MD5MD5CRC32CastagnoliFileChecksum();
        break;
      default:
        throw new IOException("Unknown algorithm: " + algorithm);
    }
    checksum.readFields(in);

    if (!checksum.getAlgorithmName().equals(algorithm)) {
      throw new IOException("Algorithm not matched. Expected " + algorithm
          + ", Received " + checksum.getAlgorithmName());
    }
    if (length != checksum.getLength()) {
      throw new IOException("Length not matched: length=" + length
          + ", checksum.getLength()=" + checksum.getLength());
    }

    return checksum;
  }

  static AclStatus toAclStatus(final Map<?, ?> json) {
    if (json == null) {
      return null;
    }

    final Map<?, ?> m = (Map<?, ?>) json.get(AclStatus.class.getSimpleName());

    AclStatus.Builder aclStatusBuilder = new AclStatus.Builder();
    aclStatusBuilder.owner((String) m.get("owner"));
    aclStatusBuilder.group((String) m.get("group"));
    aclStatusBuilder.stickyBit((Boolean) m.get("stickyBit"));
    String permString = (String) m.get("permission");
    if (permString != null) {
      final FsPermission permission = toFsPermission(permString,
          (Boolean) m.get("aclBit"), (Boolean) m.get("encBit"));
      aclStatusBuilder.setPermission(permission);
    }
    final List<?> entries = (List<?>) m.get("entries");

    List<AclEntry> aclEntryList = new ArrayList<>();
    for (Object entry : entries) {
      AclEntry aclEntry = AclEntry.parseAclEntry((String) entry, true);
      aclEntryList.add(aclEntry);
    }
    aclStatusBuilder.addEntries(aclEntryList);
    return aclStatusBuilder.build();
  }

  static byte[] getXAttr(final Map<?, ?> json, final String name)
      throws IOException {
    if (json == null) {
      return null;
    }

    Map<String, byte[]> xAttrs = toXAttrs(json);
    if (xAttrs != null) {
      return xAttrs.get(name);
    }

    return null;
  }

  static Map<String, byte[]> toXAttrs(final Map<?, ?> json)
      throws IOException {
    if (json == null) {
      return null;
    }
    return toXAttrMap(getList(json, "XAttrs"));
  }

  static List<String> toXAttrNames(final Map<?, ?> json)
      throws IOException {
    if (json == null) {
      return null;
    }

    final String namesInJson = (String) json.get("XAttrNames");
    ObjectReader reader = new ObjectMapper().reader(List.class);
    final List<Object> xattrs = reader.readValue(namesInJson);
    final List<String> names =
      Lists.newArrayListWithCapacity(json.keySet().size());

    for (Object xattr : xattrs) {
      names.add((String) xattr);
    }
    return names;
  }

  static Map<String, byte[]> toXAttrMap(final List<?> objects)
      throws IOException {
    if (objects == null) {
      return null;
    } else if (objects.isEmpty()) {
      return Maps.newHashMap();
    } else {
      final Map<String, byte[]> xAttrs = Maps.newHashMap();
      for (Object object : objects) {
        Map<?, ?> m = (Map<?, ?>) object;
        String name = (String) m.get("name");
        String value = (String) m.get("value");
        xAttrs.put(name, decodeXAttrValue(value));
      }
      return xAttrs;
    }
  }

  static byte[] decodeXAttrValue(String value) throws IOException {
    if (value != null) {
      return XAttrCodec.decodeValue(value);
    } else {
      return new byte[0];
    }
  }

  @SuppressWarnings("unchecked")
  static Token<DelegationTokenIdentifier> toDelegationToken(
      final Map<?, ?> json) throws IOException {
    final Map<?, ?> m = (Map<?, ?>)json.get(Token.class.getSimpleName());
    return (Token<DelegationTokenIdentifier>) toToken(m);
  }

  static LocatedBlocks toLocatedBlocks(
      final Map<?, ?> json) throws IOException {
    if (json == null) {
      return null;
    }

    final Map<?, ?> m = (Map<?, ?>)json.get(LocatedBlocks.class.getSimpleName());
    final long fileLength = ((Number) m.get("fileLength")).longValue();
    final boolean isUnderConstruction = (Boolean)m.get("isUnderConstruction");
    final List<LocatedBlock> locatedBlocks = toLocatedBlockList(
        getList(m, "locatedBlocks"));
    final LocatedBlock lastLocatedBlock = toLocatedBlock(
        (Map<?, ?>) m.get("lastLocatedBlock"));
    final boolean isLastBlockComplete = (Boolean)m.get("isLastBlockComplete");
    return new LocatedBlocks(fileLength, isUnderConstruction, locatedBlocks,
        lastLocatedBlock, isLastBlockComplete, null);
  }

}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/KerberosUgiAuthenticator.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/KerberosUgiAuthenticator.java
package org.apache.hadoop.hdfs.web;

import java.io.IOException;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;

public class KerberosUgiAuthenticator extends KerberosAuthenticator {
  @Override
  protected Authenticator getFallBackAuthenticator() {
    return new PseudoAuthenticator() {
      @Override
      protected String getUserName() {
        try {
          return UserGroupInformation.getLoginUser().getUserName();
        } catch (IOException e) {
          throw new SecurityException("Failed to obtain current username", e);
        }
      }
    };
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/SWebHdfsFileSystem.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/SWebHdfsFileSystem.java
package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.io.Text;

import com.google.common.annotations.VisibleForTesting;

public class SWebHdfsFileSystem extends WebHdfsFileSystem {

  @Override
  public String getScheme() {
    return WebHdfsConstants.SWEBHDFS_SCHEME;
  }

  @Override
  protected String getTransportScheme() {
    return "https";
  }

  @Override
  protected Text getTokenKind() {
    return WebHdfsConstants.SWEBHDFS_TOKEN_KIND;
  }

  @VisibleForTesting
  @Override
  public int getDefaultPort() {
    return getConf().getInt(HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY,
        HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_PORT_DEFAULT);
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/TokenAspect.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/TokenAspect.java
package org.apache.hadoop.hdfs.web;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegationTokenRenewer;
import org.apache.hadoop.fs.DelegationTokenRenewer.Renewable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;

import com.google.common.annotations.VisibleForTesting;

final class TokenAspect<T extends FileSystem & Renewable> {
  @InterfaceAudience.Private
  public static class TokenManager extends TokenRenewer {

    @Override
    public void cancel(Token<?> token, Configuration conf) throws IOException {
      getInstance(token, conf).cancelDelegationToken(token);
    }

    @Override
    public boolean handleKind(Text kind) {
      return kind.equals(WebHdfsConstants.HFTP_TOKEN_KIND)
          || kind.equals(WebHdfsConstants.HSFTP_TOKEN_KIND)
          || kind.equals(WebHdfsConstants.WEBHDFS_TOKEN_KIND)
          || kind.equals(WebHdfsConstants.SWEBHDFS_TOKEN_KIND);
    }

    @Override
    public boolean isManaged(Token<?> token) throws IOException {
      return true;
    }

    @Override
    public long renew(Token<?> token, Configuration conf) throws IOException {
      return getInstance(token, conf).renewDelegationToken(token);
    }

    private TokenManagementDelegator getInstance(Token<?> token,
                                                 Configuration conf)
            throws IOException {
      final URI uri;
      final String scheme = getSchemeByKind(token.getKind());
      if (HAUtilClient.isTokenForLogicalUri(token)) {
        uri = HAUtilClient.getServiceUriFromToken(scheme, token);
      } else {
        final InetSocketAddress address = SecurityUtil.getTokenServiceAddr
                (token);
        uri = URI.create(scheme + "://" + NetUtils.getHostPortString(address));
      }
      return (TokenManagementDelegator) FileSystem.get(uri, conf);
    }

    private static String getSchemeByKind(Text kind) {
      if (kind.equals(WebHdfsConstants.HFTP_TOKEN_KIND)) {
        return WebHdfsConstants.HFTP_SCHEME;
      } else if (kind.equals(WebHdfsConstants.HSFTP_TOKEN_KIND)) {
        return WebHdfsConstants.HSFTP_SCHEME;
      } else if (kind.equals(WebHdfsConstants.WEBHDFS_TOKEN_KIND)) {
        return WebHdfsConstants.WEBHDFS_SCHEME;
      } else if (kind.equals(WebHdfsConstants.SWEBHDFS_TOKEN_KIND)) {
        return WebHdfsConstants.SWEBHDFS_SCHEME;
      } else {
        throw new IllegalArgumentException("Unsupported scheme");
      }
    }
  }

  private static class DTSelecorByKind extends
      AbstractDelegationTokenSelector<DelegationTokenIdentifier> {
    public DTSelecorByKind(final Text kind) {
      super(kind);
    }
  }

  interface TokenManagementDelegator {
    void cancelDelegationToken(final Token<?> token) throws IOException;
    long renewDelegationToken(final Token<?> token) throws IOException;
  }

  private DelegationTokenRenewer.RenewAction<?> action;
  private DelegationTokenRenewer dtRenewer = null;
  private final DTSelecorByKind dtSelector;
  private final T fs;
  private boolean hasInitedToken;
  private final Log LOG;
  private final Text serviceName;

  TokenAspect(T fs, final Text serviceName, final Text kind) {
    this.LOG = LogFactory.getLog(fs.getClass());
    this.fs = fs;
    this.dtSelector = new DTSelecorByKind(kind);
    this.serviceName = serviceName;
  }

  synchronized void ensureTokenInitialized() throws IOException {
    if (!hasInitedToken || (action != null && !action.isValid())) {
      Token<?> token = fs.getDelegationToken(null);
      if (token != null) {
        fs.setDelegationToken(token);
        addRenewAction(fs);
        if(LOG.isDebugEnabled()) {
          LOG.debug("Created new DT for " + token.getService());
        }
      }
      hasInitedToken = true;
    }
  }

  public synchronized void reset() {
    hasInitedToken = false;
  }

  synchronized void initDelegationToken(UserGroupInformation ugi) {
    Token<?> token = selectDelegationToken(ugi);
    if (token != null) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Found existing DT for " + token.getService());
      }
      fs.setDelegationToken(token);
      hasInitedToken = true;
    }
  }

  synchronized void removeRenewAction() throws IOException {
    if (dtRenewer != null) {
      dtRenewer.removeRenewAction(fs);
    }
  }

  @VisibleForTesting
  Token<DelegationTokenIdentifier> selectDelegationToken(
      UserGroupInformation ugi) {
    return dtSelector.selectToken(serviceName, ugi.getTokens());
  }

  private synchronized void addRenewAction(final T webhdfs) {
    if (dtRenewer == null) {
      dtRenewer = DelegationTokenRenewer.getInstance();
    }

    action = dtRenewer.addRenewAction(webhdfs);
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/URLConnectionFactory.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/URLConnectionFactory.java

package org.apache.hadoop.hdfs.web;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.security.GeneralSecurityException;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.ssl.SSLFactory;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.LimitedPrivate({ "HDFS" })
@InterfaceStability.Unstable
public class URLConnectionFactory {
  private static final Log LOG = LogFactory.getLog(URLConnectionFactory.class);

  public final static int DEFAULT_SOCKET_TIMEOUT = 1 * 60 * 1000; // 1 minute
  private final ConnectionConfigurator connConfigurator;

  private static final ConnectionConfigurator DEFAULT_TIMEOUT_CONN_CONFIGURATOR = new ConnectionConfigurator() {
    @Override
    public HttpURLConnection configure(HttpURLConnection conn)
        throws IOException {
      URLConnectionFactory.setTimeouts(conn, DEFAULT_SOCKET_TIMEOUT);
      return conn;
    }
  };

  public static final URLConnectionFactory DEFAULT_SYSTEM_CONNECTION_FACTORY = new URLConnectionFactory(
      DEFAULT_TIMEOUT_CONN_CONFIGURATOR);

  public static URLConnectionFactory newDefaultURLConnectionFactory(Configuration conf) {
    ConnectionConfigurator conn = null;
    try {
      conn = newSslConnConfigurator(DEFAULT_SOCKET_TIMEOUT, conf);
    } catch (Exception e) {
      LOG.debug(
          "Cannot load customized ssl related configuration. Fallback to system-generic settings.",
          e);
      conn = DEFAULT_TIMEOUT_CONN_CONFIGURATOR;
    }
    return new URLConnectionFactory(conn);
  }

  @VisibleForTesting
  URLConnectionFactory(ConnectionConfigurator connConfigurator) {
    this.connConfigurator = connConfigurator;
  }

  private static ConnectionConfigurator newSslConnConfigurator(final int timeout,
      Configuration conf) throws IOException, GeneralSecurityException {
    final SSLFactory factory;
    final SSLSocketFactory sf;
    final HostnameVerifier hv;

    factory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    factory.init();
    sf = factory.createSSLSocketFactory();
    hv = factory.getHostnameVerifier();

    return new ConnectionConfigurator() {
      @Override
      public HttpURLConnection configure(HttpURLConnection conn)
          throws IOException {
        if (conn instanceof HttpsURLConnection) {
          HttpsURLConnection c = (HttpsURLConnection) conn;
          c.setSSLSocketFactory(sf);
          c.setHostnameVerifier(hv);
        }
        URLConnectionFactory.setTimeouts(conn, timeout);
        return conn;
      }
    };
  }

  public URLConnection openConnection(URL url) throws IOException {
    try {
      return openConnection(url, false);
    } catch (AuthenticationException e) {
      return null;
    }
  }

  public URLConnection openConnection(URL url, boolean isSpnego)
      throws IOException, AuthenticationException {
    if (isSpnego) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("open AuthenticatedURL connection" + url);
      }
      UserGroupInformation.getCurrentUser().checkTGTAndReloginFromKeytab();
      final AuthenticatedURL.Token authToken = new AuthenticatedURL.Token();
      return new AuthenticatedURL(new KerberosUgiAuthenticator(),
          connConfigurator).openConnection(url, authToken);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("open URL connection");
      }
      URLConnection connection = url.openConnection();
      if (connection instanceof HttpURLConnection) {
        connConfigurator.configure((HttpURLConnection) connection);
      }
      return connection;
    }
  }

  private static void setTimeouts(URLConnection connection, int socketTimeout) {
    connection.setConnectTimeout(socketTimeout);
    connection.setReadTimeout(socketTimeout);
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsConstants.java

@InterfaceAudience.Private
public class WebHdfsConstants {
  public static final String HFTP_SCHEME = "hftp";
  public static final Text HFTP_TOKEN_KIND = new Text("HFTP delegation");
  public static final Text HSFTP_TOKEN_KIND = new Text("HSFTP delegation");
  public static final String HSFTP_SCHEME = "hsftp";
  public static final String WEBHDFS_SCHEME = "webhdfs";
  public static final String SWEBHDFS_SCHEME = "swebhdfs";
  public static final Text WEBHDFS_TOKEN_KIND = new Text("WEBHDFS delegation");

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsFileSystem.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsFileSystem.java
res * Licensed to the Apache Software Foundation (ASF) under one

package org.apache.hadoop.hdfs.web;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.DelegationTokenRenewer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.resources.*;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam.Op;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class WebHdfsFileSystem extends FileSystem
    implements DelegationTokenRenewer.Renewable, TokenAspect.TokenManagementDelegator {
  public static final Log LOG = LogFactory.getLog(WebHdfsFileSystem.class);
  public static final int VERSION = 1;
  public static final String PATH_PREFIX = "/" + WebHdfsConstants.WEBHDFS_SCHEME + "/v" + VERSION;

  protected URLConnectionFactory connectionFactory;

  @VisibleForTesting
  public static final String CANT_FALLBACK_TO_INSECURE_MSG =
      "The client is configured to only allow connecting to secure cluster";

  private boolean canRefreshDelegationToken;

  private UserGroupInformation ugi;
  private URI uri;
  private Token<?> delegationToken;
  protected Text tokenServiceName;
  private RetryPolicy retryPolicy = null;
  private Path workingDir;
  private InetSocketAddress nnAddrs[];
  private int currentNNAddrIndex;
  private boolean disallowFallbackToInsecureCluster;

  @Override
  public String getScheme() {
    return WebHdfsConstants.WEBHDFS_SCHEME;
  }

  protected String getTransportScheme() {
    return "http";
  }

  protected Text getTokenKind() {
    return WebHdfsConstants.WEBHDFS_TOKEN_KIND;
  }

  @Override
  public synchronized void initialize(URI uri, Configuration conf
      ) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);
    UserParam.setUserPattern(conf.get(
        HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY,
        HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_DEFAULT));

    connectionFactory = URLConnectionFactory
        .newDefaultURLConnectionFactory(conf);

    ugi = UserGroupInformation.getCurrentUser();
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.nnAddrs = resolveNNAddr();

    boolean isHA = HAUtilClient.isClientFailoverConfigured(conf, this.uri);
    boolean isLogicalUri = isHA && HAUtilClient.isLogicalUri(conf, this.uri);
    this.tokenServiceName = isLogicalUri ?
        HAUtilClient.buildTokenServiceForLogicalUri(uri, getScheme())
        : SecurityUtil.buildTokenService(getCanonicalUri());

    if (!isHA) {
      this.retryPolicy =
          RetryUtils.getDefaultRetryPolicy(
              conf,
              HdfsClientConfigKeys.HttpClient.RETRY_POLICY_ENABLED_KEY,
              HdfsClientConfigKeys.HttpClient.RETRY_POLICY_ENABLED_DEFAULT,
              HdfsClientConfigKeys.HttpClient.RETRY_POLICY_SPEC_KEY,
              HdfsClientConfigKeys.HttpClient.RETRY_POLICY_SPEC_DEFAULT,
              HdfsConstantsClient.SAFEMODE_EXCEPTION_CLASS_NAME);
    } else {

      int maxFailoverAttempts = conf.getInt(
          HdfsClientConfigKeys.HttpClient.FAILOVER_MAX_ATTEMPTS_KEY,
          HdfsClientConfigKeys.HttpClient.FAILOVER_MAX_ATTEMPTS_DEFAULT);
      int maxRetryAttempts = conf.getInt(
          HdfsClientConfigKeys.HttpClient.RETRY_MAX_ATTEMPTS_KEY,
          HdfsClientConfigKeys.HttpClient.RETRY_MAX_ATTEMPTS_DEFAULT);
      int failoverSleepBaseMillis = conf.getInt(
          HdfsClientConfigKeys.HttpClient.FAILOVER_SLEEPTIME_BASE_KEY,
          HdfsClientConfigKeys.HttpClient.FAILOVER_SLEEPTIME_BASE_DEFAULT);
      int failoverSleepMaxMillis = conf.getInt(
          HdfsClientConfigKeys.HttpClient.FAILOVER_SLEEPTIME_MAX_KEY,
          HdfsClientConfigKeys.HttpClient.FAILOVER_SLEEPTIME_MAX_DEFAULT);

      this.retryPolicy = RetryPolicies
          .failoverOnNetworkException(RetryPolicies.TRY_ONCE_THEN_FAIL,
              maxFailoverAttempts, maxRetryAttempts, failoverSleepBaseMillis,
              failoverSleepMaxMillis);
    }

    this.workingDir = getHomeDirectory();
    this.canRefreshDelegationToken = UserGroupInformation.isSecurityEnabled();
    this.disallowFallbackToInsecureCluster = !conf.getBoolean(
        CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
        CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
    this.delegationToken = null;
  }

  @Override
  public URI getCanonicalUri() {
    return super.getCanonicalUri();
  }

  public static boolean isEnabled(final Configuration conf, final Log log) {
    final boolean b = conf.getBoolean(
        HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY,
        HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_DEFAULT);
    return b;
  }

  TokenSelector<DelegationTokenIdentifier> tokenSelector =
      new AbstractDelegationTokenSelector<DelegationTokenIdentifier>(getTokenKind()){};

  protected synchronized Token<?> getDelegationToken() throws IOException {
    if (canRefreshDelegationToken && delegationToken == null) {
      Token<?> token = tokenSelector.selectToken(
          new Text(getCanonicalServiceName()), ugi.getTokens());
      if (token != null) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Using UGI token: " + token);
        }
        canRefreshDelegationToken = false;
      } else {
        token = getDelegationToken(null);
        if (token != null) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("Fetched new token: " + token);
          }
        } else { // security is disabled
          canRefreshDelegationToken = false;
        }
      }
      setDelegationToken(token);
    }
    return delegationToken;
  }

  @VisibleForTesting
  synchronized boolean replaceExpiredDelegationToken() throws IOException {
    boolean replaced = false;
    if (canRefreshDelegationToken) {
      Token<?> token = getDelegationToken(null);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Replaced expired token: " + token);
      }
      setDelegationToken(token);
      replaced = (token != null);
    }
    return replaced;
  }

  @Override
  @VisibleForTesting
  public int getDefaultPort() {
    return getConf().getInt(HdfsClientConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY,
        HdfsClientConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT);
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  protected URI canonicalizeUri(URI uri) {
    return NetUtils.getCanonicalUri(uri, getDefaultPort());
  }

  public static String getHomeDirectoryString(final UserGroupInformation ugi) {
    return "/user/" + ugi.getShortUserName();
  }

  @Override
  public Path getHomeDirectory() {
    return makeQualified(new Path(getHomeDirectoryString(ugi)));
  }

  @Override
  public synchronized Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public synchronized void setWorkingDirectory(final Path dir) {
    String result = makeAbsolute(dir).toUri().getPath();
    if (!DFSUtilClient.isValidName(result)) {
      throw new IllegalArgumentException("Invalid DFS directory name " +
                                         result);
    }
    workingDir = makeAbsolute(dir);
  }

  private Path makeAbsolute(Path f) {
    return f.isAbsolute()? f: new Path(workingDir, f);
  }

  static Map<?, ?> jsonParse(final HttpURLConnection c, final boolean useErrorStream
      ) throws IOException {
    if (c.getContentLength() == 0) {
      return null;
    }
    final InputStream in = useErrorStream? c.getErrorStream(): c.getInputStream();
    if (in == null) {
      throw new IOException("The " + (useErrorStream? "error": "input") + " stream is null.");
    }
    try {
      final String contentType = c.getContentType();
      if (contentType != null) {
        final MediaType parsed = MediaType.valueOf(contentType);
        if (!MediaType.APPLICATION_JSON_TYPE.isCompatible(parsed)) {
          throw new IOException("Content-Type \"" + contentType
              + "\" is incompatible with \"" + MediaType.APPLICATION_JSON
              + "\" (parsed=\"" + parsed + "\")");
        }
      }
      ObjectMapper mapper = new ObjectMapper();
      return mapper.reader(Map.class).readValue(in);
    } finally {
      in.close();
    }
  }

  private static Map<?, ?> validateResponse(final HttpOpParam.Op op,
      final HttpURLConnection conn, boolean unwrapException) throws IOException {
    final int code = conn.getResponseCode();
    if (code == HttpURLConnection.HTTP_UNAUTHORIZED) {
      throw new AccessControlException(conn.getResponseMessage());
    }
    if (code != op.getExpectedHttpResponseCode()) {
      final Map<?, ?> m;
      try {
        m = jsonParse(conn, true);
      } catch(Exception e) {
        throw new IOException("Unexpected HTTP response: code=" + code + " != "
            + op.getExpectedHttpResponseCode() + ", " + op.toQueryString()
            + ", message=" + conn.getResponseMessage(), e);
      }

      if (m == null) {
        throw new IOException("Unexpected HTTP response: code=" + code + " != "
            + op.getExpectedHttpResponseCode() + ", " + op.toQueryString()
            + ", message=" + conn.getResponseMessage());
      } else if (m.get(RemoteException.class.getSimpleName()) == null) {
        return m;
      }

      IOException re = JsonUtilClient.toRemoteException(m);
      if (re.getMessage() != null && re.getMessage().startsWith(
          SecurityUtil.FAILED_TO_GET_UGI_MSG_HEADER)) {
        String[] parts = re.getMessage().split(":\\s+", 3);
        re = new RemoteException(parts[1], parts[2]);
        re = ((RemoteException)re).unwrapRemoteException(InvalidToken.class);
      }
      throw unwrapException? toIOException(re): re;
    }
    return null;
  }

  private static IOException toIOException(Exception e) {
    if (!(e instanceof IOException)) {
      return new IOException(e);
    }

    final IOException ioe = (IOException)e;
    if (!(ioe instanceof RemoteException)) {
      return ioe;
    }

    return ((RemoteException)ioe).unwrapRemoteException();
  }

  private synchronized InetSocketAddress getCurrentNNAddr() {
    return nnAddrs[currentNNAddrIndex];
  }

  private synchronized void resetStateToFailOver() {
    currentNNAddrIndex = (currentNNAddrIndex + 1) % nnAddrs.length;
  }

  private URL getNamenodeURL(String path, String query) throws IOException {
    InetSocketAddress nnAddr = getCurrentNNAddr();
    final URL url = new URL(getTransportScheme(), nnAddr.getHostName(),
          nnAddr.getPort(), path + '?' + query);
    if (LOG.isTraceEnabled()) {
      LOG.trace("url=" + url);
    }
    return url;
  }

  Param<?,?>[] getAuthParameters(final HttpOpParam.Op op) throws IOException {
    List<Param<?,?>> authParams = Lists.newArrayList();
    Token<?> token = null;
    if (!op.getRequireAuth()) {
      token = getDelegationToken();
    }
    if (token != null) {
      authParams.add(new DelegationParam(token.encodeToUrlString()));
    } else {
      UserGroupInformation userUgi = ugi;
      UserGroupInformation realUgi = userUgi.getRealUser();
      if (realUgi != null) { // proxy user
        authParams.add(new DoAsParam(userUgi.getShortUserName()));
        userUgi = realUgi;
      }
      authParams.add(new UserParam(userUgi.getShortUserName()));
    }
    return authParams.toArray(new Param<?,?>[0]);
  }

  URL toUrl(final HttpOpParam.Op op, final Path fspath,
      final Param<?,?>... parameters) throws IOException {
    final String path = PATH_PREFIX
        + (fspath == null? "/": makeQualified(fspath).toUri().getRawPath());
    final String query = op.toQueryString()
        + Param.toSortedString("&", getAuthParameters(op))
        + Param.toSortedString("&", parameters);
    final URL url = getNamenodeURL(path, query);
    if (LOG.isTraceEnabled()) {
      LOG.trace("url=" + url);
    }
    return url;
  }

  abstract class AbstractRunner<T> {
    abstract protected URL getUrl() throws IOException;

    protected final HttpOpParam.Op op;
    private final boolean redirected;
    protected ExcludeDatanodesParam excludeDatanodes = new ExcludeDatanodesParam("");

    private boolean checkRetry;

    protected AbstractRunner(final HttpOpParam.Op op, boolean redirected) {
      this.op = op;
      this.redirected = redirected;
    }

    T run() throws IOException {
      UserGroupInformation connectUgi = ugi.getRealUser();
      if (connectUgi == null) {
        connectUgi = ugi;
      }
      if (op.getRequireAuth()) {
        connectUgi.checkTGTAndReloginFromKeytab();
      }
      try {
        return connectUgi.doAs(
            new PrivilegedExceptionAction<T>() {
              @Override
              public T run() throws IOException {
                return runWithRetry();
              }
            });
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    private HttpURLConnection connect(URL url) throws IOException {
      String redirectHost = null;


      if (op.getRedirect() && !redirected) {
        final HttpOpParam.Op redirectOp =
            HttpOpParam.TemporaryRedirectOp.valueOf(op);
        final HttpURLConnection conn = connect(redirectOp, url);
        if (conn.getResponseCode() == op.getExpectedHttpResponseCode()) {
          return conn;
        }
        try {
          validateResponse(redirectOp, conn, false);
          url = new URL(conn.getHeaderField("Location"));
          redirectHost = url.getHost() + ":" + url.getPort();
        } finally {
          conn.disconnect();
        }
      }
      try {
        return connect(op, url);
      } catch (IOException ioe) {
        if (redirectHost != null) {
          if (excludeDatanodes.getValue() != null) {
            excludeDatanodes = new ExcludeDatanodesParam(redirectHost + ","
                + excludeDatanodes.getValue());
          } else {
            excludeDatanodes = new ExcludeDatanodesParam(redirectHost);
          }
        }
        throw ioe;
      }
    }

    private HttpURLConnection connect(final HttpOpParam.Op op, final URL url)
        throws IOException {
      final HttpURLConnection conn =
          (HttpURLConnection)connectionFactory.openConnection(url);
      final boolean doOutput = op.getDoOutput();
      conn.setRequestMethod(op.getType().toString());
      conn.setInstanceFollowRedirects(false);
      switch (op.getType()) {
        case POST:
        case PUT: {
          conn.setDoOutput(true);
          if (!doOutput) {
            conn.getOutputStream().close();
          } else {
            conn.setRequestProperty("Content-Type",
                MediaType.APPLICATION_OCTET_STREAM);
            conn.setChunkedStreamingMode(32 << 10); //32kB-chunk
          }
          break;
        }
        default: {
          conn.setDoOutput(doOutput);
          break;
        }
      }
      conn.connect();
      return conn;
    }

    private T runWithRetry() throws IOException {
      for(int retry = 0; ; retry++) {
        checkRetry = !redirected;
        final URL url = getUrl();
        try {
          final HttpURLConnection conn = connect(url);
          if (!op.getDoOutput()) {
            validateResponse(op, conn, false);
          }
          return getResponse(conn);
        } catch (AccessControlException ace) {
          throw ace;
        } catch (InvalidToken it) {
          if (op.getRequireAuth() || !replaceExpiredDelegationToken()) {
            throw it;
          }
        } catch (IOException ioe) {
          shouldRetry(ioe, retry);
        }
      }
    }

    private void shouldRetry(final IOException ioe, final int retry
        ) throws IOException {
      InetSocketAddress nnAddr = getCurrentNNAddr();
      if (checkRetry) {
        try {
          final RetryPolicy.RetryAction a = retryPolicy.shouldRetry(
              ioe, retry, 0, true);

          boolean isRetry = a.action == RetryPolicy.RetryAction.RetryDecision.RETRY;
          boolean isFailoverAndRetry =
              a.action == RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY;

          if (isRetry || isFailoverAndRetry) {
            LOG.info("Retrying connect to namenode: " + nnAddr
                + ". Already tried " + retry + " time(s); retry policy is "
                + retryPolicy + ", delay " + a.delayMillis + "ms.");

            if (isFailoverAndRetry) {
              resetStateToFailOver();
            }

            Thread.sleep(a.delayMillis);
            return;
          }
        } catch(Exception e) {
          LOG.warn("Original exception is ", ioe);
          throw toIOException(e);
        }
      }
      throw toIOException(ioe);
    }

    abstract T getResponse(HttpURLConnection conn) throws IOException;
  }

  abstract class AbstractFsPathRunner<T> extends AbstractRunner<T> {
    private final Path fspath;
    private final Param<?,?>[] parameters;

    AbstractFsPathRunner(final HttpOpParam.Op op, final Path fspath,
        Param<?,?>... parameters) {
      super(op, false);
      this.fspath = fspath;
      this.parameters = parameters;
    }

    AbstractFsPathRunner(final HttpOpParam.Op op, Param<?,?>[] parameters,
        final Path fspath) {
      super(op, false);
      this.fspath = fspath;
      this.parameters = parameters;
    }

    @Override
    protected URL getUrl() throws IOException {
      if (excludeDatanodes.getValue() != null) {
        Param<?, ?>[] tmpParam = new Param<?, ?>[parameters.length + 1];
        System.arraycopy(parameters, 0, tmpParam, 0, parameters.length);
        tmpParam[parameters.length] = excludeDatanodes;
        return toUrl(op, fspath, tmpParam);
      } else {
        return toUrl(op, fspath, parameters);
      }
    }
  }

  class FsPathRunner extends AbstractFsPathRunner<Void> {
    FsPathRunner(Op op, Path fspath, Param<?,?>... parameters) {
      super(op, fspath, parameters);
    }

    @Override
    Void getResponse(HttpURLConnection conn) throws IOException {
      return null;
    }
  }

  abstract class FsPathResponseRunner<T> extends AbstractFsPathRunner<T> {
    FsPathResponseRunner(final HttpOpParam.Op op, final Path fspath,
        Param<?,?>... parameters) {
      super(op, fspath, parameters);
    }

    FsPathResponseRunner(final HttpOpParam.Op op, Param<?,?>[] parameters,
        final Path fspath) {
      super(op, parameters, fspath);
    }

    @Override
    final T getResponse(HttpURLConnection conn) throws IOException {
      try {
        final Map<?,?> json = jsonParse(conn, false);
        if (json == null) {
          throw new IllegalStateException("Missing response");
        }
        return decodeResponse(json);
      } catch (IOException ioe) {
        throw ioe;
      } catch (Exception e) { // catch json parser errors
        final IOException ioe =
            new IOException("Response decoding failure: "+e.toString(), e);
        if (LOG.isDebugEnabled()) {
          LOG.debug(ioe);
        }
        throw ioe;
      } finally {
        conn.disconnect();
      }
    }

    abstract T decodeResponse(Map<?,?> json) throws IOException;
  }

  class FsPathBooleanRunner extends FsPathResponseRunner<Boolean> {
    FsPathBooleanRunner(Op op, Path fspath, Param<?,?>... parameters) {
      super(op, fspath, parameters);
    }

    @Override
    Boolean decodeResponse(Map<?,?> json) throws IOException {
      return (Boolean)json.get("boolean");
    }
  }

  class FsPathOutputStreamRunner extends AbstractFsPathRunner<FSDataOutputStream> {
    private final int bufferSize;

    FsPathOutputStreamRunner(Op op, Path fspath, int bufferSize,
        Param<?,?>... parameters) {
      super(op, fspath, parameters);
      this.bufferSize = bufferSize;
    }

    @Override
    FSDataOutputStream getResponse(final HttpURLConnection conn)
        throws IOException {
      return new FSDataOutputStream(new BufferedOutputStream(
          conn.getOutputStream(), bufferSize), statistics) {
        @Override
        public void close() throws IOException {
          try {
            super.close();
          } finally {
            try {
              validateResponse(op, conn, true);
            } finally {
              conn.disconnect();
            }
          }
        }
      };
    }
  }

  class FsPathConnectionRunner extends AbstractFsPathRunner<HttpURLConnection> {
    FsPathConnectionRunner(Op op, Path fspath, Param<?,?>... parameters) {
      super(op, fspath, parameters);
    }
    @Override
    HttpURLConnection getResponse(final HttpURLConnection conn)
        throws IOException {
      return conn;
    }
  }

  final class URLRunner extends AbstractRunner<HttpURLConnection> {
    private final URL url;
    @Override
    protected URL getUrl() {
      return url;
    }

    protected URLRunner(final HttpOpParam.Op op, final URL url, boolean redirected) {
      super(op, redirected);
      this.url = url;
    }

    @Override
    HttpURLConnection getResponse(HttpURLConnection conn) throws IOException {
      return conn;
    }
  }

  private FsPermission applyUMask(FsPermission permission) {
    if (permission == null) {
      permission = FsPermission.getDefault();
    }
    return permission.applyUMask(FsPermission.getUMask(getConf()));
  }

  private HdfsFileStatus getHdfsFileStatus(Path f) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETFILESTATUS;
    HdfsFileStatus status = new FsPathResponseRunner<HdfsFileStatus>(op, f) {
      @Override
      HdfsFileStatus decodeResponse(Map<?,?> json) {
        return JsonUtilClient.toFileStatus(json, true);
      }
    }.run();
    if (status == null) {
      throw new FileNotFoundException("File does not exist: " + f);
    }
    return status;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    statistics.incrementReadOps(1);
    return makeQualified(getHdfsFileStatus(f), f);
  }

  private FileStatus makeQualified(HdfsFileStatus f, Path parent) {
    return new FileStatus(f.getLen(), f.isDir(), f.getReplication(),
        f.getBlockSize(), f.getModificationTime(), f.getAccessTime(),
        f.getPermission(), f.getOwner(), f.getGroup(),
        f.isSymlink() ? new Path(f.getSymlink()) : null,
        f.getFullPath(parent).makeQualified(getUri(), getWorkingDirectory()));
  }

  @Override
  public AclStatus getAclStatus(Path f) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETACLSTATUS;
    AclStatus status = new FsPathResponseRunner<AclStatus>(op, f) {
      @Override
      AclStatus decodeResponse(Map<?,?> json) {
        return JsonUtilClient.toAclStatus(json);
      }
    }.run();
    if (status == null) {
      throw new FileNotFoundException("File does not exist: " + f);
    }
    return status;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.MKDIRS;
    return new FsPathBooleanRunner(op, f,
        new PermissionParam(applyUMask(permission))
    ).run();
  }

  public void createSymlink(Path destination, Path f, boolean createParent
      ) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.CREATESYMLINK;
    new FsPathRunner(op, f,
        new DestinationParam(makeQualified(destination).toUri().getPath()),
        new CreateParentParam(createParent)
    ).run();
  }

  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.RENAME;
    return new FsPathBooleanRunner(op, src,
        new DestinationParam(makeQualified(dst).toUri().getPath())
    ).run();
  }

  @SuppressWarnings("deprecation")
  @Override
  public void rename(final Path src, final Path dst,
      final Options.Rename... options) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.RENAME;
    new FsPathRunner(op, src,
        new DestinationParam(makeQualified(dst).toUri().getPath()),
        new RenameOptionSetParam(options)
    ).run();
  }

  @Override
  public void setXAttr(Path p, String name, byte[] value,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETXATTR;
    if (value != null) {
      new FsPathRunner(op, p, new XAttrNameParam(name), new XAttrValueParam(
          XAttrCodec.encodeValue(value, XAttrCodec.HEX)),
          new XAttrSetFlagParam(flag)).run();
    } else {
      new FsPathRunner(op, p, new XAttrNameParam(name),
          new XAttrSetFlagParam(flag)).run();
    }
  }

  @Override
  public byte[] getXAttr(Path p, final String name) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETXATTRS;
    return new FsPathResponseRunner<byte[]>(op, p, new XAttrNameParam(name),
        new XAttrEncodingParam(XAttrCodec.HEX)) {
      @Override
      byte[] decodeResponse(Map<?, ?> json) throws IOException {
        return JsonUtilClient.getXAttr(json, name);
      }
    }.run();
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path p) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETXATTRS;
    return new FsPathResponseRunner<Map<String, byte[]>>(op, p,
        new XAttrEncodingParam(XAttrCodec.HEX)) {
      @Override
      Map<String, byte[]> decodeResponse(Map<?, ?> json) throws IOException {
        return JsonUtilClient.toXAttrs(json);
      }
    }.run();
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path p, final List<String> names)
      throws IOException {
    Preconditions.checkArgument(names != null && !names.isEmpty(),
        "XAttr names cannot be null or empty.");
    Param<?,?>[] parameters = new Param<?,?>[names.size() + 1];
    for (int i = 0; i < parameters.length - 1; i++) {
      parameters[i] = new XAttrNameParam(names.get(i));
    }
    parameters[parameters.length - 1] = new XAttrEncodingParam(XAttrCodec.HEX);

    final HttpOpParam.Op op = GetOpParam.Op.GETXATTRS;
    return new FsPathResponseRunner<Map<String, byte[]>>(op, parameters, p) {
      @Override
      Map<String, byte[]> decodeResponse(Map<?, ?> json) throws IOException {
        return JsonUtilClient.toXAttrs(json);
      }
    }.run();
  }

  @Override
  public List<String> listXAttrs(Path p) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.LISTXATTRS;
    return new FsPathResponseRunner<List<String>>(op, p) {
      @Override
      List<String> decodeResponse(Map<?, ?> json) throws IOException {
        return JsonUtilClient.toXAttrNames(json);
      }
    }.run();
  }

  @Override
  public void removeXAttr(Path p, String name) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.REMOVEXATTR;
    new FsPathRunner(op, p, new XAttrNameParam(name)).run();
  }

  @Override
  public void setOwner(final Path p, final String owner, final String group
      ) throws IOException {
    if (owner == null && group == null) {
      throw new IOException("owner == null && group == null");
    }

    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETOWNER;
    new FsPathRunner(op, p,
        new OwnerParam(owner), new GroupParam(group)
    ).run();
  }

  @Override
  public void setPermission(final Path p, final FsPermission permission
      ) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETPERMISSION;
    new FsPathRunner(op, p,new PermissionParam(permission)).run();
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.MODIFYACLENTRIES;
    new FsPathRunner(op, path, new AclPermissionParam(aclSpec)).run();
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.REMOVEACLENTRIES;
    new FsPathRunner(op, path, new AclPermissionParam(aclSpec)).run();
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.REMOVEDEFAULTACL;
    new FsPathRunner(op, path).run();
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.REMOVEACL;
    new FsPathRunner(op, path).run();
  }

  @Override
  public void setAcl(final Path p, final List<AclEntry> aclSpec)
      throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETACL;
    new FsPathRunner(op, p, new AclPermissionParam(aclSpec)).run();
  }

  @Override
  public Path createSnapshot(final Path path, final String snapshotName)
      throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.CREATESNAPSHOT;
    Path spath = new FsPathResponseRunner<Path>(op, path,
        new SnapshotNameParam(snapshotName)) {
      @Override
      Path decodeResponse(Map<?,?> json) {
        return new Path((String) json.get(Path.class.getSimpleName()));
      }
    }.run();
    return spath;
  }

  @Override
  public void deleteSnapshot(final Path path, final String snapshotName)
      throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = DeleteOpParam.Op.DELETESNAPSHOT;
    new FsPathRunner(op, path, new SnapshotNameParam(snapshotName)).run();
  }

  @Override
  public void renameSnapshot(final Path path, final String snapshotOldName,
      final String snapshotNewName) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.RENAMESNAPSHOT;
    new FsPathRunner(op, path, new OldSnapshotNameParam(snapshotOldName),
        new SnapshotNameParam(snapshotNewName)).run();
  }

  @Override
  public boolean setReplication(final Path p, final short replication
     ) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETREPLICATION;
    return new FsPathBooleanRunner(op, p,
        new ReplicationParam(replication)
    ).run();
  }

  @Override
  public void setTimes(final Path p, final long mtime, final long atime
      ) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETTIMES;
    new FsPathRunner(op, p,
        new ModificationTimeParam(mtime),
        new AccessTimeParam(atime)
    ).run();
  }

  @Override
  public long getDefaultBlockSize() {
    return getConf().getLongBytes(HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY,
        HdfsClientConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
  }

  @Override
  public short getDefaultReplication() {
    return (short)getConf().getInt(HdfsClientConfigKeys.DFS_REPLICATION_KEY,
        HdfsClientConfigKeys.DFS_REPLICATION_DEFAULT);
  }

  @Override
  public void concat(final Path trg, final Path [] srcs) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PostOpParam.Op.CONCAT;
    new FsPathRunner(op, trg, new ConcatSourcesParam(srcs)).run();
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);

    final HttpOpParam.Op op = PutOpParam.Op.CREATE;
    return new FsPathOutputStreamRunner(op, f, bufferSize,
        new PermissionParam(applyUMask(permission)),
        new OverwriteParam(overwrite),
        new BufferSizeParam(bufferSize),
        new ReplicationParam(replication),
        new BlockSizeParam(blockSize)
    ).run();
  }

  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);

    final HttpOpParam.Op op = PostOpParam.Op.APPEND;
    return new FsPathOutputStreamRunner(op, f, bufferSize,
        new BufferSizeParam(bufferSize)
    ).run();
  }

  @Override
  public boolean truncate(Path f, long newLength) throws IOException {
    statistics.incrementWriteOps(1);

    final HttpOpParam.Op op = PostOpParam.Op.TRUNCATE;
    return new FsPathBooleanRunner(op, f, new NewLengthParam(newLength)).run();
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    final HttpOpParam.Op op = DeleteOpParam.Op.DELETE;
    return new FsPathBooleanRunner(op, f,
        new RecursiveParam(recursive)
    ).run();
  }

  @Override
  public FSDataInputStream open(final Path f, final int buffersize
      ) throws IOException {
    statistics.incrementReadOps(1);
    final HttpOpParam.Op op = GetOpParam.Op.OPEN;
    FsPathConnectionRunner runner =
        new FsPathConnectionRunner(op, f, new BufferSizeParam(buffersize));
    return new FSDataInputStream(new OffsetUrlInputStream(
        new UnresolvedUrlOpener(runner), new OffsetUrlOpener(null)));
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      if (canRefreshDelegationToken && delegationToken != null) {
        cancelDelegationToken(delegationToken);
      }
    } catch (IOException ioe) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Token cancel failed: " + ioe);
      }
    } finally {
      super.close();
    }
  }

  class UnresolvedUrlOpener extends ByteRangeInputStream.URLOpener {
    private final FsPathConnectionRunner runner;
    UnresolvedUrlOpener(FsPathConnectionRunner runner) {
      super(null);
      this.runner = runner;
    }

    @Override
    protected HttpURLConnection connect(long offset, boolean resolved)
        throws IOException {
      assert offset == 0;
      HttpURLConnection conn = runner.run();
      setURL(conn.getURL());
      return conn;
    }
  }

  class OffsetUrlOpener extends ByteRangeInputStream.URLOpener {
    OffsetUrlOpener(final URL url) {
      super(url);
    }

    @Override
    protected HttpURLConnection connect(final long offset,
        final boolean resolved) throws IOException {
      final URL offsetUrl = offset == 0L? url
          : new URL(url + "&" + new OffsetParam(offset));
      return new URLRunner(GetOpParam.Op.OPEN, offsetUrl, resolved).run();
    }
  }

  private static final String OFFSET_PARAM_PREFIX = OffsetParam.NAME + "=";

  static URL removeOffsetParam(final URL url) throws MalformedURLException {
    String query = url.getQuery();
    if (query == null) {
      return url;
    }
    final String lower = StringUtils.toLowerCase(query);
    if (!lower.startsWith(OFFSET_PARAM_PREFIX)
        && !lower.contains("&" + OFFSET_PARAM_PREFIX)) {
      return url;
    }

    StringBuilder b = null;
    for(final StringTokenizer st = new StringTokenizer(query, "&");
        st.hasMoreTokens();) {
      final String token = st.nextToken();
      if (!StringUtils.toLowerCase(token).startsWith(OFFSET_PARAM_PREFIX)) {
        if (b == null) {
          b = new StringBuilder("?").append(token);
        } else {
          b.append('&').append(token);
        }
      }
    }
    query = b == null? "": b.toString();

    final String urlStr = url.toString();
    return new URL(urlStr.substring(0, urlStr.indexOf('?')) + query);
  }

  static class OffsetUrlInputStream extends ByteRangeInputStream {
    OffsetUrlInputStream(UnresolvedUrlOpener o, OffsetUrlOpener r)
        throws IOException {
      super(o, r);
    }

    @Override
    protected URL getResolvedUrl(final HttpURLConnection connection
        ) throws MalformedURLException {
      return removeOffsetParam(connection.getURL());
    }
  }

  @Override
  public FileStatus[] listStatus(final Path f) throws IOException {
    statistics.incrementReadOps(1);

    final HttpOpParam.Op op = GetOpParam.Op.LISTSTATUS;
    return new FsPathResponseRunner<FileStatus[]>(op, f) {
      @Override
      FileStatus[] decodeResponse(Map<?,?> json) {
        final Map<?, ?> rootmap = (Map<?, ?>)json.get(FileStatus.class.getSimpleName() + "es");
        final List<?> array = JsonUtilClient.getList(rootmap,
                                                     FileStatus.class.getSimpleName());

        final FileStatus[] statuses = new FileStatus[array.size()];
        int i = 0;
        for (Object object : array) {
          final Map<?, ?> m = (Map<?, ?>) object;
          statuses[i++] = makeQualified(JsonUtilClient.toFileStatus(m, false), f);
        }
        return statuses;
      }
    }.run();
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(
      final String renewer) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETDELEGATIONTOKEN;
    Token<DelegationTokenIdentifier> token =
        new FsPathResponseRunner<Token<DelegationTokenIdentifier>>(
            op, null, new RenewerParam(renewer)) {
      @Override
      Token<DelegationTokenIdentifier> decodeResponse(Map<?,?> json)
          throws IOException {
        return JsonUtilClient.toDelegationToken(json);
      }
    }.run();
    if (token != null) {
      token.setService(tokenServiceName);
    } else {
      if (disallowFallbackToInsecureCluster) {
        throw new AccessControlException(CANT_FALLBACK_TO_INSECURE_MSG);
      }
    }
    return token;
  }

  @Override
  public synchronized Token<?> getRenewToken() {
    return delegationToken;
  }

  @Override
  public <T extends TokenIdentifier> void setDelegationToken(
      final Token<T> token) {
    synchronized (this) {
      delegationToken = token;
    }
  }

  @Override
  public synchronized long renewDelegationToken(final Token<?> token
      ) throws IOException {
    final HttpOpParam.Op op = PutOpParam.Op.RENEWDELEGATIONTOKEN;
    return new FsPathResponseRunner<Long>(op, null,
        new TokenArgumentParam(token.encodeToUrlString())) {
      @Override
      Long decodeResponse(Map<?,?> json) throws IOException {
        return ((Number) json.get("long")).longValue();
      }
    }.run();
  }

  @Override
  public synchronized void cancelDelegationToken(final Token<?> token
      ) throws IOException {
    final HttpOpParam.Op op = PutOpParam.Op.CANCELDELEGATIONTOKEN;
    new FsPathRunner(op, null,
        new TokenArgumentParam(token.encodeToUrlString())
    ).run();
  }

  @Override
  public BlockLocation[] getFileBlockLocations(final FileStatus status,
      final long offset, final long length) throws IOException {
    if (status == null) {
      return null;
    }
    return getFileBlockLocations(status.getPath(), offset, length);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(final Path p,
      final long offset, final long length) throws IOException {
    statistics.incrementReadOps(1);

    final HttpOpParam.Op op = GetOpParam.Op.GET_BLOCK_LOCATIONS;
    return new FsPathResponseRunner<BlockLocation[]>(op, p,
        new OffsetParam(offset), new LengthParam(length)) {
      @Override
      BlockLocation[] decodeResponse(Map<?,?> json) throws IOException {
        return DFSUtilClient.locatedBlocks2Locations(
            JsonUtilClient.toLocatedBlocks(json));
      }
    }.run();
  }

  @Override
  public void access(final Path path, final FsAction mode) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.CHECKACCESS;
    new FsPathRunner(op, path, new FsActionParam(mode)).run();
  }

  @Override
  public ContentSummary getContentSummary(final Path p) throws IOException {
    statistics.incrementReadOps(1);

    final HttpOpParam.Op op = GetOpParam.Op.GETCONTENTSUMMARY;
    return new FsPathResponseRunner<ContentSummary>(op, p) {
      @Override
      ContentSummary decodeResponse(Map<?,?> json) {
        return JsonUtilClient.toContentSummary(json);
      }
    }.run();
  }

  @Override
  public MD5MD5CRC32FileChecksum getFileChecksum(final Path p
      ) throws IOException {
    statistics.incrementReadOps(1);

    final HttpOpParam.Op op = GetOpParam.Op.GETFILECHECKSUM;
    return new FsPathResponseRunner<MD5MD5CRC32FileChecksum>(op, p) {
      @Override
      MD5MD5CRC32FileChecksum decodeResponse(Map<?,?> json) throws IOException {
        return JsonUtilClient.toMD5MD5CRC32FileChecksum(json);
      }
    }.run();
  }

  private InetSocketAddress[] resolveNNAddr() throws IOException {
    Configuration conf = getConf();
    final String scheme = uri.getScheme();

    ArrayList<InetSocketAddress> ret = new ArrayList<InetSocketAddress>();

    if (!HAUtilClient.isLogicalUri(conf, uri)) {
      InetSocketAddress addr = NetUtils.createSocketAddr(uri.getAuthority(),
          getDefaultPort());
      ret.add(addr);

    } else {
      Map<String, Map<String, InetSocketAddress>> addresses = DFSUtilClient
          .getHaNnWebHdfsAddresses(conf, scheme);

      Map<String, InetSocketAddress> addrs = addresses.get(uri.getHost());
      for (InetSocketAddress addr : addrs.values()) {
        ret.add(addr);
      }
    }

    InetSocketAddress[] r = new InetSocketAddress[ret.size()];
    return ret.toArray(r);
  }

  @Override
  public String getCanonicalServiceName() {
    return tokenServiceName == null ? super.getCanonicalServiceName()
        : tokenServiceName.toString();
  }

  @VisibleForTesting
  InetSocketAddress[] getResolvedNNAddr() {
    return nnAddrs;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/BufferSizeParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/BufferSizeParam.java
package org.apache.hadoop.hdfs.web.resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

public class BufferSizeParam extends IntegerParam {
  public static final String NAME = "buffersize";
  public static final String DEFAULT = NULL;

  private static final Domain DOMAIN = new Domain(NAME);

  public BufferSizeParam(final Integer value) {
    super(DOMAIN, value, 1, null);
  }

  public BufferSizeParam(final String str) {
    this(DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }

  public int getValue(final Configuration conf) {
    return getValue() != null? getValue()
        : conf.getInt(
            CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
            CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-httpfs/src/main/java/org/apache/hadoop/fs/http/client/HttpFSFileSystem.java
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.lib.wsrs.EnumSetParam;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
  @Override
  protected int getDefaultPort() {
    return getConf().getInt(HdfsClientConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY,
        DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/BlockStorageLocationUtil.java
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.token.Token;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
      List<LocatedBlock> blocks, 
      Map<LocatedBlock, List<VolumeId>> blockVolumeIds) throws IOException {
    BlockLocation[] locations = DFSUtilClient.locatedBlocks2Locations(blocks);
    List<BlockStorageLocation> volumeBlockLocs = 
        new ArrayList<BlockStorageLocation>(locations.length);
    for (int i = 0; i < locations.length; i++) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
    TraceScope scope = getPathTraceScope("getBlockLocations", src);
    try {
      LocatedBlocks blocks = getLocatedBlocks(src, start, length);
      BlockLocation[] locations =  DFSUtilClient.locatedBlocks2Locations(blocks);
      HdfsBlockLocation[] hdfsLocations = new HdfsBlockLocation[locations.length];
      for (int i = 0; i < locations.length; i++) {
        hdfsLocations[i] = new HdfsBlockLocation(locations[i], blocks.get(i));

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
    "dfs.namenode.path.based.cache.block.map.allocation.percent";
  public static final float    DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT_DEFAULT = 0.25f;

  public static final int     DFS_NAMENODE_HTTP_PORT_DEFAULT =
      HdfsClientConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_HTTP_ADDRESS_KEY =
  public static final String  DFS_WEBHDFS_AUTHENTICATION_FILTER_DEFAULT =
      "org.apache.hadoop.hdfs.web.AuthFilter".toString();
  @Deprecated
  public static final String  DFS_WEBHDFS_USER_PATTERN_KEY =
      HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY;
  @Deprecated
  public static final String  DFS_WEBHDFS_USER_PATTERN_DEFAULT =
      HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_DEFAULT;
  public static final String  DFS_PERMISSIONS_ENABLED_KEY = "dfs.permissions.enabled";

  public static final String  DFS_DATANODE_DATA_DIR_KEY = "dfs.datanode.data.dir";
  public static final int     DFS_NAMENODE_HTTPS_PORT_DEFAULT =
      HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_HTTPS_ADDRESS_KEY =

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSUtil.java
  public static boolean isValidName(String src) {
    return DFSUtilClient.isValidName(src);
  }

  public static byte[] string2Bytes(String str) {
    return DFSUtilClient.string2Bytes(str);
  }

    return result;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/NameNodeProxies.java
            HdfsClientConfigKeys.Retry.POLICY_ENABLED_DEFAULT, 
            HdfsClientConfigKeys.Retry.POLICY_SPEC_KEY,
            HdfsClientConfigKeys.Retry.POLICY_SPEC_DEFAULT,
            SafeModeException.class.getName());
    
    final long version = RPC.getProtocolVersion(ClientNamenodeProtocolPB.class);
    ClientNamenodeProtocolPB proxy = RPC.getProtocolProxy(

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocol/HdfsLocatedFileStatus.java
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtilClient;

        isSymlink() ? new Path(getSymlink()) : null,
        (getFullPath(path)).makeQualified(
            defaultUri, null), // fully-qualify path
        DFSUtilClient.locatedBlocks2Locations(getBlockLocations()));
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNodeHttpServer.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/DelegationTokenFetcher.java
import org.apache.hadoop.hdfs.server.namenode.CancelDelegationTokenServlet;
import org.apache.hadoop.hdfs.server.namenode.GetDelegationTokenServlet;
import org.apache.hadoop.hdfs.server.namenode.RenewDelegationTokenServlet;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
      dis = new DataInputStream(in);
      ts.readFields(dis);
      for (Token<?> token : ts.getAllTokens()) {
        token.setKind(isHttps ? WebHdfsConstants.HSFTP_TOKEN_KIND : WebHdfsConstants.HFTP_TOKEN_KIND);
        SecurityUtil.setTokenService(token, serviceAddr);
      }
      return ts;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/HftpFileSystem.java
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.tools.DelegationTokenFetcher;
@InterfaceStability.Evolving
public class HftpFileSystem extends FileSystem
    implements DelegationTokenRenewer.Renewable, TokenAspect.TokenManagementDelegator {

  static {
    HttpURLConnection.setFollowRedirects(true);

  URLConnectionFactory connectionFactory;

  protected UserGroupInformation ugi;
  private URI hftpURI;


  @Override
  protected int getDefaultPort() {
    return getConf().getInt(HdfsClientConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY,
        DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT);
  }

  @Override
  public String getScheme() {
    return WebHdfsConstants.HFTP_SCHEME;
  }

  protected void initTokenAspect() {
    tokenAspect = new TokenAspect<HftpFileSystem>(this, tokenServiceName, WebHdfsConstants.HFTP_TOKEN_KIND);
  }

  @Override

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/HsftpFileSystem.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HsftpFileSystem extends HftpFileSystem {

  @Override
  public String getScheme() {
    return WebHdfsConstants.HSFTP_SCHEME;
  }

  @Override
  protected void initTokenAspect() {
    tokenAspect = new TokenAspect<HsftpFileSystem>(this, tokenServiceName,
        WebHdfsConstants.HSFTP_TOKEN_KIND);
  }

  @Override
  protected int getDefaultPort() {
    return getConf().getInt(HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY,
                            DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_DEFAULT);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/fs/TestSymlinkHdfs.java
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
  @BeforeClass
  public static void beforeClassSetup() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    conf.set(FsPermission.UMASK_LABEL, "000");
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 0);
    cluster = new MiniDFSCluster.Builder(conf).build();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSUtil.java
    List<LocatedBlock> ls = Arrays.asList(l1, l2);
    LocatedBlocks lbs = new LocatedBlocks(10, false, ls, l2, true, null);

    BlockLocation[] bs = DFSUtilClient.locatedBlocks2Locations(lbs);

    assertTrue("expected 2 blocks but got " + bs.length,
               bs.length == 2);
        corruptCount == 1);

    bs = DFSUtilClient.locatedBlocks2Locations(new LocatedBlocks());
    assertEquals(0, bs.length);
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDistributedFileSystem.java
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
    RAN.setSeed(seed);

    final Configuration conf = getTestConfiguration();
    conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    final FileSystem hdfs = cluster.getFileSystem();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestQuota.java
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
    Configuration conf = new HdfsConfiguration();
    final int BLOCK_SIZE = 6 * 1024;
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    MiniDFSCluster cluster = 
      new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    Configuration conf = new HdfsConfiguration();
    final int BLOCK_SIZE = 6 * 1024;
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_KEY, 2);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/security/TestDelegationToken.java
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
  @Before
  public void setUp() throws Exception {
    config = new HdfsConfiguration();
    config.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    config.setLong(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY, 10000);
    config.setLong(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY, 5000);
    config.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/security/TestDelegationTokenForProxyUser.java
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
  @BeforeClass
  public static void setUp() throws Exception {
    config = new HdfsConfiguration();
    config.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    config.setLong(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY, 10000);
    config.setLong(

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestAuditLogs.java
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.HftpFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
    final long precision = 1L;
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, precision);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);
    conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_ASYNC_KEY, useAsyncLog);
    util = new DFSTestUtil.Builder().setName("TestAuditAllowed").
        setNumFiles(20).build();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestNameNodeRespectsBindHostKeys.java
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;

import org.apache.hadoop.fs.FileUtil;

  private static void setupSsl() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestFSMainOperationsWebHdfs.java
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.resources.ExceptionHandler;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
  @BeforeClass
  public static void setupCluster() {
    final Configuration conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestHftpDelegationToken.java
        DelegationTokenIdentifier.HDFS_DELEGATION_KIND, new Text(
            "127.0.0.1:8020"));
    Credentials cred = new Credentials();
    cred.addToken(WebHdfsConstants.HFTP_TOKEN_KIND, token);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    cred.write(new DataOutputStream(os));

        new String[] { "bar" });

    TokenAspect<HftpFileSystem> tokenAspect = new TokenAspect<HftpFileSystem>(
        fs, SecurityUtil.buildTokenService(uri), WebHdfsConstants.HFTP_TOKEN_KIND);

    tokenAspect.initDelegationToken(ugi);
    tokenAspect.ensureTokenInitialized();

    Assert.assertSame(WebHdfsConstants.HFTP_TOKEN_KIND, fs.getRenewToken().getKind());

    Token<?> tok = (Token<?>) Whitebox.getInternalState(fs, "delegationToken");
    Assert.assertNotSame("Not making a copy of the remote token", token, tok);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestHftpFileSystem.java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
  @Test
  public void testHftpCustomDefaultPorts() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(HdfsClientConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY, 123);

    URI uri = URI.create("hftp://localhost");
    HftpFileSystem fs = (HftpFileSystem) FileSystem.get(uri, conf);
  @Test
  public void testHftpCustomUriPortWithCustomDefaultPorts() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(HdfsClientConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY, 123);

    URI uri = URI.create("hftp://localhost:789");
    HftpFileSystem fs = (HftpFileSystem) FileSystem.get(uri, conf);
  @Test
  public void testHsftpCustomDefaultPorts() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(HdfsClientConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY, 123);
    conf.setInt(HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY, 456);

    URI uri = URI.create("hsftp://localhost");
    HsftpFileSystem fs = (HsftpFileSystem) FileSystem.get(uri, conf);
  @Test
  public void testHsftpCustomUriPortWithCustomDefaultPorts() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(HdfsClientConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY, 123);
    conf.setInt(HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY, 456);

    URI uri = URI.create("hsftp://localhost:789");
    HsftpFileSystem fs = (HsftpFileSystem) FileSystem.get(uri, conf);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestHttpFSPorts.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Before;
import org.junit.Test;


  @Before
  public void setupConfig() {
    conf.setInt(HdfsClientConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY, 123);
    conf.setInt(HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY, 456);
  }

  @Test

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestHttpsFileSystem.java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
  @BeforeClass
  public static void setUp() throws Exception {
    conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHDFS.java
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestDFSClientRetries;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
  @Test(timeout=300000)
  public void testNumericalUserName() throws Exception {
    final Configuration conf = WebHdfsTestUtil.createConf();
    conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY, "^[A-Za-z0-9_][A-Za-z0-9" +
        "._-]*[$]?$");
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
  @Test
  public void testWebHdfsEnabledByDefault() throws Exception {
    Configuration conf = new HdfsConfiguration();
    Assert.assertTrue(conf.getBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY,
        false));
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHdfsFileSystemContract.java
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.resources.*;
import org.apache.hadoop.hdfs.web.resources.NamenodeAddressParam;
import org.apache.hadoop.io.IOUtils;
  private UserGroupInformation ugi;

  static {
    conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      cluster.waitActive();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHdfsTokens.java
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.resources.*;
import org.apache.hadoop.http.HttpConfig;
      String keystoresDir;
      String sslConfDir;
	    
      clusterConf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
      clusterConf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
      clusterConf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
      clusterConf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHdfsWithMultipleNameNodes.java
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
      throws Exception {
    LOG.info("nNameNodes=" + nNameNodes + ", nDataNodes=" + nDataNodes);

    conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);

    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(nNameNodes))

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/WebHdfsTestUtil.java
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.security.UserGroupInformation;

  public static Configuration createConf() {
    final Configuration conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    return conf;
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/tools/TestDelegationTokenRemoteFetcher.java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.tools.DelegationTokenFetcher;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
        "renewer"), new Text("realuser")).getBytes();
    Text service = new Text(serviceUri.toString());
    return new Token<DelegationTokenIdentifier>(ident, pw,
        WebHdfsConstants.HFTP_TOKEN_KIND, service);
  }

  private interface Handler {

