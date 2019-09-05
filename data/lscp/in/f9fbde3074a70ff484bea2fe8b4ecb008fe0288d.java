hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/JsonUtil.java
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.*;

public class JsonUtil {
  private static final Object[] EMPTY_OBJECT_ARRAY = {};

  public static String toJsonString(final Token<? extends TokenIdentifier> token
    return m;
  }

  public static String toJsonString(final Exception e) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    return toJsonString(RemoteException.class, m);
  }

  private static String toJsonString(final Class<?> clazz, final Object value) {
    return toJsonString(clazz.getSimpleName(), value);
  }
    return String.format("%o", permission.toShort());
  }

  public static String toJsonString(final HdfsFileStatus status,
      boolean includeType) {
    }
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("pathSuffix", status.getLocalName());
    m.put("type", WebHdfsConstants.PathType.valueOf(status));
    if (status.isSymlink()) {
      m.put("symlink", status.getSymlink());
    }
    return null;
  }

  private static Map<String, Object> toJsonMap(final ExtendedBlock extendedblock) {
    if (extendedblock == null) {
    return m;
  }

  static Map<String, Object> toJsonMap(final DatanodeInfo datanodeinfo) {
    if (datanodeinfo == null) {
    return m;
  }

  private static Object[] toJsonArray(final DatanodeInfo[] array) {
    if (array == null) {
    }
  }

  private static Map<String, Object> toJsonMap(final LocatedBlock locatedblock
      ) throws IOException {
    return m;
  }

  private static Object[] toJsonArray(final List<LocatedBlock> array
      ) throws IOException {
    }
  }

  public static String toJsonString(final LocatedBlocks locatedblocks
      ) throws IOException {
    return toJsonString(LocatedBlocks.class, m);
  }

  public static String toJsonString(final ContentSummary contentsummary) {
    if (contentsummary == null) {
    return toJsonString(ContentSummary.class, m);
  }

  public static String toJsonString(final MD5MD5CRC32FileChecksum checksum) {
    if (checksum == null) {
    return toJsonString(FileChecksum.class, m);
  }

  public static String toJsonString(final AclStatus status) {
    if (status == null) {
    return null;
  }

  private static Map<String, Object> toJsonMap(final XAttr xAttr,
      final XAttrCodec encoding) throws IOException {
    if (xAttr == null) {
    finalMap.put("XAttrNames", ret);
    return mapper.writeValueAsString(finalMap);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/JsonUtilClient.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsConstants.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsFileSystem.java
hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestDelegationTokensWithHA.java
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.SecurityUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashSet;

import static org.junit.Assert.*;

    token.cancel(conf);
  }

  @SuppressWarnings("unchecked")
  private Token<DelegationTokenIdentifier> getDelegationToken(FileSystem fs,
      String renewer) throws IOException {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestJsonUtil.java
    System.out.println("json    = " + json.replace(",", ",\n  "));
    ObjectReader reader = new ObjectMapper().reader(Map.class);
    final HdfsFileStatus s2 =
        JsonUtilClient.toFileStatus((Map<?, ?>) reader.readValue(json), true);
    final FileStatus fs2 = toFileStatus(s2, parent);
    System.out.println("s2      = " + s2);
    System.out.println("fs2     = " + fs2);
    response.put("cacheCapacity", 123l);
    response.put("cacheUsed", 321l);
    
    JsonUtilClient.toDatanodeInfo(response);
  }

  @Test
    response.put("cacheCapacity", 123l);
    response.put("cacheUsed", 321l);

    DatanodeInfo di = JsonUtilClient.toDatanodeInfo(response);
    Assert.assertEquals(name, di.getXferAddr());

    aclStatusBuilder.stickyBit(false);

    Assert.assertEquals("Should be equal", aclStatusBuilder.build(),
        JsonUtilClient.toAclStatus(json));
  }

  @Test
    xAttrs.add(xAttr1);
    xAttrs.add(xAttr2);
    Map<String, byte[]> xAttrMap = XAttrHelper.buildXAttrMap(xAttrs);
    Map<String, byte[]> parsedXAttrMap = JsonUtilClient.toXAttrs(json);
    
    Assert.assertEquals(xAttrMap.size(), parsedXAttrMap.size());
    Iterator<Entry<String, byte[]>> iter = xAttrMap.entrySet().iterator();
    Map<?, ?> json = reader.readValue(jsonString);

    byte[] value = JsonUtilClient.getXAttr(json, "user.a2");
    Assert.assertArrayEquals(XAttrCodec.decodeValue("0x313131"), value);
  }

  private void checkDecodeFailure(Map<String, Object> map) {
    try {
      JsonUtilClient.toDatanodeInfo(map);
      Assert.fail("Exception not thrown against bad input.");
    } catch (Exception e) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHDFSForHA.java
package org.apache.hadoop.hdfs.web;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.web.resources.ExceptionHandler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mortbay.util.ajax.JSON;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

public class TestWebHDFSForHA {
  private static final String LOGICAL_NAME = "minidfs";
    }
  }

  @Test
  public void testClientFailoverWhenStandbyNNHasStaleCredentials()
      throws IOException {
    Configuration conf = DFSTestUtil.newHAConfiguration(LOGICAL_NAME);
    conf.setBoolean(DFSConfigKeys
                        .DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);

    MiniDFSCluster cluster = null;
    WebHdfsFileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).nnTopology(topo).numDataNodes(
          0).build();

      HATestUtil.setFailoverConfigurations(cluster, conf, LOGICAL_NAME);
      cluster.waitActive();

      fs = (WebHdfsFileSystem) FileSystem.get(WEBHDFS_URI, conf);

      cluster.transitionToActive(0);
      Token<?> token = fs.getDelegationToken(null);
      final DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
      identifier.readFields(
          new DataInputStream(new ByteArrayInputStream(token.getIdentifier())));
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);

      final DelegationTokenSecretManager secretManager = NameNodeAdapter.getDtSecretManager(
          cluster.getNamesystem(0));

      ExceptionHandler eh = new ExceptionHandler();
      eh.initResponse(mock(HttpServletResponse.class));
      Response resp = null;
      try {
        secretManager.retrievePassword(identifier);
      } catch (IOException e) {
        Assert.assertTrue(e instanceof SecretManager.InvalidToken);
        resp = eh.toResponse(new SecurityException(e));
      }

      Map<?, ?> m = (Map<?, ?>) JSON.parse(resp.getEntity().toString());
      RemoteException re = JsonUtilClient.toRemoteException(m);
      Exception unwrapped = re.unwrapRemoteException(StandbyException.class);
      Assert.assertTrue(unwrapped instanceof StandbyException);
    } finally {
      IOUtils.cleanup(null, fs);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testFailoverAfterOpen() throws IOException {
    Configuration conf = DFSTestUtil.newHAConfiguration(LOGICAL_NAME);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHdfsTokens.java
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
            @Override
            Token<DelegationTokenIdentifier> decodeResponse(Map<?, ?> json)
                throws IOException {
              return JsonUtilClient.toDelegationToken(json);
            }
          }.run();


