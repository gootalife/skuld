hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/DFSUtilClient.java
package org.apache.hadoop.hdfs;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_NAMESERVICES;

public class DFSUtilClient {
  private static final Logger LOG = LoggerFactory.getLogger(
      DFSUtilClient.class);
    return StringUtils.format("%.2f%%", percentage);
  }

  public static Collection<String> getNameServiceIds(Configuration conf) {
    return conf.getTrimmedStringCollection(DFS_NAMESERVICES);
  }

  public static Collection<String> getNameNodeIds(Configuration conf, String nsId) {
    String key = addSuffix(DFS_HA_NAMENODES_KEY_PREFIX, nsId);
    return conf.getTrimmedStringCollection(key);
  }

  static String addSuffix(String key, String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return key;
    }
    assert !suffix.startsWith(".") :
      "suffix '" + suffix + "' should not already have '.' prepended.";
    return key + "." + suffix;
  }

  public static Map<String, Map<String, InetSocketAddress>> getHaNnWebHdfsAddresses(
      Configuration conf, String scheme) {
    if (WebHdfsConstants.WEBHDFS_SCHEME.equals(scheme)) {
      return getAddresses(conf, null,
          HdfsClientConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    } else if (WebHdfsConstants.SWEBHDFS_SCHEME.equals(scheme)) {
      return getAddresses(conf, null,
          HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY);
    } else {
      throw new IllegalArgumentException("Unsupported scheme: " + scheme);
    }
  }

    return null;
  }

  static Collection<String> emptyAsSingletonNull(Collection<String> coll) {
    if (coll == null || coll.isEmpty()) {
      return Collections.singletonList(null);
    } else {
      return coll;
    }
  }

  static String concatSuffixes(String... suffixes) {
    if (suffixes == null) {
      return null;
    }
    return Joiner.on(".").skipNulls().join(suffixes);
  }

  static Map<String, Map<String, InetSocketAddress>>
    getAddresses(Configuration conf, String defaultAddress, String... keys) {
    Collection<String> nameserviceIds = getNameServiceIds(conf);
    return getAddressesForNsIds(conf, nameserviceIds, defaultAddress, keys);
  }

  static Map<String, Map<String, InetSocketAddress>>
    getAddressesForNsIds(
      Configuration conf, Collection<String> nsIds, String defaultAddress,
      String... keys) {
    Map<String, Map<String, InetSocketAddress>> ret = Maps.newLinkedHashMap();
    for (String nsId : emptyAsSingletonNull(nsIds)) {
      Map<String, InetSocketAddress> isas =
        getAddressesForNameserviceId(conf, nsId, defaultAddress, keys);
      if (!isas.isEmpty()) {
        ret.put(nsId, isas);
      }
    }
    return ret;
  }

  static Map<String, InetSocketAddress> getAddressesForNameserviceId(
      Configuration conf, String nsId, String defaultValue, String... keys) {
    Collection<String> nnIds = getNameNodeIds(conf, nsId);
    Map<String, InetSocketAddress> ret = Maps.newHashMap();
    for (String nnId : emptyAsSingletonNull(nnIds)) {
      String suffix = concatSuffixes(nsId, nnId);
      String address = getConfValue(defaultValue, suffix, conf, keys);
      if (address != null) {
        InetSocketAddress isa = NetUtils.createSocketAddr(address);
        if (isa.isUnresolved()) {
          LOG.warn("Namenode for " + nsId +
                       " remains unresolved for ID " + nnId +
                   ".  Check your hdfs-site.xml file to " +
                   "ensure namenodes are configured properly.");
        }
        ret.put(nnId, isa);
      }
    }
    return ret;
  }

  private static String getConfValue(String defaultValue, String keySuffix,
      Configuration conf, String... keys) {
    String value = null;
    for (String key : keys) {
      key = addSuffix(key, keySuffix);
      value = conf.get(key);
      if (value != null) {
        break;
      }
    }
    if (value == null) {
      value = defaultValue;
    }
    return value;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/HAUtilClient.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/HAUtilClient.java
package org.apache.hadoop.hdfs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;

import java.net.URI;

import static org.apache.hadoop.hdfs.protocol.HdfsConstantsClient.HA_DT_SERVICE_PREFIX;

@InterfaceAudience.Private
public class HAUtilClient {
  public static boolean isLogicalUri(
      Configuration conf, URI nameNodeUri) {
    String host = nameNodeUri.getHost();
    return DFSUtilClient.getNameServiceIds(conf).contains(host);
  }

  public static boolean isClientFailoverConfigured(
      Configuration conf, URI nameNodeUri) {
    String host = nameNodeUri.getHost();
    String configKey = HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX
        + "." + host;
    return conf.get(configKey) != null;
  }

  public static Text buildTokenServiceForLogicalUri(final URI uri,
      final String scheme) {
    return new Text(buildTokenServicePrefixForLogicalUri(scheme)
        + uri.getHost());
  }

  public static String buildTokenServicePrefixForLogicalUri(String scheme) {
    return HA_DT_SERVICE_PREFIX + scheme + ":";
  }

  public static URI getServiceUriFromToken(final String scheme, Token<?> token) {
    String tokStr = token.getService().toString();
    final String prefix = buildTokenServicePrefixForLogicalUri(
        scheme);
    if (tokStr.startsWith(prefix)) {
      tokStr = tokStr.replaceFirst(prefix, "");
    }
    return URI.create(scheme + "://" + tokStr);
  }

  public static boolean isTokenForLogicalUri(Token<?> token) {
    return token.getService().toString().startsWith(HA_DT_SERVICE_PREFIX);
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsClientConfigKeys.java
      "^(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?(,(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?)*$";

  static final String PREFIX = "dfs.client.";
  String  DFS_NAMESERVICES = "dfs.nameservices";
  int     DFS_NAMENODE_HTTP_PORT_DEFAULT = 50070;
  String  DFS_NAMENODE_HTTP_ADDRESS_KEY = "dfs.namenode.http-address";
  int     DFS_NAMENODE_HTTPS_PORT_DEFAULT = 50470;
  String  DFS_NAMENODE_HTTPS_ADDRESS_KEY = "dfs.namenode.https-address";
  String DFS_HA_NAMENODES_KEY_PREFIX = "dfs.ha.namenodes";

  interface Retry {

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/HdfsConstantsClient.java
hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsConstants.java

@InterfaceAudience.Private
public class WebHdfsConstants {
  public static final String WEBHDFS_SCHEME = "webhdfs";
  public static final String SWEBHDFS_SCHEME = "swebhdfs";
  public static final Text WEBHDFS_TOKEN_KIND = new Text("WEBHDFS delegation");
  public static final Text SWEBHDFS_TOKEN_KIND = new Text("SWEBHDFS delegation");


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
    private static ClientProtocol getNNProxy(
        Token<DelegationTokenIdentifier> token, Configuration conf)
        throws IOException {
      URI uri = HAUtilClient.getServiceUriFromToken(
          HdfsConstants.HDFS_URI_SCHEME, token);
      if (HAUtilClient.isTokenForLogicalUri(token) &&
          !HAUtilClient.isLogicalUri(conf, uri)) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  public static final float    DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT_DEFAULT = 0.25f;

  public static final String  DFS_NAMENODE_HTTP_PORT_KEY = "dfs.http.port";
  public static final int     DFS_NAMENODE_HTTP_PORT_DEFAULT =
      HdfsClientConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_HTTP_ADDRESS_KEY =
      HdfsClientConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
  public static final String  DFS_NAMENODE_HTTP_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_NAMENODE_HTTP_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_HTTP_BIND_HOST_KEY = "dfs.namenode.http-bind-host";
  public static final String  DFS_NAMENODE_RPC_ADDRESS_KEY = "dfs.namenode.rpc-address";
  public static final String  DFS_DATANODE_DATA_DIR_KEY = "dfs.datanode.data.dir";
  public static final String  DFS_NAMENODE_HTTPS_PORT_KEY = "dfs.https.port";
  public static final int     DFS_NAMENODE_HTTPS_PORT_DEFAULT =
      HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_HTTPS_ADDRESS_KEY =
      HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
  public static final String  DFS_NAMENODE_HTTPS_BIND_HOST_KEY = "dfs.namenode.https-bind-host";
  public static final String  DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_NAMENODE_HTTPS_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_NAME_DIR_KEY = "dfs.namenode.name.dir";
  public static final boolean DFS_QUOTA_BY_STORAGETYPE_ENABLED_DEFAULT = true;

  public static final String DFS_HA_NAMENODES_KEY_PREFIX =
      HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
  public static final String DFS_HA_NAMENODE_ID_KEY = "dfs.ha.namenode.id";
  public static final String  DFS_HA_STANDBY_CHECKPOINTS_KEY = "dfs.ha.standby.checkpoints";
  public static final boolean DFS_HA_STANDBY_CHECKPOINTS_DEFAULT = true;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSUtil.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ADMIN;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYPASSWORD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import org.apache.hadoop.hdfs.protocolPB.ClientDatanodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.SignedBytes;
import com.google.protobuf.BlockingService;

    return blkLocations;
  }

  public static String addKeySuffixes(String key, String... suffixes) {
    String keySuffix = DFSUtilClient.concatSuffixes(suffixes);
    return DFSUtilClient.addSuffix(key, keySuffix);
  }

  public static Map<String, InetSocketAddress> getRpcAddressesForNameserviceId(
      Configuration conf, String nsId, String defaultValue) {
    return DFSUtilClient.getAddressesForNameserviceId(conf, nsId, defaultValue,
                                                      DFS_NAMENODE_RPC_ADDRESS_KEY);
  }

  public static Set<String> getAllNnPrincipals(Configuration conf) throws IOException {
    Set<String> principals = new HashSet<String>();
    for (String nsId : DFSUtilClient.getNameServiceIds(conf)) {
      if (HAUtil.isHAEnabled(conf, nsId)) {
        for (String nnId : DFSUtilClient.getNameNodeIds(conf, nsId)) {
          Configuration confForNn = new Configuration(conf);
          NameNode.initializeGenericKeys(confForNn, nsId, nnId);
          String principal = SecurityUtil.getServerPrincipal(confForNn
  public static Map<String, Map<String, InetSocketAddress>> getHaNnRpcAddresses(
      Configuration conf) {
    return DFSUtilClient.getAddresses(conf, null,
                                      DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
  }

  public static Map<String, Map<String, InetSocketAddress>> getBackupNodeAddresses(
      Configuration conf) throws IOException {
    Map<String, Map<String, InetSocketAddress>> addressList = DFSUtilClient.getAddresses(
        conf, null, DFS_NAMENODE_BACKUP_ADDRESS_KEY);
    if (addressList.isEmpty()) {
      throw new IOException("Incorrect configuration: backup node address "
          + DFS_NAMENODE_BACKUP_ADDRESS_KEY + " is not configured.");
  public static Map<String, Map<String, InetSocketAddress>> getSecondaryNameNodeAddresses(
      Configuration conf) throws IOException {
    Map<String, Map<String, InetSocketAddress>> addressList = DFSUtilClient.getAddresses(
        conf, null, DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY);
    if (addressList.isEmpty()) {
      throw new IOException("Incorrect configuration: secondary namenode address "
          + DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY + " is not configured.");
    }
    
    Map<String, Map<String, InetSocketAddress>> addressList =
      DFSUtilClient.getAddresses(conf, defaultAddress,
                                 DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
                                 DFS_NAMENODE_RPC_ADDRESS_KEY);
    if (addressList.isEmpty()) {
      throw new IOException("Incorrect configuration: namenode address "
          + DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + " or "  
    }

    Map<String, Map<String, InetSocketAddress>> addressList =
            DFSUtilClient.getAddressesForNsIds(conf, parentNameServices,
                                               defaultAddress,
                                               DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
                                               DFS_NAMENODE_RPC_ADDRESS_KEY);
    if (addressList.isEmpty()) {
      throw new IOException("Incorrect configuration: namenode address "
              + DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + " or "
    Set<URI> nonPreferredUris = new HashSet<URI>();
    
    for (String nsId : DFSUtilClient.getNameServiceIds(conf)) {
      if (HAUtil.isHAEnabled(conf, nsId)) {
        try {
        boolean uriFound = false;
        for (String key : keys) {
          String addr = conf.get(DFSUtilClient.concatSuffixes(key, nsId));
          if (addr != null) {
            URI uri = createUri(HdfsConstants.HDFS_URI_SCHEME,
                NetUtils.createSocketAddr(addr));
    if (nameserviceId != null) {
      return nameserviceId;
    }
    Collection<String> nsIds = DFSUtilClient.getNameServiceIds(conf);
    if (1 == nsIds.size()) {
      return nsIds.toArray(new String[1])[0];
    }
    String namenodeId = null;
    int found = 0;
    
    Collection<String> nsIds = DFSUtilClient.getNameServiceIds(conf);
    for (String nsId : DFSUtilClient.emptyAsSingletonNull(nsIds)) {
      if (knownNsId != null && !knownNsId.equals(nsId)) {
        continue;
      }
      
      Collection<String> nnIds = DFSUtilClient.getNameNodeIds(conf, nsId);
      for (String nnId : DFSUtilClient.emptyAsSingletonNull(nnIds)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(String.format("addressKey: %s nsId: %s nnId: %s",
              addressKey, nsId, nnId));
      nsId = getOnlyNameServiceIdOrNull(conf);
    }

    String serviceAddrKey = DFSUtilClient.concatSuffixes(
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, nsId, nnId);

    String addrKey = DFSUtilClient.concatSuffixes(
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, nsId, nnId);

    String serviceRpcAddr = conf.get(serviceAddrKey);
  public static String getOnlyNameServiceIdOrNull(Configuration conf) {
    Collection<String> nsIds = DFSUtilClient.getNameServiceIds(conf);
    if (1 == nsIds.size()) {
      return nsIds.toArray(new String[1])[0];
    } else {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DistributedFileSystem.java
  
  @Override
  protected URI canonicalizeUri(URI uri) {
    if (HAUtilClient.isLogicalUri(getConf(), uri)) {
      return uri;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/HAUtil.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.NameNodeProxies.ProxyAndInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
        "machine is one of the machines listed as a NN RPC address, " +
        "or configure " + DFSConfigKeys.DFS_NAMESERVICE_ID);
    
    Collection<String> nnIds = DFSUtilClient.getNameNodeIds(conf, nsId);
    String myNNId = conf.get(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY);
    Preconditions.checkArgument(nnIds != null,
        "Could not determine namenode ids in namespace '%s'. " +
    conf.setBoolean("dfs.ha.allow.stale.reads", val);
  }

    return provider.useLogicalURI();
  }

      UserGroupInformation ugi, URI haUri,
      Collection<InetSocketAddress> nnAddrs) {
    Text haService = HAUtilClient.buildTokenServiceForLogicalUri(haUri,
                                                                 HdfsConstants.HDFS_URI_SCHEME);
    Token<DelegationTokenIdentifier> haToken =
        tokenSelector.selectToken(haService, ugi.getTokens());
            new Token.PrivateToken<DelegationTokenIdentifier>(haToken);
        SecurityUtil.setTokenService(specificToken, singleNNAddr);
        Text alias = new Text(
            HAUtilClient.buildTokenServicePrefixForLogicalUri(
                HdfsConstants.HDFS_URI_SCHEME)
                + "//" + specificToken.getService());
        ugi.addToken(alias, specificToken);
        if (LOG.isDebugEnabled()) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/NameNodeProxies.java

      Text dtService;
      if (failoverProxyProvider.useLogicalURI()) {
        dtService = HAUtilClient.buildTokenServiceForLogicalUri(nameNodeUri,
                                                                HdfsConstants.HDFS_URI_SCHEME);
      } else {
        dtService = SecurityUtil.buildTokenService(
          new Class[] { xface }, dummyHandler);
      Text dtService;
      if (failoverProxyProvider.useLogicalURI()) {
        dtService = HAUtilClient.buildTokenServiceForLogicalUri(nameNodeUri,
                                                                HdfsConstants.HDFS_URI_SCHEME);
      } else {
        dtService = SecurityUtil.buildTokenService(

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocol/HdfsConstants.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/webhdfs/ParameterParser.java
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.resources.BlockSizeParam;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
      Token<DelegationTokenIdentifier>();
    token.decodeFromUrlString(delegation);
    URI nnUri = URI.create(HDFS_URI_SCHEME + "://" + namenodeId());
    boolean isLogical = HAUtilClient.isLogicalUri(conf, nnUri);
    if (isLogical) {
      token.setService(
          HAUtilClient.buildTokenServiceForLogicalUri(nnUri, HDFS_URI_SCHEME));
    } else {
      token.setService(SecurityUtil.buildTokenService(nnUri));
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNode.java
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

      return;
    }

    if (DFSUtilClient.getNameServiceIds(conf).contains(nnHost)) {
      clientNamenodeAddress = nnHost;
    } else if (nnUri.getPort() > 0) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/DFSAdmin.java
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RefreshCallQueueProtocol;
    DistributedFileSystem dfs = getDFS();
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(dfsConf, dfsUri);

    if (isHaEnabled) {
      String nsId = dfsUri.getHost();
    DistributedFileSystem dfs = getDFS();
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(dfsConf, dfsUri);
    if (isHaEnabled) {
      String nsId = dfsUri.getHost();
      List<ProxyAndInfo<ClientProtocol>> proxies =
    DistributedFileSystem dfs = getDFS();
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(dfsConf, dfsUri);

    if (isHaEnabled) {
      String nsId = dfsUri.getHost();
    DistributedFileSystem dfs = getDFS();
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(dfsConf, dfsUri);

    if (isHaEnabled) {
      String nsId = dfsUri.getHost();
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(dfsConf, dfsUri);

    if (isHaEnabled) {
      String nsId = dfsUri.getHost();
    
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaAndLogicalUri = HAUtilClient.isLogicalUri(dfsConf, dfsUri);
    if (isHaAndLogicalUri) {
    DistributedFileSystem dfs = getDFS();
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(dfsConf, dfsUri);

    if (isHaEnabled) {
      String nsId = dfsUri.getHost();

    DistributedFileSystem dfs = getDFS();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(conf, dfsUri);

    if (isHaEnabled) {

    DistributedFileSystem dfs = getDFS();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(conf, dfsUri);

    if (isHaEnabled) {

    DistributedFileSystem dfs = getDFS();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(conf, dfsUri);

    if (isHaEnabled) {

    DistributedFileSystem dfs = getDFS();
    URI dfsUri = dfs.getUri();
    boolean isHaEnabled = HAUtilClient.isLogicalUri(conf, dfsUri);

    if (isHaEnabled) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/DFSHAAdmin.java
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.ToolRunner;

  @Override
  protected Collection<String> getTargetIds(String namenodeToActivate) {
    return DFSUtilClient.getNameNodeIds(getConf(),
                                        (nameserviceId != null) ? nameserviceId : DFSUtil.getNamenodeNameServiceId(
                                            getConf()));
  }
  
  public static void main(String[] argv) throws Exception {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/DelegationTokenFetcher.java
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;

import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/SWebHdfsFileSystem.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/TokenAspect.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsFileSystem.java
hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/fs/TestSymlinkHdfs.java
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.ipc.RemoteException;
    conf.set(FsPermission.UMASK_LABEL, "000");
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 0);
    cluster = new MiniDFSCluster.Builder(conf).build();
    webhdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME);
    dfs = cluster.getFileSystem();
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/DFSTestUtil.java
    } else { // append the nsid
      conf.set(DFSConfigKeys.DFS_NAMESERVICES, nsIds + "," + logicalName);
    }
    conf.set(DFSUtil.addKeySuffixes(HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX,
            logicalName), "nn1,nn2");
    conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX +
            "." + logicalName,

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSClientRetries.java
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final FileSystem fs = isWebHDFS ? WebHdfsTestUtil.getWebHdfsFileSystem(
          conf, WebHdfsConstants.WEBHDFS_SCHEME) : dfs;
      final URI uri = dfs.getUri();
      assertTrue(HdfsUtils.isHealthy(uri));

    final UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        username, new String[]{"supergroup"});

    return isWebHDFS? WebHdfsTestUtil.getWebHdfsFileSystemAs(ugi, conf, WebHdfsConstants.WEBHDFS_SCHEME)
        : DFSTestUtil.getFileSystemAs(ugi, conf);
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSUtil.java
  }

  @Test
  public void testGetNameServiceIds() {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFS_NAMESERVICES, "nn1,nn2");
    Collection<String> nameserviceIds = DFSUtilClient.getNameServiceIds(conf);
    Iterator<String> it = nameserviceIds.iterator();
    assertEquals(2, nameserviceIds.size());
    assertEquals("nn1", it.next().toString());
    Configuration conf = createWebHDFSHAConfiguration(LOGICAL_HOST_NAME, NS1_NN1_ADDR, NS1_NN2_ADDR);

    Map<String, Map<String, InetSocketAddress>> map =
        DFSUtilClient.getHaNnWebHdfsAddresses(conf, "webhdfs");

    assertEquals(NS1_NN1_ADDR, map.get("ns1").get("nn1").toString());
    assertEquals(NS1_NN2_ADDR, map.get("ns1").get("nn2").toString());

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDistributedFileSystem.java
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.web.HftpFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
    });

    final String webhdfsuri = WebHdfsConstants.WEBHDFS_SCHEME + "://" + nnAddr;
    System.out.println("webhdfsuri=" + webhdfsuri);
    final FileSystem webhdfs = ugi.doAs(
        new PrivilegedExceptionAction<FileSystem>() {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestEncryptionZones.java
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.key.JavaKeyStoreProvider;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.hdfs.server.namenode.NamenodeFsck;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.security.AccessControlException;
    final HdfsAdmin dfsAdmin =
        new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
    final FileSystem webHdfsFs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
        WebHdfsConstants.WEBHDFS_SCHEME);

    final Path zone = new Path("/zone");
    fs.mkdirs(zone);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestQuota.java
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
    DFSAdmin admin = new DFSAdmin(conf);

    final String nnAddr = conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    final String webhdfsuri = WebHdfsConstants.WEBHDFS_SCHEME + "://" + nnAddr;
    System.out.println("webhdfsuri=" + webhdfsuri);
    final FileSystem webhdfs = new Path(webhdfsuri).getFileSystem(conf);

    DFSAdmin admin = new DFSAdmin(conf);

    final String nnAddr = conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    final String webhdfsuri = WebHdfsConstants.WEBHDFS_SCHEME + "://" + nnAddr;
    System.out.println("webhdfsuri=" + webhdfsuri);
    final FileSystem webhdfs = new Path(webhdfsuri).getFileSystem(conf);
    

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/protocol/datatransfer/sasl/SaslDataTransferTestCase.java
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.AfterClass;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/security/TestDelegationToken.java
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
  @Test
  public void testDelegationTokenWebHdfsApi() throws Exception {
    ((Log4JLogger)NamenodeWebHdfsMethods.LOG).getLogger().setLevel(Level.ALL);
    final String uri = WebHdfsConstants.WEBHDFS_SCHEME + "://"
        + config.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    final UserGroupInformation ugi = UserGroupInformation.createUserForTesting(

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/security/TestDelegationTokenForProxyUser.java
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.security.TestDoAsEffectiveUser;
  public void testWebHdfsDoAs() throws Exception {
    WebHdfsTestUtil.LOG.info("START: testWebHdfsDoAs()");
    WebHdfsTestUtil.LOG.info("ugi.getShortUserName()=" + ugi.getShortUserName());
    final WebHdfsFileSystem webhdfs = WebHdfsTestUtil.getWebHdfsFileSystemAs(ugi, config, WebHdfsConstants.WEBHDFS_SCHEME);
    
    final Path root = new Path("/");
    cluster.getFileSystem().setPermission(root, new FsPermission((short)0777));

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestReplicationPolicy.java
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestBlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager.StatefulBlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.test.PathUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/web/webhdfs/TestParameterParser.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.NamenodeAddressParam;

import io.netty.handler.codec.http.QueryStringDecoder;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class TestParameterParser {
      + DelegationParam.NAME + "=" + token.encodeToUrlString());
    ParameterParser testParser = new ParameterParser(decoder, conf);
    final Token<DelegationTokenIdentifier> tok2 = testParser.delegationToken();
    Assert.assertTrue(HAUtilClient.isTokenForLogicalUri(tok2));
  }

  @Test

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestAuditLogs.java
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.HftpFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.AccessControlException;

    setupAuditLogs();

    WebHdfsFileSystem webfs = WebHdfsTestUtil.getWebHdfsFileSystemAs(userGroupInfo, conf, WebHdfsConstants.WEBHDFS_SCHEME);
    InputStream istream = webfs.open(file);
    int val = istream.read();
    istream.close();

    setupAuditLogs();

    WebHdfsFileSystem webfs = WebHdfsTestUtil.getWebHdfsFileSystemAs(userGroupInfo, conf, WebHdfsConstants.WEBHDFS_SCHEME);
    FileStatus st = webfs.getFileStatus(file);

    verifyAuditLogs(true);

    setupAuditLogs();
    try {
      WebHdfsFileSystem webfs = WebHdfsTestUtil.getWebHdfsFileSystemAs(userGroupInfo, conf, WebHdfsConstants.WEBHDFS_SCHEME);
      InputStream istream = webfs.open(file);
      int val = istream.read();
      fail("open+read must not succeed, got " + val);

    setupAuditLogs();

    WebHdfsFileSystem webfs = WebHdfsTestUtil.getWebHdfsFileSystemAs(userGroupInfo, conf, WebHdfsConstants.WEBHDFS_SCHEME);
    webfs.open(file);

    verifyAuditLogsCheckPattern(true, 3, webOpenPattern);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestMalformedURLs.java
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import static org.junit.Assert.assertNotEquals;
import org.junit.Before;
import org.junit.Test;

public class TestMalformedURLs {
  private MiniDFSCluster cluster;
  Configuration config;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestNameNodeRespectsBindHostKeys.java
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

    File base = new File(BASEDIR);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestDelegationTokensWithHA.java
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("test");
    
    URI haUri = new URI("hdfs://my-ha-uri/");
    token.setService(HAUtilClient.buildTokenServiceForLogicalUri(haUri,
                                                                 HdfsConstants.HDFS_URI_SCHEME));
    ugi.addToken(token);

  @Test(timeout = 300000)
  public void testDFSGetCanonicalServiceName() throws Exception {
    URI hAUri = HATestUtil.getLogicalUri(cluster);
    String haService = HAUtilClient.buildTokenServiceForLogicalUri(hAUri,
                                                                   HdfsConstants.HDFS_URI_SCHEME).toString();
    assertEquals(haService, dfs.getCanonicalServiceName());
    final String renewer = UserGroupInformation.getCurrentUser().getShortUserName();
    Configuration conf = dfs.getConf();
    URI haUri = HATestUtil.getLogicalUri(cluster);
    AbstractFileSystem afs =  AbstractFileSystem.createFileSystem(haUri, conf);    
    String haService = HAUtilClient.buildTokenServiceForLogicalUri(haUri,
                                                                   HdfsConstants.HDFS_URI_SCHEME).toString();
    assertEquals(haService, afs.getCanonicalServiceName());
    Token<?> token = afs.getDelegationTokens(

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestHAConfiguration.java
hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/tools/TestDFSHAAdmin.java
hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestFSMainOperationsWebHdfs.java
      cluster.getFileSystem().setPermission(
          new Path("/"), new FsPermission((short)0777));

      final String uri = WebHdfsConstants.WEBHDFS_SCHEME + "://"
          + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHDFS.java
    try {
      cluster.waitActive();

      final FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME);
      final Path dir = new Path("/test/largeFile");
      Assert.assertTrue(fs.mkdirs(dir));

        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      cluster.waitActive();
      WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME)
          .setPermission(new Path("/"),
              new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

          @Override
          public Void run() throws IOException, URISyntaxException {
              FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
                  WebHdfsConstants.WEBHDFS_SCHEME);
              Path d = new Path("/my-dir");
            Assert.assertTrue(fs.mkdirs(d));
            for (int i=0; i < listLimit*3; i++) {
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
      cluster.waitActive();
      WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME)
          .setPermission(new Path("/"),
              new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

          @Override
          public Void run() throws IOException, URISyntaxException {
            FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
                WebHdfsConstants.WEBHDFS_SCHEME);
            Path d = new Path("/my-dir");
            Assert.assertTrue(fs.mkdirs(d));
            return null;
      conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
      cluster.waitActive();
      FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);
      fs.create(new Path("/testnodatanode"));
      Assert.fail("No exception was thrown");
    } catch (IOException ex) {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final FileSystem webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);

      final Path foo = new Path("/foo");
      dfs.mkdirs(foo);
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final FileSystem webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);

      final Path foo = new Path("/foo");
      dfs.mkdirs(foo);
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final FileSystem webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);

      final Path foo = new Path("/foo");
      dfs.mkdirs(foo);

      final Path foo = new Path("/foo");
      final FileSystem webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);
      try {
        webHdfs.mkdirs(foo);
        fail("Expected RetriableException");
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      final FileSystem webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);
      Assert.assertNull(webHdfs.getDelegationToken(null));
    } finally {
      if (cluster != null) {
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      final FileSystem webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);
      webHdfs.getDelegationToken(null);
      fail("No exception is thrown.");
    } catch (AccessControlException ace) {
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      final WebHdfsFileSystem fs =
          WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME);
      try (OutputStream os = fs.create(new Path(PATH))) {
        os.write(CONTENTS);
      }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHDFSAcl.java
package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.hdfs.server.namenode.FSAclBaseTest;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.BeforeClass;
import org.junit.Ignore;
  @Override
  protected WebHdfsFileSystem createFileSystem() throws Exception {
    return WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME);
  }

  protected WebHdfsFileSystem createFileSystem(UserGroupInformation user)
      throws Exception {
    return WebHdfsTestUtil.getWebHdfsFileSystemAs(user, conf,
      WebHdfsConstants.WEBHDFS_SCHEME);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHDFSForHA.java
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import org.mortbay.util.ajax.JSON;


public class TestWebHDFSForHA {
  private static final String LOGICAL_NAME = "minidfs";
  private static final URI WEBHDFS_URI = URI.create(WebHdfsConstants.WEBHDFS_SCHEME +
          "://" + LOGICAL_NAME);
  private static final MiniDFSNNTopology topo = new MiniDFSNNTopology()
      .addNameservice(new MiniDFSNNTopology.NSConf(LOGICAL_NAME).addNN(

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHDFSXAttr.java
  @Override
  protected WebHdfsFileSystem createFileSystem() throws Exception {
    return WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHdfsFileSystemContract.java
    final UserGroupInformation current = UserGroupInformation.getCurrentUser();
    ugi = UserGroupInformation.createUserForTesting(
        current.getShortUserName() + "x", new String[]{"user"});
    fs = WebHdfsTestUtil.getWebHdfsFileSystemAs(ugi, conf, WebHdfsConstants.WEBHDFS_SCHEME);
    defaultWorkingDirectory = fs.getWorkingDirectory().toUri().getPath();
  }

      UserGroupInformation ugi = UserGroupInformation.createUserForTesting("alpha",
          new String[]{"beta"});
      WebHdfsFileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystemAs(ugi, conf,
          WebHdfsConstants.WEBHDFS_SCHEME);

      fs.mkdirs(p1);
      fs.setPermission(p1, new FsPermission((short) 0444));

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHdfsTimeouts.java
    serverSocket = new ServerSocket(0, CONNECTION_BACKLOG);
    nnHttpAddress = new InetSocketAddress("localhost", serverSocket.getLocalPort());
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "localhost:" + serverSocket.getLocalPort());
    fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME);
    fs.connectionFactory = connectionFactory;
    clients = new ArrayList<SocketChannel>();
    serverThread = null;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHdfsUrl.java

public class TestWebHdfsUrl {
  final URI uri = URI.create(WebHdfsConstants.WEBHDFS_SCHEME + "://" + "127.0.0.1:0");

  @Before
  public void resetUGI() {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHdfsWithMultipleNameNodes.java
    webhdfs = new WebHdfsFileSystem[nNameNodes];
    for(int i = 0; i < webhdfs.length; i++) {
      final InetSocketAddress addr = cluster.getNameNode(i).getHttpAddress();
      final String uri = WebHdfsConstants.WEBHDFS_SCHEME + "://"
          + addr.getHostName() + ":" + addr.getPort() + "/";
      webhdfs[i] = (WebHdfsFileSystem)FileSystem.get(new URI(uri), conf);
    }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/WebHdfsTestUtil.java
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
      URISyntaxException {
    final String uri;

    if (WebHdfsConstants.WEBHDFS_SCHEME.equals(scheme)) {
      uri = WebHdfsConstants.WEBHDFS_SCHEME + "://"
          + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    } else if (WebHdfsConstants.SWEBHDFS_SCHEME.equals(scheme)) {
      uri = WebHdfsConstants.SWEBHDFS_SCHEME + "://"
          + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY);
    } else {
      throw new IllegalArgumentException("unknown scheme:" + scheme);
  public static WebHdfsFileSystem getWebHdfsFileSystemAs(
  final UserGroupInformation ugi, final Configuration conf
  ) throws IOException, InterruptedException {
    return getWebHdfsFileSystemAs(ugi, conf, WebHdfsConstants.WEBHDFS_SCHEME);
  }

  public static WebHdfsFileSystem getWebHdfsFileSystemAs(
    return ugi.doAs(new PrivilegedExceptionAction<WebHdfsFileSystem>() {
      @Override
      public WebHdfsFileSystem run() throws Exception {
        return getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME);
      }
    });
  }

