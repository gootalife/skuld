hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java
package org.apache.hadoop.hdfs.server.datanode;


import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.datanode.web.DatanodeHttpServer;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.tracing.SpanReceiverHost;
  private volatile boolean heartbeatsDisabledForTests = false;
  private DataStorage storage = null;

  private DatanodeHttpServer httpServer = null;
  private int infoPort;
  private int infoSecurePort;
  private void startInfoServer(Configuration conf)
    throws IOException {
    ServerSocketChannel httpServerChannel = secureResources != null ?
        secureResources.getHttpServerChannel() : null;

    this.httpServer = new DatanodeHttpServer(conf, httpServerChannel);
    httpServer.start();
    if (httpServer.getHttpAddress() != null) {
      infoPort = httpServer.getHttpAddress().getPort();
    shutdownPeriodicScanners();

    if (httpServer != null) {
      try {
        httpServer.close();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/DatanodeHttpServer.java
package org.apache.hadoop.hdfs.server.datanode.web;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.datanode.BlockScanner;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FileChecksumServlets;
import org.apache.hadoop.hdfs.server.namenode.StreamFile;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.ssl.SSLFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.nio.channels.ServerSocketChannel;
import java.security.GeneralSecurityException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ADMIN;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY;

public class DatanodeHttpServer implements Closeable {
  private final HttpServer2 infoServer;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final ServerSocketChannel externalHttpChannel;

  static final Log LOG = LogFactory.getLog(DatanodeHttpServer.class);

  public DatanodeHttpServer(final Configuration conf,
      final ServerSocketChannel externalHttpChannel)
    throws IOException {
    this.conf = conf;

    Configuration confForInfoServer = new Configuration(conf);
    confForInfoServer.setInt(HttpServer2.HTTP_MAX_THREADS, 10);
    HttpServer2.Builder builder = new HttpServer2.Builder()
        .setName("datanode")
        .setConf(confForInfoServer)
        .setACL(new AccessControlList(conf.get(DFS_ADMIN, " ")))
        .hostName(getHostnameForSpnegoPrincipal(confForInfoServer))
        .addEndpoint(URI.create("http://localhost:0"))
        .setFindPort(true);

    this.infoServer = builder.build();

    this.infoServer.addInternalServlet(null, "/streamFile/*", StreamFile.class);
    this.infoServer.addInternalServlet(null, "/getFileChecksum/*",
        FileChecksumServlets.GetServlet.class);

    this.infoServer.setAttribute("datanode", this);
    this.infoServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    this.infoServer.addServlet(null, "/blockScannerReport",
                               BlockScanner.Servlet.class);

    this.infoServer.start();
    final InetSocketAddress jettyAddr = infoServer.getConnectorAddress(0);

    this.confForCreate = new Configuration(conf);
    confForCreate.set(FsPermission.UMASK_LABEL, "000");

    if (externalHttpChannel != null) {
      externalHttpChannel.close();
    }
    try {
      infoServer.stop();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private static String getHostnameForSpnegoPrincipal(Configuration conf) {
    String addr = conf.getTrimmed(DFS_DATANODE_HTTP_ADDRESS_KEY, null);
    if (addr == null) {
      addr = conf.getTrimmed(DFS_DATANODE_HTTPS_ADDRESS_KEY,
                             DFS_DATANODE_HTTPS_ADDRESS_DEFAULT);
    }
    InetSocketAddress inetSocker = NetUtils.createSocketAddr(addr);
    return inetSocker.getHostString();
  }
}

