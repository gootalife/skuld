hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/DatanodeHttpServer.java

import java.io.Closeable;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ServerSocketChannel;
import java.security.GeneralSecurityException;

    return httpsAddress;
  }

  public void start() throws IOException {
    if (httpServer != null) {
      InetSocketAddress infoAddr = DataNode.getInfoAddr(conf);
      ChannelFuture f = httpServer.bind(infoAddr);
      try {
        f.syncUninterruptibly();
      } catch (Throwable e) {
        if (e instanceof BindException) {
          throw NetUtils.wrapException(null, 0, infoAddr.getHostName(),
              infoAddr.getPort(), (SocketException) e);
        } else {
          throw e;
        }
      }
      httpAddress = (InetSocketAddress) f.channel().localAddress();
      LOG.info("Listening HTTP traffic on " + httpAddress);
    }

    if (httpsServer != null) {
      InetSocketAddress secInfoSocAddr =
          NetUtils.createSocketAddr(conf.getTrimmed(
              DFS_DATANODE_HTTPS_ADDRESS_KEY,
              DFS_DATANODE_HTTPS_ADDRESS_DEFAULT));
      ChannelFuture f = httpsServer.bind(secInfoSocAddr);

      try {
        f.syncUninterruptibly();
      } catch (Throwable e) {
        if (e instanceof BindException) {
          throw NetUtils.wrapException(null, 0, secInfoSocAddr.getHostName(),
              secInfoSocAddr.getPort(), (SocketException) e);
        } else {
          throw e;
        }
      }
      httpsAddress = (InetSocketAddress) f.channel().localAddress();
      LOG.info("Listening HTTPS traffic on " + httpsAddress);
    }

