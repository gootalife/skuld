hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/DatanodeHttpServer.java
package org.apache.hadoop.hdfs.server.datanode.web;

import io.netty.channel.ChannelFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
        .childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
          ch.pipeline().addLast(new PortUnificationServerHandler(jettyAddr,
              conf, confForCreate));
        }
      });
      if (externalHttpChannel == null) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/PortUnificationServerHandler.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/PortUnificationServerHandler.java
package org.apache.hadoop.hdfs.server.datanode.web;

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.web.dtp.DtpHttp2Handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.stream.ChunkedWriteHandler;

@InterfaceAudience.Private
public class PortUnificationServerHandler extends ByteToMessageDecoder {

  private static final ByteBuf HTTP2_CLIENT_CONNECTION_PREFACE = Http2CodecUtil
      .connectionPrefaceBuf();

  private static final int MAGIC_HEADER_LENGTH = 3;

  private final InetSocketAddress proxyHost;

  private final Configuration conf;

  private final Configuration confForCreate;

  public PortUnificationServerHandler(InetSocketAddress proxyHost,
      Configuration conf, Configuration confForCreate) {
    this.proxyHost = proxyHost;
    this.conf = conf;
    this.confForCreate = confForCreate;
  }

  private void configureHttp1(ChannelHandlerContext ctx) {
    ctx.pipeline().addLast(new HttpServerCodec(), new ChunkedWriteHandler(),
        new URLDispatcher(proxyHost, conf, confForCreate));
  }

  private void configureHttp2(ChannelHandlerContext ctx) {
    ctx.pipeline().addLast(new DtpHttp2Handler());
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in,
      List<Object> out) throws Exception {
    if (in.readableBytes() < MAGIC_HEADER_LENGTH) {
      return;
    }
    if (ByteBufUtil.equals(in, 0, HTTP2_CLIENT_CONNECTION_PREFACE, 0,
        MAGIC_HEADER_LENGTH)) {
      configureHttp2(ctx);
    } else {
      configureHttp1(ctx);
    }
    ctx.pipeline().remove(this);
  }

}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/SimpleHttpProxyHandler.java
package org.apache.hadoop.hdfs.server.datanode.web;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;

  @Override
  public void channelRead0
    (final ChannelHandlerContext ctx, final HttpRequest req) {
    uri = req.uri();
    final Channel client = ctx.channel();
    Bootstrap proxiedServer = new Bootstrap()
      .group(client.eventLoop())
        if (future.isSuccess()) {
          ctx.channel().pipeline().remove(HttpResponseEncoder.class);
          HttpRequest newReq = new DefaultFullHttpRequest(HTTP_1_1,
            req.method(), req.uri());
          newReq.headers().add(req.headers());
          newReq.headers().set(CONNECTION, HttpHeaderValues.CLOSE);
          future.channel().writeAndFlush(newReq);
        } else {
          DefaultHttpResponse resp = new DefaultHttpResponse(HTTP_1_1,
            INTERNAL_SERVER_ERROR);
          resp.headers().set(CONNECTION, HttpHeaderValues.CLOSE);
          LOG.info("Proxy " + uri + " failed. Cause: ", future.cause());
          ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
          client.close();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/URLDispatcher.java
package org.apache.hadoop.hdfs.server.datanode.web;

import static org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler.WEBHDFS_PREFIX;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler;

class URLDispatcher extends SimpleChannelInboundHandler<HttpRequest> {
  private final InetSocketAddress proxyHost;
  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpRequest req)
    throws Exception {
    String uri = req.uri();
    ChannelPipeline p = ctx.pipeline();
    if (uri.startsWith(WEBHDFS_PREFIX)) {
      WebHdfsHandler h = new WebHdfsHandler(conf, confForCreate);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/dtp/DtpHttp2FrameListener.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/dtp/DtpHttp2FrameListener.java
package org.apache.hadoop.hdfs.server.datanode.web.dtp;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;

import java.nio.charset.StandardCharsets;

class DtpHttp2FrameListener extends Http2FrameAdapter {

  private Http2ConnectionEncoder encoder;

  public void encoder(Http2ConnectionEncoder encoder) {
    this.encoder = encoder;
  }

  @Override
  public void onHeadersRead(ChannelHandlerContext ctx, int streamId,
      Http2Headers headers, int streamDependency, short weight,
      boolean exclusive, int padding, boolean endStream) throws Http2Exception {
    encoder.writeHeaders(ctx, streamId,
      new DefaultHttp2Headers().status(HttpResponseStatus.OK.codeAsText()), 0,
      false, ctx.newPromise());
    encoder.writeData(
      ctx,
      streamId,
      ctx.alloc().buffer()
          .writeBytes("HTTP/2 DTP".getBytes(StandardCharsets.UTF_8)), 0, true,
      ctx.newPromise());
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/dtp/DtpHttp2Handler.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/dtp/DtpHttp2Handler.java
package org.apache.hadoop.hdfs.server.datanode.web.dtp;

import org.apache.hadoop.classification.InterfaceAudience;

import io.netty.handler.codec.http2.Http2ConnectionHandler;

@InterfaceAudience.Private
public class DtpHttp2Handler extends Http2ConnectionHandler {

  public DtpHttp2Handler() {
    super(true, new DtpHttp2FrameListener());
    ((DtpHttp2FrameListener) decoder().listener()).encoder(encoder());
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/webhdfs/ExceptionHandler.java
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler.APPLICATION_JSON_UTF8;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.token.SecretManager;

import com.google.common.base.Charsets;
import com.sun.jersey.api.ParamException;
import com.sun.jersey.api.container.ContainerException;

class ExceptionHandler {
  static Log LOG = WebHdfsHandler.LOG;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/webhdfs/HdfsWriter.java
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.io.IOUtils;

class HdfsWriter extends SimpleChannelInboundHandler<HttpContent> {
  private final DFSClient client;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/webhdfs/WebHdfsHandler.java
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderNames.LOCATION;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HDFS_URI_SCHEME;
import static org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier.HDFS_DELEGATION_KIND;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.stream.ChunkedStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;

import org.apache.commons.io.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.LimitInputStream;

import com.google.common.base.Preconditions;

public class WebHdfsHandler extends SimpleChannelInboundHandler<HttpRequest> {
  static final Log LOG = LogFactory.getLog(WebHdfsHandler.class);
  @Override
  public void channelRead0(final ChannelHandlerContext ctx,
                           final HttpRequest req) throws Exception {
    Preconditions.checkArgument(req.uri().startsWith(WEBHDFS_PREFIX));
    QueryStringDecoder queryString = new QueryStringDecoder(req.uri());
    params = new ParameterParser(queryString, conf);
    DataNodeUGIProvider ugiProvider = new DataNodeUGIProvider(params);
    ugi = ugiProvider.ugi();
  public void handle(ChannelHandlerContext ctx, HttpRequest req)
    throws IOException, URISyntaxException {
    String op = params.op();
    HttpMethod method = req.method();
    if (PutOpParam.Op.CREATE.name().equalsIgnoreCase(op)
      && method == PUT) {
      onCreate(ctx);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/FSImageHandler.java
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler.APPLICATION_JSON_UTF8;
import static org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler.WEBHDFS_PREFIX;
import static org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler.WEBHDFS_PREFIX_LENGTH;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Charsets;

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest request)
          throws Exception {
    if (request.method() != HttpMethod.GET) {
      DefaultHttpResponse resp = new DefaultHttpResponse(HTTP_1_1,
        METHOD_NOT_ALLOWED);
      resp.headers().set(CONNECTION, CLOSE);
      return;
    }

    QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
    final String op = getOp(decoder);

    final String content;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/web/dtp/Http2ResponseHandler.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/web/dtp/Http2ResponseHandler.java
package org.apache.hadoop.hdfs.server.datanode.web.dtp;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http2.HttpUtil;
import io.netty.util.concurrent.Promise;

import java.util.HashMap;
import java.util.Map;

public class Http2ResponseHandler extends
    SimpleChannelInboundHandler<FullHttpResponse> {

  private Map<Integer, Promise<FullHttpResponse>> streamId2Promise =
      new HashMap<>();

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg)
      throws Exception {
    Integer streamId =
        msg.headers().getInt(HttpUtil.ExtensionHeaderNames.STREAM_ID.text());
    if (streamId == null) {
      System.err.println("HttpResponseHandler unexpected message received: "
          + msg);
      return;
    }
    if (streamId.intValue() == 1) {
      return;
    }
    Promise<FullHttpResponse> promise;
    synchronized (this) {
      promise = streamId2Promise.get(streamId);
    }
    if (promise == null) {
      System.err.println("Message received for unknown stream id " + streamId);
    } else {
      promise.setSuccess(msg.retain());

    }
  }

  public void put(Integer streamId, Promise<FullHttpResponse> promise) {
    streamId2Promise.put(streamId, promise);
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/web/dtp/TestDtpHttp2.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/web/dtp/TestDtpHttp2.java
package org.apache.hadoop.hdfs.server.datanode.web.dtp;

import static org.junit.Assert.assertEquals;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DefaultHttp2FrameReader;
import io.netty.handler.codec.http2.DefaultHttp2FrameWriter;
import io.netty.handler.codec.http2.DelegatingDecompressorFrameListener;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2FrameReader;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.Http2InboundFrameLogger;
import io.netty.handler.codec.http2.Http2OutboundFrameLogger;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;
import io.netty.handler.codec.http2.HttpUtil;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapter;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.timeout.TimeoutException;
import io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDtpHttp2 {

  private static final Http2FrameLogger FRAME_LOGGER = new Http2FrameLogger(
      LogLevel.INFO, TestDtpHttp2.class);

  private static final Configuration CONF = WebHdfsTestUtil.createConf();

  private static MiniDFSCluster CLUSTER;

  private static final EventLoopGroup WORKER_GROUP = new NioEventLoopGroup();

  private static Channel CHANNEL;

  private static Http2ResponseHandler RESPONSE_HANDLER;

  @BeforeClass
  public static void setUp() throws IOException, URISyntaxException,
      TimeoutException {
    CLUSTER = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    CLUSTER.waitActive();

    RESPONSE_HANDLER = new Http2ResponseHandler();
    Bootstrap bootstrap =
        new Bootstrap()
            .group(WORKER_GROUP)
            .channel(NioSocketChannel.class)
            .remoteAddress("127.0.0.1",
              CLUSTER.getDataNodes().get(0).getInfoPort())
            .handler(new ChannelInitializer<Channel>() {

              @Override
              protected void initChannel(Channel ch) throws Exception {
                Http2Connection connection = new DefaultHttp2Connection(false);
                Http2ConnectionHandler connectionHandler =
                    new HttpToHttp2ConnectionHandler(connection, frameReader(),
                        frameWriter(), new DelegatingDecompressorFrameListener(
                            connection, new InboundHttp2ToHttpAdapter.Builder(
                                connection).maxContentLength(Integer.MAX_VALUE)
                                .propagateSettings(true).build()));
                ch.pipeline().addLast(connectionHandler, RESPONSE_HANDLER);
              }
            });
    CHANNEL = bootstrap.connect().syncUninterruptibly().channel();

  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (CHANNEL != null) {
      CHANNEL.close().syncUninterruptibly();
    }
    WORKER_GROUP.shutdownGracefully();
    if (CLUSTER != null) {
      CLUSTER.shutdown();
    }
  }

  private static Http2FrameReader frameReader() {
    return new Http2InboundFrameLogger(new DefaultHttp2FrameReader(),
        FRAME_LOGGER);
  }

  private static Http2FrameWriter frameWriter() {
    return new Http2OutboundFrameLogger(new DefaultHttp2FrameWriter(),
        FRAME_LOGGER);
  }

  @Test
  public void test() throws InterruptedException, ExecutionException {
    int streamId = 3;
    FullHttpRequest request =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    request.headers().add(HttpUtil.ExtensionHeaderNames.STREAM_ID.text(),
      streamId);
    Promise<FullHttpResponse> promise = CHANNEL.eventLoop().newPromise();
    synchronized (RESPONSE_HANDLER) {
      CHANNEL.writeAndFlush(request);
      RESPONSE_HANDLER.put(streamId, promise);
    }
    assertEquals(HttpResponseStatus.OK, promise.get().status());
    ByteBuf content = promise.get().content();
    assertEquals("HTTP/2 DTP", content.toString(StandardCharsets.UTF_8));
  }
}

