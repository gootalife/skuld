hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/FSImageHandler.java
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
    case "GETACLSTATUS":
      content = image.getAclStatus(path);
      break;
    case "GETXATTRS":
      List<String> names = getXattrNames(decoder);
      String encoder = getEncoder(decoder);
      content = image.getXAttrs(path, names, encoder);
      break;
    case "LISTXATTRS":
      content = image.listXAttrs(path);
      break;
    default:
      throw new IllegalArgumentException("Invalid value for webhdfs parameter"
          + " \"op\"");
    }

    LOG.info("op=" + op + " target=" + path);

    DefaultFullHttpResponse resp = new DefaultFullHttpResponse(HTTP_1_1,
        HttpResponseStatus.OK, Unpooled.wrappedBuffer(content
            .getBytes(Charsets.UTF_8)));
    resp.headers().set(CONTENT_TYPE, APPLICATION_JSON_UTF8);
    resp.headers().set(CONTENT_LENGTH, resp.content().readableBytes());
    resp.headers().set(CONNECTION, CLOSE);
      resp.setStatus(BAD_REQUEST);
    } else if (e instanceof FileNotFoundException) {
      resp.setStatus(NOT_FOUND);
    } else if (e instanceof IOException) {
      resp.setStatus(FORBIDDEN);
    }
    resp.headers().set(CONTENT_LENGTH, resp.content().readableBytes());
    resp.headers().set(CONNECTION, CLOSE);
    ctx.write(resp).addListener(ChannelFutureListener.CLOSE);
        ? StringUtils.toUpperCase(parameters.get("op").get(0)) : null;
  }

  private static List<String> getXattrNames(QueryStringDecoder decoder) {
    Map<String, List<String>> parameters = decoder.parameters();
    return parameters.get("xattr.name");
  }

  private static String getEncoder(QueryStringDecoder decoder) {
    Map<String, List<String>> parameters = decoder.parameters();
    return parameters.containsKey("encoding") ? parameters.get("encoding").get(
        0) : null;
  }

  private static String getPath(QueryStringDecoder decoder)
          throws FileNotFoundException {
    String path = decoder.path();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/FSImageLoader.java
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.resources.XAttrEncodingParam;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.LimitInputStream;
import org.codehaus.jackson.map.ObjectMapper;
    return list;
  }

  String listXAttrs(String path) throws IOException {
    return JsonUtil.toJsonString(getXAttrList(path));
  }

  String getXAttrs(String path, List<String> names, String encoder)
      throws IOException {

    List<XAttr> xAttrs = getXAttrList(path);
    List<XAttr> filtered;
    if (names == null || names.size() == 0) {
      filtered = xAttrs;
    } else {
      filtered = Lists.newArrayListWithCapacity(names.size());
      for (String name : names) {
        XAttr search = XAttrHelper.buildXAttr(name);

        boolean found = false;
        for (XAttr aXAttr : xAttrs) {
          if (aXAttr.getNameSpace() == search.getNameSpace()
              && aXAttr.getName().equals(search.getName())) {

            filtered.add(aXAttr);
            found = true;
            break;
          }
        }

        if (!found) {
          throw new IOException(
              "At least one of the attributes provided was not found.");
        }
      }

    }
    return JsonUtil.toJsonString(filtered,
        new XAttrEncodingParam(encoder).getEncoding());
  }

  private List<XAttr> getXAttrList(String path) throws IOException {
    long id = lookup(path);
    FsImageProto.INodeSection.INode inode = fromINodeId(id);
    switch (inode.getType()) {
    case FILE:
      return FSImageFormatPBINode.Loader.loadXAttrs(
          inode.getFile().getXAttrs(), stringTable);
    case DIRECTORY:
      return FSImageFormatPBINode.Loader.loadXAttrs(inode.getDirectory()
          .getXAttrs(), stringTable);
    default:
      return null;
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/TestOfflineImageViewerForXAttr.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/TestOfflineImageViewerForXAttr.java
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.net.NetUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestOfflineImageViewerForXAttr {

  private static final Log LOG = LogFactory
      .getLog(TestOfflineImageViewerForXAttr.class);

  private static File originalFsimage = null;

  static String attr1JSon;

  @BeforeClass
  public static void createOriginalFSImage() throws IOException {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();

    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      DistributedFileSystem hdfs = cluster.getFileSystem();
      Path dir = new Path("/dir1");
      hdfs.mkdirs(dir);
      hdfs.setXAttr(dir, "user.attr1", "value1".getBytes());
      hdfs.setXAttr(dir, "user.attr2", "value2".getBytes());
      hdfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER, false);
      hdfs.saveNamespace();

      List<XAttr> attributes = new ArrayList<XAttr>();
      attributes.add(XAttrHelper.buildXAttr("user.attr1", "value1".getBytes()));

      attr1JSon = JsonUtil.toJsonString(attributes, null);

      attributes.add(XAttrHelper.buildXAttr("user.attr2", "value2".getBytes()));

      originalFsimage = FSImageTestUtil.findLatestImageFile(FSImageTestUtil
          .getFSImage(cluster.getNameNode()).getStorage().getStorageDir(0));
      if (originalFsimage == null) {
        throw new RuntimeException("Didn't generate or can't find fsimage");
      }
      LOG.debug("original FS image file is " + originalFsimage);
    } finally {
      if (cluster != null)
        cluster.shutdown();
    }
  }

  @AfterClass
  public static void deleteOriginalFSImage() throws IOException {
    if (originalFsimage != null && originalFsimage.exists()) {
      originalFsimage.delete();
    }
  }

  @Test
  public void testWebImageViewerForListXAttrs() throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();

      URL url = new URL("http://localhost:" + port
          + "/webhdfs/v1/dir1/?op=LISTXATTRS");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();

      assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());

      String content = IOUtils.toString(connection.getInputStream());

      assertTrue("Missing user.attr1 in response ",
          content.contains("user.attr1"));
      assertTrue("Missing user.attr2 in response ",
          content.contains("user.attr2"));

    }
  }

  @Test
  public void testWebImageViewerForGetXAttrsWithOutParameters()
      throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();

      URL url = new URL("http://localhost:" + port
          + "/webhdfs/v1/dir1/?op=GETXATTRS");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();

      assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
      String content = IOUtils.toString(connection.getInputStream());

      assertTrue("Missing user.attr1 in response ",
          content.contains("user.attr1"));
      assertTrue("Missing user.attr2 in response ",
          content.contains("user.attr2"));
    }
  }

  @Test
  public void testWebImageViewerForGetXAttrsWithParameters() throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {

      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();

      URL url = new URL("http://localhost:" + port
          + "/webhdfs/v1/dir1/?op=GETXATTRS&xattr.name=attr8");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();

      assertEquals(HttpURLConnection.HTTP_BAD_REQUEST,
          connection.getResponseCode());

      url = new URL("http://localhost:" + port
          + "/webhdfs/v1/dir1/?op=GETXATTRS&xattr.name=user.attr1");
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();

      assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
      String content = IOUtils.toString(connection.getInputStream());
      assertEquals(attr1JSon, content);
    }
  }

  @Test
  public void testWebImageViewerForGetXAttrsWithCodecParameters()
      throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();

      URL url = new URL(
          "http://localhost:"
              + port
              + "/webhdfs/v1/dir1/?op=GETXATTRS&xattr.name=USER.attr1&encoding=TEXT");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();

      assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
      String content = IOUtils.toString(connection.getInputStream());
      assertEquals(attr1JSon, content);

    }
  }

  @Test
  public void testWithWebHdfsFileSystem() throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();

      URI uri = new URI("webhdfs://localhost:" + String.valueOf(port));
      Configuration conf = new Configuration();
      WebHdfsFileSystem webhdfs = (WebHdfsFileSystem) FileSystem.get(uri, conf);

      List<String> names = webhdfs.listXAttrs(new Path("/dir1"));
      assertTrue(names.contains("user.attr1"));
      assertTrue(names.contains("user.attr2"));

      String value = new String(webhdfs.getXAttr(new Path("/dir1"),
          "user.attr1"));
      assertEquals("value1", value);

      Map<String, byte[]> contentMap = webhdfs.getXAttrs(new Path("/dir1"),
          names);

      assertEquals("value1", new String(contentMap.get("user.attr1")));
      assertEquals("value2", new String(contentMap.get("user.attr2")));
    }
  }

  @Test
  public void testResponseCode() throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();

      URL url = new URL(
          "http://localhost:"
              + port
              + "/webhdfs/v1/dir1/?op=GETXATTRS&xattr.name=user.notpresent&encoding=TEXT");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();

      assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
          connection.getResponseCode());

    }
  }
}

