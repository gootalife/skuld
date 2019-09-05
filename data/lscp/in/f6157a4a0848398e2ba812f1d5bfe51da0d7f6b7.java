hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/JsonUtilClient.java
    return aclStatusBuilder.build();
  }

  static String getPath(final Map<?, ?> json)
      throws IOException {
    if (json == null) {
      return null;
    }

    String path = (String) json.get("Path");
    return path;
  }

  static byte[] getXAttr(final Map<?, ?> json, final String name)
      throws IOException {
    if (json == null) {

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsFileSystem.java
  protected Text tokenServiceName;
  private RetryPolicy retryPolicy = null;
  private Path workingDir;
  private Path cachedHomeDirectory;
  private InetSocketAddress nnAddrs[];
  private int currentNNAddrIndex;
  private boolean disallowFallbackToInsecureCluster;
              failoverSleepMaxMillis);
    }

    this.workingDir = makeQualified(new Path(getHomeDirectoryString(ugi)));
    this.canRefreshDelegationToken = UserGroupInformation.isSecurityEnabled();
    this.disallowFallbackToInsecureCluster = !conf.getBoolean(
        CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
    return NetUtils.getCanonicalUri(uri, getDefaultPort());
  }

  @Deprecated
  public static String getHomeDirectoryString(final UserGroupInformation ugi) {
    return "/user/" + ugi.getShortUserName();
  }

  @Override
  public Path getHomeDirectory() {
    if (cachedHomeDirectory == null) {
      final HttpOpParam.Op op = GetOpParam.Op.GETHOMEDIRECTORY;
      try {
        String pathFromDelegatedFS = new FsPathResponseRunner<String>(op, null,
            new UserParam(ugi)) {
          @Override
          String decodeResponse(Map<?, ?> json) throws IOException {
            return JsonUtilClient.getPath(json);
          }
        }   .run();

        cachedHomeDirectory = new Path(pathFromDelegatedFS).makeQualified(
            this.getUri(), null);

      } catch (IOException e) {
        LOG.error("Unable to get HomeDirectory from original File System", e);
        cachedHomeDirectory = new Path("/user/" + ugi.getShortUserName())
            .makeQualified(this.getUri(), null);
      }
    }
    return cachedHomeDirectory;
  }

  @Override

  @Override
  public synchronized void setWorkingDirectory(final Path dir) {
    Path absolutePath = makeAbsolute(dir);
    String result = absolutePath.toUri().getPath();
    if (!DFSUtilClient.isValidName(result)) {
      throw new IllegalArgumentException("Invalid DFS directory name " +
                                         result);
    }
    workingDir = absolutePath;
  }

  private Path makeAbsolute(Path f) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/web/resources/NamenodeWebHdfsMethods.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclStatus;
      final TokenServiceParam tokenService
      ) throws IOException, URISyntaxException {
    final NameNode namenode = (NameNode)context.getAttribute("name.node");
    final Configuration conf = (Configuration) context
        .getAttribute(JspHelper.CURRENT_CONF);
    final NamenodeProtocols np = getRPCServer(namenode);

    switch(op.getValue()) {
      final String js = JsonUtil.toJsonString(token);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETHOMEDIRECTORY: {
      final String js = JsonUtil.toJsonString("Path",
          FileSystem.get(conf != null ? conf : new Configuration())
              .getHomeDirectory().toUri().getPath());
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETACLSTATUS: {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHDFS.java

package org.apache.hadoop.hdfs.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
      }
    }
  }

  @Test(timeout = 30000)
  public void testGetHomeDirectory() throws Exception {

    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      DistributedFileSystem hdfs = cluster.getFileSystem();

      final URI uri = new URI(WebHdfsConstants.WEBHDFS_SCHEME + "://"
          + cluster.getHttpUri(0).replace("http://", ""));
      final Configuration confTemp = new Configuration();

      {
        WebHdfsFileSystem webhdfs = (WebHdfsFileSystem) FileSystem.get(uri,
            confTemp);

        assertEquals(hdfs.getHomeDirectory().toUri().getPath(), webhdfs
            .getHomeDirectory().toUri().getPath());

        webhdfs.close();
      }

      {
        WebHdfsFileSystem webhdfs = createWebHDFSAsTestUser(confTemp, uri,
            "XXX");

        assertNotEquals(hdfs.getHomeDirectory().toUri().getPath(), webhdfs
            .getHomeDirectory().toUri().getPath());

        webhdfs.close();
      }

    } finally {
      if (cluster != null)
        cluster.shutdown();
    }
  }

  private WebHdfsFileSystem createWebHDFSAsTestUser(final Configuration conf,
      final URI uri, final String userName) throws Exception {

    final UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        userName, new String[] { "supergroup" });

    return ugi.doAs(new PrivilegedExceptionAction<WebHdfsFileSystem>() {
      @Override
      public WebHdfsFileSystem run() throws IOException {
        WebHdfsFileSystem webhdfs = (WebHdfsFileSystem) FileSystem.get(uri,
            conf);
        return webhdfs;
      }
    });
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHdfsFileSystemContract.java
      final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      final Map<?, ?> m = WebHdfsTestUtil.connectAndGetJson(
          conn, HttpServletResponse.SC_OK);
      assertEquals(webhdfs.getHomeDirectory().toUri().getPath(),
          m.get(Path.class.getSimpleName()));
      conn.disconnect();
    }

