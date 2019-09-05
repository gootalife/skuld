hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java
    ServerSocketChannel httpServerChannel = secureResources != null ?
        secureResources.getHttpServerChannel() : null;

    this.httpServer = new DatanodeHttpServer(conf, this, httpServerChannel);
    httpServer.start();
    if (httpServer.getHttpAddress() != null) {
      infoPort = httpServer.getHttpAddress().getPort();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/DatanodeHttpServer.java
  static final Log LOG = LogFactory.getLog(DatanodeHttpServer.class);

  public DatanodeHttpServer(final Configuration conf,
      final DataNode datanode,
      final ServerSocketChannel externalHttpChannel)
    throws IOException {
    this.conf = conf;
    this.infoServer.addInternalServlet(null, "/getFileChecksum/*",
        FileChecksumServlets.GetServlet.class);

    this.infoServer.setAttribute("datanode", datanode);
    this.infoServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    this.infoServer.addServlet(null, "/blockScannerReport",
                               BlockScanner.Servlet.class);

