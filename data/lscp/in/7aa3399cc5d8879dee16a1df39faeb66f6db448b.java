hadoop-hdfs-project/hadoop-hdfs-httpfs/src/main/java/org/apache/hadoop/fs/http/client/HttpFSFileSystem.java
  private HttpURLConnection getConnection(final String method,
      Map<String, String> params, Path path, boolean makeQualified)
  private HttpURLConnection getConnection(final String method,
      Map<String, String> params, Map<String, List<String>> multiValuedParams,
  private HttpURLConnection getConnection(URL url, String method) throws IOException {
    try {

hadoop-hdfs-project/hadoop-hdfs-httpfs/src/main/java/org/apache/hadoop/fs/http/server/CheckUploadContentTypeFilter.java
  @Override
  public void doFilter(ServletRequest request, ServletResponse response,

hadoop-hdfs-project/hadoop-hdfs-httpfs/src/main/java/org/apache/hadoop/fs/http/server/HttpFSServer.java

hadoop-hdfs-project/hadoop-hdfs-httpfs/src/main/java/org/apache/hadoop/lib/servlet/FileSystemReleaseFilter.java
  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)

hadoop-hdfs-project/hadoop-hdfs-httpfs/src/main/java/org/apache/hadoop/lib/servlet/HostnameFilter.java
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)

hadoop-hdfs-project/hadoop-hdfs-httpfs/src/main/java/org/apache/hadoop/lib/servlet/MDCFilter.java
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)

