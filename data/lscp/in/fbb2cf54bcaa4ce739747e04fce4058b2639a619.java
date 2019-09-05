hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DataStreamer.java
    try {
      sock = createSocketForPipeline(src, 2, dfsClient);
      final long writeTimeout = dfsClient.getDatanodeWriteTimeout(2);
      final long readTimeout = dfsClient.getDatanodeReadTimeout(2);

      OutputStream unbufOut = NetUtils.getOutputStream(sock, writeTimeout);
      InputStream unbufIn = NetUtils.getInputStream(sock, readTimeout);
      IOStreamPair saslStreams = dfsClient.saslClient.socketSend(sock,
          unbufOut, unbufIn, dfsClient, blockToken, src);
      unbufOut = saslStreams.out;
        assert null == blockReplyStream : "Previous blockReplyStream unclosed";
        s = createSocketForPipeline(nodes[0], nodes.length, dfsClient);
        long writeTimeout = dfsClient.getDatanodeWriteTimeout(nodes.length);
        long readTimeout = dfsClient.getDatanodeReadTimeout(nodes.length);

        OutputStream unbufOut = NetUtils.getOutputStream(s, writeTimeout);
        InputStream unbufIn = NetUtils.getInputStream(s, readTimeout);
        IOStreamPair saslStreams = dfsClient.saslClient.socketSend(s,
            unbufOut, unbufIn, dfsClient, accessToken, nodes[0]);
        unbufOut = saslStreams.out;

