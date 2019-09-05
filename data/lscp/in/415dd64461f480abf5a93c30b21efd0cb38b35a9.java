hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-shuffle/src/main/java/org/apache/hadoop/mapred/ShuffleHandler.java
        try {
          MapOutputInfo info = mapOutputInfoMap.get(mapId);
          if (info == null) {
            info = getMapOutputInfo(outputBasePathStr + mapId,
                mapId, reduceId, user);
          }
          lastMap =
              sendMapOutput(ctx, ch, user, mapId,

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-shuffle/src/test/java/org/apache/hadoop/mapred/TestShuffleHandler.java
      Assert.assertTrue((new String(byteArr)).contains(message));
    } finally {
      shuffleHandler.stop();
      FileUtil.fullyDelete(absLogDir);
    }
  }

    conn.disconnect();
    return rc;
  }

  @Test(timeout = 100000)
  public void testGetMapOutputInfo() throws Exception {
    final ArrayList<Throwable> failures = new ArrayList<Throwable>(1);
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    File absLogDir = new File("target", TestShuffleHandler.class.
        getSimpleName() + "LocDir").getAbsoluteFile();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, absLogDir.getAbsolutePath());
    ApplicationId appId = ApplicationId.newInstance(12345, 1);
    String appAttemptId = "attempt_12345_1_m_1_0";
    String user = "randomUser";
    String reducerId = "0";
    List<File> fileMap = new ArrayList<File>();
    createShuffleHandlerFiles(absLogDir, user, appId.toString(), appAttemptId,
        conf, fileMap);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(Configuration conf) {
        return new Shuffle(conf) {
          @Override
          protected void populateHeaders(List<String> mapIds,
              String outputBaseStr, String user, int reduce,
              HttpRequest request, HttpResponse response,
              boolean keepAliveParam, Map<String, MapOutputInfo> infoMap)
              throws IOException {
            super.setResponseHeaders(response, keepAliveParam, 100);
          }
          @Override
          protected void verifyRequest(String appid,
              ChannelHandlerContext ctx, HttpRequest request,
              HttpResponse response, URL requestUri) throws IOException {
          }
          @Override
          protected void sendError(ChannelHandlerContext ctx, String message,
              HttpResponseStatus status) {
            if (failures.size() == 0) {
              failures.add(new Error(message));
              ctx.getChannel().close();
            }
          }
          @Override
          protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx,
              Channel ch, String user, String mapId, int reduce,
              MapOutputInfo info) throws IOException {
            ShuffleHeader header =
                new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
            DataOutputBuffer dob = new DataOutputBuffer();
            header.write(dob);
            return ch.write(wrappedBuffer(dob.getData(), 0, dob.getLength()));
          }
        };
      }
    };
    shuffleHandler.init(conf);
    try {
      shuffleHandler.start();
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt =
          new Token<JobTokenIdentifier>("identifier".getBytes(),
          "password".getBytes(), new Text(user), new Text("shuffleService"));
      jt.write(outputBuffer);
      shuffleHandler
          .initializeApplication(new ApplicationInitializationContext(user,
          appId, ByteBuffer.wrap(outputBuffer.getData(), 0,
          outputBuffer.getLength())));
      URL url =
          new URL(
              "http://127.0.0.1:"
                  + shuffleHandler.getConfig().get(
                      ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
                  + "/mapOutput?job=job_12345_0001&reduce=" + reducerId
                  + "&map=attempt_12345_1_m_1_0");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      conn.connect();
      try {
        DataInputStream is = new DataInputStream(conn.getInputStream());
        ShuffleHeader header = new ShuffleHeader();
        header.readFields(is);
        is.close();
      } catch (EOFException e) {
      }
      Assert.assertEquals(failures.size(), 0);
    } finally {
      shuffleHandler.stop();
      FileUtil.fullyDelete(absLogDir);
    }
  }
}

