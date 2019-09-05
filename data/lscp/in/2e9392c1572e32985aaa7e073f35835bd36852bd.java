hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/tracing/TestTraceAdmin.java
      Assert.assertEquals("ret:0, [no span receivers found]" + NEWLINE,
          runTraceCommand(trace, "-list", "-host", getHostPortForNN(cluster)));
      Assert.assertEquals("ret:0, Added trace span receiver 1 with " +
          "configuration dfs.htrace.local-file-span-receiver.path = " + tracePath + NEWLINE,
          runTraceCommand(trace, "-add", "-host", getHostPortForNN(cluster),
              "-class", "org.apache.htrace.impl.LocalFileSpanReceiver",
              "-Cdfs.htrace.local-file-span-receiver.path=" + tracePath));
      String list =
          runTraceCommand(trace, "-list", "-host", getHostPortForNN(cluster));
      Assert.assertTrue(list.startsWith("ret:0"));
      Assert.assertEquals("ret:0, [no span receivers found]" + NEWLINE,
          runTraceCommand(trace, "-list", "-host", getHostPortForNN(cluster)));
      Assert.assertEquals("ret:0, Added trace span receiver 2 with " +
          "configuration dfs.htrace.local-file-span-receiver.path = " + tracePath + NEWLINE,
          runTraceCommand(trace, "-add", "-host", getHostPortForNN(cluster),
              "-class", "LocalFileSpanReceiver",
              "-Cdfs.htrace.local-file-span-receiver.path=" + tracePath));
      Assert.assertEquals("ret:0, Removed trace span receiver 2" + NEWLINE,
          runTraceCommand(trace, "-remove", "2", "-host",
              getHostPortForNN(cluster)));

