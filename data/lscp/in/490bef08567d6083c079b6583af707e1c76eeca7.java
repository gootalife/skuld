hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/RpcClientUtil.java
    }
    return clazz.getSimpleName() + "#" + method.getName();
  }

  public static String toTraceName(String fullName) {
    int lastPeriod = fullName.lastIndexOf('.');
    if (lastPeriod < 0) {
      return fullName;
    }
    int secondLastPeriod = fullName.lastIndexOf('.', lastPeriod - 1);
    if (secondLastPeriod < 0) {
      return fullName;
    }
    return fullName.substring(secondLastPeriod + 1, lastPeriod) + "#" +
        fullName.substring(lastPeriod + 1);
  }
}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/Server.java
        TraceInfo parentSpan = new TraceInfo(header.getTraceInfo().getTraceId(),
                                             header.getTraceInfo().getParentId());
        traceSpan = Trace.startSpan(
            RpcClientUtil.toTraceName(rpcRequest.toString()),
            parentSpan).detach();
      }

      Call call = new Call(header.getCallId(), header.getRetryCount(),

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/tracing/TestTracing.java

    String[] expectedSpanNames = {
      "testWriteTraceHooks",
      "ClientProtocol#create",
      "ClientNamenodeProtocol#create",
      "ClientProtocol#fsync",
      "ClientNamenodeProtocol#fsync",
      "ClientProtocol#complete",
      "ClientNamenodeProtocol#complete",
      "newStreamForCreate",
      "DFSOutputStream#write",
      "DFSOutputStream#close",
      "dataStreamer",
      "OpWriteBlockProto",
      "ClientProtocol#addBlock",
      "ClientNamenodeProtocol#addBlock"
    };
    SetSpanReceiver.assertSpanNamesFound(expectedSpanNames);
    String[] spansInTopTrace = {
      "testWriteTraceHooks",
      "ClientProtocol#create",
      "ClientNamenodeProtocol#create",
      "ClientProtocol#fsync",
      "ClientNamenodeProtocol#fsync",
      "ClientProtocol#complete",
      "ClientNamenodeProtocol#complete",
      "newStreamForCreate",
      "DFSOutputStream#write",

    Assert.assertEquals("called",
        map.get("ClientProtocol#create")
           .get(0).getTimelineAnnotations()
           .get(0).getMessage());


    String[] expectedSpanNames = {
      "testReadTraceHooks",
      "ClientProtocol#getBlockLocations",
      "ClientNamenodeProtocol#getBlockLocations",
      "OpReadBlockProto"
    };

