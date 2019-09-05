hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/Server.java
          CurCall.set(call);
          if (call.traceSpan != null) {
            traceScope = Trace.continueSpan(call.traceSpan);
            traceScope.getSpan().addTimelineAnnotation("called");
          }

          try {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/tracing/TestTracing.java
        Assert.assertEquals(ts.getSpan().getTraceId(), span.getTraceId());
      }
    }

    Assert.assertEquals("called",
        map.get("org.apache.hadoop.hdfs.protocol.ClientProtocol.create")
           .get(0).getTimelineAnnotations()
           .get(0).getMessage());

    SetSpanReceiver.SetHolder.spans.clear();
  }


