hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/ProtobufRpcEngine.java
        }
        if (Trace.isTracing()) {
          traceScope.getSpan().addTimelineAnnotation(
              "Call got exception: " + e.toString());
        }
        throw new ServiceException(e);
      } finally {

