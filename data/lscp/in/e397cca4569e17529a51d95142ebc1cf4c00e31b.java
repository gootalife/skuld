hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/tracing/SetSpanReceiver.java
++ b/hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/tracing/SetSpanReceiver.java
package org.apache.hadoop.tracing;

import com.google.common.base.Supplier;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.HTraceConfiguration;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import org.junit.Assert;

public class SetSpanReceiver implements SpanReceiver {

  public SetSpanReceiver(HTraceConfiguration conf) {
  }

  public void receiveSpan(Span span) {
    SetHolder.spans.put(span.getSpanId(), span);
  }

  public void close() {
  }

  public static void clear() {
    SetHolder.spans.clear();
  }

  public static int size() {
    return SetHolder.spans.size();
  }

  public static Collection<Span> getSpans() {
    return SetHolder.spans.values();
  }

  public static Map<String, List<Span>> getMap() {
    return SetHolder.getMap();
  }

  public static class SetHolder {
    public static ConcurrentHashMap<Long, Span> spans =
        new ConcurrentHashMap<Long, Span>();

    public static Map<String, List<Span>> getMap() {
      Map<String, List<Span>> map = new HashMap<String, List<Span>>();

      for (Span s : spans.values()) {
        List<Span> l = map.get(s.getDescription());
        if (l == null) {
          l = new LinkedList<Span>();
          map.put(s.getDescription(), l);
        }
        l.add(s);
      }
      return map;
    }
  }

  static void assertSpanNamesFound(final String[] expectedSpanNames) {
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          Map<String, List<Span>> map = SetSpanReceiver.SetHolder.getMap();
          for (String spanName : expectedSpanNames) {
            if (!map.containsKey(spanName)) {
              return false;
            }
          }
          return true;
        }
      }, 100, 1000);
    } catch (TimeoutException e) {
      Assert.fail("timed out to get expected spans: " + e.getMessage());
    } catch (InterruptedException e) {
      Assert.fail("interrupted while waiting spans: " + e.getMessage());
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/tracing/TestTracing.java
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.htrace.Sampler;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class TestTracing {

    String fileName = "testTracingDisabled.dat";
    writeTestFile(fileName);
    Assert.assertTrue(SetSpanReceiver.size() == 0);
    readTestFile(fileName);
    Assert.assertTrue(SetSpanReceiver.size() == 0);

    writeWithTracing();
    readWithTracing();
      "org.apache.hadoop.hdfs.protocol.ClientProtocol.addBlock",
      "ClientNamenodeProtocol#addBlock"
    };
    SetSpanReceiver.assertSpanNamesFound(expectedSpanNames);

    Map<String, List<Span>> map = SetSpanReceiver.getMap();
    Span s = map.get("testWriteTraceHooks").get(0);
    Assert.assertNotNull(s);
    long spanStart = s.getStartTimeMillis();
           .get(0).getTimelineAnnotations()
           .get(0).getMessage());

    SetSpanReceiver.clear();
  }

  public void readWithTracing() throws Exception {
      "ClientNamenodeProtocol#getBlockLocations",
      "OpReadBlockProto"
    };
    SetSpanReceiver.assertSpanNamesFound(expectedSpanNames);

    Map<String, List<Span>> map = SetSpanReceiver.getMap();
    Span s = map.get("testReadTraceHooks").get(0);
    Assert.assertNotNull(s);


    for (Span span : SetSpanReceiver.getSpans()) {
      Assert.assertEquals(ts.getSpan().getTraceId(), span.getTraceId());
    }
    SetSpanReceiver.clear();
  }

  private void writeTestFile(String testFileName) throws Exception {
        .build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    SetSpanReceiver.clear();
  }

  @After
    cluster.shutdown();
  }

}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/tracing/TestTracingShortCircuitLocalRead.java
    conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_CLIENT_HTRACE_PREFIX +
        SpanReceiverHost.SPAN_RECEIVERS_CONF_SUFFIX,
        SetSpanReceiver.class.getName());
    conf.setLong("dfs.blocksize", 100 * 1024);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY, false);
        "OpRequestShortCircuitAccessProto",
        "ShortCircuitShmRequestProto"
      };
      SetSpanReceiver.assertSpanNamesFound(expectedSpanNames);
    } finally {
      dfs.close();
      cluster.shutdown();

