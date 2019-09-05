hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/tracing/TestTracing.java
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
  private static SpanReceiverHost spanReceiverHost;

  @Test
  public void testTracing() throws Exception {
    Assert.assertEquals(spanReceiverHost,
        SpanReceiverHost.getInstance(new Configuration()));

    String fileName = "testTracingDisabled.dat";
    writeTestFile(fileName);
    Assert.assertTrue(SetSpanReceiver.SetHolder.size() == 0);
    readTestFile(fileName);
    Assert.assertTrue(SetSpanReceiver.SetHolder.size() == 0);

    writeWithTracing();
    readWithTracing();
  }

  public void writeWithTracing() throws Exception {
    long startTime = System.currentTimeMillis();
    TraceScope ts = Trace.startSpan("testWriteTraceHooks", Sampler.ALWAYS);
    writeTestFile("testWriteTraceHooks.dat");
    long endTime = System.currentTimeMillis();
    ts.close();

        Assert.assertEquals(ts.getSpan().getTraceId(), span.getTraceId());
      }
    }
    SetSpanReceiver.SetHolder.spans.clear();
  }

  public void readWithTracing() throws Exception {
    String fileName = "testReadTraceHooks.dat";
    writeTestFile(fileName);
    long startTime = System.currentTimeMillis();
    TraceScope ts = Trace.startSpan("testReadTraceHooks", Sampler.ALWAYS);
    readTestFile(fileName);
    ts.close();
    long endTime = System.currentTimeMillis();

    String[] expectedSpanNames = {
      "testReadTraceHooks",
    for (Span span : SetSpanReceiver.SetHolder.spans.values()) {
      Assert.assertEquals(ts.getSpan().getTraceId(), span.getTraceId());
    }
    SetSpanReceiver.SetHolder.spans.clear();
  }

  private void writeTestFile(String testFileName) throws Exception {
    Path filePath = new Path(testFileName);
    FSDataOutputStream stream = dfs.create(filePath);
    for (int i = 0; i < 10; i++) {
      byte[] data = RandomStringUtils.randomAlphabetic(102400).getBytes();
      stream.write(data);
    }
    stream.hsync();
    stream.close();
  }

  private void readTestFile(String testFileName) throws Exception {
    Path filePath = new Path(testFileName);
    FSDataInputStream istream = dfs.open(filePath, 10240);
    ByteBuffer buf = ByteBuffer.allocate(10240);

    } finally {
      istream.close();
    }
  }

  @BeforeClass
  public static void setup() throws IOException {
    conf = new Configuration();
    conf.setLong("dfs.blocksize", 100 * 1024);
    conf.set(SpanReceiverHost.SPAN_RECEIVERS_CONF_KEY,
        SetSpanReceiver.class.getName());
    spanReceiverHost = SpanReceiverHost.getInstance(conf);
  }

  @Before
  public void startCluster() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    SetSpanReceiver.SetHolder.spans.clear();
  }

  @After
  public void shutDown() throws IOException {
    cluster.shutdown();
  }


