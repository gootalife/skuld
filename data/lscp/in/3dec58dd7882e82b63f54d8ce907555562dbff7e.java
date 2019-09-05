hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/MD5Hash.java
public class MD5Hash implements WritableComparable<MD5Hash> {
  public static final int MD5_LEN = 16;

  private static final ThreadLocal<MessageDigest> DIGESTER_FACTORY =
      new ThreadLocal<MessageDigest>() {
    @Override
    protected MessageDigest initialValue() {
      try {

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/Text.java
public class Text extends BinaryComparable
    implements WritableComparable<BinaryComparable> {
  
  private static final ThreadLocal<CharsetEncoder> ENCODER_FACTORY =
    new ThreadLocal<CharsetEncoder>() {
      @Override
      protected CharsetEncoder initialValue() {
    }
  };
  
  private static final ThreadLocal<CharsetDecoder> DECODER_FACTORY =
    new ThreadLocal<CharsetDecoder>() {
    @Override
    protected CharsetDecoder initialValue() {

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/record/BinaryRecordInput.java
    this.in = inp;
  }
    
  private static final ThreadLocal<BinaryRecordInput> B_IN =
      new ThreadLocal<BinaryRecordInput>() {
      @Override
      protected BinaryRecordInput initialValue() {
        return new BinaryRecordInput();
      }
    };
  public static BinaryRecordInput get(DataInput inp) {
    BinaryRecordInput bin = B_IN.get();
    bin.setDataInput(inp);
    return bin;
  }

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/record/BinaryRecordOutput.java
    this.out = out;
  }
    
  private static final ThreadLocal<BinaryRecordOutput> B_OUT =
      new ThreadLocal<BinaryRecordOutput>() {
    @Override
    protected BinaryRecordOutput initialValue() {
      return new BinaryRecordOutput();
    }
  };
  public static BinaryRecordOutput get(DataOutput out) {
    BinaryRecordOutput bout = B_OUT.get();
    bout.setDataOutput(out);
    return bout;
  }

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/ReflectionUtils.java
  private static final ThreadLocal<CopyInCopyOutBuffer> CLONE_BUFFERS
      = new ThreadLocal<CopyInCopyOutBuffer>() {
      @Override
      protected synchronized CopyInCopyOutBuffer initialValue() {
  @SuppressWarnings("unchecked")
  public static <T> T copy(Configuration conf, 
                                T src, T dst) throws IOException {
    CopyInCopyOutBuffer buffer = CLONE_BUFFERS.get();
    buffer.outBuffer.reset();
    SerializationFactory factory = getFactory(conf);
    Class<T> cls = (Class<T>) src.getClass();
  @Deprecated
  public static void cloneWritableInto(Writable dst, 
                                       Writable src) throws IOException {
    CopyInCopyOutBuffer buffer = CLONE_BUFFERS.get();
    buffer.outBuffer.reset();
    src.write(buffer.outBuffer);
    buffer.moveData();

hadoop-common-project/hadoop-kms/src/main/java/org/apache/hadoop/crypto/key/kms/server/KMSMDCFilter.java
    }
  }

  private static final ThreadLocal<Data> DATA_TL = new ThreadLocal<Data>();

  public static UserGroupInformation getUgi() {
    return DATA_TL.get().ugi;

hadoop-hdfs-project/hadoop-hdfs-httpfs/src/main/java/org/apache/hadoop/lib/servlet/ServerWebApp.java
  private static final String HTTP_PORT = ".http.port";
  public static final String SSL_ENABLED = ".ssl.enabled";

  private static final ThreadLocal<String> HOME_DIR_TL =
      new ThreadLocal<String>();

  private InetSocketAddress authority;


hadoop-hdfs-project/hadoop-hdfs-httpfs/src/test/java/org/apache/hadoop/test/TestDirHelper.java
    }
  }

  private static final ThreadLocal<File> TEST_DIR_TL = new InheritableThreadLocal<File>();

  @Override
  public Statement apply(final Statement statement, final FrameworkMethod frameworkMethod, final Object o) {

hadoop-hdfs-project/hadoop-hdfs-httpfs/src/test/java/org/apache/hadoop/test/TestHdfsHelper.java

  public static final String HADOOP_MINI_HDFS = "test.hadoop.hdfs";

  private static final ThreadLocal<Configuration> HDFS_CONF_TL = new InheritableThreadLocal<Configuration>();

  private static final ThreadLocal<Path> HDFS_TEST_DIR_TL = new InheritableThreadLocal<Path>();

  @Override
  public Statement apply(Statement statement, FrameworkMethod frameworkMethod, Object o) {

hadoop-hdfs-project/hadoop-hdfs-httpfs/src/test/java/org/apache/hadoop/test/TestJettyHelper.java
    this.keyStorePassword = keyStorePassword;
  }

  private static final ThreadLocal<TestJettyHelper> TEST_JETTY_TL =
      new InheritableThreadLocal<TestJettyHelper>();

  @Override

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/lib/Chain.java
  private final ThreadLocal<DataOutputBuffer> threadLocalDataOutputBuffer =
    new ThreadLocal<DataOutputBuffer>() {
      protected DataOutputBuffer initialValue() {
        return new DataOutputBuffer(1024);

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/pipes/PipesPartitioner.java
                       V extends Writable>
  implements Partitioner<K, V> {
  
  private static final ThreadLocal<Integer> CACHE = new ThreadLocal<Integer>();
  private Partitioner<K, V> part = null;
  
  @SuppressWarnings("unchecked")
  static void setNextPartition(int newValue) {
    CACHE.set(newValue);
  }

  public int getPartition(K key, V value, 
                          int numPartitions) {
    Integer result = CACHE.get();
    if (result == null) {
      return part.getPartition(key, value, numPartitions);
    } else {

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/task/reduce/ShuffleSchedulerImpl.java
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ShuffleSchedulerImpl<K,V> implements ShuffleScheduler<K,V> {
  private static final ThreadLocal<Long> SHUFFLE_START =
      new ThreadLocal<Long>() {
    protected Long initialValue() {
      return 0L;
    }

      LOG.debug("Assigning " + host + " with " + host.getNumKnownMapOutputs() +
               " to " + Thread.currentThread().getName());
      SHUFFLE_START.set(Time.monotonicNow());

      return host;
  }
      }
    }
    LOG.info(host + " freed by " + Thread.currentThread().getName() + " in " +
             (Time.monotonicNow()-SHUFFLE_START.get()) + "ms");
  }

  public synchronized void resetKnownMaps() {

hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/util/DistCpUtils.java
  private static final ThreadLocal<DecimalFormat> FORMATTER
                        = new ThreadLocal<DecimalFormat>() {
    @Override
    protected DecimalFormat initialValue() {

hadoop-tools/hadoop-streaming/src/main/java/org/apache/hadoop/typedbytes/TypedBytesInput.java
    this.in = in;
  }

  private static final ThreadLocal<TypedBytesInput> TB_IN =
      new ThreadLocal<TypedBytesInput>() {
    @Override
    protected TypedBytesInput initialValue() {
      return new TypedBytesInput();
    }
  };
  public static TypedBytesInput get(DataInput in) {
    TypedBytesInput bin = TB_IN.get();
    bin.setDataInput(in);
    return bin;
  }

hadoop-tools/hadoop-streaming/src/main/java/org/apache/hadoop/typedbytes/TypedBytesOutput.java
    this.out = out;
  }

  private static final ThreadLocal<TypedBytesOutput> TB_OUT =
      new ThreadLocal<TypedBytesOutput>() {
    @Override
    protected TypedBytesOutput initialValue() {
      return new TypedBytesOutput();
    }
  };
  public static TypedBytesOutput get(DataOutput out) {
    TypedBytesOutput bout = TB_OUT.get();
    bout.setDataOutput(out);
    return bout;
  }

hadoop-tools/hadoop-streaming/src/main/java/org/apache/hadoop/typedbytes/TypedBytesRecordInput.java
    this.in = in;
  }

  private static final ThreadLocal<TypedBytesRecordInput> TB_IN =
      new ThreadLocal<TypedBytesRecordInput>() {
    @Override
    protected TypedBytesRecordInput initialValue() {
      return new TypedBytesRecordInput();
    }
  };
  public static TypedBytesRecordInput get(TypedBytesInput in) {
    TypedBytesRecordInput bin = TB_IN.get();
    bin.setTypedBytesInput(in);
    return bin;
  }

hadoop-tools/hadoop-streaming/src/main/java/org/apache/hadoop/typedbytes/TypedBytesRecordOutput.java
    this.out = out;
  }

  private static final ThreadLocal<TypedBytesRecordOutput> TB_OUT =
      new ThreadLocal<TypedBytesRecordOutput>() {
    @Override
    protected TypedBytesRecordOutput initialValue() {
      return new TypedBytesRecordOutput();
    }
  };
  public static TypedBytesRecordOutput get(TypedBytesOutput out) {
    TypedBytesRecordOutput bout = TB_OUT.get();
    bout.setTypedBytesOutput(out);
    return bout;
  }

hadoop-tools/hadoop-streaming/src/main/java/org/apache/hadoop/typedbytes/TypedBytesWritableInput.java
    this.in = in;
  }

  private static final ThreadLocal<TypedBytesWritableInput> TB_IN =
      new ThreadLocal<TypedBytesWritableInput>() {
    @Override
    protected TypedBytesWritableInput initialValue() {
      return new TypedBytesWritableInput();
    }
  };
  public static TypedBytesWritableInput get(TypedBytesInput in) {
    TypedBytesWritableInput bin = TB_IN.get();
    bin.setTypedBytesInput(in);
    return bin;
  }

hadoop-tools/hadoop-streaming/src/main/java/org/apache/hadoop/typedbytes/TypedBytesWritableOutput.java
    this.out = out;
  }

  private static final ThreadLocal<TypedBytesWritableOutput> TB_OUT =
      new ThreadLocal<TypedBytesWritableOutput>() {
    @Override
    protected TypedBytesWritableOutput initialValue() {
      return new TypedBytesWritableOutput();
    }
  };
  public static TypedBytesWritableOutput get(TypedBytesOutput out) {
    TypedBytesWritableOutput bout = TB_OUT.get();
    bout.setTypedBytesOutput(out);
    return bout;
  }

