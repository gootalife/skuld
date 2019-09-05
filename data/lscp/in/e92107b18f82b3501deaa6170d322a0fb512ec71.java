hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/crypto/TestCryptoCodec.java
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RandomDatum;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;

  @Test(timeout=120000)
  public void testJceAesCtrCryptoCodec() throws Exception {
    GenericTestUtils.assumeInNativeProfile();
    if (!NativeCodeLoader.buildSupportsOpenssl()) {
      LOG.warn("Skipping test since openSSL library not loaded");
      Assume.assumeTrue(false);
  
  @Test(timeout=120000)
  public void testOpensslAesCtrCryptoCodec() throws Exception {
    GenericTestUtils.assumeInNativeProfile();
    if (!NativeCodeLoader.buildSupportsOpenssl()) {
      LOG.warn("Skipping test since openSSL library not loaded");
      Assume.assumeTrue(false);

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/crypto/TestCryptoStreamsWithOpensslAesCtrCryptoCodec.java
  
  @BeforeClass
  public static void init() throws Exception {
    GenericTestUtils.assumeInNativeProfile();
    Configuration conf = new Configuration();
    conf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_AES_CTR_NOPADDING_KEY,

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/io/TestSequenceFileAppend.java

  @Test(timeout = 30000)
  public void testAppendRecordCompression() throws Exception {
    GenericTestUtils.assumeInNativeProfile();

    Path file = new Path(ROOT_PATH, "testseqappendblockcompr.seq");
    fs.delete(file, true);

  @Test(timeout = 30000)
  public void testAppendBlockCompression() throws Exception {
    GenericTestUtils.assumeInNativeProfile();

    Path file = new Path(ROOT_PATH, "testseqappendblockcompr.seq");
    fs.delete(file, true);

  @Test(timeout = 30000)
  public void testAppendSort() throws Exception {
    GenericTestUtils.assumeInNativeProfile();

    Path file = new Path(ROOT_PATH, "testseqappendSort.seq");
    fs.delete(file, true);

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/test/GenericTestUtils.java
  }

  public static void assumeInNativeProfile() {
    Assume.assumeTrue(
        Boolean.valueOf(System.getProperty("runningWithNative", "false")));
  }
}

