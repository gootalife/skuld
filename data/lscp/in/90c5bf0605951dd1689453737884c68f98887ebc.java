hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/crypto/TestCryptoStreamsWithOpensslAesCtrCryptoCodec.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.BeforeClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestCryptoStreamsWithOpensslAesCtrCryptoCodec 
    extends TestCryptoStreams {
  
  @BeforeClass
  public static void init() throws Exception {
    GenericTestUtils.assumeNativeCodeLoaded();
    Configuration conf = new Configuration();
    conf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_AES_CTR_NOPADDING_KEY,

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/io/TestSequenceFileAppend.java
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.serializer.JavaSerializationComparator;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

  @Test(timeout = 30000)
  public void testAppendRecordCompression() throws Exception {
    GenericTestUtils.assumeNativeCodeLoaded();

    Path file = new Path(ROOT_PATH, "testseqappendblockcompr.seq");
    fs.delete(file, true);

  @Test(timeout = 30000)
  public void testAppendBlockCompression() throws Exception {
    GenericTestUtils.assumeNativeCodeLoaded();

    Path file = new Path(ROOT_PATH, "testseqappendblockcompr.seq");
    fs.delete(file, true);

  @Test(timeout = 30000)
  public void testAppendSort() throws Exception {
    GenericTestUtils.assumeNativeCodeLoaded();

    Path file = new Path(ROOT_PATH, "testseqappendSort.seq");
    fs.delete(file, true);


hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/test/GenericTestUtils.java
import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;
import org.junit.Assert;
import org.junit.Assume;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

      }
    }
  }

  public static void assumeNativeCodeLoaded() {
    Assume.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
  }
}

