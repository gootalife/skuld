hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/crypto/TestCryptoStreamsWithOpensslAesCtrCryptoCodec.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.BeforeClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

public class TestCryptoStreamsWithOpensslAesCtrCryptoCodec 
    extends TestCryptoStreams {
  
  @BeforeClass
  public static void init() throws Exception {
    assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    Configuration conf = new Configuration();
    conf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_AES_CTR_NOPADDING_KEY,

