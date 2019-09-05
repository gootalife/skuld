hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/crypto/TestCryptoStreamsWithOpensslAesCtrCryptoCodec.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.BeforeClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestCryptoStreamsWithOpensslAesCtrCryptoCodec 
    extends TestCryptoStreams {
  
        CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_AES_CTR_NOPADDING_KEY,
        OpensslAesCtrCryptoCodec.class.getName());
    codec = CryptoCodec.getInstance(conf);
    assertNotNull("Unable to instantiate codec " +
        OpensslAesCtrCryptoCodec.class.getName() + ", is the required "
        + "version of OpenSSL installed?", codec);
    assertEquals(OpensslAesCtrCryptoCodec.class.getCanonicalName(),
        codec.getClass().getCanonicalName());
  }
}

