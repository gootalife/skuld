hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/fs/contract/hdfs/TestHDFSContractAppend.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractAppendTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

public class TestHDFSContractAppend extends AbstractContractAppendTest {
  protected AbstractFSContract createContract(Configuration conf) {
    return new HDFSContract(conf);
  }
}

