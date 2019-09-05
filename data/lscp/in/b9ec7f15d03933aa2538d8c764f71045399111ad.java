hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/contract/AbstractContractAppendTest.java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;

public abstract class AbstractContractAppendTest extends AbstractFSContractTestBase {
  private static final Logger LOG =
    FSDataOutputStream outputStream = getFileSystem().append(target);
    outputStream.write(dataset);
    Path renamed = new Path(testPath, "renamed");
    rename(target, renamed);
    outputStream.close();
    String listing = ls(testPath);


