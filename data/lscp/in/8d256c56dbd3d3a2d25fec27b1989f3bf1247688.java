hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestDiskspaceQuotaUpdate.java
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.Assert;
    try {
      DFSTestUtil.appendFile(dfs, file, BLOCKSIZE);
      Assert.fail("append didn't fail");
    } catch (QuotaByStorageTypeExceededException e) {
    }


