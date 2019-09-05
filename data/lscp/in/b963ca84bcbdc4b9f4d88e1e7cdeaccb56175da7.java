hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/DebugAdmin.java

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
        IOException ioe = null;
        try {
          recovered = dfs.recoverLease(new Path(pathStr));
        } catch (FileNotFoundException e) {
          System.err.println("recoverLease got exception: " + e.getMessage());
          System.err.println("Giving up on recoverLease for " + pathStr +
              " after 1 try");
          return 1;
        } catch (IOException e) {
          ioe = e;
        }
          return 0;
        }
        if (ioe != null) {
          System.err.println("recoverLease got exception: " +
              ioe.getMessage());
        } else {
          System.err.println("recoverLease returned false.");
        }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/tools/TestDebugAdmin.java

import static org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetTestUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDebugAdmin {
  private MiniDFSCluster cluster;
            "-block", blockFile.getAbsolutePath()})
    );
  }

  @Test(timeout = 60000)
  public void testRecoverLeaseforFileNotFound() throws Exception {
    assertTrue(runCmd(new String[] {
        "recoverLease", "-path", "/foo", "-retries", "2" }).contains(
        "Giving up on recoverLease for /foo after 1 try"));
  }
}

