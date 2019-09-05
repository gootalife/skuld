hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
    final String ident = src + " (inode " + fileId + ")";
    if (inode == null) {
      Lease lease = leaseManager.getLease(holder);
      throw new FileNotFoundException(
          "No lease on " + ident + ": File does not exist. "
          + (lease != null ? lease.toString()
              : "Holder " + holder + " does not have any open files."));

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestFileCreation.java
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
        cluster.getNameNodeRpc()
            .complete(f.toString(), client.clientName, null, someOtherFileId);
        fail();
      } catch(FileNotFoundException e) {
        FileSystem.LOG.info("Caught Expected FileNotFoundException: ", e);
      }
    } finally {
      IOUtils.closeStream(dfs);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestLease.java
import static org.mockito.Mockito.spy;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;
      Assert.assertTrue(!hasLease(cluster, a));
      Assert.assertTrue(!hasLease(cluster, b));

      Path fileA = new Path(dir, "fileA");
      FSDataOutputStream fileA_out = fs.create(fileA);
      fileA_out.writeBytes("something");
      Assert.assertTrue("Failed to get the lease!", hasLease(cluster, fileA));

      fs.delete(dir, true);
      try {
        fileA_out.hflush();
        Assert.fail("Should validate file existence!");
      } catch (FileNotFoundException e) {
        GenericTestUtils.assertExceptionContains("File does not exist", e);
      }
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }

