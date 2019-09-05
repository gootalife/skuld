hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/Trash.java

import java.io.IOException;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Trash extends Configured {
  private static final org.apache.commons.logging.Log LOG =
      LogFactory.getLog(Trash.class);

  private TrashPolicy trashPolicy; // configured trash policy instance

    } catch (Exception e) {
      LOG.warn("Failed to get server trash configuration", e);
      throw new IOException("Failed to get server trash configuration", e);
    }
    Trash trash = new Trash(fullyResolvedFs, conf);

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/TrashPolicyDefault.java
          return false;
        }
      } catch (IOException e) {
        LOG.warn("Can't create trash directory: " + baseTrashPath, e);
        cause = e;
        break;
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
  public FsServerDefaults getServerDefaults() throws IOException {
    long now = Time.monotonicNow();
    if ((serverDefaults == null) ||
        (now - serverDefaultsLastUpdate > SERVER_DEFAULTS_VALIDITY_PERIOD)) {
      serverDefaults = namenode.getServerDefaults();
      serverDefaultsLastUpdate = now;
    }
    assert serverDefaults != null;
    return serverDefaults;
  }
  

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDistributedFileSystem.java
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
      cluster.shutdown();
    }
  }

  @Test(timeout=60000)
  public void testGetServerDefaults() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      FsServerDefaults fsServerDefaults = dfs.getServerDefaults();
      Assert.assertNotNull(fsServerDefaults);
    } finally {
      cluster.shutdown();
    }
  }
}

