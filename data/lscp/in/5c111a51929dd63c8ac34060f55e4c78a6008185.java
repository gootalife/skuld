hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
  boolean recoverLease(String src, String holder, String clientMachine)
        dir.checkPathAccess(pc, iip, FsAction.WRITE);
      }
  
      return recoverLeaseInternal(RecoverLeaseOp.RECOVER_LEASE,
          iip, src, holder, clientMachine, true);
    } catch (StandbyException se) {
      skipSync = true;
        getEditLog().logSync();
      }
    }
  }

  enum RecoverLeaseOp {
    }
  }

  boolean recoverLeaseInternal(RecoverLeaseOp op, INodesInPath iip,
      String src, String holder, String clientMachine, boolean force)
      throws IOException {
    assert hasWriteLock();
    INodeFile file = iip.getLastINode().asFile();
    if (file.isUnderConstruction()) {
        LOG.info("recoverLease: " + lease + ", src=" + src +
          " from client " + clientName);
        return internalReleaseLease(lease, src, iip, holder);
      } else {
        assert lease.getHolder().equals(clientName) :
          "Current lease holder " + lease.getHolder() +
        if (lease.expiredSoftLimit()) {
          LOG.info("startFile: recover " + lease + ", src=" + src + " client "
              + clientName);
          if (internalReleaseLease(lease, src, iip, null)) {
            return true;
          } else {
            throw new RecoveryInProgressException(
                op.getExceptionMessage(src, holder, clientMachine,
                    "lease recovery is in progress. Try again later."));
          }
        } else {
          final BlockInfo lastBlock = file.getLastBlock();
          if (lastBlock != null
          }
        }
      }
    } else {
      return true;
     }
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestLeaseRecovery.java
package org.apache.hadoop.hdfs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Test;
    assertTrue("File should be closed", newdfs.recoverLease(file));

  }

  @Test
  public void testLeaseRecoveryAndAppend() throws Exception {
    Configuration conf = new Configuration();
    try{
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    Path file = new Path("/testLeaseRecovery");
    DistributedFileSystem dfs = cluster.getFileSystem();

    FSDataOutputStream out = dfs.create(file);
    out.hflush();
    out.hsync();

    ((DFSOutputStream) out.getWrappedStream()).abort();
    DistributedFileSystem newdfs =
        (DistributedFileSystem) FileSystem.newInstance
        (cluster.getConfiguration(0));

    try {
        newdfs.append(file);
        fail("Append to a file(lease is held by another client) should fail");
    } catch (RemoteException e) {
      assertTrue(e.getMessage().contains("file lease is currently owned"));
    }

    boolean recoverLease = newdfs.recoverLease(file);
    assertTrue(recoverLease);
    FSDataOutputStream append = newdfs.append(file);
    append.write("test".getBytes());
    append.close();
    }finally{
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }
}

