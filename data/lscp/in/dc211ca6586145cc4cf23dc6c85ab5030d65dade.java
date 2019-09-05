hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockReportRateLimiting.java
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
    conf.setLong(DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS, 100L);

    final Semaphore gotFbrSem = new Semaphore(0);
    final AtomicReference<String> failure = new AtomicReference<>();
    final AtomicReference<MiniDFSCluster> cluster =
        new AtomicReference<>();
    final AtomicReference<String> datanodeToStop = new AtomicReference<>();
    final BlockManagerFaultInjector injector = new BlockManagerFaultInjector() {

      @Override
      public void incomingBlockReportRpc(DatanodeID nodeID,
          setFailure(failure, "Got unexpected rate-limiting-" +
              "bypassing full block report RPC from " + nodeID);
        }
        if (nodeID.getXferAddr().equals(datanodeToStop.get())) {
          throw new IOException("Injecting failure into block " +
              "report RPC for " + nodeID);
        }
        gotFbrSem.release();
      }

        if (leaseId == 0) {
          return;
        }
        datanodeToStop.compareAndSet(null, node.getXferAddr());
      }

      @Override
      public void removeBlockReportLease(DatanodeDescriptor node, long leaseId) {
      }
    };
    try {
      BlockManagerFaultInjector.instance = injector;
      cluster.set(new MiniDFSCluster.Builder(conf).numDataNodes(2).build());
      cluster.get().waitActive();
      Assert.assertNotNull(cluster.get().stopDataNode(datanodeToStop.get()));
      gotFbrSem.acquire();
      Assert.assertNull(failure.get());
    } finally {
      if (cluster.get() != null) {
        cluster.get().shutdown();
      }
    }
  }
}

