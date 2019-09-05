hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestHostsFiles.java
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.io.File;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
      assertTrue("Live nodes should contain the decommissioned node",
          nodes.contains("Decommissioned"));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (localFileSys.exists(dir)) {
        FileUtils.deleteQuietly(new File(dir.toUri().getPath()));
      }
    }
  }

  @Test
      if (cluster != null) {
        cluster.shutdown();
      }
      if (localFileSys.exists(dir)) {
        FileUtils.deleteQuietly(new File(dir.toUri().getPath()));
      }
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestNameNodeMXBean.java
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.io.FileUtils;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1);
    MiniDFSCluster cluster = null;
    FileSystem localFileSys = null;
    Path dir = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
        "Hadoop:service=NameNode,name=NameNodeInfo");

      localFileSys = FileSystem.getLocal(conf);
      Path workingDir = localFileSys.getWorkingDirectory();
      dir = new Path(workingDir,"build/test/data/temp/TestNameNodeMXBean");
      Path includeFile = new Path(dir, "include");
      assertTrue(localFileSys.mkdirs(dir));
      StringBuilder includeHosts = new StringBuilder();
        assertTrue(deadNode.containsKey("decommissioned"));
        assertTrue(deadNode.containsKey("xferaddr"));
      }
    } finally {
      if ((localFileSys != null) && localFileSys.exists(dir)) {
        FileUtils.deleteQuietly(new File(dir.toUri().getPath()));
      }
      if (cluster != null) {
        cluster.shutdown();
      }

