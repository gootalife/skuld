hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/ClientProtocol.java
  @Idempotent
  public RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action)

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
    checkOperation(OperationCategory.READ);
    readLock();
    try {
      if (!isRollingUpgrade()) {
        return null;
      }
      Preconditions.checkNotNull(rollingUpgradeInfo);
      boolean hasRollbackImage = this.getFSImage().hasRollbackFSImage();
      rollingUpgradeInfo.setCreatedRollbackImages(hasRollbackImage);
      return rollingUpgradeInfo;
    } finally {
      readUnlock();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNodeMXBean.java
  public boolean isUpgradeFinalized();

  public RollingUpgradeInfo.Bean getRollingUpgradeStatus();


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestRollingUpgrade.java

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.ThreadLocalRandom;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeDataSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

  public static void runCmd(DFSAdmin dfsadmin, boolean success,
      String... args) throws  Exception {
    if (success) {
      assertEquals(0, dfsadmin.run(args));
    } else {
      Assert.assertTrue(dfsadmin.run(args) != 0);
    }
        runCmd(dfsadmin, false, "-rollingUpgrade", "abc");

        checkMxBeanIsNull();
        runCmd(dfsadmin, true, "-rollingUpgrade");


        runCmd(dfsadmin, true, "-rollingUpgrade", "query");
        checkMxBean();

        dfs.mkdirs(bar);
        
        runCmd(dfsadmin, true, "-rollingUpgrade", "finalize");
        assertNull(dfs.rollingUpgrade(RollingUpgradeAction.QUERY));
        checkMxBeanIsNull();

        dfs.mkdirs(baz);

        LOG.info("START\n" + info1);

        assertEquals(info1, dfs.rollingUpgrade(RollingUpgradeAction.QUERY));

        dfs.mkdirs(bar);
        cluster.shutdown();
      Assert.assertFalse(dfs2.exists(baz));

      assertEquals(info1, dfs2.rollingUpgrade(RollingUpgradeAction.QUERY));

      dfs2.mkdirs(baz);

      LOG.info("RESTART cluster 2");
      cluster2.restartNameNode();
      assertEquals(info1, dfs2.rollingUpgrade(RollingUpgradeAction.QUERY));
      Assert.assertTrue(dfs2.exists(foo));
      Assert.assertTrue(dfs2.exists(bar));
      Assert.assertTrue(dfs2.exists(baz));

      LOG.info("RESTART cluster 2 again");
      cluster2.restartNameNode();
      assertEquals(info1, dfs2.rollingUpgrade(RollingUpgradeAction.QUERY));
      Assert.assertTrue(dfs2.exists(foo));
      Assert.assertTrue(dfs2.exists(bar));
      Assert.assertTrue(dfs2.exists(baz));
    }
  }

  private static CompositeDataSupport getBean()
      throws MalformedObjectNameException, MBeanException,
      AttributeNotFoundException, InstanceNotFoundException,
      ReflectionException {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbeanName =
        new ObjectName("Hadoop:service=NameNode,name=NameNodeInfo");
    return (CompositeDataSupport)mbs.getAttribute(mxbeanName,
        "RollingUpgradeStatus");
  }

  private static void checkMxBeanIsNull() throws Exception {
    CompositeDataSupport ruBean = getBean();
    assertNull(ruBean);
  }

  private static void checkMxBean() throws Exception {
    CompositeDataSupport ruBean = getBean();
    assertNotEquals(0l, ruBean.get("startTime"));
    assertEquals(0l, ruBean.get("finalizeTime"));
  }

  @Test
  public void testRollback() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
      out.write(data, 0, data.length);
      out.close();

      checkMxBeanIsNull();
      startRollingUpgrade(foo, bar, file, data, cluster);
      checkMxBean();
      cluster.getFileSystem().rollEdits();
      cluster.getFileSystem().rollEdits();
      rollbackRollingUpgrade(foo, bar, file, data, cluster);
      checkMxBeanIsNull();

      startRollingUpgrade(foo, bar, file, data, cluster);
      cluster.getFileSystem().rollEdits();
      final String dnAddr = dn.getDatanodeId().getIpcAddr(false);
      final String[] args1 = {"-getDatanodeInfo", dnAddr};
      runCmd(dfsadmin, true, args1);

      final String[] args2 = {"-shutdownDatanode", dnAddr, "upgrade" };
      runCmd(dfsadmin, true, args2);

      Thread.sleep(2000);
      Assert.assertFalse("DataNode should exit", dn.isDatanodeUp());

      assertEquals(-1, dfsadmin.run(args1));
    } finally {
      if (cluster != null) cluster.shutdown();
    }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestNameNodeMXBean.java
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.NoMlockCacheManipulator;
import org.apache.hadoop.util.VersionInfo;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
      assertEquals(NativeIO.POSIX.getCacheManipulator().getMemlockLimit() *
          cluster.getDataNodes().size(),
              mbs.getAttribute(mxbeanName, "CacheCapacity"));
      assertNull("RollingUpgradeInfo should be null when there is no rolling"
          + " upgrade", mbs.getAttribute(mxbeanName, "RollingUpgradeStatus"));
    } finally {
      if (cluster != null) {
        for (URI dir : cluster.getNameDirs(0)) {

