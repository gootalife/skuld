hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/balancer/NameNodeConnector.java
  private OutputStream checkAndMarkRunning() throws IOException {
    try {
      if (fs.exists(idPath)) {
        IOUtils.closeStream(fs.append(idPath));
        fs.delete(idPath, true);
      }
      final FSDataOutputStream fsout = fs.create(idPath, false);
      fs.deleteOnExit(idPath);
      if (write2IdFile) {
        fsout.writeBytes(InetAddress.getLocalHost().getHostName());
        fsout.hflush();
      }
      return fsout;
    } catch(RemoteException e) {
      if(AlreadyBeingCreatedException.class.getName().equals(e.getClassName())){
        return null;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/balancer/TestBalancer.java
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
      cluster.shutdown();
    }
  }

  @Test(timeout = 100000)
  public void testManyBalancerSimultaneously() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    long[] capacities = new long[] { 4 * CAPACITY };
    String[] racks = new String[] { RACK0 };
    long newCapacity = 2 * CAPACITY;
    String newRack = RACK0;
    LOG.info("capacities = " + long2String(capacities));
    LOG.info("racks      = " + Arrays.asList(racks));
    LOG.info("newCapacity= " + newCapacity);
    LOG.info("newRack    = " + newRack);
    LOG.info("useTool    = " + false);
    assertEquals(capacities.length, racks.length);
    int numOfDatanodes = capacities.length;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(capacities.length)
        .racks(racks).simulatedCapacities(capacities).build();
    try {
      cluster.waitActive();
      client = NameNodeProxies.createProxy(conf,
          cluster.getFileSystem(0).getUri(), ClientProtocol.class).getProxy();

      long totalCapacity = sum(capacities);

      final long totalUsedSpace = totalCapacity * 3 / 10;
      createFile(cluster, filePath, totalUsedSpace / numOfDatanodes,
          (short) numOfDatanodes, 0);
      cluster.startDataNodes(conf, 1, true, null, new String[] { newRack },
          new long[] { newCapacity });

      FileSystem fs = cluster.getFileSystem(0);
      final FSDataOutputStream out = fs
          .create(Balancer.BALANCER_ID_PATH, false);
      out.writeBytes(InetAddress.getLocalHost().getHostName());
      out.hflush();
      assertTrue("'balancer.id' file doesn't exist!",
          fs.exists(Balancer.BALANCER_ID_PATH));

      final String[] args = { "-policy", "datanode" };
      final Tool tool = new Cli();
      tool.setConf(conf);
      int exitCode = tool.run(args); // start balancing
      assertEquals("Exit status code mismatches",
          ExitStatus.IO_EXCEPTION.getExitCode(), exitCode);

      out.close();
      assertTrue("'balancer.id' file doesn't exist!",
          fs.exists(Balancer.BALANCER_ID_PATH));
      exitCode = tool.run(args); // start balancing
      assertEquals("Exit status code mismatches",
          ExitStatus.SUCCESS.getExitCode(), exitCode);
    } finally {
      cluster.shutdown();
    }
  }


