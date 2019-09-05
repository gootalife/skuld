hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
  public long getBlockSize(String f) throws IOException {
    checkOpen();
    TraceScope scope = getPathTraceScope("getBlockSize", f);
    try {
      return namenode.getPreferredBlockSize(f);
  public FsServerDefaults getServerDefaults() throws IOException {
    checkOpen();
    long now = Time.monotonicNow();
    if ((serverDefaults == null) ||
        (now - serverDefaultsLastUpdate > SERVER_DEFAULTS_VALIDITY_PERIOD)) {
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    checkOpen();
    namenode.reportBadBlocks(blocks);
  }
  
  public BlockLocation[] getBlockLocations(String src, long start, 
        long length) throws IOException, UnresolvedLinkException {
    checkOpen();
    TraceScope scope = getPathTraceScope("getBlockLocations", src);
    try {
      LocatedBlocks blocks = getLocatedBlocks(src, start, length);
  public BlockStorageLocation[] getBlockStorageLocations(
      List<BlockLocation> blockLocations) throws IOException,
      UnsupportedOperationException, InvalidBlockTokenException {
    checkOpen();
    if (!getConf().isHdfsBlocksMetadataEnabled()) {
      throw new UnsupportedOperationException("Datanode-side support for " +
          "getVolumeBlockLocations() must also be enabled in the client " +
  public void createSymlink(String target, String link, boolean createParent)
      throws IOException {
    checkOpen();
    TraceScope scope = getPathTraceScope("createSymlink", target);
    try {
      final FsPermission dirPerm = applyUMask(null);
  public boolean setReplication(String src, short replication)
      throws IOException {
    checkOpen();
    TraceScope scope = getPathTraceScope("setReplication", src);
    try {
      return namenode.setReplication(src, replication);
  public void setStoragePolicy(String src, String policyName)
      throws IOException {
    checkOpen();
    TraceScope scope = getPathTraceScope("setStoragePolicy", src);
    try {
      namenode.setStoragePolicy(src, policyName);
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    checkOpen();
    TraceScope scope = Trace.startSpan("getStoragePolicies", traceSampler);
    try {
      return namenode.getStoragePolicies();
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    checkOpen();
    return setSafeMode(action, false);
  }
  
  
  public RemoteIterator<CacheDirectiveEntry> listCacheDirectives(
      CacheDirectiveInfo filter) throws IOException {
    checkOpen();
    return new CacheDirectiveIterator(namenode, filter, traceSampler);
  }

  }

  public RemoteIterator<CachePoolEntry> listCachePools() throws IOException {
    checkOpen();
    return new CachePoolIterator(namenode, traceSampler);
  }

  void saveNamespace() throws AccessControlException, IOException {
    checkOpen();
    TraceScope scope = Trace.startSpan("saveNamespace", traceSampler);
    try {
      namenode.saveNamespace();
  long rollEdits() throws AccessControlException, IOException {
    checkOpen();
    TraceScope scope = Trace.startSpan("rollEdits", traceSampler);
    try {
      return namenode.rollEdits();
  boolean restoreFailedStorage(String arg)
      throws AccessControlException, IOException{
    checkOpen();
    TraceScope scope = Trace.startSpan("restoreFailedStorage", traceSampler);
    try {
      return namenode.restoreFailedStorage(arg);
  public void refreshNodes() throws IOException {
    checkOpen();
    TraceScope scope = Trace.startSpan("refreshNodes", traceSampler);
    try {
      namenode.refreshNodes();
  public void metaSave(String pathname) throws IOException {
    checkOpen();
    TraceScope scope = Trace.startSpan("metaSave", traceSampler);
    try {
      namenode.metaSave(pathname);
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    checkOpen();
    TraceScope scope = Trace.startSpan("setBalancerBandwidth", traceSampler);
    try {
      namenode.setBalancerBandwidth(bandwidth);
  public void finalizeUpgrade() throws IOException {
    checkOpen();
    TraceScope scope = Trace.startSpan("finalizeUpgrade", traceSampler);
    try {
      namenode.finalizeUpgrade();
  }

  RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action) throws IOException {
    checkOpen();
    TraceScope scope = Trace.startSpan("rollingUpgrade", traceSampler);
    try {
      return namenode.rollingUpgrade(action);
  ContentSummary getContentSummary(String src) throws IOException {
    checkOpen();
    TraceScope scope = getPathTraceScope("getContentSummary", src);
    try {
      return namenode.getContentSummary(src);
  void setQuota(String src, long namespaceQuota, long storagespaceQuota)
      throws IOException {
    checkOpen();
    if ((namespaceQuota <= 0 && namespaceQuota != HdfsConstants.QUOTA_DONT_SET &&
         namespaceQuota != HdfsConstants.QUOTA_RESET) ||
  void setQuotaByStorageType(String src, StorageType type, long quota)
      throws IOException {
    checkOpen();
    if (quota <= 0 && quota != HdfsConstants.QUOTA_DONT_SET &&
        quota != HdfsConstants.QUOTA_RESET) {
      throw new IllegalArgumentException("Invalid values for quota :" +
  }

  public DFSInotifyEventInputStream getInotifyEventStream() throws IOException {
    checkOpen();
    return new DFSInotifyEventInputStream(traceSampler, namenode);
  }

  public DFSInotifyEventInputStream getInotifyEventStream(long lastReadTxid)
      throws IOException {
    checkOpen();
    return new DFSInotifyEventInputStream(traceSampler, namenode, lastReadTxid);
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDistributedFileSystem.java
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.web.HftpFileSystem;
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      DistributedFileSystem fileSys = cluster.getFileSystem();

      fileSys.create(new Path("/test/dfsclose/file-0"));

      fileSys.close();

      DFSClient dfsClient = fileSys.getClient();
      verifyOpsUsingClosedClient(dfsClient);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  private void verifyOpsUsingClosedClient(DFSClient dfsClient) {
    Path p = new Path("/non-empty-file");
    try {
      dfsClient.getBlockSize(p.getName());
      fail("getBlockSize using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getServerDefaults();
      fail("getServerDefaults using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.reportBadBlocks(new LocatedBlock[0]);
      fail("reportBadBlocks using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getBlockLocations(p.getName(), 0, 1);
      fail("getBlockLocations using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getBlockStorageLocations(new ArrayList<BlockLocation>());
      fail("getBlockStorageLocations using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.createSymlink("target", "link", true);
      fail("createSymlink using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getLinkTarget(p.getName());
      fail("getLinkTarget using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setReplication(p.getName(), (short) 3);
      fail("setReplication using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setStoragePolicy(p.getName(),
          HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
      fail("setStoragePolicy using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getStoragePolicies();
      fail("getStoragePolicies using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
      fail("setSafeMode using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.refreshNodes();
      fail("refreshNodes using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.metaSave(p.getName());
      fail("metaSave using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setBalancerBandwidth(1000L);
      fail("setBalancerBandwidth using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.finalizeUpgrade();
      fail("finalizeUpgrade using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.rollingUpgrade(RollingUpgradeAction.QUERY);
      fail("rollingUpgrade using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getInotifyEventStream();
      fail("getInotifyEventStream using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getInotifyEventStream(100L);
      fail("getInotifyEventStream using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.saveNamespace();
      fail("saveNamespace using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.rollEdits();
      fail("rollEdits using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.restoreFailedStorage("");
      fail("restoreFailedStorage using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getContentSummary(p.getName());
      fail("getContentSummary using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setQuota(p.getName(), 1000L, 500L);
      fail("setQuota using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setQuotaByStorageType(p.getName(), StorageType.DISK, 500L);
      fail("setQuotaByStorageType using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
  }

  @Test
  public void testDFSCloseOrdering() throws Exception {
    DistributedFileSystem fs = new MyDistributedFileSystem();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestRollingUpgradeRollback.java
      dfs.mkdirs(bar);
      dfs.close();

      dfs = dfsCluster.getFileSystem(0);
      TestRollingUpgrade.queryForPreparation(dfs);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestCacheDirectives.java
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;

    info = new CachePoolInfo("pool2");
    dfs.addCachePool(info);

    DistributedFileSystem dfs1 = (DistributedFileSystem) cluster
        .getNewFileSystemInstance(0);
    dfs1.close();
    try {
      dfs1.listCachePools();
      fail("listCachePools using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfs1.addCachePool(info);
      fail("addCachePool using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfs1.modifyCachePool(info);
      fail("modifyCachePool using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfs1.removeCachePool(poolName);
      fail("removeCachePool using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
  }

  @Test(timeout=60000)
    dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder(
        directive).setId(id).setReplication((short)2).build());
    dfs.removeCacheDirective(id);

    DistributedFileSystem dfs1 = (DistributedFileSystem) cluster
        .getNewFileSystemInstance(0);
    dfs1.close();
    try {
      dfs1.listCacheDirectives(null);
      fail("listCacheDirectives using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfs1.addCacheDirective(alpha);
      fail("addCacheDirective using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfs1.modifyCacheDirective(alpha);
      fail("modifyCacheDirective using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfs1.removeCacheDirective(alphaId);
      fail("removeCacheDirective using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
  }

  @Test(timeout=60000)

