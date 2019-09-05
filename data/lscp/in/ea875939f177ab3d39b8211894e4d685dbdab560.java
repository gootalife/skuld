hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
  public long getBlockSize(String f) throws IOException {
    TraceScope scope = getPathTraceScope("getBlockSize", f);
    try {
      return namenode.getPreferredBlockSize(f);
  public FsServerDefaults getServerDefaults() throws IOException {
    long now = Time.monotonicNow();
    if ((serverDefaults == null) ||
        (now - serverDefaultsLastUpdate > SERVER_DEFAULTS_VALIDITY_PERIOD)) {
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    namenode.reportBadBlocks(blocks);
  }
  
  public BlockLocation[] getBlockLocations(String src, long start, 
        long length) throws IOException, UnresolvedLinkException {
    TraceScope scope = getPathTraceScope("getBlockLocations", src);
    try {
      LocatedBlocks blocks = getLocatedBlocks(src, start, length);
  public BlockStorageLocation[] getBlockStorageLocations(
      List<BlockLocation> blockLocations) throws IOException,
      UnsupportedOperationException, InvalidBlockTokenException {
    if (!getConf().isHdfsBlocksMetadataEnabled()) {
      throw new UnsupportedOperationException("Datanode-side support for " +
          "getVolumeBlockLocations() must also be enabled in the client " +
  public void createSymlink(String target, String link, boolean createParent)
      throws IOException {
    TraceScope scope = getPathTraceScope("createSymlink", target);
    try {
      final FsPermission dirPerm = applyUMask(null);
  public boolean setReplication(String src, short replication)
      throws IOException {
    TraceScope scope = getPathTraceScope("setReplication", src);
    try {
      return namenode.setReplication(src, replication);
  public void setStoragePolicy(String src, String policyName)
      throws IOException {
    TraceScope scope = getPathTraceScope("setStoragePolicy", src);
    try {
      namenode.setStoragePolicy(src, policyName);
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    TraceScope scope = Trace.startSpan("getStoragePolicies", traceSampler);
    try {
      return namenode.getStoragePolicies();
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return setSafeMode(action, false);
  }
  
  
  public RemoteIterator<CacheDirectiveEntry> listCacheDirectives(
      CacheDirectiveInfo filter) throws IOException {
    return new CacheDirectiveIterator(namenode, filter, traceSampler);
  }

  }

  public RemoteIterator<CachePoolEntry> listCachePools() throws IOException {
    return new CachePoolIterator(namenode, traceSampler);
  }

  void saveNamespace() throws AccessControlException, IOException {
    TraceScope scope = Trace.startSpan("saveNamespace", traceSampler);
    try {
      namenode.saveNamespace();
  long rollEdits() throws AccessControlException, IOException {
    TraceScope scope = Trace.startSpan("rollEdits", traceSampler);
    try {
      return namenode.rollEdits();
  boolean restoreFailedStorage(String arg)
      throws AccessControlException, IOException{
    TraceScope scope = Trace.startSpan("restoreFailedStorage", traceSampler);
    try {
      return namenode.restoreFailedStorage(arg);
  public void refreshNodes() throws IOException {
    TraceScope scope = Trace.startSpan("refreshNodes", traceSampler);
    try {
      namenode.refreshNodes();
  public void metaSave(String pathname) throws IOException {
    TraceScope scope = Trace.startSpan("metaSave", traceSampler);
    try {
      namenode.metaSave(pathname);
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    TraceScope scope = Trace.startSpan("setBalancerBandwidth", traceSampler);
    try {
      namenode.setBalancerBandwidth(bandwidth);
  public void finalizeUpgrade() throws IOException {
    TraceScope scope = Trace.startSpan("finalizeUpgrade", traceSampler);
    try {
      namenode.finalizeUpgrade();
  }

  RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action) throws IOException {
    TraceScope scope = Trace.startSpan("rollingUpgrade", traceSampler);
    try {
      return namenode.rollingUpgrade(action);
  ContentSummary getContentSummary(String src) throws IOException {
    TraceScope scope = getPathTraceScope("getContentSummary", src);
    try {
      return namenode.getContentSummary(src);
  void setQuota(String src, long namespaceQuota, long storagespaceQuota)
      throws IOException {
    if ((namespaceQuota <= 0 && namespaceQuota != HdfsConstants.QUOTA_DONT_SET &&
         namespaceQuota != HdfsConstants.QUOTA_RESET) ||
  void setQuotaByStorageType(String src, StorageType type, long quota)
      throws IOException {
    if (quota <= 0 && quota != HdfsConstants.QUOTA_DONT_SET &&
        quota != HdfsConstants.QUOTA_RESET) {
      throw new IllegalArgumentException("Invalid values for quota :" +
  }

  public DFSInotifyEventInputStream getInotifyEventStream() throws IOException {
    return new DFSInotifyEventInputStream(traceSampler, namenode);
  }

  public DFSInotifyEventInputStream getInotifyEventStream(long lastReadTxid)
      throws IOException {
    return new DFSInotifyEventInputStream(traceSampler, namenode, lastReadTxid);
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDistributedFileSystem.java
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.LeaseRenewer;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.web.HftpFileSystem;
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      FileSystem fileSys = cluster.getFileSystem();
      
      fileSys.create(new Path("/test/dfsclose/file-0"));
      
      fileSys.close();
      
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  @Test
  public void testDFSCloseOrdering() throws Exception {
    DistributedFileSystem fs = new MyDistributedFileSystem();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestRollingUpgradeRollback.java
      dfs.mkdirs(bar);
      dfs.close();

      TestRollingUpgrade.queryForPreparation(dfs);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestCacheDirectives.java
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;

    info = new CachePoolInfo("pool2");
    dfs.addCachePool(info);
  }

  @Test(timeout=60000)
    dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder(
        directive).setId(id).setReplication((short)2).build());
    dfs.removeCacheDirective(id);
  }

  @Test(timeout=60000)

