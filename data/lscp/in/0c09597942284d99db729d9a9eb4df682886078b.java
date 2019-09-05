hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/HdfsConstants.java
  public static final String MEMORY_STORAGE_POLICY_NAME = "LAZY_PERSIST";
  public static final String ALLSSD_STORAGE_POLICY_NAME = "ALL_SSD";
  public static final String ONESSD_STORAGE_POLICY_NAME = "ONE_SSD";
  public static final String HOT_STORAGE_POLICY_NAME = "HOT";
  public static final String WARM_STORAGE_POLICY_NAME = "WARM";
  public static final String COLD_STORAGE_POLICY_NAME = "COLD";
  public static final int DEFAULT_DATA_SOCKET_SIZE = 128 * 1024;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockStoragePolicySuite.java
        new StorageType[]{StorageType.SSD, StorageType.DISK});
    final byte hotId = HdfsServerConstants.HOT_STORAGE_POLICY_ID;
    policies[hotId] = new BlockStoragePolicy(hotId,
        HdfsConstants.HOT_STORAGE_POLICY_NAME,
        new StorageType[]{StorageType.DISK}, StorageType.EMPTY_ARRAY,
        new StorageType[]{StorageType.ARCHIVE});
    final byte warmId = HdfsServerConstants.WARM_STORAGE_POLICY_ID;
    policies[warmId] = new BlockStoragePolicy(warmId,
        HdfsConstants.WARM_STORAGE_POLICY_NAME,
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE},
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE},
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE});
    final byte coldId = HdfsServerConstants.COLD_STORAGE_POLICY_ID;
    policies[coldId] = new BlockStoragePolicy(coldId,
        HdfsConstants.COLD_STORAGE_POLICY_NAME,
        new StorageType[]{StorageType.ARCHIVE}, StorageType.EMPTY_ARRAY,
        StorageType.EMPTY_ARRAY);
    return new BlockStoragePolicySuite(hotId, policies);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/common/HdfsServerConstants.java
  };
  byte[] DOT_SNAPSHOT_DIR_BYTES
              = DFSUtil.string2Bytes(HdfsConstants.DOT_SNAPSHOT_DIR);
  byte MEMORY_STORAGE_POLICY_ID = 15;
  byte ALLSSD_STORAGE_POLICY_ID = 12;
  byte ONESSD_STORAGE_POLICY_ID = 10;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/DFSTestUtil.java
    s2.close();
    filesystem.setStoragePolicy(pathFileCreate,
        HdfsConstants.HOT_STORAGE_POLICY_NAME);
    final Path pathFileMoved = new Path("/file_moved");
    filesystem.rename(pathFileCreate, pathFileMoved);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockStoragePolicy.java
    try {
      cluster.waitActive();
      cluster.getFileSystem().setStoragePolicy(new Path("/"),
          HdfsConstants.COLD_STORAGE_POLICY_NAME);
    } finally {
      cluster.shutdown();
    }
    try {
      cluster.waitActive();
      cluster.getFileSystem().setStoragePolicy(new Path("/"),
          HdfsConstants.COLD_STORAGE_POLICY_NAME);
    } finally {
      cluster.shutdown();
    }

      final Path invalidPath = new Path("/invalidPath");
      try {
        fs.setStoragePolicy(invalidPath, HdfsConstants.WARM_STORAGE_POLICY_NAME);
        Assert.fail("Should throw a FileNotFoundException");
      } catch (FileNotFoundException e) {
        GenericTestUtils.assertExceptionContains(invalidPath.toString(), e);
        GenericTestUtils.assertExceptionContains(invalidPath.toString(), e);
      }

      fs.setStoragePolicy(fooFile, HdfsConstants.COLD_STORAGE_POLICY_NAME);
      fs.setStoragePolicy(barDir, HdfsConstants.WARM_STORAGE_POLICY_NAME);
      fs.setStoragePolicy(barFile2, HdfsConstants.HOT_STORAGE_POLICY_NAME);
      Assert.assertEquals("File storage policy should be COLD",
          HdfsConstants.COLD_STORAGE_POLICY_NAME,
          fs.getStoragePolicy(fooFile).getName());
      Assert.assertEquals("File storage policy should be WARM",
          HdfsConstants.WARM_STORAGE_POLICY_NAME,
          fs.getStoragePolicy(barDir).getName());
      Assert.assertEquals("File storage policy should be HOT",
          HdfsConstants.HOT_STORAGE_POLICY_NAME,
          fs.getStoragePolicy(barFile2).getName());

      dirList = fs.getClient().listPaths(dir.toString(),
      DFSTestUtil.createFile(fs, fooFile1, FILE_LEN, REPLICATION, 0L);
      DFSTestUtil.createFile(fs, fooFile2, FILE_LEN, REPLICATION, 0L);

      fs.setStoragePolicy(fooDir, HdfsConstants.WARM_STORAGE_POLICY_NAME);

      HdfsFileStatus[] dirList = fs.getClient().listPaths(dir.toString(),
          HdfsFileStatus.EMPTY_NAME, true).getPartialListing();
      SnapshotTestHelper.createSnapshot(fs, dir, "s1");
      fs.setStoragePolicy(fooFile1, HdfsConstants.COLD_STORAGE_POLICY_NAME);

      fooList = fs.getClient().listPaths(fooDir.toString(),
          HdfsFileStatus.EMPTY_NAME).getPartialListing();
          HdfsFileStatus.EMPTY_NAME).getPartialListing(), COLD);

      fs.setStoragePolicy(fooDir, HdfsConstants.HOT_STORAGE_POLICY_NAME);
      dirList = fs.getClient().listPaths(dir.toString(),
          HdfsFileStatus.EMPTY_NAME, true).getPartialListing();
  @Test
  public void testChangeHotFileRep() throws Exception {
    testChangeFileRep(HdfsConstants.HOT_STORAGE_POLICY_NAME, HOT,
        new StorageType[]{StorageType.DISK, StorageType.DISK,
            StorageType.DISK},
        new StorageType[]{StorageType.DISK, StorageType.DISK, StorageType.DISK,
  @Test
  public void testChangeWarmRep() throws Exception {
    testChangeFileRep(HdfsConstants.WARM_STORAGE_POLICY_NAME, WARM,
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE,
            StorageType.ARCHIVE},
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE,
  @Test
  public void testChangeColdRep() throws Exception {
    testChangeFileRep(HdfsConstants.COLD_STORAGE_POLICY_NAME, COLD,
        new StorageType[]{StorageType.ARCHIVE, StorageType.ARCHIVE,
            StorageType.ARCHIVE},
        new StorageType[]{StorageType.ARCHIVE, StorageType.ARCHIVE,

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/mover/TestStorageMover.java
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
    DEFAULT_CONF.setLong(DFSConfigKeys.DFS_MOVER_MOVEDWINWIDTH_KEY, 2000L);

    DEFAULT_POLICIES = BlockStoragePolicySuite.createDefaultSuite();
    HOT = DEFAULT_POLICIES.getPolicy(HdfsConstants.HOT_STORAGE_POLICY_NAME);
    WARM = DEFAULT_POLICIES.getPolicy(HdfsConstants.WARM_STORAGE_POLICY_NAME);
    COLD = DEFAULT_POLICIES.getPolicy(HdfsConstants.COLD_STORAGE_POLICY_NAME);
    TestBalancer.initTestSetup();
    Dispatcher.setDelayAfterErrors(1000L);
  }

