hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/StorageType.java
@InterfaceAudience.Public
@InterfaceStability.Unstable
public enum StorageType {
  RAM_DISK(true),
  SSD(false),
  DISK(false),
  ARCHIVE(false);

  private final boolean isTransient;


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockStoragePolicy.java
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetworkTopology;
    }
  }

  @Test
  public void testChooseSsdOverDisk() throws Exception {
    BlockStoragePolicy policy = new BlockStoragePolicy((byte) 9, "TEST1",
        new StorageType[]{StorageType.SSD, StorageType.DISK,
            StorageType.ARCHIVE}, new StorageType[]{}, new StorageType[]{});

    final String[] racks = {"/d1/r1", "/d1/r1", "/d1/r1"};
    final String[] hosts = {"host1", "host2", "host3"};
    final StorageType[] disks = {StorageType.DISK, StorageType.DISK, StorageType.DISK};

    final DatanodeStorageInfo[] diskStorages
        = DFSTestUtil.createDatanodeStorageInfos(3, racks, hosts, disks);
    final DatanodeDescriptor[] dataNodes
        = DFSTestUtil.toDatanodeDescriptor(diskStorages);
    for(int i = 0; i < dataNodes.length; i++) {
      BlockManagerTestUtil.updateStorage(dataNodes[i],
          new DatanodeStorage("ssd" + i, DatanodeStorage.State.NORMAL,
              StorageType.SSD));
    }

    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    File baseDir = PathUtils.getTestDir(TestReplicationPolicy.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        new File(baseDir, "name").getPath());
    DFSTestUtil.formatNameNode(conf);
    NameNode namenode = new NameNode(conf);

    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    BlockPlacementPolicy replicator = bm.getBlockPlacementPolicy();
    NetworkTopology cluster = bm.getDatanodeManager().getNetworkTopology();
    for (DatanodeDescriptor datanode : dataNodes) {
      cluster.add(datanode);
    }

    DatanodeStorageInfo[] targets = replicator.chooseTarget("/foo", 3,
        dataNodes[0], Collections.<DatanodeStorageInfo>emptyList(), false,
        new HashSet<Node>(), 0, policy);
    System.out.println(policy.getName() + ": " + Arrays.asList(targets));
    Assert.assertEquals(2, targets.length);
    Assert.assertEquals(StorageType.SSD, targets[0].getStorageType());
    Assert.assertEquals(StorageType.DISK, targets[1].getStorageType());
  }

  @Test
  public void testGetFileStoragePolicyAfterRestartNN() throws Exception {
      cluster.shutdown();
    }
  }

  @Test
  public void testStorageType() {
    final EnumMap<StorageType, Integer> map = new EnumMap<>(StorageType.class);

    map.put(StorageType.ARCHIVE, 1);
    map.put(StorageType.DISK, 1);
    map.put(StorageType.SSD, 1);
    map.put(StorageType.RAM_DISK, 1);

    {
      final Iterator<StorageType> i = map.keySet().iterator();
      Assert.assertEquals(StorageType.RAM_DISK, i.next());
      Assert.assertEquals(StorageType.SSD, i.next());
      Assert.assertEquals(StorageType.DISK, i.next());
      Assert.assertEquals(StorageType.ARCHIVE, i.next());
    }

    {
      final Iterator<Map.Entry<StorageType, Integer>> i
          = map.entrySet().iterator();
      Assert.assertEquals(StorageType.RAM_DISK, i.next().getKey());
      Assert.assertEquals(StorageType.SSD, i.next().getKey());
      Assert.assertEquals(StorageType.DISK, i.next().getKey());
      Assert.assertEquals(StorageType.ARCHIVE, i.next().getKey());
    }
  }
}

