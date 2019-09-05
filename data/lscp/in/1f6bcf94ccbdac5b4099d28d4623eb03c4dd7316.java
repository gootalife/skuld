hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DirectoryScanner.java
    diffRecord.add(new ScanInfo(blockId, null, null, vol));
  }

  private Map<String, ScanInfo[]> getDiskReport() {
    ScanInfoPerBlockPool list = new ScanInfoPerBlockPool();
    ScanInfoPerBlockPool[] dirReports = null;
    try (FsDatasetSpi.FsVolumeReferences volumes =
        dataset.getFsVolumeReferences()) {

      dirReports = new ScanInfoPerBlockPool[volumes.size()];

      Map<Integer, Future<ScanInfoPerBlockPool>> compilersInProgress =
          new HashMap<Integer, Future<ScanInfoPerBlockPool>>();

      for (int i = 0; i < volumes.size(); i++) {
        ReportCompiler reportCompiler =
            new ReportCompiler(datanode, volumes.get(i));
        Future<ScanInfoPerBlockPool> result =
            reportCompileThreadPool.submit(reportCompiler);
        compilersInProgress.put(i, result);
      }

      for (Entry<Integer, Future<ScanInfoPerBlockPool>> report :
          compilersInProgress.entrySet()) {
          throw new RuntimeException(ex);
        }
      }
    } catch (IOException e) {
      LOG.error("Unexpected IOException by closing FsVolumeReference", e);
    }
    if (dirReports != null) {
      for (ScanInfoPerBlockPool report : dirReports) {
        list.addAll(report);
      }
    }
    return list.toSortedArrays();
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/FsDatasetSpi.java
package org.apache.hadoop.hdfs.server.datanode.fsdataset;


import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.UnexpectedReplicaStateException;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetFactory;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.util.ReflectionUtils;

    }
  }

  class FsVolumeReferences implements Iterable<FsVolumeSpi>, Closeable {
    private final List<FsVolumeReference> references;

    public <S extends FsVolumeSpi> FsVolumeReferences(List<S> curVolumes) {
      references = new ArrayList<>();
      for (FsVolumeSpi v : curVolumes) {
        try {
          references.add(v.obtainReference());
        } catch (ClosedChannelException e) {
        }
      }
    }

    private static class FsVolumeSpiIterator implements
        Iterator<FsVolumeSpi> {
      private final List<FsVolumeReference> references;
      private int idx = 0;

      FsVolumeSpiIterator(List<FsVolumeReference> refs) {
        references = refs;
      }

      @Override
      public boolean hasNext() {
        return idx < references.size();
      }

      @Override
      public FsVolumeSpi next() {
        int refIdx = idx++;
        return references.get(refIdx).getVolume();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public Iterator<FsVolumeSpi> iterator() {
      return new FsVolumeSpiIterator(references);
    }

    public int size() {
      return references.size();
    }

    public FsVolumeSpi get(int index) {
      return references.get(index).getVolume();
    }

    @Override
    public void close() throws IOException {
      IOException ioe = null;
      for (FsVolumeReference ref : references) {
        try {
          ref.close();
        } catch (IOException e) {
          ioe = e;
        }
      }
      references.clear();
      if (ioe != null) {
        throw ioe;
      }
    }
  }

  public FsVolumeReferences getFsVolumeReferences();


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/FsVolumeReference.java
import java.io.IOException;

public interface FsVolumeReference extends Closeable {
  @Override
  void close() throws IOException;

  FsVolumeSpi getVolume();
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetImpl.java
  }

  @Override // FsDatasetSpi
  public FsVolumeReferences getFsVolumeReferences() {
    return new FsVolumeReferences(volumes.getVolumes());
  }

  @Override
      throws IOException {
    List<StorageReport> reports;
    synchronized (statsLock) {
      List<FsVolumeImpl> curVolumes = volumes.getVolumes();
      reports = new ArrayList<>(curVolumes.size());
      for (FsVolumeImpl volume : curVolumes) {
        try (FsVolumeReference ref = volume.obtainReference()) {
    
  final DataNode datanode;
  final DataStorage dataStorage;
  private final FsVolumeList volumes;
  final Map<String, DatanodeStorage> storageMap;
  final FsDatasetAsyncDiskService asyncDiskService;
  final Daemon lazyWriter;
  @Override // FsDatasetSpi
  public boolean hasEnoughResource() {
    return volumes.getVolumes().size() >= validVolsRequired;
  }

    Map<String, BlockListAsLongs.Builder> builders =
        new HashMap<String, BlockListAsLongs.Builder>();

    List<FsVolumeImpl> curVolumes = volumes.getVolumes();
    for (FsVolumeSpi v : curVolumes) {
      builders.put(v.getStorageID(), BlockListAsLongs.builder());
    }

  private Collection<VolumeInfo> getVolumeInfo() {
    Collection<VolumeInfo> info = new ArrayList<VolumeInfo>();
    for (FsVolumeImpl volume : volumes.getVolumes()) {
      long used = 0;
      long free = 0;
      try (FsVolumeReference ref = volume.obtainReference()) {
  @Override //FsDatasetSpi
  public synchronized void deleteBlockPool(String bpid, boolean force)
      throws IOException {
    List<FsVolumeImpl> curVolumes = volumes.getVolumes();
    if (!force) {
      for (FsVolumeImpl volume : curVolumes) {
        try (FsVolumeReference ref = volume.obtainReference()) {
  @Override // FsDatasetSpi
  public HdfsBlocksMetadata getHdfsBlocksMetadata(String poolId,
      long[] blockIds) throws IOException {
    List<FsVolumeImpl> curVolumes = volumes.getVolumes();
    List<byte[]> blocksVolumeIds = new ArrayList<>(curVolumes.size());
  }

  private boolean ramDiskConfigured() {
    for (FsVolumeImpl v: volumes.getVolumes()){
      if (v.isTransientStorage()) {
        return true;
      }
  private void setupAsyncLazyPersistThreads() {
    for (FsVolumeImpl v: volumes.getVolumes()){
      setupAsyncLazyPersistThread(v);
    }
  }

      try (FsVolumeReferences volumes = getFsVolumeReferences()) {
        for (FsVolumeSpi fvs : volumes) {
          FsVolumeImpl v = (FsVolumeImpl) fvs;
          if (v.isTransientStorage()) {
            capacity += v.getCapacity();
            free += v.getAvailable();
          }
        }
      }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsVolumeImpl.java
  }

  private static class FsVolumeReferenceImpl implements FsVolumeReference {
    private FsVolumeImpl volume;

    FsVolumeReferenceImpl(FsVolumeImpl volume) throws ClosedChannelException {
      this.volume = volume;
    @Override
    public void close() throws IOException {
      if (volume != null) {
        volume.unreference();
        volume = null;
      }
    }

    @Override

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsVolumeList.java
  void addVolume(FsVolumeReference ref) {
    FsVolumeImpl volume = (FsVolumeImpl) ref.getVolume();
    while (true) {
      final FsVolumeImpl[] curVolumes = volumes.get();
      final List<FsVolumeImpl> volumeList = Lists.newArrayList(curVolumes);
      volumeList.add(volume);
      if (volumes.compareAndSet(curVolumes,
          volumeList.toArray(new FsVolumeImpl[volumeList.size()]))) {
        break;
    }
    removeVolumeFailureInfo(new File(volume.getBasePath()));
    FsDatasetImpl.LOG.info("Added new volume: " +
        volume.getStorageID());
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/MiniDFSCluster.java
    if (storageCapacities != null) {
      for (int i = curDatanodesNumSaved; i < curDatanodesNumSaved+numDataNodes; ++i) {
        final int index = i - curDatanodesNum;
        try (FsDatasetSpi.FsVolumeReferences volumes =
            dns[index].getFSDataset().getFsVolumeReferences()) {
          assert storageCapacities[index].length == storagesPerDatanode;
          assert volumes.size() == storagesPerDatanode;

          int j = 0;
          for (FsVolumeSpi fvs : volumes) {
            FsVolumeImpl volume = (FsVolumeImpl) fvs;
            LOG.info("setCapacityForTesting " + storageCapacities[index][j]
                + " for [" + volume.getStorageType() + "]" + volume
                .getStorageID());
            volume.setCapacityForTesting(storageCapacities[index][j]);
            j++;
          }
        }
      }
    }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/MiniDFSClusterWithNodeGroup.java
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.StaticMapping;

    if (storageCapacities != null) {
      for (int i = curDatanodesNum; i < curDatanodesNum+numDataNodes; ++i) {
        try (FsDatasetSpi.FsVolumeReferences volumes =
            dns[i].getFSDataset().getFsVolumeReferences()) {
          assert volumes.size() == storagesPerDatanode;

          for (int j = 0; j < volumes.size(); ++j) {
        }
      }
    }
  }

  public synchronized void startDataNodes(Configuration conf, int numDataNodes, 
      boolean manageDfsDirs, StartupOption operation, 

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestNameNodePrunesMissingStorages.java
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
        datanodeToRemoveStorageFromIdx++;
      }
      String volumeDirectoryToRemove = null;
      try (FsDatasetSpi.FsVolumeReferences volumes =
          datanodeToRemoveStorageFrom.getFSDataset().getFsVolumeReferences()) {
        assertEquals(NUM_STORAGES_PER_DN, volumes.size());
        for (FsVolumeSpi volume : volumes) {
          if (volume.getStorageID().equals(storageIdToRemove)) {
            volumeDirectoryToRemove = volume.getBasePath();
          }
        }
      };

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/SimulatedFSDataset.java
  }

  @Override
  public FsVolumeReferences getFsVolumeReferences() {
    throw new UnsupportedOperationException();
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestBlockHasMultipleReplicasOnSameDN.java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
      blocks.add(new FinalizedReplica(localBlock, null, null));
    }

    try (FsDatasetSpi.FsVolumeReferences volumes =
      dn.getFSDataset().getFsVolumeReferences()) {
      BlockListAsLongs bll = BlockListAsLongs.encode(blocks);
      for (int i = 0; i < cluster.getStoragesPerDatanode(); ++i) {
        DatanodeStorage dns = new DatanodeStorage(volumes.get(i).getStorageID());
        reports[i] = new StorageBlockReport(dns, bll);
      }
    }

    cluster.getNameNodeRpc().blockReport(dnReg, bpid, reports,

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestBlockScanner.java
    final DataNode datanode;
    final BlockScanner blockScanner;
    final FsDatasetSpi<? extends FsVolumeSpi> data;
    final FsDatasetSpi.FsVolumeReferences volumes;

    TestContext(Configuration conf, int numNameServices) throws Exception {
      this.numNameServices = numNameServices;
        dfs[i].mkdirs(new Path("/test"));
      }
      data = datanode.getFSDataset();
      volumes = data.getFsVolumeReferences();
    }

    @Override
    public void close() throws IOException {
      volumes.close();
      if (cluster != null) {
        for (int i = 0; i < numNameServices; i++) {
          dfs[i].delete(new Path("/test"), true);
    ctx.createFiles(0, NUM_EXPECTED_BLOCKS, 1);
    final TestScanResultHandler.Info info =
        TestScanResultHandler.getInfo(ctx.volumes.get(0));
    String storageID = ctx.volumes.get(0).getStorageID();
    synchronized (info) {
      info.sem = new Semaphore(4);
      info.shouldRun = true;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDataNodeHotSwapVolumes.java

    FsDatasetSpi<?> dataset = dn.getFSDataset();
    try (FsDatasetSpi.FsVolumeReferences volumes =
        dataset.getFsVolumeReferences()) {
      for (FsVolumeSpi volume : volumes) {
        assertThat(volume.getBasePath(), is(not(anyOf(
            is(newDirs.get(0)), is(newDirs.get(2))))));
      }
    }
    DataStorage storage = dn.getStorage();
    for (int i = 0; i < storage.getNumStorageDirs(); i++) {
      Storage.StorageDirectory sd = storage.getStorageDir(i);
  }

  private FsVolumeImpl getVolume(DataNode dn, File basePath)
      throws IOException {
    try (FsDatasetSpi.FsVolumeReferences volumes =
      dn.getFSDataset().getFsVolumeReferences()) {
      for (FsVolumeSpi vol : volumes) {
        if (vol.getBasePath().equals(basePath.getPath())) {
          return (FsVolumeImpl) vol;
        }
      }
    }
    return null;
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDataNodeVolumeFailure.java
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetTestUtil;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;

    FsDatasetSpi<? extends FsVolumeSpi> data = dn0.getFSDataset();
    try (FsDatasetSpi.FsVolumeReferences vols = data.getFsVolumeReferences()) {
      for (FsVolumeSpi volume : vols) {
        assertNotEquals(new File(volume.getBasePath()).getAbsoluteFile(),
            dn0Vol1.getAbsoluteFile());
      }
    }

    for (ReplicaInfo replica : FsDatasetTestUtil.getReplicas(data, bpid)) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDirectoryScanner.java
  private void duplicateBlock(long blockId) throws IOException {
    synchronized (fds) {
      ReplicaInfo b = FsDatasetTestUtil.fetchReplicaInfo(fds, bpid, blockId);
      try (FsDatasetSpi.FsVolumeReferences volumes =
          fds.getFsVolumeReferences()) {
        for (FsVolumeSpi v : volumes) {
          if (v.getStorageID().equals(b.getVolume().getStorageID())) {
            continue;
          }
          String sourceRoot = b.getVolume().getBasePath();
          String destRoot = v.getBasePath();

          String relativeBlockPath =
              new File(sourceRoot).toURI().relativize(sourceBlock.toURI())
                  .getPath();
          String relativeMetaPath =
              new File(sourceRoot).toURI().relativize(sourceMeta.toURI())
                  .getPath();

          File destBlock = new File(destRoot, relativeBlockPath);
          File destMeta = new File(destRoot, relativeMetaPath);
        }
      }
    }
  }

  private long getFreeBlockId() {

  private long createBlockFile() throws IOException {
    long id = getFreeBlockId();
    try (FsDatasetSpi.FsVolumeReferences volumes = fds.getFsVolumeReferences()) {
      int numVolumes = volumes.size();
      int index = rand.nextInt(numVolumes - 1);
      File finalizedDir = volumes.get(index).getFinalizedDir(bpid);
      File file = new File(finalizedDir, getBlockFile(id));
      if (file.createNewFile()) {
        LOG.info("Created block file " + file.getName());
      }
    }
    return id;
  }

  private long createMetaFile() throws IOException {
    long id = getFreeBlockId();
    try (FsDatasetSpi.FsVolumeReferences refs = fds.getFsVolumeReferences()) {
      int numVolumes = refs.size();
      int index = rand.nextInt(numVolumes - 1);

      File finalizedDir = refs.get(index).getFinalizedDir(bpid);
      File file = new File(finalizedDir, getMetaFile(id));
      if (file.createNewFile()) {
        LOG.info("Created metafile " + file.getName());
      }
    }
    return id;
  }

  private long createBlockMetaFile() throws IOException {
    long id = getFreeBlockId();

    try (FsDatasetSpi.FsVolumeReferences refs = fds.getFsVolumeReferences()) {
      int numVolumes = refs.size();
      int index = rand.nextInt(numVolumes - 1);

      File finalizedDir = refs.get(index).getFinalizedDir(bpid);
      File file = new File(finalizedDir, getBlockFile(id));
      if (file.createNewFile()) {
        LOG.info("Created block file " + file.getName());
          LOG.info("Created metafile " + file.getName());
        }
      }
    }
    return id;
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDiskError.java
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.util.DataChecksum;
    FileSystem localFS = FileSystem.getLocal(conf);
    for (DataNode dn : cluster.getDataNodes()) {
      try (FsDatasetSpi.FsVolumeReferences volumes =
          dn.getFSDataset().getFsVolumeReferences()) {
        for (FsVolumeSpi vol : volumes) {
          String dir = vol.getBasePath();
          Path dataDir = new Path(dir);
          FsPermission actual = localFS.getFileStatus(dataDir).getPermission();
          assertEquals("Permission for dir: " + dataDir + ", is " + actual +
        }
      }
    }
  }
  

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestIncrementalBlockReports.java
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
    singletonDn = cluster.getDataNodes().get(0);
    bpos = singletonDn.getAllBpOs().get(0);
    actor = bpos.getBPServiceActors().get(0);
    try (FsDatasetSpi.FsVolumeReferences volumes =
        singletonDn.getFSDataset().getFsVolumeReferences()) {
      storageUuid = volumes.get(0).getStorageID();
    }
  }

  private static Block getDummyBlock() {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestIncrementalBrVariations.java

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
    LocatedBlocks blocks = createFileGetBlocks(GenericTestUtils.getMethodName());

    try (FsDatasetSpi.FsVolumeReferences volumes
        = dn0.getFSDataset().getFsVolumeReferences()) {
      StorageReceivedDeletedBlocks reports[] =
          new StorageReceivedDeletedBlocks[volumes.size()];

      for (int i = 0; i < reports.length; ++i) {
        FsVolumeSpi volume = volumes.get(i);

        boolean foundBlockOnStorage = false;
        ReceivedDeletedBlockInfo rdbi[] = new ReceivedDeletedBlockInfo[1];
        for (LocatedBlock block : blocks.getLocatedBlocks()) {
          if (block.getStorageIDs()[0].equals(volume.getStorageID())) {
            rdbi[0] =
                new ReceivedDeletedBlockInfo(block.getBlock().getLocalBlock(),
                    ReceivedDeletedBlockInfo.BlockStatus.DELETED_BLOCK, null);
            foundBlockOnStorage = true;
            break;
        }

        assertTrue(foundBlockOnStorage);
        reports[i] =
            new StorageReceivedDeletedBlocks(volume.getStorageID(), rdbi);

        if (splitReports) {

      if (!splitReports) {
        cluster.getNameNodeRpc()
            .blockReceivedAndDeleted(dn0Reg, poolId, reports);
      }

      assertThat(cluster.getNamesystem().getMissingBlocksCount(),
          is((long) reports.length));
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestTriggerBlockReport.java
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
    DataNode datanode = cluster.getDataNodes().get(0);
    BPServiceActor actor =
        datanode.getAllBpOs().get(0).getBPServiceActors().get(0);
    String storageUuid;
    try (FsDatasetSpi.FsVolumeReferences volumes =
        datanode.getFSDataset().getFsVolumeReferences()) {
      storageUuid = volumes.get(0).getStorageID();
    }
    actor.notifyNamenodeDeletedBlock(rdbi, storageUuid);


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/extdataset/ExternalDatasetImpl.java
      StorageType.DEFAULT);

  @Override
  public FsVolumeReferences getFsVolumeReferences() {
    return null;
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/LazyPersistTestCase.java
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.tools.JMXGet;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
  protected final void ensureLazyPersistBlocksAreSaved(
      LocatedBlocks locatedBlocks) throws IOException, InterruptedException {
    final String bpid = cluster.getNamesystem().getBlockPoolId();

    final Set<Long> persistedBlockIds = new HashSet<Long>();

    try (FsDatasetSpi.FsVolumeReferences volumes =
        cluster.getDataNodes().get(0).getFSDataset().getFsVolumeReferences()) {
      while (persistedBlockIds.size() < locatedBlocks.getLocatedBlocks()
          .size()) {
        Thread.sleep(1000);

            }

            FsVolumeImpl volume = (FsVolumeImpl) v;
            File lazyPersistDir =
                volume.getBlockPoolSlice(bpid).getLazypersistDir();

            long blockId = lb.getBlock().getBlockId();
            File targetDir =
          }
        }
      }
    }

    assertThat(persistedBlockIds.size(), is(locatedBlocks.getLocatedBlocks().size()));
    }

    final String bpid = cluster.getNamesystem().getBlockPoolId();
    final FsDatasetSpi<?> dataset =
        cluster.getDataNodes().get(0).getFSDataset();

    try (FsDatasetSpi.FsVolumeReferences volumes =
        dataset.getFsVolumeReferences()) {
      for (FsVolumeSpi vol : volumes) {
        FsVolumeImpl volume = (FsVolumeImpl) vol;
        File targetDir = (volume.isTransientStorage()) ?
            volume.getBlockPoolSlice(bpid).getFinalizedDir() :
            volume.getBlockPoolSlice(bpid).getLazypersistDir();
        if (verifyBlockDeletedFromDir(targetDir, locatedBlocks) == false) {
          return false;
        }
      }
    }
    return true;
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestDatanodeRestart.java
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
      out.write(writeBuf);
      out.hflush();
      DataNode dn = cluster.getDataNodes().get(0);
      try (FsDatasetSpi.FsVolumeReferences volumes =
          dataset(dn).getFsVolumeReferences()) {
        for (FsVolumeSpi vol : volumes) {
          final FsVolumeImpl volume = (FsVolumeImpl) vol;
          File currentDir =
              volume.getCurrentDir().getParentFile().getParentFile();
          File rbwDir = new File(currentDir, "rbw");
          for (File file : rbwDir.listFiles()) {
            if (isCorrupt && Block.isBlockFilename(file)) {
              new RandomAccessFile(file, "rw")
                  .setLength(fileLen - 1); // corrupt
            }
          }
        }
      }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestFsDatasetImpl.java
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
    when(storage.getNumStorageDirs()).thenReturn(numDirs);
  }

  private int getNumVolumes() {
    try (FsDatasetSpi.FsVolumeReferences volumes =
        dataset.getFsVolumeReferences()) {
      return volumes.size();
    } catch (IOException e) {
      return 0;
    }
  }

  @Before
  public void setUp() throws IOException {
    datanode = mock(DataNode.class);
      dataset.addBlockPool(bpid, conf);
    }

    assertEquals(NUM_INIT_VOLUMES, getNumVolumes());
    assertEquals(0, dataset.getNumFailedVolumes());
  }

  @Test
  public void testAddVolumes() throws IOException {
    final int numNewVolumes = 3;
    final int numExistingVolumes = getNumVolumes();
    final int totalVolumes = numNewVolumes + numExistingVolumes;
    Set<String> expectedVolumes = new HashSet<String>();
    List<NamespaceInfo> nsInfos = Lists.newArrayList();
      dataset.addVolume(loc, nsInfos);
    }

    assertEquals(totalVolumes, getNumVolumes());
    assertEquals(totalVolumes, dataset.storageMap.size());

    Set<String> actualVolumes = new HashSet<String>();
    try (FsDatasetSpi.FsVolumeReferences volumes =
        dataset.getFsVolumeReferences()) {
      for (int i = 0; i < numNewVolumes; i++) {
        actualVolumes.add(volumes.get(numExistingVolumes + i).getBasePath());
      }
    }
    assertEquals(actualVolumes.size(), expectedVolumes.size());
    assertTrue(actualVolumes.containsAll(expectedVolumes));
    dataset.removeVolumes(volumesToRemove, true);
    int expectedNumVolumes = dataDirs.length - 1;
    assertEquals("The volume has been removed from the volumeList.",
        expectedNumVolumes, getNumVolumes());
    assertEquals("The volume has been removed from the storageMap.",
        expectedNumVolumes, dataset.storageMap.size());


  @Test(timeout = 5000)
  public void testRemoveNewlyAddedVolume() throws IOException {
    final int numExistingVolumes = getNumVolumes();
    List<NamespaceInfo> nsInfos = new ArrayList<>();
    for (String bpid : BLOCK_POOL_IDS) {
      nsInfos.add(new NamespaceInfo(0, CLUSTER_ID, bpid, 1));
        .thenReturn(builder);

    dataset.addVolume(loc, nsInfos);
    assertEquals(numExistingVolumes + 1, getNumVolumes());

    when(storage.getNumStorageDirs()).thenReturn(numExistingVolumes + 1);
    when(storage.getStorageDir(numExistingVolumes)).thenReturn(sd);
    Set<File> volumesToRemove = new HashSet<>();
    volumesToRemove.add(loc.getFile());
    dataset.removeVolumes(volumesToRemove, true);
    assertEquals(numExistingVolumes, getNumVolumes());
  }

  @Test(timeout = 5000)
      DataNode dn = cluster.getDataNodes().get(0);
      
      FsDatasetImpl ds = (FsDatasetImpl) DataNodeTestUtils.getFSDataset(dn);
      FsVolumeImpl vol;
      try (FsDatasetSpi.FsVolumeReferences volumes = ds.getFsVolumeReferences()) {
        vol = (FsVolumeImpl)volumes.get(0);
      }

      ExtendedBlock eb;
      ReplicaInfo info;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestFsVolumeList.java
import java.util.List;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class TestFsVolumeList {
        conf, StorageType.DEFAULT);
    FsVolumeReference ref = volume.obtainReference();
    volumeList.addVolume(ref);
    assertNull(ref.getVolume());
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestRbwSpaceReservation.java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Daemon;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;
import java.util.concurrent.TimeoutException;

  private Configuration conf;
  private DistributedFileSystem fs = null;
  private DFSClient client = null;
  FsVolumeReference singletonVolumeRef = null;
  FsVolumeImpl singletonVolume = null;

  private static Random rand = new Random();
    cluster.waitActive();

    if (perVolumeCapacity >= 0) {
      try (FsDatasetSpi.FsVolumeReferences volumes =
          cluster.getDataNodes().get(0).getFSDataset().getFsVolumeReferences()) {
        singletonVolumeRef = volumes.get(0).obtainReference();
      }
      singletonVolume = ((FsVolumeImpl) singletonVolumeRef.getVolume());
      singletonVolume.setCapacityForTesting(perVolumeCapacity);
    }
  }

  @After
  public void shutdownCluster() throws IOException {
    if (singletonVolumeRef != null) {
      singletonVolumeRef.close();
      singletonVolumeRef = null;
    }

    if (client != null) {
      client.close();
      client = null;
    for (DataNode dn : cluster.getDataNodes()) {
      try (FsDatasetSpi.FsVolumeReferences volumes =
          dn.getFSDataset().getFsVolumeReferences()) {
        final FsVolumeImpl volume = (FsVolumeImpl) volumes.get(0);
        GenericTestUtils.waitFor(new Supplier<Boolean>() {
          @Override
          public Boolean get() {
        }, 500, Integer.MAX_VALUE); // Wait until the test times out.
      }
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestWriteToReplica.java
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaUnderRecovery;
import org.apache.hadoop.hdfs.server.datanode.ReplicaWaitingToBeRecovered;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.junit.Assert;
    };
    
    ReplicaMap replicasMap = dataSet.volumeMap;
    try (FsDatasetSpi.FsVolumeReferences references =
        dataSet.getFsVolumeReferences()) {
      FsVolumeImpl vol = (FsVolumeImpl) references.get(0);
      ReplicaInfo replicaInfo = new FinalizedReplica(
          blocks[FINALIZED].getLocalBlock(), vol,
          vol.getCurrentDir().getParentFile());
      replicasMap.add(bpid, replicaInfo);
      replicaInfo.getBlockFile().createNewFile();
      replicaInfo.getMetaFile().createNewFile();
      replicasMap.add(bpid, new ReplicaInPipeline(
          blocks[TEMPORARY].getBlockId(),
          blocks[TEMPORARY].getGenerationStamp(), vol,
          vol.createTmpFile(bpid, blocks[TEMPORARY].getLocalBlock())
              .getParentFile(), 0));

      replicaInfo = new ReplicaBeingWritten(blocks[RBW].getLocalBlock(), vol,
          vol.createRbwFile(bpid, blocks[RBW].getLocalBlock()).getParentFile(),
          null);
      replicasMap.add(bpid, replicaInfo);
      replicaInfo.getBlockFile().createNewFile();
      replicaInfo.getMetaFile().createNewFile();
      replicasMap.add(bpid, new ReplicaWaitingToBeRecovered(
          blocks[RWR].getLocalBlock(), vol, vol.createRbwFile(bpid,
          blocks[RWR].getLocalBlock()).getParentFile()));
      replicasMap
          .add(bpid, new ReplicaUnderRecovery(new FinalizedReplica(blocks[RUR]
              .getLocalBlock(), vol, vol.getCurrentDir().getParentFile()),
              2007));
    }
    return blocks;
  }
  
          getFSDataset(dn);
      ReplicaMap replicaMap = dataSet.volumeMap;
      
      List<FsVolumeImpl> volumes = null;
      try (FsDatasetSpi.FsVolumeReferences referredVols = dataSet.getFsVolumeReferences()) {
        assertEquals("number of volumes is wrong", 2, referredVols.size());
        volumes = new ArrayList<>(referredVols.size());
        for (FsVolumeSpi vol : referredVols) {
          volumes.add((FsVolumeImpl) vol);
        }
      }
      ArrayList<String> bpList = new ArrayList<String>(Arrays.asList(
          cluster.getNamesystem(0).getBlockPoolId(), 
          cluster.getNamesystem(1).getBlockPoolId()));

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/mover/TestStorageMover.java
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
  }

  private void setVolumeFull(DataNode dn, StorageType type) {
    try (FsDatasetSpi.FsVolumeReferences refs = dn.getFSDataset()
        .getFsVolumeReferences()) {
      for (FsVolumeSpi fvs : refs) {
        FsVolumeImpl volume = (FsVolumeImpl) fvs;
        if (volume.getStorageType() == type) {
          LOG.info("setCapacity to 0 for [" + volume.getStorageType() + "]"
              + volume.getStorageID());
          volume.setCapacityForTesting(0);
        }
      }
    } catch (IOException e) {
      LOG.error("Unexpected exception by closing FsVolumeReference", e);
    }
  }


