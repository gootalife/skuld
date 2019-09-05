hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java
    checkDiskError();

    data.addBlockPool(nsInfo.getBlockPoolID(), conf);
    blockScanner.enableBlockPoolId(bpos.getBlockPoolId());
    initDirectoryScanner(conf);
  }

  List<BPOfferService> getAllBpOs() {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/BlockPoolSlice.java
      return replica1;
    }
    final ReplicaInfo replicaToDelete =
        selectReplicaToDelete(replica1, replica2);
    final ReplicaInfo replicaToKeep =
        (replicaToDelete != replica1) ? replica1 : replica2;
    volumeMap.add(bpid, replicaToKeep);
    if (replicaToDelete != null) {
      deleteReplica(replicaToDelete);
    }
    return replicaToKeep;
  }

  static ReplicaInfo selectReplicaToDelete(final ReplicaInfo replica1,
      final ReplicaInfo replica2) {
    ReplicaInfo replicaToKeep;
    ReplicaInfo replicaToDelete;

    if (replica1.getBlockFile().equals(replica2.getBlockFile())) {
      return null;
    }
    if (replica1.getGenerationStamp() != replica2.getGenerationStamp()) {
      replicaToKeep = replica1.getGenerationStamp() > replica2.getGenerationStamp()
          ? replica1 : replica2;
      LOG.debug("resolveDuplicateReplicas decide to keep " + replicaToKeep
          + ".  Will try to delete " + replicaToDelete);
    }
    return replicaToDelete;
  }

  private void deleteReplica(final ReplicaInfo replicaToDelete) {
    final File blockFile = replicaToDelete.getBlockFile();
    if (!blockFile.delete()) {
    if (!metaFile.delete()) {
      LOG.warn("Failed to delete meta file " + metaFile);
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/TestFsDatasetImpl.java
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
      cluster.shutdown();
    }
  }

  @Test
  public void testDuplicateReplicaResolution() throws IOException {
    FsVolumeImpl fsv1 = Mockito.mock(FsVolumeImpl.class);
    FsVolumeImpl fsv2 = Mockito.mock(FsVolumeImpl.class);

    File f1 = new File("d1/block");
    File f2 = new File("d2/block");

    ReplicaInfo replicaOlder = new FinalizedReplica(1,1,1,fsv1,f1);
    ReplicaInfo replica = new FinalizedReplica(1,2,2,fsv1,f1);
    ReplicaInfo replicaSame = new FinalizedReplica(1,2,2,fsv1,f1);
    ReplicaInfo replicaNewer = new FinalizedReplica(1,3,3,fsv1,f1);

    ReplicaInfo replicaOtherOlder = new FinalizedReplica(1,1,1,fsv2,f2);
    ReplicaInfo replicaOtherSame = new FinalizedReplica(1,2,2,fsv2,f2);
    ReplicaInfo replicaOtherNewer = new FinalizedReplica(1,3,3,fsv2,f2);

    assertNull(BlockPoolSlice.selectReplicaToDelete(replicaSame, replica));
    assertNull(BlockPoolSlice.selectReplicaToDelete(replicaOlder, replica));
    assertNull(BlockPoolSlice.selectReplicaToDelete(replicaNewer, replica));

    assertSame(replica,
        BlockPoolSlice.selectReplicaToDelete(replicaOtherSame, replica));
    assertSame(replicaOtherOlder,
        BlockPoolSlice.selectReplicaToDelete(replicaOtherOlder, replica));
    assertSame(replica,
        BlockPoolSlice.selectReplicaToDelete(replicaOtherNewer, replica));
  }
}

