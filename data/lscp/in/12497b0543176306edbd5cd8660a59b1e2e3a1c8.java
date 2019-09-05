hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/BlockPoolSlice.java
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Files;

    return replicaToKeep;
  }

  @VisibleForTesting
  static ReplicaInfo selectReplicaToDelete(final ReplicaInfo replica1,
      final ReplicaInfo replica2) {
    ReplicaInfo replicaToKeep;

