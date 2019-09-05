hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
    } finally {
      readUnlock();
    }
    String fromSnapshotRoot = (fromSnapshot == null || fromSnapshot.isEmpty()) ?
        path : Snapshot.getSnapshotPath(path, fromSnapshot);
    String toSnapshotRoot = (toSnapshot == null || toSnapshot.isEmpty()) ?
        path : Snapshot.getSnapshotPath(path, toSnapshot);
    logAuditEvent(diffs != null, "computeSnapshotDiff", fromSnapshotRoot,
        toSnapshotRoot, null);
    return diffs;
  }
  

