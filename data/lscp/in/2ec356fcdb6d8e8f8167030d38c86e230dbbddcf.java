hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
                                     ParentNotDirectoryException.class,
                                     NSQuotaExceededException.class, 
                                     DSQuotaExceededException.class,
                                     QuotaByStorageTypeExceededException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    } finally {
                                     FileNotFoundException.class,
                                     SafeModeException.class,
                                     DSQuotaExceededException.class,
                                     QuotaByStorageTypeExceededException.class,
                                     UnsupportedOperationException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
                                     FileNotFoundException.class,
                                     SafeModeException.class,
                                     DSQuotaExceededException.class,
                                     QuotaByStorageTypeExceededException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    } finally {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     QuotaByStorageTypeExceededException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    } finally {
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     DSQuotaExceededException.class,
                                     QuotaByStorageTypeExceededException.class,
                                     FileAlreadyExistsException.class,
                                     FileNotFoundException.class,
                                     ParentNotDirectoryException.class,
                                     SafeModeException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     QuotaByStorageTypeExceededException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    } finally {
                                     FileNotFoundException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     QuotaByStorageTypeExceededException.class,
                                     UnresolvedPathException.class,
                                     SnapshotAccessControlException.class);
    } finally {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSOutputStream.java
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
          IOException e = re.unwrapRemoteException(
              AccessControlException.class,
              DSQuotaExceededException.class,
              QuotaByStorageTypeExceededException.class,
              FileAlreadyExistsException.class,
              FileNotFoundException.class,
              ParentNotDirectoryException.class,

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DataStreamer.java
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
                  AccessControlException.class,
                  NSQuotaExceededException.class,
                  DSQuotaExceededException.class,
                  QuotaByStorageTypeExceededException.class,
                  UnresolvedPathException.class);
          if (ue != e) {
            throw ue; // no need to retry these exceptions

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNodeRpcServer.java
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
        LeaseExpiredException.class,
        NSQuotaExceededException.class,
        DSQuotaExceededException.class,
        QuotaByStorageTypeExceededException.class,
        AclException.class,
        FSLimitException.PathComponentTooLongException.class,
        FSLimitException.MaxDirectoryItemsExceededException.class,

