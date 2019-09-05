hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/common/HdfsServerConstants.java
@InterfaceAudience.Private
public interface HdfsServerConstants {
  int MIN_BLOCKS_FOR_WRITE = 1;
  long LEASE_SOFTLIMIT_PERIOD = 60 * 1000;
  long LEASE_HARDLIMIT_PERIOD = 60 * LEASE_SOFTLIMIT_PERIOD;
  long LEASE_RECOVER_PERIOD = 10 * 1000; // in ms

