hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/ReportBadBlockAction.java
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;


    try {
      bpNamenode.reportBadBlocks(locatedBlock);
    } catch (IOException e) {
      throw new BPServiceActorActionException("Failed to report bad block "
          + block + " to namenode: ");

