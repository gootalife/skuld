hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BPServiceActor.java
  private volatile boolean shouldServiceRun = true;
  private final DataNode dn;
  private final DNConf dnConf;
  private long prevBlockReportId;

  private DatanodeRegistration bpRegistration;
  final LinkedList<BPServiceActorAction> bpThreadQueue 
    this.dn = bpos.getDataNode();
    this.nnAddr = nnAddr;
    this.dnConf = dn.getDnConf();
    prevBlockReportId = DFSUtil.getRandom().nextLong();
  }

  boolean isAlive() {
    return sendImmediateIBR;
  }

  private long generateUniqueBlockReportId() {
    prevBlockReportId++;
    while (prevBlockReportId == 0) {
      prevBlockReportId = DFSUtil.getRandom().nextLong();
    }
    return prevBlockReportId;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/protocol/BlockReportContext.java

package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class BlockReportContext {
  private final int totalRpcs;
  private final int curRpc;

