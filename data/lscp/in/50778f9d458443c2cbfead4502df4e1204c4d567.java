hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/ErrorReportAction.java

import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.ipc.RemoteException;


    DatanodeRegistration bpRegistration) throws BPServiceActorActionException {
    try {
      bpNamenode.errorReport(bpRegistration, errorCode, errorMessage);
    } catch (RemoteException re) {
      DataNode.LOG.info("trySendErrorReport encountered RemoteException  "
          + "errorMessage: " + errorMessage + "  errorCode: " + errorCode, re);
    } catch(IOException e) {
      throw new BPServiceActorActionException("Error reporting "
          + "an error to namenode: ");

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/ReportBadBlockAction.java
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.ipc.RemoteException;


    try {
      bpNamenode.reportBadBlocks(locatedBlock);
    } catch (RemoteException re) {
      DataNode.LOG.info("reportBadBlock encountered RemoteException for "
          + "block:  " + block , re);
    } catch (IOException e) {
      throw new BPServiceActorActionException("Failed to report bad block "
          + block + " to namenode: ");

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestBPOfferService.java
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.Time;
      bpos.stop();
    }
  } 

 @Test
  public void testReportBadBlocksWhenNNThrowsStandbyException()
      throws Exception {
    BPOfferService bpos = setupBPOSForNNs(mockNN1, mockNN2);
    bpos.start();
    try {
      waitForInitialization(bpos);
      assertNull(bpos.getActiveNN());
      mockHaStatuses[0] = new NNHAStatusHeartbeat(HAServiceState.ACTIVE, 1);
      bpos.triggerHeartbeatForTests();
      assertSame(mockNN1, bpos.getActiveNN());
      Mockito.doNothing().when(mockNN1).reportBadBlocks
          (Mockito.any(LocatedBlock[].class));

      RemoteException re = new RemoteException(StandbyException.class.
          getName(), "Operation category WRITE is not supported in state "
          + "standby", RpcErrorCodeProto.ERROR_APPLICATION);
      Mockito.doThrow(re).when(mockNN2).reportBadBlocks
          (Mockito.any(LocatedBlock[].class));

      bpos.reportBadBlocks(FAKE_BLOCK, mockFSDataset.getVolume(FAKE_BLOCK)
          .getStorageID(), mockFSDataset.getVolume(FAKE_BLOCK)
          .getStorageType());
      bpos.triggerHeartbeatForTests();
      Mockito.verify(mockNN2, Mockito.times(1))
      .reportBadBlocks(Mockito.any(LocatedBlock[].class));

      bpos.triggerHeartbeatForTests();
      Mockito.verify(mockNN2, Mockito.times(1))
          .reportBadBlocks(Mockito.any(LocatedBlock[].class));
    } finally {
      bpos.stop();
    }
  }
}

