hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/ClientProtocol.java
  void setStoragePolicy(String src, String policyName)
      throws IOException;

  @Idempotent
  BlockStoragePolicy getStoragePolicy(String path) throws IOException;


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
  }

  public BlockStoragePolicy getStoragePolicy(String path) throws IOException {
    checkOpen();
    TraceScope scope = getPathTraceScope("getStoragePolicy", path);
    try {
      return namenode.getStoragePolicy(path);
    } catch (RemoteException e) {
      throw e.unwrapRemoteException(AccessControlException.class,
                                    FileNotFoundException.class,
                                    SafeModeException.class,
                                    UnresolvedPathException.class);
    } finally {
      scope.close();
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/ClientNamenodeProtocolServerSideTranslatorPB.java
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePoliciesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePolicyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.IsFileClosedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.IsFileClosedResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCacheDirectivesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.GetEZForPathRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListEncryptionZonesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListEncryptionZonesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockStoragePolicyProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeIDProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto;
    return VOID_SET_STORAGE_POLICY_RESPONSE;
  }

  @Override
  public GetStoragePolicyResponseProto getStoragePolicy(
      RpcController controller, GetStoragePolicyRequestProto request)
      throws ServiceException {
    try {
      BlockStoragePolicyProto policy = PBHelper.convert(server
          .getStoragePolicy(request.getPath()));
      return GetStoragePolicyResponseProto.newBuilder()
          .setStoragePolicy(policy).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetStoragePoliciesResponseProto getStoragePolicies(
      RpcController controller, GetStoragePoliciesRequestProto request)

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/ClientNamenodeProtocolTranslatorPB.java
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePoliciesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.IsFileClosedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCacheDirectivesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto;
    }
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(String path) throws IOException {
    GetStoragePolicyRequestProto request = GetStoragePolicyRequestProto
        .newBuilder().setPath(path).build();
    try {
      return PBHelper.convert(rpcProxy.getStoragePolicy(null, request)
          .getStoragePolicy());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    try {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirAttrOp.java
    return bm.getStoragePolicies();
  }

  static BlockStoragePolicy getStoragePolicy(FSDirectory fsd, BlockManager bm,
      String path) throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    byte[][] pathComponents = FSDirectory
        .getPathComponentsForReservedPath(path);
    fsd.readLock();
    try {
      path = fsd.resolvePath(pc, path, pathComponents);
      final INodesInPath iip = fsd.getINodesInPath(path, false);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.READ);
      }
      INode inode = iip.getLastINode();
      if (inode == null) {
        throw new FileNotFoundException("File/Directory does not exist: "
            + iip.getPath());
      }
      return bm.getStoragePolicy(inode.getStoragePolicyID());
    } finally {
      fsd.readUnlock();
    }
  }

  static long getPreferredBlockSize(FSDirectory fsd, String src)
      throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
    logAuditEvent(true, "setStoragePolicy", src, null, auditStat);
  }

  BlockStoragePolicy getStoragePolicy(String src) throws IOException {
    checkOperation(OperationCategory.READ);
    waitForLoadingFSImage();
    readLock();
    try {
      checkOperation(OperationCategory.READ);
      return FSDirAttrOp.getStoragePolicy(dir, blockManager, src);
    } finally {
      readUnlock();
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNodeRpcServer.java
    namesystem.setStoragePolicy(src, policyName);
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(String path) throws IOException {
    checkNNStartup();
    return namesystem.getStoragePolicy(path);
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    checkNNStartup();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockStoragePolicy.java
    }
  }

  @Test
  public void testGetStoragePolicy() throws Exception {
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(REPLICATION).build();
    cluster.waitActive();
    final DistributedFileSystem fs = cluster.getFileSystem();
    try {
      final Path dir = new Path("/testGetStoragePolicy");
      final Path fooFile = new Path(dir, "foo");
      DFSTestUtil.createFile(fs, fooFile, FILE_LEN, REPLICATION, 0L);
      DFSClient client = new DFSClient(cluster.getNameNode(0)
          .getNameNodeAddress(), conf);
      client.setStoragePolicy("/testGetStoragePolicy/foo",
          HdfsConstants.COLD_STORAGE_POLICY_NAME);
      String policyName = client.getStoragePolicy("/testGetStoragePolicy/foo")
          .getName();
      Assert.assertEquals("File storage policy should be COLD",
          HdfsConstants.COLD_STORAGE_POLICY_NAME, policyName);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testSetStoragePolicyWithSnapshot() throws Exception {
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)

