hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/DFSTestUtil.java
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.Assume;
import org.mockito.internal.util.reflection.Whitebox;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
    return false;
  }

  @VisibleForTesting
  public static NamenodeProtocol getNamenodeProtocolProxy(Configuration conf,
      URI nameNodeUri, UserGroupInformation ugi)
      throws IOException {
    return NameNodeProxies.createNonHAProxy(conf,
        NameNode.getAddress(nameNodeUri), NamenodeProtocol.class, ugi, false).
        getProxy();
  }

  @VisibleForTesting
  public static RefreshUserMappingsProtocol getRefreshUserMappingsProtocolProxy(
      Configuration conf, URI nameNodeUri) throws IOException {
    final AtomicBoolean nnFallbackToSimpleAuth = new AtomicBoolean(false);
    return NameNodeProxies.createProxy(conf,
        nameNodeUri, RefreshUserMappingsProtocol.class,
        nnFallbackToSimpleAuth).getProxy();
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/NNThroughputBenchmark.java
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
  private static final Log LOG = LogFactory.getLog(NNThroughputBenchmark.class);
  private static final int BLOCK_SIZE = 16;
  private static final String GENERAL_OPTIONS_USAGE = 
    "     [-keepResults] | [-logLevel L] | [-UGCacheRefreshCount G] |" +
    " [-namenode <namenode URI>]\n" +
    "     If using -namenode, set the namenode's" +
    "         dfs.namenode.fs-limits.min-block-size to 16.";

  static Configuration config;
  static NameNode nameNode;
  static NamenodeProtocol nameNodeProto;
  static ClientProtocol clientProto;
  static DatanodeProtocol dataNodeProto;
  static RefreshUserMappingsProtocol refreshUserMappingsProto;
  static String bpid = null;

  private String namenodeUri = null; // NN URI to use, if specified

  NNThroughputBenchmark(Configuration conf) throws IOException {
    config = conf;
        for(StatsDaemon d : daemons)
          d.start();
      } finally {
        while(isInProgress()) {
        }
        elapsedTime = Time.now() - start;
      }
    }

    private boolean isInProgress() {
      for(StatsDaemon d : daemons)
        if(d.isInProgress())
          return true;
    }

    void cleanUp() throws IOException {
      clientProto.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
          false);
      if(!keepResults)
        clientProto.delete(getBaseDir(), true);
    }

    int getNumOpsExecuted() {
        args.remove(ugrcIndex);
      }

      try {
        namenodeUri = StringUtils.popOptionWithArgument("-namenode", args);
      } catch (IllegalArgumentException iae) {
        printUsage();
      }

      String type = args.get(1);
      if(OP_ALL_NAME.equals(type)) {
        type = getOpName();
    void benchmarkOne() throws IOException {
      for(int idx = 0; idx < opsPerThread; idx++) {
        if((localNumOpsExecuted+1) % statsOp.ugcRefreshCount == 0)
          refreshUserMappingsProto.refreshUserToGroupsMappings();
        long stat = statsOp.executeOp(daemonId, idx, arg1);
        localNumOpsExecuted++;
        localCumulativeTime += stat;
    @Override
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      clientProto.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
          false);
      long start = Time.now();
      clientProto.delete(BASE_DIR_NAME, true);
      long end = Time.now();
      return end-start;
    }
    @Override
    void generateInputs(int[] opsPerThread) throws IOException {
      assert opsPerThread.length == numThreads : "Error opsPerThread.length"; 
      clientProto.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
          false);
      LOG.info("Generate " + numOpsRequired + " intputs for " + getOpName());
    throws IOException {
      long start = Time.now();
      clientProto.create(fileNames[daemonId][inputIdx], FsPermission.getDefault(),
                      clientName, new EnumSetWritable<CreateFlag>(EnumSet
              .of(CreateFlag.CREATE, CreateFlag.OVERWRITE)), true, 
          replication, BLOCK_SIZE, CryptoProtocolVersion.supported());
      long end = Time.now();
      for(boolean written = !closeUponCreate; !written; 
        written = clientProto.complete(fileNames[daemonId][inputIdx],
                                    clientName, null, HdfsConstants.GRANDFATHER_INODE_ID));
      return end-start;
    }
    @Override
    void generateInputs(int[] opsPerThread) throws IOException {
      assert opsPerThread.length == numThreads : "Error opsPerThread.length";
      clientProto.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
          false);
      LOG.info("Generate " + numOpsRequired + " inputs for " + getOpName());
      dirPaths = new String[numThreads][];
    long executeOp(int daemonId, int inputIdx, String clientName)
        throws IOException {
      long start = Time.now();
      clientProto.mkdirs(dirPaths[daemonId][inputIdx],
          FsPermission.getDefault(), true);
      long end = Time.now();
      return end-start;
      }
      super.generateInputs(opsPerThread);
      if(clientProto.getFileInfo(opCreate.getBaseDir()) != null
          && clientProto.getFileInfo(getBaseDir()) == null) {
        clientProto.rename(opCreate.getBaseDir(), getBaseDir());
      }
      if(clientProto.getFileInfo(getBaseDir()) == null) {
        throw new IOException(getBaseDir() + " does not exist.");
      }
    }
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      long start = Time.now();
      clientProto.getBlockLocations(fileNames[daemonId][inputIdx], 0L, BLOCK_SIZE);
      long end = Time.now();
      return end-start;
    }
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      long start = Time.now();
      clientProto.delete(fileNames[daemonId][inputIdx], false);
      long end = Time.now();
      return end-start;
    }
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      long start = Time.now();
      clientProto.getFileInfo(fileNames[daemonId][inputIdx]);
      long end = Time.now();
      return end-start;
    }
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      long start = Time.now();
      clientProto.rename(fileNames[daemonId][inputIdx],
                      destNames[daemonId][inputIdx]);
      long end = Time.now();
      return end-start;
          new DataStorage(nsInfo),
          new ExportedBlockKeys(), VersionInfo.getVersion());
      dnRegistration = dataNodeProto.registerDatanode(dnRegistration);
      dnRegistration.setNamespaceInfo(nsInfo);
      storage = new DatanodeStorage(DatanodeStorage.generateUuid());
      final StorageBlockReport[] reports = {
          new StorageBlockReport(storage, BlockListAsLongs.EMPTY)
      };
      dataNodeProto.blockReport(dnRegistration, bpid, reports,
              new BlockReportContext(1, 0, System.nanoTime()));
    }

      StorageReport[] rep = { new StorageReport(storage, false,
          DF_CAPACITY, DF_USED, DF_CAPACITY - DF_USED, DF_USED) };
      DatanodeCommand[] cmds = dataNodeProto.sendHeartbeat(dnRegistration, rep,
          0L, 0L, 0, 0, 0, null).getCommands();
      if(cmds != null) {
        for (DatanodeCommand cmd : cmds ) {
      StorageReport[] rep = { new StorageReport(storage,
          false, DF_CAPACITY, DF_USED, DF_CAPACITY - DF_USED, DF_USED) };
      DatanodeCommand[] cmds = dataNodeProto.sendHeartbeat(dnRegistration,
          rep, 0L, 0L, 0, 0, 0, null).getCommands();
      if (cmds != null) {
        for (DatanodeCommand cmd : cmds) {
                  null) };
          StorageReceivedDeletedBlocks[] report = { new StorageReceivedDeletedBlocks(
              targetStorageID, rdBlocks) };
          dataNodeProto.blockReceivedAndDeleted(receivedDNReg, bpid, report);
        }
      }
      return blocks.length;
      FileNameGenerator nameGenerator;
      nameGenerator = new FileNameGenerator(getBaseDir(), 100);
      String clientName = getClientName(007);
      clientProto.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
          false);
      for(int idx=0; idx < nrFiles; idx++) {
        String fileName = nameGenerator.getNextFileName("ThroughputBench");
        clientProto.create(fileName, FsPermission.getDefault(), clientName,
            new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)), true, replication,
            BLOCK_SIZE, CryptoProtocolVersion.supported());
        ExtendedBlock lastBlock = addBlocks(fileName, clientName);
        clientProto.complete(fileName, clientName, lastBlock, HdfsConstants.GRANDFATHER_INODE_ID);
      }
      for(int idx=0; idx < nrDatanodes; idx++) {
    throws IOException {
      ExtendedBlock prevBlock = null;
      for(int jdx = 0; jdx < blocksPerFile; jdx++) {
        LocatedBlock loc = clientProto.addBlock(fileName, clientName,
            prevBlock, null, HdfsConstants.GRANDFATHER_INODE_ID, null);
        prevBlock = loc.getBlock();
        for(DatanodeInfo dnInfo : loc.getLocations()) {
              ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, null) };
          StorageReceivedDeletedBlocks[] report = { new StorageReceivedDeletedBlocks(
              datanodes[dnIdx].storage.getStorageID(), rdBlocks) };
          dataNodeProto.blockReceivedAndDeleted(datanodes[dnIdx].dnRegistration,
              bpid, report);
        }
      }
      return prevBlock;
      long start = Time.now();
      StorageBlockReport[] report = { new StorageBlockReport(
          dn.storage, dn.getBlockReportList()) };
      dataNodeProto.blockReport(dn.dnRegistration, bpid, report,
          new BlockReportContext(1, 0, System.nanoTime()));
      long end = Time.now();
      return end-start;
        LOG.info("Datanode " + dn + " is decommissioned.");
      }
      excludeFile.close();
      clientProto.refreshNodes();
    }


    String[] argv = new String[] {};

    List<OperationStatsBase> ops = new ArrayList<OperationStatsBase>();
    OperationStatsBase opStat = null;
        opStat = new CleanAllStats(args);
        ops.add(opStat);
      }

      if (namenodeUri == null) {
        nameNode = NameNode.createNameNode(argv, config);
        NamenodeProtocols nnProtos = nameNode.getRpcServer();
        nameNodeProto = nnProtos;
        clientProto = nnProtos;
        dataNodeProto = nnProtos;
        refreshUserMappingsProto = nnProtos;
        bpid = nameNode.getNamesystem().getBlockPoolId();
      } else {
        FileSystem.setDefaultUri(getConf(), namenodeUri);
        DistributedFileSystem dfs = (DistributedFileSystem)
            FileSystem.get(getConf());
        final URI nnUri = new URI(namenodeUri);
        nameNodeProto = DFSTestUtil.getNamenodeProtocolProxy(config, nnUri,
            UserGroupInformation.getCurrentUser());
        clientProto = dfs.getClient().getNamenode();
        dataNodeProto = new DatanodeProtocolClientSideTranslatorPB(
            NameNode.getAddress(nnUri), config);
        refreshUserMappingsProto =
            DFSTestUtil.getRefreshUserMappingsProtocolProxy(config, nnUri);
        getBlockPoolId(dfs);
      }
      if(ops.size() == 0)
        printUsage();
    return 0;
  }

  private void getBlockPoolId(DistributedFileSystem unused)
    throws IOException {
    final NamespaceInfo nsInfo = nameNodeProto.versionRequest();
    bpid = nsInfo.getBlockPoolID();
  }

  public static void main(String[] args) throws Exception {
    NNThroughputBenchmark bench = null;
    try {

