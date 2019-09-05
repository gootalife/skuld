hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/conf/ReconfigurableBase.java
    reconfigurationUtil = Preconditions.checkNotNull(ru);
  }

  protected abstract Configuration getNewConf();

  @VisibleForTesting
  public Collection<PropertyChange> getChangedProperties(
      Configuration newConf, Configuration oldConf) {
    public void run() {
      LOG.info("Starting reconfiguration task.");
      Configuration oldConf = this.parent.getConf();
      Configuration newConf = this.parent.getNewConf();
      Collection<PropertyChange> changes =
          this.parent.getChangedProperties(newConf, oldConf);
      Map<PropertyChange, Optional<String>> results = Maps.newHashMap();
      for (PropertyChange change : changes) {
        String errorMessage = null;
        if (!this.parent.isPropertyReconfigurable(change.prop)) {
          LOG.info(String.format(
              "Property %s is not configurable: old value: %s, new value: %s",
              change.prop, change.oldVal, change.newVal));
          continue;
        }
        LOG.info("Change property: " + change.prop + " from \""

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/conf/TestReconfiguration.java
      super(conf);
    }

    @Override
    protected Configuration getNewConf() {
      return new Configuration();
    }

    @Override 
    public Collection<String> getReconfigurableProperties() {
      return Arrays.asList(PROP1, PROP2, PROP4);
      super(conf);
    }

    @Override
    protected Configuration getNewConf() {
      return new Configuration();
    }

    final CountDownLatch latch = new CountDownLatch(1);

    @Override

    waitAsyncReconfigureTaskFinish(dummy);
    ReconfigurationTaskStatus status = dummy.getReconfigurationTaskStatus();
    assertEquals(2, status.getStatus().size());
    for (Map.Entry<PropertyChange, Optional<String>> result :
        status.getStatus().entrySet()) {
      PropertyChange change = result.getKey();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocol/ClientDatanodeProtocol.java
  ReconfigurationTaskStatus getReconfigurationStatus() throws IOException;

  List<String> listReconfigurableProperties() throws IOException;


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/ClientDatanodeProtocolServerSideTranslatorPB.java
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetHdfsBlockLocationsResponseProto.Builder;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReplicaVisibleLengthResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.ListReconfigurablePropertiesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.ListReconfigurablePropertiesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.RefreshNamenodesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.RefreshNamenodesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.ShutdownDatanodeRequestProto;
    return START_RECONFIG_RESP;
  }

  @Override
  public ListReconfigurablePropertiesResponseProto listReconfigurableProperties(
        RpcController controller,
        ListReconfigurablePropertiesRequestProto request)
      throws ServiceException {
    ListReconfigurablePropertiesResponseProto.Builder builder =
        ListReconfigurablePropertiesResponseProto.newBuilder();
    try {
      for (String name : impl.listReconfigurableProperties()) {
        builder.addName(name);
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return builder.build();
  }

  @Override
  public GetReconfigurationStatusResponseProto getReconfigurationStatus(
      RpcController unused, GetReconfigurationStatusRequestProto request)

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/ClientDatanodeProtocolTranslatorPB.java
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetHdfsBlockLocationsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetHdfsBlockLocationsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.ListReconfigurablePropertiesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.ListReconfigurablePropertiesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.RefreshNamenodesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReconfigurationStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReconfigurationStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.ShutdownDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.StartReconfigurationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.TriggerBlockReportRequestProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
      GetReconfigurationStatusRequestProto.newBuilder().build();
  private final static StartReconfigurationRequestProto VOID_START_RECONFIG =
      StartReconfigurationRequestProto.newBuilder().build();
  private static final ListReconfigurablePropertiesRequestProto
      VOID_LIST_RECONFIGURABLE_PROPERTIES =
      ListReconfigurablePropertiesRequestProto.newBuilder().build();

  public ClientDatanodeProtocolTranslatorPB(DatanodeID datanodeid,
      Configuration conf, int socketTimeout, boolean connectToDnViaHostname,
    return new ReconfigurationTaskStatus(startTime, endTime, statusMap);
  }

  @Override
  public List<String> listReconfigurableProperties()
      throws IOException {
    ListReconfigurablePropertiesResponseProto response;
    try {
      response = rpcProxy.listReconfigurableProperties(NULL_CONTROLLER,
          VOID_LIST_RECONFIGURABLE_PROPERTIES);
      return response.getNameList();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void triggerBlockReport(BlockReportOptions options)
      throws IOException {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java

  static final int CURRENT_BLOCK_FORMAT_VERSION = 1;

  private static final List<String> RECONFIGURABLE_PROPERTIES =
      Collections.unmodifiableList(
          Arrays.asList(DFS_DATANODE_DATA_DIR_KEY));

            });
  }

  @Override  // ReconfigurableBase
  protected Configuration getNewConf() {
    return new HdfsConfiguration();
  }

  @Override
  public void reconfigurePropertyImpl(String property, String newVal)
      throws ReconfigurationException {
  @Override // Reconfigurable
  public Collection<String> getReconfigurableProperties() {
    return RECONFIGURABLE_PROPERTIES;
  }

    return getReconfigurationTaskStatus();
  }

  @Override // ClientDatanodeProtocol
  public List<String> listReconfigurableProperties()
      throws IOException {
    return RECONFIGURABLE_PROPERTIES;
  }

  @Override // ClientDatanodeProtocol
  public void triggerBlockReport(BlockReportOptions options)
      throws IOException {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/DFSAdmin.java
    "\t[-refreshSuperUserGroupsConfiguration]\n" +
    "\t[-refreshCallQueue]\n" +
    "\t[-refresh <host:ipc_port> <key> [arg1..argn]\n" +
    "\t[-reconfig <datanode|...> <host:ipc_port> <start|status|properties>]\n" +
    "\t[-printTopology]\n" +
    "\t[-refreshNamenodes datanode_host:ipc_port]\n"+
    "\t[-deleteBlockPool datanode_host:ipc_port blockpoolId [force]]\n"+

    String refreshCallQueue = "-refreshCallQueue: Reload the call queue from config\n";

    String reconfig = "-reconfig <datanode|...> <host:ipc_port> <start|status|properties>:\n" +
        "\tStarts or gets the status of a reconfiguration operation, \n" +
        "\tor gets a list of reconfigurable properties.\n" +
        "\tThe second parameter specifies the node type.\n" +
        "\tCurrently, only reloading DataNode's configuration is supported.\n";

      return startReconfiguration(nodeType, address);
    } else if ("status".equals(op)) {
      return getReconfigurationStatus(nodeType, address, System.out, System.err);
    } else if ("properties".equals(op)) {
      return getReconfigurableProperties(
          nodeType, address, System.out, System.err);
    }
    System.err.println("Unknown operation: " + op);
    return -1;

        out.println(" and finished at " +
            new Date(status.getEndTime()).toString() + ".");
        if (status.getStatus() == null) {
          return 0;
        }
        for (Map.Entry<PropertyChange, Optional<String>> result :
            status.getStatus().entrySet()) {
          if (!result.getValue().isPresent()) {
            out.printf(
                "SUCCESS: Changed property %s%n\tFrom: \"%s\"%n\tTo: \"%s\"%n",
                result.getKey().prop, result.getKey().oldVal,
                result.getKey().newVal);
          } else {
            final String errorMsg = result.getValue().get();
            out.printf(
                  "FAILED: Change property %s%n\tFrom: \"%s\"%n\tTo: \"%s\"%n",
                  result.getKey().prop, result.getKey().oldVal,
                  result.getKey().newVal);
            out.println("\tError: " + errorMsg + ".");
          }
        }
      } catch (IOException e) {
        return 1;
      }
    } else {
      err.println("Node type " + nodeType +
          " does not support reconfiguration.");
      return 1;
    }
    return 0;
  }

  int getReconfigurableProperties(String nodeType, String address,
      PrintStream out, PrintStream err) throws IOException {
    if ("datanode".equals(nodeType)) {
      ClientDatanodeProtocol dnProxy = getDataNodeProxy(address);
      try {
        List<String> properties =
            dnProxy.listReconfigurableProperties();
        out.println(
            "Configuration properties that are allowed to be reconfigured:");
        for (String name : properties) {
          out.println(name);
        }
      } catch (IOException e) {
        err.println("DataNode reconfiguration: " + e + ".");
        return 1;
      }
    } else {
      err.println("Node type " + nodeType +
          " does not support reconfiguration.");
      return 1;
    }
    return 0;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/tools/TestDFSAdmin.java
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.containsString;
import static org.mockito.Mockito.when;

public class TestDFSAdmin {
  private Configuration conf = null;
  private MiniDFSCluster cluster;
  private DFSAdmin admin;
  private DataNode datanode;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    restartCluster();

    admin = new DFSAdmin();
  }

  @After
    }
  }

  private void restartCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    datanode = cluster.getDataNodes().get(0);
  }

  private List<String> getReconfigureStatus(String nodeType, String address)
      throws IOException {
    ByteArrayOutputStream bufOut = new ByteArrayOutputStream();
    return outputs;
  }

  private void testGetReconfigurationStatus(boolean expectedSuccuss)
      throws IOException, InterruptedException {
    ReconfigurationUtil ru = mock(ReconfigurationUtil.class);
    datanode.setReconfigurationUtil(ru);

    List<ReconfigurationUtil.PropertyChange> changes =
        new ArrayList<>();
    File newDir = new File(cluster.getDataDirectory(), "data_new");
    if (expectedSuccuss) {
      newDir.mkdirs();
    } else {
      newDir.createNewFile();
    }
    changes.add(new ReconfigurationUtil.PropertyChange(
        DFS_DATANODE_DATA_DIR_KEY, newDir.toString(),
        datanode.getConf().get(DFS_DATANODE_DATA_DIR_KEY)));
      Thread.sleep(100);
    }
    assertTrue(count > 0);
    if (expectedSuccuss) {
      assertThat(outputs.size(), is(4));
    } else {
      assertThat(outputs.size(), is(6));
    }

    List<StorageLocation> locations = DataNode.getStorageLocations(
        datanode.getConf());
    if (expectedSuccuss) {
      assertThat(locations.size(), is(1));
      assertThat(locations.get(0).getFile(), is(newDir));
      assertTrue(new File(newDir, Storage.STORAGE_DIR_CURRENT).isDirectory());
    } else {
      assertTrue(locations.isEmpty());
    }

    int offset = 1;
    if (expectedSuccuss) {
      assertThat(outputs.get(offset),
          containsString("SUCCESS: Changed property " +
              DFS_DATANODE_DATA_DIR_KEY));
    } else {
      assertThat(outputs.get(offset),
          containsString("FAILED: Change property " +
              DFS_DATANODE_DATA_DIR_KEY));
    }
    assertThat(outputs.get(offset + 1),
        is(allOf(containsString("From:"), containsString("data1"),
            containsString("data2"))));
    assertThat(outputs.get(offset + 2),
        is(not(anyOf(containsString("data1"), containsString("data2")))));
    assertThat(outputs.get(offset + 2),
        is(allOf(containsString("To"), containsString("data_new"))));
  }

  @Test(timeout = 30000)
  public void testGetReconfigurationStatus()
      throws IOException, InterruptedException {
    testGetReconfigurationStatus(true);
    restartCluster();
    testGetReconfigurationStatus(false);
  }

  private List<String> getReconfigurationAllowedProperties(
      String nodeType, String address)
      throws IOException {
    ByteArrayOutputStream bufOut = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bufOut);
    ByteArrayOutputStream bufErr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(bufErr);
    admin.getReconfigurableProperties(nodeType, address, out, err);
    Scanner scanner = new Scanner(bufOut.toString());
    List<String> outputs = Lists.newArrayList();
    while (scanner.hasNextLine()) {
      outputs.add(scanner.nextLine());
    }
    return outputs;
  }

  @Test(timeout = 30000)
  public void testGetReconfigAllowedProperties() throws IOException {
    final int port = datanode.getIpcPort();
    final String address = "localhost:" + port;
    List<String> outputs =
        getReconfigurationAllowedProperties("datanode", address);
    assertEquals(2, outputs.size());
    assertEquals(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
        outputs.get(1));
  }
}
\No newline at end of file

