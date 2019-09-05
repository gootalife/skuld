hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/CacheReplicationMonitor.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DatanodeDescriptor.java

  public final DecommissioningStatus decommissioningStatus =
      new DecommissioningStatus();

  private long curBlockReportId = 0;

        return null;
      }

      List<E> results = new ArrayList<>();
      for(; !blockq.isEmpty() && numBlocks > 0; numBlocks--) {
        results.add(blockq.poll());
      }
  }

  private final Map<String, DatanodeStorageInfo> storageMap = 
      new HashMap<>();

  private long bandwidth;

  private final BlockQueue<BlockTargetPair> replicateBlocks =
      new BlockQueue<>();
  private final BlockQueue<BlockInfoUnderConstruction> recoverBlocks =
      new BlockQueue<>();
  private final LightWeightHashSet<Block> invalidateBlocks =
      new LightWeightHashSet<>();

  private EnumCounters<StorageType> currApproxBlocksScheduled
      = new EnumCounters<>(StorageType.class);
  private EnumCounters<StorageType> prevApproxBlocksScheduled
      = new EnumCounters<>(StorageType.class);
  private long lastBlocksScheduledRollTime = 0;
  private static final int BLOCKS_SCHEDULED_ROLL_INTERVAL = 600*1000; //10min
  private int volumeFailures = 0;
              Long.toHexString(curBlockReportId));
          iter.remove();
          if (zombies == null) {
            zombies = new LinkedList<>();
          }
          zombies.add(storageInfo);
        }
  boolean removeBlock(String storageID, BlockInfo b) {
    DatanodeStorageInfo s = getStorageInfo(storageID);
    return s != null && s.removeBlock(b);
  }

  public void resetBlocks() {
          + this.volumeFailures + " to " + volFailures);
      synchronized (storageMap) {
        failedStorageInfos =
            new HashSet<>(storageMap.values());
      }
    }

      HashMap<String, DatanodeStorageInfo> excessStorages;

      excessStorages = new HashMap<>(storageMap);

      for (final StorageReport report : reports) {
    private final List<Iterator<BlockInfo>> iterators;
    
    private BlockIterator(final DatanodeStorageInfo... storages) {
      List<Iterator<BlockInfo>> iterators = new ArrayList<>();
      for (DatanodeStorageInfo e : storages) {
        iterators.add(e.getBlockIterator());
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DatanodeManager.java
  private final Map<String, DatanodeDescriptor> datanodeMap
      = new HashMap<>();

  private final NetworkTopology networktopology;
  private HashMap<String, Integer> datanodesSoftwareVersions =
    new HashMap<>(4, 0.75f);
  
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      final ArrayList<String> locations = new ArrayList<>();
      for (InetSocketAddress addr : hostFileManager.getIncludes()) {
        locations.add(addr.getAddress().getHostAddress());
      }
    Node client = getDatanodeByHost(targethost);
    if (client == null) {
      List<String> hosts = new ArrayList<> (1);
      hosts.add(targethost);
      List<String> resolvedHosts = dnsToSwitchMapping.resolve(hosts);
      if (resolvedHosts != null && !resolvedHosts.isEmpty()) {
  void datanodeDump(final PrintWriter out) {
    synchronized (datanodeMap) {
      Map<String,DatanodeDescriptor> sortedDatanodeMap =
          new TreeMap<>(datanodeMap);
      out.println("Metasave: Number of datanodes: " + datanodeMap.size());
      for (DatanodeDescriptor node : sortedDatanodeMap.values()) {
        out.println(node.dumpDatanode());

  private void countSoftwareVersions() {
    synchronized(datanodeMap) {
      HashMap<String, Integer> versionCount = new HashMap<>();
      for(DatanodeDescriptor dn: datanodeMap.values()) {

  public HashMap<String, Integer> getDatanodesSoftwareVersions() {
    synchronized(datanodeMap) {
      return new HashMap<> (this.datanodesSoftwareVersions);
    }
  }
  
  private String resolveNetworkLocation (DatanodeID node) 
      throws UnresolvedTopologyException {
    List<String> names = new ArrayList<>(1);
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      names.add(node.getIpAddr());
    } else {
      List<String> invalidNodeNames = new ArrayList<>(3);
      invalidNodeNames.add(nodeReg.getIpAddr());
      invalidNodeNames.add(nodeReg.getHostName());
    final HostFileManager.HostSet excludedNodes = hostFileManager.getExcludes();

    synchronized(datanodeMap) {
      nodes = new ArrayList<>(datanodeMap.size());
      for (DatanodeDescriptor dn : datanodeMap.values()) {
        final boolean isDead = isDatanodeDead(dn);
        final boolean isDecommissioning = dn.isDecommissionInProgress();
      VolumeFailureSummary volumeFailureSummary) throws IOException {
    synchronized (heartbeatManager) {
      synchronized (datanodeMap) {
        DatanodeDescriptor nodeinfo;
        try {
          nodeinfo = getDatanode(nodeReg);
        } catch(UnregisteredNodeException e) {
            final DatanodeStorageInfo[] storages = b.getExpectedStorageLocations();
            final List<DatanodeStorageInfo> recoveryLocations =
                new ArrayList<>(storages.length);
            for (int i = 0; i < storages.length; i++) {
              if (!storages[i].getDatanodeDescriptor().isStale(staleInterval)) {
                recoveryLocations.add(storages[i]);
          return new DatanodeCommand[] { brCommand };
        }

        final List<DatanodeCommand> cmds = new ArrayList<>();
        List<BlockTargetPair> pendingList = nodeinfo.getReplicationCommand(
              maxTransfers);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DatanodeStorageInfo.java
public class DatanodeStorageInfo {
  public static final DatanodeStorageInfo[] EMPTY_ARRAY = {};

  public static DatanodeInfo[] toDatanodeInfos(
      DatanodeStorageInfo[] storages) {
    return storages == null ? null: toDatanodeInfos(Arrays.asList(storages));
  }
  static DatanodeInfo[] toDatanodeInfos(List<DatanodeStorageInfo> storages) {
    final DatanodeInfo[] datanodes = new DatanodeInfo[storages.size()];
  }

  public static String[] toStorageIDs(DatanodeStorageInfo[] storages) {
    if (storages == null) {
      return null;
    }
    String[] storageIDs = new String[storages.length];
    for(int i = 0; i < storageIDs.length; i++) {
      storageIDs[i] = storages[i].getStorageID();
  }

  public static StorageType[] toStorageTypes(DatanodeStorageInfo[] storages) {
    if (storages == null) {
      return null;
    }
    StorageType[] storageTypes = new StorageType[storages.length];
    for(int i = 0; i < storageTypes.length; i++) {
      storageTypes[i] = storages[i].getStorageType();
  }

  static enum AddBlockResult {
    ADDED, REPLACED, ALREADY_EXIST
  }
}

