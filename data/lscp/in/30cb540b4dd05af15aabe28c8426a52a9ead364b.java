hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DatanodeManager.java
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.*;
import org.apache.hadoop.net.NetworkTopology.InvalidTopologyException;
  private final Map<String, DatanodeDescriptor> datanodeMap
      = new HashMap<String, DatanodeDescriptor>();

  private final NetworkTopology networktopology;
    }
  }
  

  public DatanodeDescriptor getDatanodeByHost(final String host) {
  void datanodeDump(final PrintWriter out) {
    synchronized (datanodeMap) {
      Map<String,DatanodeDescriptor> sortedDatanodeMap =
          new TreeMap<String,DatanodeDescriptor>(datanodeMap);
      out.println("Metasave: Number of datanodes: " + datanodeMap.size());
      for (DatanodeDescriptor node : sortedDatanodeMap.values()) {
        out.println(node.dumpDatanode());
      }
    }
        foundNodes.add(HostFileManager.resolvedAddressFromDatanodeID(dn));
      }
    }
    Collections.sort(nodes);

    if (listDeadNodes) {
      for (InetSocketAddress addr : includedNodes) {

