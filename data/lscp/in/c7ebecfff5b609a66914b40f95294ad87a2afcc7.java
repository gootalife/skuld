hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DatanodeManager.java
    if (client == null) {
      List<String> hosts = new ArrayList<String> (1);
      hosts.add(targethost);
      List<String> resolvedHosts = dnsToSwitchMapping.resolve(hosts);
      if (resolvedHosts != null && !resolvedHosts.isEmpty()) {
        String rName = resolvedHosts.get(0);
        if (rName != null) {
          client = new NodeBase(rName + NodeBase.PATH_SEPARATOR_STR +
            targethost);
        }
      } else {
        LOG.error("Node Resolution failed. Please make sure that rack " +
          "awareness scripts are functional.");
      }
    }
    
    Comparator<DatanodeInfo> comparator = avoidStaleDataNodesForRead ?

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestDatanodeManager.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.util.Shell;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
  @Test
  public void testSortLocatedBlocks() throws IOException, URISyntaxException {
    HelperFunction(null);
  }

  @Test
  public void testgoodScript() throws IOException, URISyntaxException {
    HelperFunction("/" + Shell.appendScriptExtension("topology-script"));
  }


  @Test
  public void testBadScript() throws IOException, URISyntaxException {
    HelperFunction("/"+ Shell.appendScriptExtension("topology-broken-script"));
  }


  public void HelperFunction(String scriptFileName)
    throws URISyntaxException, IOException {
    Configuration conf = new Configuration();
    FSNamesystem fsn = Mockito.mock(FSNamesystem.class);
    Mockito.when(fsn.hasWriteLock()).thenReturn(true);
    if (scriptFileName != null && !scriptFileName.isEmpty()) {
      URL shellScript = getClass().getResource(scriptFileName);
      Path resourcePath = Paths.get(shellScript.toURI());
      FileUtil.setExecutable(resourcePath.toFile(), true);
      conf.set(DFSConfigKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
        resourcePath.toString());
    }
    DatanodeManager dm = new DatanodeManager(Mockito.mock(BlockManager.class),
      fsn, conf);

    DatanodeInfo[] locs = new DatanodeInfo[5];
    assertThat(storageIDs.length, is(5));
    assertThat(storageTypes.length, is(5));
    for (int i = 0; i < sortedLocs.length; i++) {
      assertThat(((DatanodeInfoWithStorage) sortedLocs[i]).getStorageID(),
        is(storageIDs[i]));
      assertThat(((DatanodeInfoWithStorage) sortedLocs[i]).getStorageType(),
        is(storageTypes[i]));
    }
    assertThat(sortedLocs[0].getIpAddr(), is(targetIp));
    assertThat(sortedLocs[sortedLocs.length - 1].getAdminState(),
      is(DatanodeInfo.AdminStates.DECOMMISSIONED));

