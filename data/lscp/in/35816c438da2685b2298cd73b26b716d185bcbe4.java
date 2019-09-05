hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/DFSHAAdmin.java
package org.apache.hadoop.hdfs.tools;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.logging.Log;
      printUsage(errOut);
      return -1;
    }

    int i = 0;
    String cmd = argv[i++];

    if ("-ns".equals(cmd)) {
      if (i == argv.length) {
        errOut.println("Missing nameservice ID");
        printUsage(errOut);
        return -1;
      }
      nameserviceId = argv[i++];
      if (i >= argv.length) {
        errOut.println("Missing command");
        printUsage(errOut);
        return -1;
      }
      argv = Arrays.copyOfRange(argv, i, argv.length);
    }

    return super.runCmd(argv);
  }
  

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/tools/TestDFSHAAdmin.java
    }
  }
  
  @Test
  public void testNameserviceOption() throws Exception {
    assertEquals(-1, runTool("-ns"));
    assertOutputContains("Missing nameservice ID");
    assertEquals(-1, runTool("-ns", "ns1"));
    assertOutputContains("Missing command");
    assertEquals(0, runTool("-ns", "ns1", "-help", "transitionToActive"));
    assertOutputContains("Transitions the service into Active");
  }

  @Test
  public void testNamenodeResolution() throws Exception {
    Mockito.doReturn(STANDBY_READY_RESULT).when(mockProtocol).getServiceStatus();
    assertEquals(0, runTool("-failover", "nn1", "nn2"));
  }

  @Test
  public void testFailoverWithFencerAndNameservice() throws Exception {
    Mockito.doReturn(STANDBY_READY_RESULT).when(mockProtocol).getServiceStatus();
    HdfsConfiguration conf = getHAConf();
    conf.set(DFSConfigKeys.DFS_HA_FENCE_METHODS_KEY, getFencerTrueCommand());
    tool.setConf(conf);
    assertEquals(0, runTool("-ns", "ns1", "-failover", "nn1", "nn2"));
  }

  @Test
  public void testFailoverWithFencerConfiguredAndForce() throws Exception {
    Mockito.doReturn(STANDBY_READY_RESULT).when(mockProtocol).getServiceStatus();

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/tools/TestDFSHAAdminMiniCluster.java
    assertEquals(0, runTool("-failover", "nn1", "nn2"));
    assertEquals(0, runTool("-failover", "nn2", "nn1"));
    
    assertEquals(0, runTool("-ns", "minidfs-ns", "-failover", "nn2", "nn1"));

    assertEquals("", Files.toString(tmpFile, Charsets.UTF_8));


