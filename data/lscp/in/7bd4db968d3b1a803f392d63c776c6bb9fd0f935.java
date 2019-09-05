hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FsckServlet.java
              namesystem.getNumberOfDatanodes(DatanodeReportType.LIVE); 
          new NamenodeFsck(conf, nn,
              bm.getDatanodeManager().getNetworkTopology(), pmap, out,
              totalDatanodes, remoteAddress).fsck();
          
          return null;
        }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NamenodeFsck.java
  private final NameNode namenode;
  private final NetworkTopology networktopology;
  private final int totalDatanodes;
  private final InetAddress remoteAddress;

  private String lostFound = null;
  NamenodeFsck(Configuration conf, NameNode namenode,
      NetworkTopology networktopology, 
      Map<String,String[]> pmap, PrintWriter out,
      int totalDatanodes, InetAddress remoteAddress) {
    this.conf = conf;
    this.namenode = namenode;
    this.networktopology = networktopology;
    this.out = out;
    this.totalDatanodes = totalDatanodes;
    this.remoteAddress = remoteAddress;
    this.bpPolicy = BlockPlacementPolicy.getInstance(conf, null,
        networktopology,
      res.numExpectedReplicas += targetFileReplication;

      if(totalReplicasPerBlock < res.minReplication){
        res.numUnderMinReplicatedBlocks++;
      }

      }

      if (totalReplicasPerBlock >= res.minReplication)
        res.numMinReplicatedBlocks++;

                    decommissioningReplicas + " decommissioning replica(s).");
      }

      BlockPlacementStatus blockPlacementStatus = bpPolicy
          .verifyBlockPlacement(path, lBlk, targetFileReplication);
      if (!blockPlacementStatus.isPlacementPolicySatisfied()) {
                ((float) (numUnderMinReplicatedBlocks * 100) / (float) totalBlocks))
                .append(" %)");
          }
          res.append("\n  ").append(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY + ":\t")
             .append(minReplication);
        }
        if(corruptFiles>0) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFsck.java
      System.out.println(outStr);
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      assertTrue(outStr.contains("UNDER MIN REPL'D BLOCKS:\t1 (100.0 %)"));
      assertTrue(outStr.contains("dfs.namenode.replication.min:\t2"));
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
      PrintWriter out = new PrintWriter(result, true);
      InetAddress remoteAddress = InetAddress.getLocalHost();
      NamenodeFsck fsck = new NamenodeFsck(conf, namenode, nettop, pmap, out, 
          NUM_REPLICAS, remoteAddress);
      
      final HdfsFileStatus file = 
      PrintWriter out = new PrintWriter(result, true);
      InetAddress remoteAddress = InetAddress.getLocalHost();
      NamenodeFsck fsck = new NamenodeFsck(conf, namenode, nettop, pmap, out, 
          NUM_DN, remoteAddress);
      
      final HdfsFileStatus file = 
    when(blockManager.getDatanodeManager()).thenReturn(dnManager);

    NamenodeFsck fsck = new NamenodeFsck(conf, namenode, nettop, pmap, out,
        NUM_REPLICAS, remoteAddress);

    String pathString = "/tmp/testFile";


