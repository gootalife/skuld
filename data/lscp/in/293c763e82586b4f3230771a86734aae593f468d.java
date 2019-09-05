hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
    assert containingLiveReplicasNodes.size() >= numReplicas.liveReplicas();
    int usableReplicas = numReplicas.liveReplicas() +
                         numReplicas.decommissionedAndDecommissioning();
    
    if (block instanceof BlockInfoContiguous) {
      BlockCollection bc = ((BlockInfoContiguous) block).getBlockCollection();
    out.print(block + ((usableReplicas > 0)? "" : " MISSING") + 
              " (replicas:" +
              " l: " + numReplicas.liveReplicas() +
              " d: " + numReplicas.decommissionedAndDecommissioning() +
              " c: " + numReplicas.corruptReplicas() +
              " e: " + numReplicas.excessReplicas() + ") "); 

    NumberReplicas replicas = countNodes(ucBlock);
    neededReplications.remove(ucBlock, replicas.liveReplicas(),
        replicas.decommissionedAndDecommissioning(), getReplication(ucBlock));
    pendingReplications.remove(ucBlock);

    DatanodeDescriptor srcNode = null;
    int live = 0;
    int decommissioned = 0;
    int decommissioning = 0;
    int corrupt = 0;
    int excess = 0;
    
      int countableReplica = storage.getState() == State.NORMAL ? 1 : 0; 
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node)))
        corrupt += countableReplica;
      else if (node.isDecommissionInProgress()) {
        decommissioning += countableReplica;
      } else if (node.isDecommissioned()) {
        decommissioned += countableReplica;
      } else if (excessBlocks != null && excessBlocks.contains(block)) {
        excess += countableReplica;
      } else {
        nodesContainingLiveReplicas.add(storage);
        srcNode = node;
    }
    if(numReplicas != null)
      numReplicas.initialize(live, decommissioned, decommissioning, corrupt,
          excess, 0);
    return srcNode;
  }

                                 num.liveReplicas())) {
            neededReplications.add(timedOutItems[i],
                                   num.liveReplicas(),
                                   num.decommissionedAndDecommissioning(),
                                   getReplication(timedOutItems[i]));
          }
        }
    short fileReplication = bc.getBlockReplication();
    if (!isNeededReplication(storedBlock, fileReplication, numCurrentReplica)) {
      neededReplications.remove(storedBlock, numCurrentReplica,
          num.decommissionedAndDecommissioning(), fileReplication);
    } else {
      updateNeededReplications(storedBlock, curReplicaDelta, 0);
    }
    if (isNeededReplication(block, expectedReplication, numCurrentReplica)) {
      if (neededReplications.add(block, numCurrentReplica, num
          .decommissionedAndDecommissioning(), expectedReplication)) {
        return MisReplicationResult.UNDER_REPLICATED;
      }
    }
  public NumberReplicas countNodes(Block b) {
    int decommissioned = 0;
    int decommissioning = 0;
    int live = 0;
    int corrupt = 0;
    int excess = 0;
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node))) {
        corrupt++;
      } else if (node.isDecommissionInProgress()) {
        decommissioning++;
      } else if (node.isDecommissioned()) {
        decommissioned++;
      } else {
        LightWeightLinkedSet<Block> blocksExcess = excessReplicateMap.get(node
        stale++;
      }
    }
    return new NumberReplicas(live, decommissioned, decommissioning, corrupt, excess, stale);
  }

      int curExpectedReplicas = getReplication(block);
      if (isNeededReplication(block, curExpectedReplicas, repl.liveReplicas())) {
        neededReplications.update(block, repl.liveReplicas(), repl
            .decommissionedAndDecommissioning(), curExpectedReplicas,
            curReplicasDelta, expectedReplicasDelta);
      } else {
        int oldReplicas = repl.liveReplicas()-curReplicasDelta;
        int oldExpectedReplicas = curExpectedReplicas-expectedReplicasDelta;
        neededReplications.remove(block, oldReplicas,
            repl.decommissionedAndDecommissioning(), oldExpectedReplicas);
      }
    } finally {
      namesystem.writeUnlock();
      final NumberReplicas n = countNodes(block);
      if (isNeededReplication(block, expected, n.liveReplicas())) { 
        neededReplications.add(block, n.liveReplicas(),
            n.decommissionedAndDecommissioning(), expected);
      } else if (n.liveReplicas() > expected) {
        processOverReplicatedBlock(block, expected, null, null);
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DecommissionManager.java
    LOG.info("Block: " + block + ", Expected Replicas: "
        + curExpectedReplicas + ", live replicas: " + curReplicas
        + ", corrupt replicas: " + num.corruptReplicas()
        + ", decommissioned replicas: " + num.decommissioned()
        + ", decommissioning replicas: " + num.decommissioning()
        + ", excess replicas: " + num.excessReplicas()
        + ", Is Open File: " + bc.isUnderConstruction()
        + ", Datanodes having this block: " + nodeList + ", Current Datanode: "
            blockManager.neededReplications.add(block,
                curReplicas,
                num.decommissionedAndDecommissioning(),
                bc.getBlockReplication());
          }
        }
        if (bc.isUnderConstruction()) {
          underReplicatedInOpenFiles++;
        }
        if ((curReplicas == 0) && (num.decommissionedAndDecommissioning() > 0)) {
          decommissionOnlyReplicas++;
        }
      }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/NumberReplicas.java

public class NumberReplicas {
  private int liveReplicas;

  private int decommissioning;
  private int decommissioned;
  private int corruptReplicas;
  private int excessReplicas;
  private int replicasOnStaleNodes;

  NumberReplicas() {
    initialize(0, 0, 0, 0, 0, 0);
  }

  NumberReplicas(int live, int decommissioned, int decommissioning, int corrupt,
                 int excess, int stale) {
    initialize(live, decommissioned, decommissioning, corrupt, excess, stale);
  }

  void initialize(int live, int decommissioned, int decommissioning,
                  int corrupt, int excess, int stale) {
    liveReplicas = live;
    this.decommissioning = decommissioning;
    this.decommissioned = decommissioned;
    corruptReplicas = corrupt;
    excessReplicas = excess;
    replicasOnStaleNodes = stale;
  public int liveReplicas() {
    return liveReplicas;
  }

  @Deprecated
  public int decommissionedReplicas() {
    return decommissionedAndDecommissioning();
  }

  public int decommissionedAndDecommissioning() {
    return decommissioned + decommissioning;
  }

  public int decommissioned() {
    return decommissioned;
  }

  public int decommissioning() {
    return decommissioning;
  }

  public int corruptReplicas() {
    return corruptReplicas;
  }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NamenodeFsck.java
      out.println("No. of live Replica: " + numberReplicas.liveReplicas());
      out.println("No. of excess Replica: " + numberReplicas.excessReplicas());
      out.println("No. of stale Replica: " + numberReplicas.replicasOnStaleNodes());
      out.println("No. of decommissioned Replica: "
          + numberReplicas.decommissioned());
      out.println("No. of decommissioning Replica: "
          + numberReplicas.decommissioning());
      out.println("No. of corrupted Replica: " + numberReplicas.corruptReplicas());
      Collection<DatanodeDescriptor> corruptionRecord = null;
      NumberReplicas numberReplicas =
          namenode.getNamesystem().getBlockManager().countNodes(block.getLocalBlock());
      int liveReplicas = numberReplicas.liveReplicas();
      int decommissionedReplicas = numberReplicas.decommissioned();;
      int decommissioningReplicas = numberReplicas.decommissioning();
      res.decommissionedReplicas +=  decommissionedReplicas;
      res.decommissioningReplicas += decommissioningReplicas;
      int totalReplicas = liveReplicas + decommissionedReplicas +
          decommissioningReplicas;
      res.totalReplicas += totalReplicas;
      short targetFileReplication = file.getReplication();
      res.numExpectedReplicas += targetFileReplication;
      if(totalReplicas < minReplication){
        res.numUnderMinReplicatedBlocks++;
      }
      if (liveReplicas > targetFileReplication) {
        out.print("\n" + path + ": CORRUPT blockpool " + block.getBlockPoolId() + 
            " block " + block.getBlockName()+"\n");
      }
      if (totalReplicas >= minReplication)
        res.numMinReplicatedBlocks++;
      if (totalReplicas < targetFileReplication && totalReplicas > 0) {
        res.missingReplicas += (targetFileReplication - totalReplicas);
        res.numUnderReplicatedBlocks += 1;
        underReplicatedPerFile++;
        if (!showFiles) {
        out.println(" Under replicated " + block +
                    ". Target Replicas is " +
                    targetFileReplication + " but found " +
                    liveReplicas + " live replica(s), " +
                    decommissionedReplicas + " decommissioned replica(s) and " +
                    decommissioningReplicas + " decommissioning replica(s).");
      }
      BlockPlacementStatus blockPlacementStatus = bpPolicy
                    block + ". " + blockPlacementStatus.getErrorDescription());
      }
      report.append(i + ". " + blkName + " len=" + block.getNumBytes());
      if (totalReplicas == 0) {
        report.append(" MISSING!");
        res.addMissing(block.toString(), block.getNumBytes());
        missing++;
    long corruptBlocks = 0L;
    long excessiveReplicas = 0L;
    long missingReplicas = 0L;
    long decommissionedReplicas = 0L;
    long decommissioningReplicas = 0L;
    long numUnderMinReplicatedBlocks=0L;
    long numOverReplicatedBlocks = 0L;
    long numUnderReplicatedBlocks = 0L;
            ((float) (missingReplicas * 100) / (float) numExpectedReplicas)).append(
            " %)");
      }
      if (decommissionedReplicas > 0) {
        res.append("\n DecommissionedReplicas:\t").append(
            decommissionedReplicas);
      }
      if (decommissioningReplicas > 0) {
        res.append("\n DecommissioningReplicas:\t").append(
            decommissioningReplicas);
      }
      return res.toString();
    }
  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestClientReportBadBlock.java
      verifyFirstBlockCorrupted(filePath, false);
      int expectedReplicaCount = repl-corruptBlocReplicas;
      verifyCorruptedBlockCount(filePath, expectedReplicaCount);
      verifyFsckHealth("Target Replicas is 3 but found 1 live replica");
      testFsckListCorruptFilesBlocks(filePath, 0);
    }
  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/datanode/TestReadOnlySharedStorage.java
    assertThat(numberReplicas.liveReplicas(), is(expectedReplicas));
    assertThat(numberReplicas.excessReplicas(), is(0));
    assertThat(numberReplicas.corruptReplicas(), is(0));
    assertThat(numberReplicas.decommissionedAndDecommissioning(), is(0));
    assertThat(numberReplicas.replicasOnStaleNodes(), is(0));
    
    BlockManagerTestUtil.updateState(blockManager);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFsck.java
      }
    }
  }

  @Test
  public void testFsckWithDecommissionedReplicas() throws Exception {

    final short REPL_FACTOR = 1;
    short NUM_DN = 2;
    final long blockSize = 512;
    final long fileSize = 1024;
    boolean checkDecommissionInProgress = false;
    String [] racks = {"/rack1", "/rack2"};
    String [] hosts = {"host1", "host2"};

    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);

    MiniDFSCluster cluster;
    DistributedFileSystem dfs ;
    cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).hosts(hosts)
            .racks(racks).build();

    assertNotNull("Failed Cluster Creation", cluster);
    cluster.waitClusterUp();
    dfs = cluster.getFileSystem();
    assertNotNull("Failed to get FileSystem", dfs);

    DFSTestUtil util = new DFSTestUtil.Builder().
        setName(getClass().getSimpleName()).setNumFiles(1).build();

    final String testFile = new String("/testfile");
    final Path path = new Path(testFile);
    util.createFile(dfs, path, fileSize, REPL_FACTOR, 1000L);
    util.waitReplication(dfs, path, REPL_FACTOR);
    try {
      String outStr = runFsck(conf, 0, true, testFile);
      System.out.println(outStr);
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));

      ExtendedBlock eb = util.getFirstBlock(dfs, path);
      DatanodeDescriptor dn = cluster.getNameNode().getNamesystem()
          .getBlockManager().getBlockCollection(eb.getLocalBlock())
          .getBlocks()[0].getDatanode(0);
      cluster.getNameNode().getNamesystem().getBlockManager()
          .getDatanodeManager().getDecomManager().startDecommission(dn);
      String dnName = dn.getXferAddr();

      DatanodeInfo datanodeInfo = null;
      int count = 0;
      do {
        Thread.sleep(2000);
        for (DatanodeInfo info : dfs.getDataNodeStats()) {
          if (dnName.equals(info.getXferAddr())) {
            datanodeInfo = info;
          }
        }
        if(!checkDecommissionInProgress && datanodeInfo != null
            && datanodeInfo.isDecommissionInProgress()) {
          String fsckOut = runFsck(conf, 0, true, testFile);
          checkDecommissionInProgress =  true;
        }
      } while (datanodeInfo != null && !datanodeInfo.isDecommissioned());

      String fsckOut = runFsck(conf, 0, true, testFile);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}

