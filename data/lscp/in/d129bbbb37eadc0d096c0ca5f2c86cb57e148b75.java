hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DataStreamer.java
            LOG.debug("Append to block " + block);
          }
          setupPipelineForAppendOrRecovery();
          if (true == streamerClosed) {
            continue;
          }
          initDataStreaming();
        }

          }
        }
        lastException.set(e);
        assert !(e instanceof NullPointerException);
        hasError = true;
        if (errorIndex == -1 && restartingNodeIndex.get() == -1) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestFileAppend.java
      cluster.shutdown();
    }
  }
  
  @Test(timeout = 10000)
  public void testAppendCorruptedBlock() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    conf.setInt("dfs.min.replication", 1);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .build();
    try {
      DistributedFileSystem fs = cluster.getFileSystem();
      Path fileName = new Path("/appendCorruptBlock");
      DFSTestUtil.createFile(fs, fileName, 512, (short) 1, 0);
      DFSTestUtil.waitReplication(fs, fileName, (short) 1);
      Assert.assertTrue("File not created", fs.exists(fileName));
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, fileName);
      cluster.corruptBlockOnDataNodes(block);
      DFSTestUtil.appendFile(fs, fileName, "appendCorruptBlock");
    } finally {
      cluster.shutdown();
    }
  }
}

