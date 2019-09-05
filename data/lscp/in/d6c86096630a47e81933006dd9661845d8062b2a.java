hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/balancer/Dispatcher.java
      public String toString() {
        return getDisplayName();
      }

      @Override
      public int hashCode() {
        return getStorageType().hashCode() ^ getDatanodeInfo().hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) {
          return true;
        } else if (obj == null || !(obj instanceof StorageGroup)) {
          return false;
        } else {
          final StorageGroup that = (StorageGroup) obj;
          return this.getStorageType() == that.getStorageType()
              && this.getDatanodeInfo().equals(that.getDatanodeInfo());
        }
      }

    }

    final DatanodeInfo datanode;
        }
      }
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return super.equals(obj);
    }
  }

  public Dispatcher(NameNodeConnector nnc, Set<String> includedNodes,

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/mover/TestMover.java
       cluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  public void testTwoReplicaSameStorageTypeShouldNotSelect() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .storageTypes(
            new StorageType[][] { { StorageType.DISK, StorageType.ARCHIVE },
                { StorageType.DISK, StorageType.DISK },
                { StorageType.DISK, StorageType.ARCHIVE } }).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testForTwoReplicaSameStorageTypeShouldNotSelect";
      final FSDataOutputStream out = dfs.create(new Path(file), (short) 2);
      out.writeChars("testForTwoReplicaSameStorageTypeShouldNotSelect");
      out.close();

      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      StorageType[] storageTypes = lb.getStorageTypes();
      for (StorageType storageType : storageTypes) {
        Assert.assertTrue(StorageType.DISK == storageType);
      }
      dfs.setStoragePolicy(new Path(file), "COLD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] { "-p", file.toString() });
      Assert.assertEquals("Movement to ARCHIVE should be successfull", 0, rc);

      Thread.sleep(3000);
      lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      storageTypes = lb.getStorageTypes();
      int archiveCount = 0;
      for (StorageType storageType : storageTypes) {
        if (StorageType.ARCHIVE == storageType) {
          archiveCount++;
        }
      }
      Assert.assertEquals(archiveCount, 2);
    } finally {
      cluster.shutdown();
    }
  }
}

