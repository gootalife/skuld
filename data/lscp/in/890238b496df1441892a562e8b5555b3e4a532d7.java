hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/MiniDFSCluster.java
        nameNode = null;
      }
    }
    if (base_dir != null) {
      if (deleteDfsDir) {
        base_dir.delete();
      } else {
      }
    }

  }
  

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSClientFailover.java
  
  @After
  public void tearDownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @After
  public void clearConfig() {

