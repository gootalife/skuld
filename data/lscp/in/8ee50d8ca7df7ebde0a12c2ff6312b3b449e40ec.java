hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileUtil.java
      }
    }

    if (entry.isLink()) {
      File src = new File(outputDir, entry.getLinkName());
      HardLink.createHardLink(src, outputFile);
      return;
    }

    int count;
    byte data[] = new byte[2048];
    BufferedOutputStream outputStream = new BufferedOutputStream(

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDatanodeLayoutUpgrade.java
    upgrade.unpackStorage(HADOOP24_DATANODE, HADOOP_DATANODE_DIR_TXT);
    Configuration conf = new Configuration(TestDFSUpgradeFromImage.upgradeConf);
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
        new File(System.getProperty("test.build.data"),
            "dfs" + File.separator + "data").toURI().toString());
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        new File(System.getProperty("test.build.data"),
            "dfs" + File.separator + "name").toURI().toString());
    upgrade.upgradeAndVerify(new MiniDFSCluster.Builder(conf).numDataNodes(1)
    .manageDataDfsDirs(false).manageNameDfsDirs(false), null);
  }

