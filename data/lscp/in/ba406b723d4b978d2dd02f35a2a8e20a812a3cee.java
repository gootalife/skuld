hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/DFSTestUtil.java
      public Boolean get() {
        try {
          FileStatus stat = dfs.getFileStatus(file);
          BlockLocation[] locs = dfs.getFileBlockLocations(stat, 0, stat
              .getLen());
          for (BlockLocation loc : locs) {
            if (replication != loc.getHosts().length) {
              return false;
            }
          }
          return true;
        } catch (IOException e) {
          LOG.info("getFileStatus on path " + file + " failed!", e);
          return false;

