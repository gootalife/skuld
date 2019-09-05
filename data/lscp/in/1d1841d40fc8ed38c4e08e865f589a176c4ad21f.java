hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/nativeio/NativeIO.java
    }
  }

  @Deprecated
  public static void link(File src, File dst) throws IOException {
    if (!nativeLoaded) {
      HardLink.createHardLink(src, dst);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataStorage.java
              idBasedLayoutSingleLinks.size());
          for (int j = iCopy; j < upperBound; j++) {
            LinkArgs cur = idBasedLayoutSingleLinks.get(j);
            HardLink.createHardLink(cur.src, cur.dst);
          }
          return null;
        }

