hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/DFSck.java
    int res = -1;
    if ((args.length == 0) || ("-files".equals(args[0]))) {
      printUsage(System.err);
    } else if (DFSUtil.parseHelpArgument(args, USAGE, System.out, true)) {
      res = 0;
    } else {

