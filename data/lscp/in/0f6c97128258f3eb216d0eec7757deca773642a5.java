hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/hdfs/NNBenchWithoutMR.java
  private static long bytesPerBlock = 1;
  private static long blocksPerFile = 0;
  private static long bytesPerFile = 1;
  private static short replicationFactorPerFile = 1; // default is 1
  private static Path baseDir = null;
    
        try {
          out = fileSys.create(
                  new Path(taskDir, "" + index), false, 512,
                  (short)replicationFactorPerFile, bytesPerBlock);
          success = true;
        } catch (IOException ioe) { 
          success=false; 
    
    String usage =
      "Usage: nnbench " +
      "  -operation <one of createWrite, openRead, rename, or delete>\n " +
      "  -baseDir <base output/input DFS path>\n " +
      "  -startTime <time to start, given in seconds from the epoch>\n" +
      "  -numFiles <number of files to create>\n " +
      "  -replicationFactorPerFile <Replication factor for the files, default is 1>\n" +
      "  -blocksPerFile <number of blocks to create per file>\n" +
      "  [-bytesPerBlock <number of bytes to write to each block, default is 1>]\n" +
      "  [-bytesPerChecksum <value for io.bytes.per.checksum>]\n" +
      "Note: bytesPerBlock MUST be a multiple of bytesPerChecksum\n";
    
    String operation = null;
    for (int i = 0; i < args.length; i++) { // parse command line
        bytesPerBlock = Long.parseLong(args[++i]);
      } else if (args[i].equals("-bytesPerChecksum")) {
        bytesPerChecksum = Integer.parseInt(args[++i]);        
      } else if (args[i].equals("-replicationFactorPerFile")) {
        replicationFactorPerFile = Short.parseShort(args[++i]);
      } else if (args[i].equals("-startTime")) {
        startTime = Long.parseLong(args[++i]) * 1000;
      } else if (args[i].equals("-operation")) {
    System.out.println("   baseDir: " + baseDir);
    System.out.println("   startTime: " + startTime);
    System.out.println("   numFiles: " + numFiles);
    System.out.println("   replicationFactorPerFile: " + replicationFactorPerFile);
    System.out.println("   blocksPerFile: " + blocksPerFile);
    System.out.println("   bytesPerBlock: " + bytesPerBlock);
    System.out.println("   bytesPerChecksum: " + bytesPerChecksum);

