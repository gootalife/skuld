hadoop-mapreduce-project/hadoop-mapreduce-examples/src/main/java/org/apache/hadoop/examples/BaileyBorweinPlouffe.java
  public int run(String[] args) throws IOException {
    if (args.length != 4) {
      System.err.println("Usage: bbp "
          + " <startDigit> <nDigits> <nMaps> <workingDir>");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;

