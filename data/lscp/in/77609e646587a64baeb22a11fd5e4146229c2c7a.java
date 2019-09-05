hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/test/MiniDFSClusterManager.java
  private String writeDetails;
  private int numDataNodes;
  private int nameNodePort;
  private int nameNodeHttpPort;
  private StartupOption dfsOpts;
  private String writeConfig;
  private Configuration conf;
        .addOption("cmdport", true,
            "Which port to listen on for commands (default 0--we choose)")
        .addOption("nnport", true, "NameNode port (default 0--we choose)")
        .addOption("httpport", true, "NameNode http port (default 0--we choose)")
        .addOption("namenode", true, "URL of the namenode (default "
            + "is either the DFS cluster or a temporary dir)")     
        .addOption(OptionBuilder
  public void start() throws IOException, FileNotFoundException {
    dfs = new MiniDFSCluster.Builder(conf).nameNodePort(nameNodePort)
                                          .nameNodeHttpPort(nameNodeHttpPort)
                                          .numDataNodes(numDataNodes)
                                          .startupOption(dfsOpts)
                                          .format(format)
    numDataNodes = intArgument(cli, "datanodes", 1);
    nameNodePort = intArgument(cli, "nnport", 0);
    nameNodeHttpPort = intArgument(cli, "httpport", 0);
    if (cli.hasOption("format")) {
      dfsOpts = StartupOption.FORMAT;
      format = true;

