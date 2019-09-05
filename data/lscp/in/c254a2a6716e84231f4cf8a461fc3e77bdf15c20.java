hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/shell/CommandFormat.java
        options.put(opt, Boolean.TRUE);
      } else if (optionsWithValue.containsKey(opt)) {
        args.remove(pos);
        if (pos < args.size() && (args.size() > minPar)
                && !args.get(pos).startsWith("-")) {
          arg = args.get(pos);
          args.remove(pos);
        } else {

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/shell/TestCount.java
    verifyNoMoreInteractions(out);
  }

  @Test
  public void processPathWithQuotasByQTVH() throws Exception {
    Path path = new Path("mockfs:/test");

    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStat);

    PrintStream out = mock(PrintStream.class);

    Count count = new Count();
    count.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-q");
    options.add("-t");
    options.add("-v");
    options.add("-h");
    options.add("dummy");
    count.processOptions(options);
    String withStorageTypeHeader =
        "   DISK_QUOTA    REM_DISK_QUOTA " +
        "    SSD_QUOTA     REM_SSD_QUOTA " +
        "ARCHIVE_QUOTA REM_ARCHIVE_QUOTA " +
        "PATHNAME";
    verify(out).println(withStorageTypeHeader);
    verifyNoMoreInteractions(out);
  }

  @Test
  public void processPathWithQuotasByMultipleStorageTypesContent() throws Exception {
    Path path = new Path("mockfs:/test");

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/DFSAdmin.java
    ClearSpaceQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
      CommandFormat c = new CommandFormat(1, Integer.MAX_VALUE);
      c.addOptionWithValue("storageType");
      List<String> parameters = c.parse(args, pos);
      String storageTypeString = c.getOptValue("storageType");
      if (storageTypeString != null) {
        this.type = StorageType.parseStorageType(storageTypeString);
      }

