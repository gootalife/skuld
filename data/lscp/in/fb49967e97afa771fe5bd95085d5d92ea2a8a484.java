hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/shell/Ls.java
    factory.addClass(Lsr.class, "-lsr");
  }

  private static final String OPTION_PATHONLY = "C";
  private static final String OPTION_DIRECTORY = "d";
  private static final String OPTION_HUMAN = "h";
  private static final String OPTION_RECURSIVE = "R";
  private static final String OPTION_SIZE = "S";

  public static final String NAME = "ls";
  public static final String USAGE = "[-" + OPTION_PATHONLY + "] [-"
      + OPTION_DIRECTORY + "] [-" + OPTION_HUMAN + "] [-" + OPTION_RECURSIVE
      + "] [-" + OPTION_MTIME + "] [-" + OPTION_SIZE + "] [-" + OPTION_REVERSE
      + "] [-" + OPTION_ATIME + "] [<path> ...]";

  public static final String DESCRIPTION =
      "List the contents that match the specified file pattern. If " +
          "\tpermissions - userId groupId sizeOfDirectory(in bytes) modificationDate(yyyy-MM-dd HH:mm) directoryName\n\n" +
          "and file entries are of the form:\n" +
          "\tpermissions numberOfReplicas userId groupId sizeOfFile(in bytes) modificationDate(yyyy-MM-dd HH:mm) fileName\n\n" +
          "  -" + OPTION_PATHONLY +
          "  Display the paths of files and directories only.\n" +
          "  -" + OPTION_DIRECTORY +
          "  Directories are listed as plain files.\n" +
          "  -" + OPTION_HUMAN +

  protected int maxRepl = 3, maxLen = 10, maxOwner = 0, maxGroup = 0;
  protected String lineFormat;
  private boolean pathOnly;
  protected boolean dirRecurse;
  private boolean orderReverse;
  private boolean orderTime;
  @Override
  protected void processOptions(LinkedList<String> args)
  throws IOException {
    CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE, OPTION_PATHONLY,
        OPTION_DIRECTORY, OPTION_HUMAN, OPTION_RECURSIVE, OPTION_REVERSE,
        OPTION_MTIME, OPTION_SIZE, OPTION_ATIME);
    cf.parse(args);
    pathOnly = cf.getOpt(OPTION_PATHONLY);
    dirRecurse = !cf.getOpt(OPTION_DIRECTORY);
    setRecursive(cf.getOpt(OPTION_RECURSIVE) && dirRecurse);
    humanReadable = cf.getOpt(OPTION_HUMAN);
    initialiseOrderComparator();
  }

  @InterfaceAudience.Private
  boolean isPathOnly() {
    return this.pathOnly;
  }

  protected void processPaths(PathData parent, PathData ... items)
  throws IOException {
    if (parent != null && !isRecursive() && items.length != 0) {
      if (!pathOnly) {
        out.println("Found " + items.length + " items");
      }
      Arrays.sort(items, getOrderComparator());
    }
    if (!pathOnly) {
      adjustColumnWidths(items);
    }
    super.processPaths(parent, items);
  }

  @Override
  protected void processPath(PathData item) throws IOException {
    if (pathOnly) {
      out.println(item.toString());
      return;
    }
    FileStatus stat = item.stat;
    String line = String.format(lineFormat,
        (stat.isDirectory() ? "d" : "-"),

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/shell/TestLs.java
    LinkedList<String> options = new LinkedList<String>();
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    assertFalse(ls.isOrderReverse());
    assertFalse(ls.isOrderSize());
    assertFalse(ls.isOrderTime());
    assertFalse(ls.isUseAtime());
  }

  @Test
  public void processOptionsPathOnly() throws IOException {
    LinkedList<String> options = new LinkedList<String>();
    options.add("-C");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertTrue(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    options.add("-d");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertFalse(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    options.add("-h");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertTrue(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    options.add("-R");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertTrue(ls.isRecursive());
    options.add("-r");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    options.add("-S");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    options.add("-t");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    options.add("-S");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    options.add("-r");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
    options.add("-u");
    Ls ls = new Ls();
    ls.processOptions(options);
    assertFalse(ls.isPathOnly());
    assertTrue(ls.isDirRecurse());
    assertFalse(ls.isHumanReadable());
    assertFalse(ls.isRecursive());
  @Test
  public void processOptionsAll() throws IOException {
    LinkedList<String> options = new LinkedList<String>();
    options.add("-C"); // show file path only
    options.add("-d"); // directory
    options.add("-h"); // human readable
    options.add("-R"); // recursive
    options.add("-u"); // show atime
    Ls ls = new Ls();
    ls.processOptions(options);
    assertTrue(ls.isPathOnly());
    assertFalse(ls.isDirRecurse());
    assertTrue(ls.isHumanReadable());
    assertFalse(ls.isRecursive()); // -d overrules -R
    verifyNoMoreInteractions(out);
  }

  @Test
  public void processPathDirectoryPathOnly() throws IOException {
    TestFile testfile01 = new TestFile("testDirectory", "testFile01");
    TestFile testfile02 = new TestFile("testDirectory", "testFile02");
    TestFile testfile03 = new TestFile("testDirectory", "testFile03");
    TestFile testfile04 = new TestFile("testDirectory", "testFile04");
    TestFile testfile05 = new TestFile("testDirectory", "testFile05");
    TestFile testfile06 = new TestFile("testDirectory", "testFile06");

    TestFile testDir = new TestFile("", "testDirectory");
    testDir.setIsDir(true);
    testDir.addContents(testfile01, testfile02, testfile03, testfile04,
        testfile05, testfile06);

    LinkedList<PathData> pathData = new LinkedList<PathData>();
    pathData.add(testDir.getPathData());

    PrintStream out = mock(PrintStream.class);

    Ls ls = new Ls();
    ls.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-C");
    ls.processOptions(options);

    ls.processArguments(pathData);
    InOrder inOrder = inOrder(out);
    inOrder.verify(out).println(testfile01.getPath().toString());
    inOrder.verify(out).println(testfile02.getPath().toString());
    inOrder.verify(out).println(testfile03.getPath().toString());
    inOrder.verify(out).println(testfile04.getPath().toString());
    inOrder.verify(out).println(testfile05.getPath().toString());
    inOrder.verify(out).println(testfile06.getPath().toString());
    verifyNoMoreInteractions(out);
  }

  @Test
  public void isDeprecated() {

