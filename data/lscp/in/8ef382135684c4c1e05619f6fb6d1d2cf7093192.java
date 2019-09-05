hadoop-tools/hadoop-archives/src/main/java/org/apache/hadoop/tools/HadoopArchives.java
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
  private static final Log LOG = LogFactory.getLog(HadoopArchives.class);
  
  private static final String NAME = "har"; 
  private static final String ARCHIVE_NAME = "archiveName";
  private static final String REPLICATION = "r";
  private static final String PARENT_PATH = "p";
  private static final String HELP = "help";
  static final String SRC_LIST_LABEL = NAME + ".src.list";
  static final String DST_DIR_LABEL = NAME + ".dest.path";
  static final String TMP_DIR_LABEL = NAME + ".tmp.dir";
  short repl = 10;

  private static final String usage = "archive"
  + " <-archiveName <NAME>.har> <-p <parent path>> [-r <replication factor>]" +
      " <src>* <dest>" +
  "\n";
  
    
  }

  private void printUsage(Options opts, boolean printDetailed) {
    HelpFormatter helpFormatter = new HelpFormatter();
    if (printDetailed) {
      helpFormatter.printHelp(usage.length() + 10, usage, null, opts, null,
          false);
    } else {
      System.out.println(usage);
    }
  }


  public int run(String[] args) throws Exception {
    try {
      Options options = new Options();
      options.addOption(ARCHIVE_NAME, true,
          "Name of the Archive. This is mandatory option");
      options.addOption(PARENT_PATH, true,
          "Parent path of sources. This is mandatory option");
      options.addOption(REPLICATION, true, "Replication factor archive files");
      options.addOption(HELP, false, "Show the usage");
      Parser parser = new GnuParser();
      CommandLine commandLine = parser.parse(options, args, true);

      if (commandLine.hasOption(HELP)) {
        printUsage(options, true);
        return 0;
      }
      if (!commandLine.hasOption(ARCHIVE_NAME)) {
        printUsage(options, false);
        throw new IOException("Archive Name not specified.");
      }
      String archiveName = commandLine.getOptionValue(ARCHIVE_NAME);
      if (!checkValidName(archiveName)) {
        printUsage(options, false);
        throw new IOException("Invalid name for archives. " + archiveName);
      }
      if (!commandLine.hasOption(PARENT_PATH)) {
        printUsage(options, false);
        throw new IOException("Parent path not specified.");
      }
      Path parentPath = new Path(commandLine.getOptionValue(PARENT_PATH));
      if (!parentPath.isAbsolute()) {
        parentPath = parentPath.getFileSystem(getConf()).makeQualified(
            parentPath);
      }

      if (commandLine.hasOption(REPLICATION)) {
        repl = Short.parseShort(commandLine.getOptionValue(REPLICATION));
      }
      args = commandLine.getArgs();
      List<Path> srcPaths = new ArrayList<Path>();
      Path destPath = null;
      for (int i = 0; i < args.length; i++) {
        if (i == (args.length - 1)) {
          destPath = new Path(args[i]);
          if (!destPath.isAbsolute()) {
        else {
          Path argPath = new Path(args[i]);
          if (argPath.isAbsolute()) {
            printUsage(options, false);
            throw new IOException("Source path " + argPath +
                " is not relative to "+ parentPath);
          }
          srcPaths.add(new Path(parentPath, argPath));
        }
      }
      if (destPath == null) {
        printUsage(options, false);
        throw new IOException("Destination path not specified.");
      }
      if (srcPaths.size() == 0) {

hadoop-tools/hadoop-archives/src/test/java/org/apache/hadoop/tools/TestHadoopArchives.java

    final String harName = "foo.har";
    final String fullHarPathStr = prefix + harName;
    final String[] args = { "-archiveName", harName, "-p", inputPathStr, "-r",
        "3", "*", archivePath.toString() };
    System.setProperty(HadoopArchives.TEST_HADOOP_ARCHIVES_JAR_PATH,
        HADOOP_ARCHIVES_JAR);
    final HadoopArchives har = new HadoopArchives(conf);

