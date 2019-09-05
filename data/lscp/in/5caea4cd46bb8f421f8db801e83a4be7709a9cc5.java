hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/CopyFilter.java
++ b/hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/CopyFilter.java
package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public abstract class CopyFilter {

  public void initialize() {}

  public abstract boolean shouldCopy(Path path);

  public static CopyFilter getCopyFilter(Configuration conf) {
    String filtersFilename = conf.get(DistCpConstants.CONF_LABEL_FILTERS_FILE);

    if (filtersFilename == null) {
      return new TrueCopyFilter();
    } else {
      String filterFilename = conf.get(
          DistCpConstants.CONF_LABEL_FILTERS_FILE);
      return new RegexCopyFilter(filterFilename);
    }
  }
}

hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/DistCpConstants.java
  public static final String CONF_LABEL_APPEND = "distcp.copy.append";
  public static final String CONF_LABEL_DIFF = "distcp.copy.diff";
  public static final String CONF_LABEL_BANDWIDTH_MB = "distcp.map.bandwidth.mb";
  public static final String CONF_LABEL_FILTERS_FILE =
      "distcp.filters.file";
  public static final String CONF_LABEL_MAX_CHUNKS_TOLERABLE =
      "distcp.dynamic.max.chunks.tolerable";
  public static final String CONF_LABEL_MAX_CHUNKS_IDEAL =

hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/DistCpOptionSwitch.java
  BANDWIDTH(DistCpConstants.CONF_LABEL_BANDWIDTH_MB,
      new Option("bandwidth", true, "Specify bandwidth per map in MB")),

  FILTERS(DistCpConstants.CONF_LABEL_FILTERS_FILE,
      new Option("filters", true, "The path to a file containing a list of"
          + " strings for paths to be excluded from the copy."));


  public static final String PRESERVE_STATUS_DEFAULT = "-prbugpct";
  private final String confLabel;

hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/DistCpOptions.java

  private Path targetPath;

  private String filtersFile;

  private boolean targetPathExists = true;
      this.sourcePaths = that.getSourcePaths();
      this.targetPath = that.getTargetPath();
      this.targetPathExists = that.getTargetPathExists();
      this.filtersFile = that.getFiltersFile();
    }
  }

    return this.targetPathExists = targetPathExists;
  }

  public final String getFiltersFile() {
    return filtersFile;
  }

  public final void setFiltersFile(String filtersFilename) {
    this.filtersFile = filtersFilename;
  }

  public void validate(DistCpOptionSwitch option, boolean value) {

    boolean syncFolder = (option == DistCpOptionSwitch.SYNC_FOLDERS ?
        String.valueOf(mapBandwidth));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.PRESERVE_STATUS,
        DistCpUtils.packAttributes(preserveStatus));
    if (filtersFile != null) {
      DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.FILTERS,
          filtersFile);
    }
  }

        ", targetPath=" + targetPath +
        ", targetPathExists=" + targetPathExists +
        ", preserveRawXattrs=" + preserveRawXattrs +
        ", filtersFile='" + filtersFile + '\'' +
        '}';
  }


hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/OptionsParser.java
        Arrays.toString(args), e);
    }

    DistCpOptions option = parseSourceAndTargetPaths(command);

    if (command.hasOption(DistCpOptionSwitch.IGNORE_FAILURES.getSwitch())) {
      option.setBlocking(false);
    }

    parseBandwidth(command, option);

    if (command.hasOption(DistCpOptionSwitch.SSL_CONF.getSwitch())) {
      option.setSslConfigurationFile(command.
          getOptionValue(DistCpOptionSwitch.SSL_CONF.getSwitch()));
    }

    parseNumListStatusThreads(command, option);

    parseMaxMaps(command, option);

    if (command.hasOption(DistCpOptionSwitch.COPY_STRATEGY.getSwitch())) {
      option.setCopyStrategy(
            getVal(command, DistCpOptionSwitch.COPY_STRATEGY.getSwitch()));
    }

    parsePreserveStatus(command, option);

    if (command.hasOption(DistCpOptionSwitch.DIFF.getSwitch())) {
      String[] snapshots = getVals(command, DistCpOptionSwitch.DIFF.getSwitch());
      Preconditions.checkArgument(snapshots != null && snapshots.length == 2,
          "Must provide both the starting and ending snapshot names");
      option.setUseDiff(true, snapshots[0], snapshots[1]);
    }

    parseFileLimit(command);

    parseSizeLimit(command);

    if (command.hasOption(DistCpOptionSwitch.FILTERS.getSwitch())) {
      option.setFiltersFile(getVal(command,
          DistCpOptionSwitch.FILTERS.getSwitch()));
    }

    return option;
  }

  private static void parseSizeLimit(CommandLine command) {
    if (command.hasOption(DistCpOptionSwitch.SIZE_LIMIT.getSwitch())) {
      String sizeLimitString = getVal(command,
                              DistCpOptionSwitch.SIZE_LIMIT.getSwitch().trim());
      try {
        Long.parseLong(sizeLimitString);
      }
      catch (NumberFormatException e) {
        throw new IllegalArgumentException("Size-limit is invalid: "
                                            + sizeLimitString, e);
      }
      LOG.warn(DistCpOptionSwitch.SIZE_LIMIT.getSwitch() + " is a deprecated" +
              " option. Ignoring.");
    }
  }

  private static void parseFileLimit(CommandLine command) {
    if (command.hasOption(DistCpOptionSwitch.FILE_LIMIT.getSwitch())) {
      String fileLimitString = getVal(command,
                              DistCpOptionSwitch.FILE_LIMIT.getSwitch().trim());
      try {
        Integer.parseInt(fileLimitString);
      }
      catch (NumberFormatException e) {
        throw new IllegalArgumentException("File-limit is invalid: "
                                            + fileLimitString, e);
      }
      LOG.warn(DistCpOptionSwitch.FILE_LIMIT.getSwitch() + " is a deprecated" +
          " option. Ignoring.");
    }
  }

  private static void parsePreserveStatus(CommandLine command,
                                          DistCpOptions option) {
    if (command.hasOption(DistCpOptionSwitch.PRESERVE_STATUS.getSwitch())) {
      String attributes =
          getVal(command, DistCpOptionSwitch.PRESERVE_STATUS.getSwitch());
        }
      }
    }
  }

  private static void parseMaxMaps(CommandLine command,
                                   DistCpOptions option) {
    if (command.hasOption(DistCpOptionSwitch.MAX_MAPS.getSwitch())) {
      try {
        Integer maps = Integer.parseInt(
            getVal(command, DistCpOptionSwitch.MAX_MAPS.getSwitch()).trim());
        option.setMaxMaps(maps);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Number of maps is invalid: " +
            getVal(command, DistCpOptionSwitch.MAX_MAPS.getSwitch()), e);
      }
    }
  }

  private static void parseNumListStatusThreads(CommandLine command,
                                                DistCpOptions option) {
    if (command.hasOption(
        DistCpOptionSwitch.NUM_LISTSTATUS_THREADS.getSwitch())) {
      try {
        Integer numThreads = Integer.parseInt(getVal(command,
              DistCpOptionSwitch.NUM_LISTSTATUS_THREADS.getSwitch()).trim());
        option.setNumListstatusThreads(numThreads);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Number of liststatus threads is invalid: " + getVal(command,
                DistCpOptionSwitch.NUM_LISTSTATUS_THREADS.getSwitch()), e);
      }
    }
  }

  private static void parseBandwidth(CommandLine command,
                                     DistCpOptions option) {
    if (command.hasOption(DistCpOptionSwitch.BANDWIDTH.getSwitch())) {
      try {
        Integer mapBandwidth = Integer.parseInt(
            getVal(command, DistCpOptionSwitch.BANDWIDTH.getSwitch()).trim());
        if (mapBandwidth <= 0) {
          throw new IllegalArgumentException("Bandwidth specified is not " +
              "positive: " + mapBandwidth);
        }
        option.setMapBandwidth(mapBandwidth);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Bandwidth specified is invalid: " +
            getVal(command, DistCpOptionSwitch.BANDWIDTH.getSwitch()), e);
      }
    }
  }

  private static DistCpOptions parseSourceAndTargetPaths(
      CommandLine command) {
    DistCpOptions option;
    Path targetPath;
    List<Path> sourcePaths = new ArrayList<Path>();

    String[] leftOverArgs = command.getArgs();
    if (leftOverArgs == null || leftOverArgs.length < 1) {
      throw new IllegalArgumentException("Target path not specified");
    }

    targetPath = new Path(leftOverArgs[leftOverArgs.length - 1].trim());

    for (int index = 0; index < leftOverArgs.length - 1; index++) {
      sourcePaths.add(new Path(leftOverArgs[index].trim()));
    }

       paths in args.  If both are present, throw exception and bail */
    if (command.hasOption(
        DistCpOptionSwitch.SOURCE_FILE_LISTING.getSwitch())) {
      if (!sourcePaths.isEmpty()) {
        throw new IllegalArgumentException("Both source file listing and " +
            "source paths present");
      }
      option = new DistCpOptions(new Path(getVal(command, DistCpOptionSwitch.
              SOURCE_FILE_LISTING.getSwitch())), targetPath);
    } else {
      if (sourcePaths.isEmpty()) {
        throw new IllegalArgumentException("Neither source file listing nor " +
            "source paths present");
      }
      option = new DistCpOptions(sourcePaths, targetPath);
    }
    return option;
  }


hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/RegexCopyFilter.java
++ b/hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/RegexCopyFilter.java

package org.apache.hadoop.tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

public class RegexCopyFilter extends CopyFilter {

  private static final Log LOG = LogFactory.getLog(RegexCopyFilter.class);
  private File filtersFile;
  private List<Pattern> filters;

  protected RegexCopyFilter(String filtersFilename) {
    filtersFile = new File(filtersFilename);
    filters = new ArrayList<>();
  }

  @Override
  public void initialize() {
    BufferedReader reader = null;
    try {
      InputStream is = new FileInputStream(filtersFile);
      reader = new BufferedReader(new InputStreamReader(is,
          Charset.forName("UTF-8")));
      String line;
      while ((line = reader.readLine()) != null) {
        Pattern pattern = Pattern.compile(line);
        filters.add(pattern);
      }
    } catch (FileNotFoundException notFound) {
      LOG.error("Can't find filters file " + filtersFile);
    } catch (IOException cantRead) {
      LOG.error("An error occurred while attempting to read from " +
          filtersFile);
    } finally {
      IOUtils.cleanup(LOG, reader);
    }
  }

  @VisibleForTesting
  protected final void setFilters(List<Pattern> filtersList) {
    this.filters = filtersList;
  }

  @Override
  public boolean shouldCopy(Path path) {
    for (Pattern filter : filters) {
      if (filter.matcher(path.toString()).matches()) {
        return false;
      }
    }
    return true;
  }
}

hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/SimpleCopyListing.java
  private long totalBytesToCopy = 0;
  private int numListstatusThreads = 1;
  private final int maxRetries = 3;
  private CopyFilter copyFilter;

    numListstatusThreads = getConf().getInt(
        DistCpConstants.CONF_LABEL_LISTSTATUS_THREADS,
        DistCpConstants.DEFAULT_LISTSTATUS_THREADS);
    copyFilter = CopyFilter.getCopyFilter(getConf());
    copyFilter.initialize();
  }

  @VisibleForTesting
                  preserveXAttrs && sourceStatus.isDirectory(),
                  preserveRawXAttrs && sourceStatus.isDirectory());
            writeToFileListing(fileListWriter, sourceCopyListingStatus,
                sourcePathRoot);

            if (sourceStatus.isDirectory()) {
              if (LOG.isDebugEnabled()) {
  protected boolean shouldCopy(Path path) {
    return copyFilter.shouldCopy(path);
  }

                preserveXAttrs && child.isDirectory(),
                preserveRawXattrs && child.isDirectory());
            writeToFileListing(fileListWriter, childCopyListingStatus,
                 sourcePathRoot);
          }
          if (retry < maxRetries) {
            if (child.isDirectory()) {
      }      
      return;
    }
    writeToFileListing(fileListWriter, fileStatus, sourcePathRoot);
  }

  private void writeToFileListing(SequenceFile.Writer fileListWriter,
                                  CopyListingFileStatus fileStatus,
                                  Path sourcePathRoot) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("REL PATH: " + DistCpUtils.getRelativePath(sourcePathRoot,
        fileStatus.getPath()) + ", FULL PATH: " + fileStatus.getPath());
    }

    if (!shouldCopy(fileStatus.getPath())) {
      return;
    }

    fileListWriter.append(new Text(DistCpUtils.getRelativePath(sourcePathRoot,
        fileStatus.getPath())), fileStatus);
    fileListWriter.sync();

    if (!fileStatus.isDirectory()) {

hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/TrueCopyFilter.java
++ b/hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/TrueCopyFilter.java

package org.apache.hadoop.tools;

import org.apache.hadoop.fs.Path;

public class TrueCopyFilter extends CopyFilter {

  @Override
  public boolean shouldCopy(Path path) {
    return true;
  }
}

hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/package-info.java
++ b/hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/package-info.java


package org.apache.hadoop.tools;
\No newline at end of file

hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/TestCopyListing.java
    return 0;
  }

  @Test(timeout=10000)
  public void testMultipleSrcToFile() {
    FileSystem fs = null;

hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/TestIntegration.java
    }
  }

  @Test(timeout=100000)
  public void testMultiFileTargetMissing() {
    caseMultiFileTargetMissing(false);

hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/TestOptionsParser.java
    String val = "DistCpOptions{atomicCommit=false, syncFolder=false, deleteMissing=false, " +
        "ignoreFailures=false, maxMaps=20, sslConfigurationFile='null', copyStrategy='uniformsize', " +
        "sourceFileListing=abc, sourcePaths=null, targetPath=xyz, targetPathExists=true, " +
        "preserveRawXattrs=false, filtersFile='null'}";
    Assert.assertEquals(val, option.toString());
    Assert.assertNotSame(DistCpOptionSwitch.ATOMIC_COMMIT.toString(),
        DistCpOptionSwitch.ATOMIC_COMMIT.name());
          "Diff is valid only with update and delete options", e);
    }
  }

  @Test
  public void testExclusionsOption() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertNull(options.getFiltersFile());

    options = OptionsParser.parse(new String[] {
        "-filters",
        "/tmp/filters.txt",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertEquals(options.getFiltersFile(), "/tmp/filters.txt");
  }
}

hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/TestRegexCopyFilter.java
++ b/hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/TestRegexCopyFilter.java

package org.apache.hadoop.tools;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class TestRegexCopyFilter {

  @Test
  public void testShouldCopyTrue() {
    List<Pattern> filters = new ArrayList<>();
    filters.add(Pattern.compile("user"));

    RegexCopyFilter regexCopyFilter = new RegexCopyFilter("fakeFile");
    regexCopyFilter.setFilters(filters);

    Path shouldCopyPath = new Path("/user/bar");
    Assert.assertTrue(regexCopyFilter.shouldCopy(shouldCopyPath));
  }

  @Test
  public void testShouldCopyFalse() {
    List<Pattern> filters = new ArrayList<>();
    filters.add(Pattern.compile(".*test.*"));

    RegexCopyFilter regexCopyFilter = new RegexCopyFilter("fakeFile");
    regexCopyFilter.setFilters(filters);

    Path shouldNotCopyPath = new Path("/user/testing");
    Assert.assertFalse(regexCopyFilter.shouldCopy(shouldNotCopyPath));
  }

  @Test
  public void testShouldCopyWithMultipleFilters() {
    List<Pattern> filters = new ArrayList<>();
    filters.add(Pattern.compile(".*test.*"));
    filters.add(Pattern.compile("/user/b.*"));
    filters.add(Pattern.compile(".*_SUCCESS"));

    List<Path> toCopy = getTestPaths();

    int shouldCopyCount = 0;

    RegexCopyFilter regexCopyFilter = new RegexCopyFilter("fakeFile");
    regexCopyFilter.setFilters(filters);

    for (Path path: toCopy) {
      if (regexCopyFilter.shouldCopy(path)) {
        shouldCopyCount++;
      }
    }

    Assert.assertEquals(2, shouldCopyCount);
  }

  @Test
  public void testShouldExcludeAll() {
    List<Pattern> filters = new ArrayList<>();
    filters.add(Pattern.compile(".*test.*"));
    filters.add(Pattern.compile("/user/b.*"));
    filters.add(Pattern.compile(".*"));           // exclude everything

    List<Path> toCopy = getTestPaths();

    int shouldCopyCount = 0;

    RegexCopyFilter regexCopyFilter = new RegexCopyFilter("fakeFile");
    regexCopyFilter.setFilters(filters);

    for (Path path: toCopy) {
      if (regexCopyFilter.shouldCopy(path)) {
        shouldCopyCount++;
      }
    }

    Assert.assertEquals(0, shouldCopyCount);
  }

  private List<Path> getTestPaths() {
    List<Path> toCopy = new ArrayList<>();
    toCopy.add(new Path("/user/bar"));
    toCopy.add(new Path("/user/foo/_SUCCESS"));
    toCopy.add(new Path("/hive/test_data"));
    toCopy.add(new Path("test"));
    toCopy.add(new Path("/user/foo/bar"));
    toCopy.add(new Path("/mapred/.staging_job"));
    return toCopy;
  }

}

hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/TestTrueCopyFilter.java
++ b/hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/TestTrueCopyFilter.java

package org.apache.hadoop.tools;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class TestTrueCopyFilter {

  @Test
  public void testShouldCopy() {
    Assert.assertTrue(new TrueCopyFilter().shouldCopy(new Path("fake")));
  }

  @Test
  public void testShouldCopyWithNull() {
    Assert.assertTrue(new TrueCopyFilter().shouldCopy(new Path("fake")));
  }
}

