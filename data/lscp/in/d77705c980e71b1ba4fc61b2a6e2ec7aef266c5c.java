hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/ContentSummary.java
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
  private static final String QUOTA_SUMMARY_FORMAT = "%12s %15s ";
  private static final String SPACE_QUOTA_SUMMARY_FORMAT = "%15s %15s ";

  private static final String STORAGE_TYPE_SUMMARY_FORMAT = "%13s %17s ";

  private static final String[] HEADER_FIELDS = new String[] { "DIR_COUNT",
      "FILE_COUNT", "CONTENT_SIZE"};
  private static final String[] QUOTA_HEADER_FIELDS = new String[] { "QUOTA",
      (Object[]) QUOTA_HEADER_FIELDS) +
      HEADER;

  private static final String QUOTA_NONE = "none";
  private static final String QUOTA_INF = "inf";

    return qOption ? QUOTA_HEADER : HEADER;
  }

  public static String getStorageTypeHeader(List<StorageType> storageTypes) {
    StringBuffer header = new StringBuffer();

    for (StorageType st : storageTypes) {
      String storageName = st.toString();
      header.append(String.format(STORAGE_TYPE_SUMMARY_FORMAT, storageName + "_QUOTA",
          "REM_" + storageName + "_QUOTA"));
    }
    return header.toString();
  }

  public String toString(boolean qOption, boolean hOption) {
    return toString(qOption, hOption, false, null);
  }

  public String toString(boolean qOption, boolean hOption,
                         boolean tOption, List<StorageType> types) {
    String prefix = "";

    if (tOption) {
      StringBuffer content = new StringBuffer();
      for (StorageType st : types) {
        long typeQuota = getTypeQuota(st);
        long typeConsumed = getTypeConsumed(st);
        String quotaStr = QUOTA_NONE;
        String quotaRem = QUOTA_INF;

        if (typeQuota > 0) {
          quotaStr = formatSize(typeQuota, hOption);
          quotaRem = formatSize(typeQuota - typeConsumed, hOption);
        }

        content.append(String.format(STORAGE_TYPE_SUMMARY_FORMAT,
            quotaStr, quotaRem));
      }
      return content.toString();
    }

    if (qOption) {
      String quotaStr = QUOTA_NONE;
      String quotaRem = QUOTA_INF;
      String spaceQuotaStr = QUOTA_NONE;
      String spaceQuotaRem = QUOTA_INF;

      if (quota>0) {
        quotaStr = formatSize(quota, hOption);

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/shell/CommandFormat.java
public class CommandFormat {
  final int minPar, maxPar;
  final Map<String, Boolean> options = new HashMap<String, Boolean>();
  final Map<String, String> optionsWithValue = new HashMap<String, String>();
  boolean ignoreUnknownOpts = false;
  
    }
  }

  public void addOptionWithValue(String option) {
    if (options.containsKey(option)) {
      throw new DuplicatedOptionException(option);
    }
    optionsWithValue.put(option, null);
  }

      if (options.containsKey(opt)) {
        args.remove(pos);
        options.put(opt, Boolean.TRUE);
      } else if (optionsWithValue.containsKey(opt)) {
        args.remove(pos);
        if (pos < args.size() && (args.size() > minPar)) {
          arg = args.get(pos);
          args.remove(pos);
        } else {
          arg = "";
        }
        if (!arg.startsWith("-") || arg.equals("-")) {
          optionsWithValue.put(opt, arg);
        }
      } else if (ignoreUnknownOpts) {
        pos++;
      } else {
    return options.containsKey(option) ? options.get(option) : false;
  }

  public String getOptValue(String option) {
    return optionsWithValue.get(option);
  }

      return option;
    }
  }

  public static class DuplicatedOptionException extends IllegalArgumentException {
    private static final long serialVersionUID = 0L;

    public DuplicatedOptionException(String duplicatedOption) {
      super("option " + duplicatedOption + " already exsits!");
    }
  }
}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/shell/Count.java
package org.apache.hadoop.fs.shell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.StorageType;

  private static final String OPTION_QUOTA = "q";
  private static final String OPTION_HUMAN = "h";
  private static final String OPTION_HEADER = "v";
  private static final String OPTION_TYPE = "t";

  public static final String NAME = "count";
  public static final String USAGE =
      "[-" + OPTION_QUOTA + "] [-" + OPTION_HUMAN + "] [-" + OPTION_HEADER
          + "] [-" + OPTION_TYPE + " [<storage type>]] <path> ...";
  public static final String DESCRIPTION =
      "Count the number of directories, files and bytes under the paths\n" +
          "that match the specified file pattern.  The output columns are:\n" +
          " PATHNAME\n" +
          "The -" + OPTION_HUMAN +
          " option shows file sizes in human readable format.\n" +
          "The -" + OPTION_HEADER + " option displays a header line.\n" +
          "The -" + OPTION_TYPE + " option displays quota by storage types.\n" +
          "It must be used with -" + OPTION_QUOTA + " option.\n" +
          "If a comma-separated list of storage types is given after the -" +
          OPTION_TYPE + " option, \n" +
          "it displays the quota and usage for the specified types. \n" +
          "Otherwise, it displays the quota and usage for all the storage \n" +
          "types that support quota";

  private boolean showQuotas;
  private boolean humanReadable;
  private boolean showQuotabyType;
  private List<StorageType> storageTypes = null;

  public Count() {}
  protected void processOptions(LinkedList<String> args) {
    CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE,
        OPTION_QUOTA, OPTION_HUMAN, OPTION_HEADER);
    cf.addOptionWithValue(OPTION_TYPE);
    cf.parse(args);
    if (args.isEmpty()) { // default path is the current working directory
      args.add(".");
    }
    showQuotas = cf.getOpt(OPTION_QUOTA);
    humanReadable = cf.getOpt(OPTION_HUMAN);

    if (showQuotas) {
      String types = cf.getOptValue(OPTION_TYPE);

      if (null != types) {
        showQuotabyType = true;
        storageTypes = getAndCheckStorageTypes(types);
      } else {
        showQuotabyType = false;
      }
    }

    if (cf.getOpt(OPTION_HEADER)) {
      if (showQuotabyType) {
        out.println(ContentSummary.getStorageTypeHeader(storageTypes) + "PATHNAME");
      } else {
        out.println(ContentSummary.getHeader(showQuotas) + "PATHNAME");
      }
    }
  }

  private List<StorageType> getAndCheckStorageTypes(String types) {
    if ("".equals(types) || "all".equalsIgnoreCase(types)) {
      return StorageType.getTypesSupportingQuota();
    }

    String[] typeArray = StringUtils.split(types, ',');
    List<StorageType> stTypes = new ArrayList<>();

    for (String t : typeArray) {
      stTypes.add(StorageType.parseStorageType(t));
    }

    return stTypes;
  }

  @Override
  protected void processPath(PathData src) throws IOException {
    ContentSummary summary = src.fs.getContentSummary(src.path);
    out.println(summary.toString(showQuotas, isHumanReadable(),
        showQuotabyType, storageTypes) + src);
  }
  
  boolean isHumanReadable() {
    return humanReadable;
  }

  @InterfaceAudience.Private
  boolean isShowQuotabyType() {
    return showQuotabyType;
  }

  @InterfaceAudience.Private
  List<StorageType> getStorageTypes() {
    return storageTypes;
  }

}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/shell/TestCount.java
import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.shell.CommandFormat.NotEnoughArgumentsException;
import org.junit.Test;
import org.junit.Before;
    LinkedList<String> options = new LinkedList<String>();
    options.add("-q");
    options.add("-h");
    options.add("-t");
    options.add("SSD");
    options.add("dummy");
    Count count = new Count();
    count.processOptions(options);
    assertTrue(count.isShowQuotas());
    assertTrue(count.isHumanReadable());
    assertTrue(count.isShowQuotabyType());
    assertEquals(1, count.getStorageTypes().size());
    assertEquals(StorageType.SSD, count.getStorageTypes().get(0));

  }

    verify(out).println(HUMAN + NO_QUOTAS + path.toString());
  }

  @Test
  public void processPathWithQuotasByStorageTypesHeader() throws Exception {
    Path path = new Path("mockfs:/test");

    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStat);

    PrintStream out = mock(PrintStream.class);

    Count count = new Count();
    count.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-q");
    options.add("-v");
    options.add("-t");
    options.add("all");
    options.add("dummy");
    count.processOptions(options);
    String withStorageTypeHeader =
        "   DISK_QUOTA    REM_DISK_QUOTA     SSD_QUOTA     REM_SSD_QUOTA " +
        "ARCHIVE_QUOTA REM_ARCHIVE_QUOTA " +
        "PATHNAME";
    verify(out).println(withStorageTypeHeader);
    verifyNoMoreInteractions(out);
  }

  @Test
  public void processPathWithQuotasBySSDStorageTypesHeader() throws Exception {
    Path path = new Path("mockfs:/test");

    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStat);

    PrintStream out = mock(PrintStream.class);

    Count count = new Count();
    count.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-q");
    options.add("-v");
    options.add("-t");
    options.add("SSD");
    options.add("dummy");
    count.processOptions(options);
    String withStorageTypeHeader =
        "    SSD_QUOTA     REM_SSD_QUOTA " +
        "PATHNAME";
    verify(out).println(withStorageTypeHeader);
    verifyNoMoreInteractions(out);
  }

  @Test
  public void processPathWithQuotasByMultipleStorageTypesContent() throws Exception {
    Path path = new Path("mockfs:/test");

    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStat);
    PathData pathData = new PathData(path.toString(), conf);

    PrintStream out = mock(PrintStream.class);

    Count count = new Count();
    count.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-q");
    options.add("-t");
    options.add("SSD,DISK");
    options.add("dummy");
    count.processOptions(options);
    count.processPath(pathData);
    String withStorageType = BYTES + StorageType.SSD.toString()
        + " " + StorageType.DISK.toString() + " " + pathData.toString();
    verify(out).println(withStorageType);
    verifyNoMoreInteractions(out);
  }

  @Test
  public void processPathWithQuotasByMultipleStorageTypes() throws Exception {
    Path path = new Path("mockfs:/test");

    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStat);

    PrintStream out = mock(PrintStream.class);

    Count count = new Count();
    count.out = out;

    LinkedList<String> options = new LinkedList<String>();
    options.add("-q");
    options.add("-v");
    options.add("-t");
    options.add("SSD,DISK");
    options.add("dummy");
    count.processOptions(options);
    String withStorageTypeHeader =
        "    SSD_QUOTA     REM_SSD_QUOTA " +
        "   DISK_QUOTA    REM_DISK_QUOTA " +
        "PATHNAME";
    verify(out).println(withStorageTypeHeader);
    verifyNoMoreInteractions(out);
  }

  @Test
  public void getCommandName() {
    Count count = new Count();
  public void getUsage() {
    Count count = new Count();
    String actual = count.getUsage();
    String expected = "-count [-q] [-h] [-v] [-t [<storage type>]] <path> ...";
    assertEquals("Count.getUsage", expected, actual);
  }

        + "QUOTA REM_QUOTA SPACE_QUOTA REM_SPACE_QUOTA\n"
        + "      DIR_COUNT FILE_COUNT CONTENT_SIZE PATHNAME\n"
        + "The -h option shows file sizes in human readable format.\n"
        + "The -v option displays a header line.\n"
        + "The -t option displays quota by storage types.\n"
        + "It must be used with -q option.\n"
        + "If a comma-separated list of storage types is given after the -t option, \n"
        + "it displays the quota and usage for the specified types. \n"
        + "Otherwise, it displays the quota and usage for all the storage \n"
        + "types that support quota";

    assertEquals("Count.getDescription", expected, actual);
  }
    }

    @Override
    public String toString(boolean qOption, boolean hOption,
                           boolean tOption, List<StorageType> types) {
      if (tOption) {
        StringBuffer result = new StringBuffer();
        result.append(hOption ? HUMAN : BYTES);

        for (StorageType type : types) {
          result.append(type.toString());
          result.append(" ");
        }
        return result.toString();
      }

      if (qOption) {
        if (hOption) {
          return (HUMAN + WITH_QUOTAS);

