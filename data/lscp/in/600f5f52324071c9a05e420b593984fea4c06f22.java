hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/CommonConfigurationKeysPublic.java
    "hadoop.security.random.device.file.path";
  public static final String HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT = 
    "/dev/urandom";

  public static final String HADOOP_SHELL_MISSING_DEFAULT_FS_WARNING_KEY =
      "hadoop.shell.missing.defaultFs.warning";
  public static final boolean HADOOP_SHELL_MISSING_DEFAULT_FS_WARNING_DEFAULT =
      false;
}


hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/shell/FsCommand.java
package org.apache.hadoop.fs.shell;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.find.Find;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SHELL_MISSING_DEFAULT_FS_WARNING_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SHELL_MISSING_DEFAULT_FS_WARNING_KEY;


@InterfaceAudience.Private
  public int runAll() {
    return run(args);
  }

  @Override
  protected void processRawArguments(LinkedList<String> args)
      throws IOException {
    LinkedList<PathData> expendedArgs = expandArguments(args);
    final boolean displayWarnings = getConf().getBoolean(
        HADOOP_SHELL_MISSING_DEFAULT_FS_WARNING_KEY,
        HADOOP_SHELL_MISSING_DEFAULT_FS_WARNING_DEFAULT);
    if (displayWarnings) {
      final String defaultFs = getConf().get(FS_DEFAULT_NAME_KEY);
      final boolean missingDefaultFs =
          defaultFs == null || defaultFs.equals(FS_DEFAULT_NAME_DEFAULT);
      if (missingDefaultFs) {
        err.printf(
            "Warning: fs.defaultFs is not set when running \"%s\" command.%n",
            getCommandName());
      }
    }
    processArguments(expendedArgs);
  }
}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/shell/Ls.java
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.classification.InterfaceAudience;

  protected boolean humanReadable = false;

  protected Ls() {}

  protected Ls(Configuration conf) {
    super(conf);
  }

  protected String formatSize(long size) {
    return humanReadable
      ? StringUtils.TraditionalBinaryPrefix.long2String(size, "", 1)

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/shell/TestLs.java
package org.apache.hadoop.fs.shell;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SHELL_MISSING_DEFAULT_FS_WARNING_KEY;
import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
    verifyNoMoreInteractions(out);
  }

  private static void displayWarningOnLocalFileSystem(boolean shouldDisplay)
      throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(
        HADOOP_SHELL_MISSING_DEFAULT_FS_WARNING_KEY, shouldDisplay);

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(buf, true);
    Ls ls = new Ls(conf);
    ls.err = err;
    ls.run("file:///.");
    assertEquals(shouldDisplay, buf.toString().contains(
        "Warning: fs.defaultFs is not set when running \"ls\" command."));
  }

  @Test
  public void displayWarningsOnLocalFileSystem() throws IOException {
    displayWarningOnLocalFileSystem(true);
    displayWarningOnLocalFileSystem(false);
  }

  @Test
  public void isDeprecated() {

