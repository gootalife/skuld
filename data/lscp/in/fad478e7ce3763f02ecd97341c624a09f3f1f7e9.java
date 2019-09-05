hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/TestFsShellReturnCode.java

package org.apache.hadoop.fs;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SHELL_MISSING_DEFAULT_FS_WARNING_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
    
  }

  private static class FakeChown extends FsShellPermissions.Chown {
    public static String NAME = "chown";
    @Override
    protected void processArgument(PathData item) {
    }
  }

  @Test
  public void testChownUserAndGroupValidity() {
    testChownUserAndGroupValidity(true);
    testChownUserAndGroupValidity(false);
  }

  private void testChownUserAndGroupValidity(boolean enableWarning) {
    Configuration conf = new Configuration();
    conf.setBoolean(
        HADOOP_SHELL_MISSING_DEFAULT_FS_WARNING_KEY, enableWarning);
    FsCommand chown = new FakeChown();
    chown.setConf(conf);

    chown.run("user", "/path");
    assertIllegalArguments(chown, ":gr%oup", "/path");
  }

  private static class FakeChgrp extends FsShellPermissions.Chgrp {
    public static String NAME = "chgrp";
    @Override
    protected void processArgument(PathData item) {
    }
  }

  @Test
  public void testChgrpGroupValidity() {
    testChgrpGroupValidity(true);
    testChgrpGroupValidity(false);
  }

  private void testChgrpGroupValidity(boolean enableWarning) {
    Configuration conf = new Configuration();
    conf.setBoolean(
        HADOOP_SHELL_MISSING_DEFAULT_FS_WARNING_KEY, enableWarning);
    FsShellPermissions.Chgrp chgrp = new FakeChgrp();
    chgrp.setConf(conf);

    chgrp.run("group", "/path");

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/shell/TestMove.java
  }
    
  private static class InstrumentedRenameCommand extends MoveCommands.Rename {
    public static String NAME = "InstrumentedRename";
    private Exception error = null;
    @Override
    public void displayError(Exception e) {

