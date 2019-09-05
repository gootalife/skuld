hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  public static final boolean DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT = false;
  public static final String  DFS_NAMENODE_AUDIT_LOG_ASYNC_KEY = "dfs.namenode.audit.log.async";
  public static final boolean DFS_NAMENODE_AUDIT_LOG_ASYNC_DEFAULT = false;
  public static final String  DFS_NAMENODE_AUDIT_LOG_DEBUG_CMDLIST = "dfs.namenode.audit.log.debug.cmdlist";

  public static final String  DFS_BALANCER_MOVEDWINWIDTH_KEY = "dfs.balancer.movedWinWidth";
  public static final long    DFS_BALANCER_MOVEDWINWIDTH_DEFAULT = 5400*1000L;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
  @VisibleForTesting
  static class DefaultAuditLogger extends HdfsAuditLogger {

    private boolean logTokenTrackingId;
    private Set<String> debugCmdSet = new HashSet<String>();

    @Override
    public void initialize(Configuration conf) {
      logTokenTrackingId = conf.getBoolean(
          DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_KEY,
          DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT);

      debugCmdSet.addAll(Arrays.asList(conf.getTrimmedStrings(
          DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_DEBUG_CMDLIST)));
    }

    @Override
        InetAddress addr, String cmd, String src, String dst,
        FileStatus status, UserGroupInformation ugi,
        DelegationTokenSecretManager dtSecretManager) {

      if (auditLog.isDebugEnabled() ||
          (auditLog.isInfoEnabled() && !debugCmdSet.contains(cmd))) {
        final StringBuilder sb = auditBuffer.get();
        sb.setLength(0);
        sb.append("allowed=").append(succeeded).append("\t");

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestAuditLogAtDebug.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestAuditLogAtDebug.java

package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.DefaultAuditLogger;
import org.apache.log4j.Level;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.Inet4Address;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class TestAuditLogAtDebug {
  static final Log LOG = LogFactory.getLog(TestAuditLogAtDebug.class);

  @Rule
  public Timeout timeout = new Timeout(300000);
  
  private static final String DUMMY_COMMAND_1 = "dummycommand1";
  private static final String DUMMY_COMMAND_2 = "dummycommand2";
  
  private DefaultAuditLogger makeSpyLogger(
      Level level, Optional<List<String>> debugCommands) {
    DefaultAuditLogger logger = new DefaultAuditLogger();
    Configuration conf = new HdfsConfiguration();
    if (debugCommands.isPresent()) {
      conf.set(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_DEBUG_CMDLIST,
               Joiner.on(",").join(debugCommands.get()));
    }
    logger.initialize(conf);
    ((Log4JLogger) FSNamesystem.auditLog).getLogger().setLevel(level);
    return spy(logger);
  }
  
  private void logDummyCommandToAuditLog(HdfsAuditLogger logger, String command) {
    logger.logAuditEvent(true, "",
                         Inet4Address.getLoopbackAddress(),
                         command, "", "",
                         null, null, null);
  }

  @Test
  public void testDebugCommandNotLoggedAtInfo() {
    DefaultAuditLogger logger =
        makeSpyLogger(
            Level.INFO, Optional.of(Arrays.asList(DUMMY_COMMAND_1)));
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_1);
    verify(logger, never()).logAuditMessage(anyString());
  }

  @Test
  public void testDebugCommandLoggedAtDebug() {
    DefaultAuditLogger logger =
        makeSpyLogger(
            Level.DEBUG, Optional.of(Arrays.asList(DUMMY_COMMAND_1)));
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_1);
    verify(logger, times(1)).logAuditMessage(anyString());
  }
  
  @Test
  public void testInfoCommandLoggedAtInfo() {
    DefaultAuditLogger logger =
        makeSpyLogger(
            Level.INFO, Optional.of(Arrays.asList(DUMMY_COMMAND_1)));
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_2);
    verify(logger, times(1)).logAuditMessage(anyString());
  }

  @Test
  public void testMultipleDebugCommandsNotLoggedAtInfo() {
    DefaultAuditLogger logger =
        makeSpyLogger(
            Level.INFO,
            Optional.of(Arrays.asList(DUMMY_COMMAND_1, DUMMY_COMMAND_2)));
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_1);
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_2);
    verify(logger, never()).logAuditMessage(anyString());
  }

  @Test
  public void testMultipleDebugCommandsLoggedAtDebug() {
    DefaultAuditLogger logger =
        makeSpyLogger(
            Level.DEBUG,
            Optional.of(Arrays.asList(DUMMY_COMMAND_1, DUMMY_COMMAND_2)));
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_1);
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_2);
    verify(logger, times(2)).logAuditMessage(anyString());
  }
  
  @Test
  public void testEmptyDebugCommands() {
    DefaultAuditLogger logger = makeSpyLogger(
        Level.INFO, Optional.<List<String>>absent());
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_1);
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_2);
    verify(logger, times(2)).logAuditMessage(anyString());
  }
}

