hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NNUpgradeUtil.java
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import org.apache.commons.logging.Log;

    for (String s : fileNameList) {
      File prevFile = new File(tmpDir, s);
      File newFile = new File(curDir, prevFile.getName());
      Files.createLink(newFile.toPath(), prevFile.toPath());
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSUpgrade.java
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.namenode.TestParallelImageWrite;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.hadoop.hdfs.inotify.Event.CreateEvent;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;

    log("Normal NameNode upgrade", 1);
    File[] created =
        UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
    for (final File createdDir : created) {
      List<String> fileNameList =
          IOUtils.listDirectory(createdDir, EditLogsFilter.INSTANCE);
      for (String fileName : fileNameList) {
        String tmpFileName = fileName + ".tmp";
        File existingFile = new File(createdDir, fileName);
        File tmpFile = new File(createdDir, tmpFileName);
        Files.move(existingFile.toPath(), tmpFile.toPath());
        File newFile = new File(createdDir, fileName);
        Preconditions.checkState(newFile.createNewFile(),
            "Cannot create new edits log file in " + createdDir);
        EditLogFileInputStream in = new EditLogFileInputStream(tmpFile,
            HdfsServerConstants.INVALID_TXID, HdfsServerConstants.INVALID_TXID,
            false);
        EditLogFileOutputStream out = new EditLogFileOutputStream(conf, newFile,
            (int)tmpFile.length());
        out.create(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION + 1);
        FSEditLogOp logOp = in.readOp();
        while (logOp != null) {
          out.write(logOp);
          logOp = in.readOp();
        }
        out.setReadyToFlush();
        out.flushAndSync(true);
        out.close();
        Files.delete(tmpFile.toPath());
      }
    }

    cluster = createCluster();

    DFSInotifyEventInputStream ieis =
        cluster.getFileSystem().getInotifyEventStream(0);
    EventBatch batch = ieis.poll();
    Event[] events = batch.getEvents();
    assertTrue("Should be able to get transactions before the upgrade.",
        events.length > 0);
    assertEquals(events[0].getEventType(), Event.EventType.CREATE);
    assertEquals(((CreateEvent) events[0]).getPath(), "/TestUpgrade");
    cluster.shutdown();
    UpgradeUtilities.createEmptyDirs(nameNodeDirs);
  }

  private enum EditLogsFilter implements FilenameFilter {
    INSTANCE;

    @Override

