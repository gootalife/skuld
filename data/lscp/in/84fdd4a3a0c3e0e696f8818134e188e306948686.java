hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/common/Storage.java
      try {
        res = file.getChannel().tryLock();
        if (null == res) {
          LOG.error("Unable to acquire file lock on path " + lockF.toString());
          throw new OverlappingFileLockException();
        }
        file.write(jvmName.getBytes(Charsets.UTF_8));
  public void writeProperties(File to, StorageDirectory sd) throws IOException {
    Properties props = new Properties();
    setPropertiesFromFields(props, sd);
    writeProperties(to, props);
  }

  public static void writeProperties(File to, Properties props)
      throws IOException {
    try (RandomAccessFile file = new RandomAccessFile(to, "rws");
        FileOutputStream out = new FileOutputStream(file.getFD())) {
      file.seek(0);
      file.setLength(out.getChannel().position());
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataStorage.java
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DiskChecker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
public class DataStorage extends Storage {

  public final static String BLOCK_SUBDIR_PREFIX = "subdir";
  final static String STORAGE_DIR_DETACHED = "detach";
  public final static String STORAGE_DIR_RBW = "rbw";
  public final static String STORAGE_DIR_FINALIZED = "finalized";
  @Override
  public boolean isPreUpgradableLayout(StorageDirectory sd) throws IOException {
    File oldF = new File(sd.getRoot(), "storage");
    if (!oldF.exists()) {
      return false;
    }
    try (RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
      FileLock oldLock = oldFile.getChannel().tryLock()) {
      if (null == oldLock) {
        LOG.error("Unable to acquire file lock on path " + oldF.toString());
        throw new OverlappingFileLockException();
      }
      oldFile.seek(0);
      int oldVersion = oldFile.readInt();
      if (oldVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION) {
        return false;
      }
    }
    return true;
  }
      return;
    }
    if (!from.isDirectory()) {
      HardLink.createHardLink(from, to);
      hl.linkStats.countSingleLinks++;
      return;
    }
    String[] otherNames = from.list(new java.io.FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.startsWith(BLOCK_SUBDIR_PREFIX);
        }
      });
    for(int i = 0; i < otherNames.length; i++)

