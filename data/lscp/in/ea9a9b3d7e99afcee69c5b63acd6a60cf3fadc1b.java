hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestFileCorruption.java
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileSystem;
      String bpid = cluster.getNamesystem().getBlockPoolId();
      File data_dir = MiniDFSCluster.getFinalizedDir(storageDir, bpid);
      assertTrue("data directory does not exist", data_dir.exists());
      Collection<File> blocks = FileUtils.listFiles(data_dir,
          new PrefixFileFilter(Block.BLOCK_FILE_PREFIX),
          DirectoryFileFilter.DIRECTORY);
      assertTrue("Blocks do not exist in data-dir", blocks.size() > 0);
      for (File block : blocks) {
        System.out.println("Deliberately removing file " + block.getName());
        assertTrue("Cannot remove file.", block.delete());
      }
      assertTrue("Corrupted replicas not handled properly.",
                 util.checkFiles(fs, "/srcdat"));

