hadoop-tools/hadoop-archives/src/main/java/org/apache/hadoop/tools/HadoopArchives.java
  static final String SRC_PARENT_LABEL = NAME + ".parent.path";
  static final String HAR_BLOCKSIZE_LABEL = NAME + ".block.size";
  static final String HAR_REPLICATION_LABEL = NAME + ".replication.factor";
  static final String HAR_PARTSIZE_LABEL = NAME + ".partfile.size";

  long partSize = 2 * 1024 * 1024 * 1024l;
  long blockSize = 512 * 1024 * 1024l;
  short repl = 3;

  private static final String usage = "archive"
  + " <-archiveName <NAME>.har> <-p <parent path>> [-r <replication factor>]" +
    conf.setLong(HAR_PARTSIZE_LABEL, partSize);
    conf.set(DST_HAR_LABEL, archiveName);
    conf.set(SRC_PARENT_LABEL, parentPath.makeQualified(fs).toString());
    conf.setInt(HAR_REPLICATION_LABEL, repl);
    Path outputPath = new Path(dest, archiveName);
    FileOutputFormat.setOutputPath(conf, outputPath);
    FileSystem outFs = outputPath.getFileSystem(conf);
    } finally {
      srcWriter.close();
    }
    conf.setInt(SRC_COUNT_LABEL, numFiles);
    conf.setLong(TOTAL_SIZE_LABEL, totalSize);
    int numMaps = (int)(totalSize/partSize);
    FileSystem destFs = null;
    byte[] buffer;
    int buf_size = 128 * 1024;
    private int replication = 3;
    long blockSize = 512 * 1024 * 1024l;

    public void configure(JobConf conf) {
      this.conf = conf;
      replication = conf.getInt(HAR_REPLICATION_LABEL, 3);
    public void close() throws IOException {
      partStream.close();
      destFs.setReplication(tmpOutput, (short) replication);
    }
  }
  
    private int numIndexes = 1000;
    private Path tmpOutputDir = null;
    private int written = 0;
    private int replication = 3;
    private int keyVal = 0;
    
      tmpOutputDir = FileOutputFormat.getWorkOutputPath(this.conf);
      masterIndex = new Path(tmpOutputDir, "_masterindex");
      index = new Path(tmpOutputDir, "_index");
      replication = conf.getInt(HAR_REPLICATION_LABEL, 3);
      try {
        fs = masterIndex.getFileSystem(conf);
        if (fs.exists(masterIndex)) {
      outStream.close();
      indexStream.close();
      fs.setReplication(index, (short) replication);
      fs.setReplication(masterIndex, (short) replication);
    }
    
  }

hadoop-tools/hadoop-archives/src/test/java/org/apache/hadoop/tools/TestHadoopArchives.java
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.JarFinder;
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".default."
        + CapacitySchedulerConfiguration.CAPACITY, "100");
    dfscluster =
        new MiniDFSCluster.Builder(conf).checkExitOnShutdown(true)
            .numDataNodes(3).format(true).racks(null).build();

    fs = dfscluster.getFileSystem();
    

    final String harName = "foo.har";
    final String fullHarPathStr = prefix + harName;
    final String[] args =
        { "-archiveName", harName, "-p", inputPathStr, "-r", "2", "*",
            archivePath.toString() };
    System.setProperty(HadoopArchives.TEST_HADOOP_ARCHIVES_JAR_PATH,
        HADOOP_ARCHIVES_JAR);
    final HadoopArchives har = new HadoopArchives(conf);
    assertEquals(0, ToolRunner.run(har, args));
    RemoteIterator<LocatedFileStatus> listFiles =
        fs.listFiles(new Path(archivePath.toString() + "/" + harName), false);
    while (listFiles.hasNext()) {
      LocatedFileStatus next = listFiles.next();
      if (!next.getPath().toString().endsWith("_SUCCESS")) {
        assertEquals(next.getPath().toString(), 2, next.getReplication());
      }
    }
    return fullHarPathStr;
  }
  

