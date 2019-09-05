hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/HistoryFileManager.java
    }
  }

  @VisibleForTesting
  protected static List<FileStatus> scanDirectory(Path path, FileContext fc,
      PathFilter pathFilter) throws IOException {
    path = fc.makeQualified(path);
    List<FileStatus> jhStatusList = new ArrayList<FileStatus>();
    try {
      RemoteIterator<FileStatus> fileStatusIter = fc.listStatus(path);
      while (fileStatusIter.hasNext()) {
        FileStatus fileStatus = fileStatusIter.next();
          jhStatusList.add(fileStatus);
        }
      }
    } catch (FileNotFoundException fe) {
      LOG.error("Error while scanning directory " + path, fe);
    }
    return jhStatusList;
  }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/test/java/org/apache/hadoop/mapreduce/v2/hs/TestHistoryFileManager.java

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.util.UUID;
import java.util.List;

import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.junit.Test;
import org.junit.rules.TestName;

import static org.mockito.Mockito.*;

public class TestHistoryFileManager {
  private static MiniDFSCluster dfsCluster = null;
  private static MiniDFSCluster dfsCluster2 = null;
    testCreateHistoryDirs(dfsCluster.getConfiguration(0), clock);
  }

  @Test
  public void testScanDirectory() throws Exception {

    Path p = new Path("any");
    FileContext fc = mock(FileContext.class);
    when(fc.makeQualified(p)).thenReturn(p);
    when(fc.listStatus(p)).thenThrow(new FileNotFoundException());

    List<FileStatus> lfs = HistoryFileManager.scanDirectory(p, fc, null);

    Assert.assertNotNull(lfs);

  }

}

