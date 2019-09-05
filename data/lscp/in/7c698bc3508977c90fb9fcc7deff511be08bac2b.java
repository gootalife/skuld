hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/Globber.java
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.LogFactory;
        (flattenedPatterns.size() <= 1)) {
      return null;
    }
    FileStatus ret[] = results.toArray(new FileStatus[0]);
    Arrays.sort(ret);
    return ret;
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/fs/TestGlobPaths.java

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.UUID;
import java.util.regex.Pattern;

import com.google.common.collect.Ordering;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
  public void testNonTerminalGlobsOnFC() throws Exception {
    testOnFileContext(new TestNonTerminalGlobs(true));
  }

  @Test
  public void testLocalFilesystem() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    String localTmp = System.getProperty("java.io.tmpdir");
    Path base = new Path(new Path(localTmp), UUID.randomUUID().toString());
    Assert.assertTrue(fs.mkdirs(base));
    Assert.assertTrue(fs.mkdirs(new Path(base, "e")));
    Assert.assertTrue(fs.mkdirs(new Path(base, "c")));
    Assert.assertTrue(fs.mkdirs(new Path(base, "a")));
    Assert.assertTrue(fs.mkdirs(new Path(base, "d")));
    Assert.assertTrue(fs.mkdirs(new Path(base, "b")));
    fs.deleteOnExit(base);
    FileStatus[] status = fs.globStatus(new Path(base, "*"));
    ArrayList list = new ArrayList();
    for (FileStatus f: status) {
        list.add(f.getPath().toString());
    }
    boolean sorted = Ordering.natural().isOrdered(list);
    Assert.assertTrue(sorted);
  }
}


