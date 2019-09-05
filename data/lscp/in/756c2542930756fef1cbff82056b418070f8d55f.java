hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-common/src/main/java/org/apache/hadoop/mapred/LocalDistributedCacheManager.java
    Path[] archiveClassPaths = DistributedCache.getArchiveClassPaths(conf);
    if (archiveClassPaths != null) {
      for (Path p : archiveClassPaths) {
        classpaths.put(p.toUri().getPath().toString(), p);
      }
    }
    Path[] fileClassPaths = DistributedCache.getFileClassPaths(conf);
    if (fileClassPaths != null) {
      for (Path p : fileClassPaths) {
        classpaths.put(p.toUri().getPath().toString(), p);
      }
    }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/JobResourceUploader.java
        Path tmp = new Path(tmpjars);
        Path newPath = copyRemoteFiles(libjarsDir, tmp, conf, replication);
        DistributedCache.addFileToClassPath(
            new Path(newPath.toUri().getPath()), conf, jtFs);
      }
    }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapred/TestLocalJobSubmission.java
++ b/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapred/TestLocalJobSubmission.java
package org.apache.hadoop.mapred;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestLocalJobSubmission {
  private static Path TEST_ROOT_DIR =
      new Path(System.getProperty("test.build.data","/tmp"));

  @Before
  public void configure() throws Exception {
  }

  @After
  public void cleanup() {
  }

  @Test
  public void testLocalJobLibjarsOption() throws IOException {
    Path jarPath = makeJar(new Path(TEST_ROOT_DIR, "test.jar"));

    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://testcluster");
    final String[] args = {
        "-jt" , "local", "-libjars", jarPath.toString(),
        "-m", "1", "-r", "1", "-mt", "1", "-rt", "1"
    };
    int res = -1;
    try {
      res = ToolRunner.run(conf, new SleepJob(), args);
    } catch (Exception e) {
      System.out.println("Job failed with " + e.getLocalizedMessage());
      e.printStackTrace(System.out);
      fail("Job failed");
    }
    assertEquals("dist job res is not 0:", 0, res);
  }

  private Path makeJar(Path p) throws IOException {
    FileOutputStream fos = new FileOutputStream(new File(p.toString()));
    JarOutputStream jos = new JarOutputStream(fos);
    ZipEntry ze = new ZipEntry("test.jar.inside");
    jos.putNextEntry(ze);
    jos.write(("inside the jar!").getBytes());
    jos.closeEntry();
    jos.close();
    return p;
  }
}

