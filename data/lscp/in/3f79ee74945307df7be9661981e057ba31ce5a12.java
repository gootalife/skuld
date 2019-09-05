hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileUtil.java
package org.apache.hadoop.fs;

import java.io.*;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
                        unexpandedWildcardClasspath.toString()};
    return jarCp;
  }

  public static boolean compareFs(FileSystem srcFs, FileSystem destFs) {
    if (srcFs==null || destFs==null) {
      return false;
    }
    URI srcUri = srcFs.getUri();
    URI dstUri = destFs.getUri();
    if (srcUri.getScheme()==null) {
      return false;
    }
    if (!srcUri.getScheme().equals(dstUri.getScheme())) {
      return false;
    }
    String srcHost = srcUri.getHost();
    String dstHost = dstUri.getHost();
    if ((srcHost!=null) && (dstHost!=null)) {
      if (srcHost.equals(dstHost)) {
        return srcUri.getPort()==dstUri.getPort();
      }
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName();
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName();
      } catch (UnknownHostException ue) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Could not compare file-systems. Unknown host: ", ue);
        }
        return false;
      }
      if (!srcHost.equals(dstHost)) {
        return false;
      }
    } else if (srcHost==null && dstHost!=null) {
      return false;
    } else if (srcHost!=null) {
      return false;
    }
    return srcUri.getPort()==dstUri.getPort();
  }
}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/TestFileUtil.java
package org.apache.hadoop.fs;

import org.junit.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.util.StringUtils;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarOutputStream;

import javax.print.attribute.URISyntax;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFileUtil {
  private static final Log LOG = LogFactory.getLog(TestFileUtil.class);
  private final File dir2 = new File(del, DIR + "2");
  private final File partitioned = new File(TEST_DIR, "partitioned");

  private InetAddress inet1;
  private InetAddress inet2;
  private InetAddress inet3;
  private InetAddress inet4;
  private InetAddress inet5;
  private InetAddress inet6;
  private URI uri1;
  private URI uri2;
  private URI uri3;
  private URI uri4;
  private URI uri5;
  private URI uri6;
  private FileSystem fs1;
  private FileSystem fs2;
  private FileSystem fs3;
  private FileSystem fs4;
  private FileSystem fs5;
  private FileSystem fs6;

  @Ignore
  private void setupDirs() throws IOException {
    Assert.assertFalse(del.exists());
    Assert.assertFalse(tmp.exists());
      }
    }
  }

  @Ignore
  public void setupCompareFs() {
    String host1 = "1.2.3.4";
    String host2 = "2.3.4.5";
    int port1 = 7000;
    int port2 = 7001;
    String uris1 = "hdfs://" + host1 + ":" + Integer.toString(port1) + "/tmp/foo";
    String uris2 = "hdfs://" + host1 + ":" + Integer.toString(port2) + "/tmp/foo";
    String uris3 = "hdfs://" + host2 + ":" + Integer.toString(port2) + "/tmp/foo";
    String uris4 = "hdfs://" + host2 + ":" + Integer.toString(port2) + "/tmp/foo";
    String uris5 = "file:///" + host1 + ":" + Integer.toString(port1) + "/tmp/foo";
    String uris6 = "hdfs:///" + host1 + "/tmp/foo";
    try {
      uri1 = new URI(uris1);
      uri2 = new URI(uris2);
      uri3 = new URI(uris3);
      uri4 = new URI(uris4);
      uri5 = new URI(uris5);
      uri6 = new URI(uris6);
    } catch (URISyntaxException use) {
    }
    inet1 = mock(InetAddress.class);
    when(inet1.getCanonicalHostName()).thenReturn(host1);
    inet2 = mock(InetAddress.class);
    when(inet2.getCanonicalHostName()).thenReturn(host1);
    inet3 = mock(InetAddress.class);
    when(inet3.getCanonicalHostName()).thenReturn(host2);
    inet4 = mock(InetAddress.class);
    when(inet4.getCanonicalHostName()).thenReturn(host2);
    inet5 = mock(InetAddress.class);
    when(inet5.getCanonicalHostName()).thenReturn(host1);
    inet6 = mock(InetAddress.class);
    when(inet6.getCanonicalHostName()).thenReturn(host1);

    try {
      when(InetAddress.getByName(uris1)).thenReturn(inet1);
      when(InetAddress.getByName(uris2)).thenReturn(inet2);
      when(InetAddress.getByName(uris3)).thenReturn(inet3);
      when(InetAddress.getByName(uris4)).thenReturn(inet4);
      when(InetAddress.getByName(uris5)).thenReturn(inet5);
    } catch (UnknownHostException ue) {
    }

    fs1 = mock(FileSystem.class);
    when(fs1.getUri()).thenReturn(uri1);
    fs2 = mock(FileSystem.class);
    when(fs2.getUri()).thenReturn(uri2);
    fs3 = mock(FileSystem.class);
    when(fs3.getUri()).thenReturn(uri3);
    fs4 = mock(FileSystem.class);
    when(fs4.getUri()).thenReturn(uri4);
    fs5 = mock(FileSystem.class);
    when(fs5.getUri()).thenReturn(uri5);
    fs6 = mock(FileSystem.class);
    when(fs6.getUri()).thenReturn(uri6);
  }

  @Test
  public void testCompareFsNull() throws Exception {
    setupCompareFs();
    assertEquals(FileUtil.compareFs(null,fs1),false);
    assertEquals(FileUtil.compareFs(fs1,null),false);
  }

  @Test
  public void testCompareFsDirectories() throws Exception {
    setupCompareFs();
    assertEquals(FileUtil.compareFs(fs1,fs1),true);
    assertEquals(FileUtil.compareFs(fs1,fs2),false);
    assertEquals(FileUtil.compareFs(fs1,fs5),false);
    assertEquals(FileUtil.compareFs(fs3,fs4),true);
    assertEquals(FileUtil.compareFs(fs1,fs6),false);
  }
}

hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/DistCp.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
      workDir = new Path(workDir, WIP_PREFIX + targetPath.getName()
                                + rand.nextInt());
      FileSystem workFS = workDir.getFileSystem(configuration);
      if (!FileUtil.compareFs(targetFS, workFS)) {
        throw new IllegalArgumentException("Work path " + workDir +
            " and target path " + targetPath + " are in different file system");
      }

hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/util/DistCpUtils.java
    return (sourceChecksum == null || targetChecksum == null ||
            sourceChecksum.equals(targetChecksum));
  }
}

