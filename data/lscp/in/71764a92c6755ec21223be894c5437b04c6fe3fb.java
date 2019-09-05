hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/ByteRangeInputStream.java

package org.apache.hadoop.hdfs.web;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
        final boolean resolved) throws IOException;
  }

  static class InputStreamAndFileLength {
    final Long length;
    final InputStream in;

    InputStreamAndFileLength(Long length, InputStream in) {
      this.length = length;
      this.in = in;
    }
  }

  enum StreamStatus {
    NORMAL, SEEK, CLOSED
  }
        if (in != null) {
          in.close();
        }
        InputStreamAndFileLength fin = openInputStream(startPos);
        in = fin.in;
        fileLength = fin.length;
        status = StreamStatus.NORMAL;
        break;
      case CLOSED:
  }

  @VisibleForTesting
  protected InputStreamAndFileLength openInputStream(long startOffset)
      throws IOException {
    final boolean resolved = resolvedURL.getURL() != null;
    final URLOpener opener = resolved? resolvedURL: originalURL;

    final HttpURLConnection connection = opener.connect(startOffset, resolved);
    resolvedURL.setURL(getResolvedUrl(connection));

    InputStream in = connection.getInputStream();
    final Long length;
    final Map<String, List<String>> headers = connection.getHeaderFields();
    if (isChunkedTransferEncoding(headers)) {
      length = null;
    } else {
      long streamlength = getStreamLength(connection, headers);
      length = startOffset + streamlength;

      in = new BoundedInputStream(in, streamlength);
    }

    return new InputStreamAndFileLength(length, in);
  }

  private static long getStreamLength(HttpURLConnection connection,
    }
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    try (InputStream in = openInputStream(position).in) {
      return in.read(buffer, offset, length);
    }
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    final InputStreamAndFileLength fin = openInputStream(position);
    if (fin.length != null && length + position > fin.length) {
      throw new EOFException("The length to read " + length
          + " exceeds the file length " + fin.length);
    }
    try {
      int nread = 0;
      while (nread < length) {
        int nbytes = fin.in.read(buffer, offset + nread, length - nread);
        if (nbytes < 0) {
          throw new EOFException("End of file reached before reading fully.");
        }
        nread += nbytes;
      }
    } finally {
      fin.in.close();
    }
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestByteRangeInputStream.java
import java.net.URL;

import com.google.common.net.HttpHeaders;
import org.apache.hadoop.hdfs.web.ByteRangeInputStream.InputStreamAndFileLength;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

public class TestByteRangeInputStream {
  public void testPropagatedClose() throws IOException {
    ByteRangeInputStream bris =
        mock(ByteRangeInputStream.class, CALLS_REAL_METHODS);
    InputStreamAndFileLength mockStream = new InputStreamAndFileLength(1L,
        mock(InputStream.class));
    doReturn(mockStream).when(bris).openInputStream(Mockito.anyLong());
    Whitebox.setInternalState(bris, "status",
                              ByteRangeInputStream.StreamStatus.SEEK);


    bris.getInputStream();
    verify(bris, times(++brisOpens)).openInputStream(Mockito.anyLong());
    verify(bris, times(brisCloses)).close();
    verify(mockStream.in, times(isCloses)).close();

    bris.getInputStream();
    verify(bris, times(brisOpens)).openInputStream(Mockito.anyLong());
    verify(bris, times(brisCloses)).close();
    verify(mockStream.in, times(isCloses)).close();

    bris.seek(1);
    bris.getInputStream();
    verify(bris, times(++brisOpens)).openInputStream(Mockito.anyLong());
    verify(bris, times(brisCloses)).close();
    verify(mockStream.in, times(++isCloses)).close();

    bris.getInputStream();
    verify(bris, times(brisOpens)).openInputStream(Mockito.anyLong());
    verify(bris, times(brisCloses)).close();
    verify(mockStream.in, times(isCloses)).close();

    bris.seek(1);
    bris.getInputStream();
    verify(bris, times(brisOpens)).openInputStream(Mockito.anyLong());
    verify(bris, times(brisCloses)).close();
    verify(mockStream.in, times(isCloses)).close();

    bris.close();
    verify(bris, times(++brisCloses)).close();
    verify(mockStream.in, times(++isCloses)).close();

    bris.close();
    verify(bris, times(++brisCloses)).close();
    verify(mockStream.in, times(isCloses)).close();

    boolean errored = false;
    } finally {
      assertTrue("Read a closed steam", errored);
    }
    verify(bris, times(brisOpens)).openInputStream(Mockito.anyLong());
    verify(bris, times(brisCloses)).close();

    verify(mockStream.in, times(isCloses)).close();
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/web/TestWebHDFS.java
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
    }
  }

  @Test
  public void testWebHdfsPread() throws Exception {
    final Configuration conf = WebHdfsTestUtil.createConf();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .build();
    byte[] content = new byte[1024];
    RANDOM.nextBytes(content);
    final Path foo = new Path("/foo");
    FSDataInputStream in = null;
    try {
      final WebHdfsFileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);
      try (OutputStream os = fs.create(foo)) {
        os.write(content);
      }

      in = fs.open(foo, 1024);
      byte[] buf = new byte[1024];
      try {
        in.readFully(1020, buf, 0, 5);
        Assert.fail("EOF expected");
      } catch (EOFException ignored) {}

      int length = in.read(buf, 0, 512);
      in.readFully(100, new byte[1024], 0, 100);
      int preadLen = in.read(200, new byte[1024], 0, 200);
      Assert.assertTrue(preadLen > 0);
      IOUtils.readFully(in, buf, length, 1024 - length);
      Assert.assertArrayEquals(content, buf);
    } finally {
      if (in != null) {
        in.close();
      }
      cluster.shutdown();
    }
  }

  @Test(timeout = 30000)
  public void testGetHomeDirectory() throws Exception {


