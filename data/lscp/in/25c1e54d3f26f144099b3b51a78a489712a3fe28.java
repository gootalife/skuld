hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/task/reduce/IFileWrappedMapOutput.java
++ b/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/task/reduce/IFileWrappedMapOutput.java
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public abstract class IFileWrappedMapOutput<K, V> extends MapOutput<K, V> {
  private final Configuration conf;
  private final MergeManagerImpl<K, V> merger;

  public IFileWrappedMapOutput(
      Configuration c, MergeManagerImpl<K, V> m, TaskAttemptID mapId,
      long size, boolean primaryMapOutput) {
    super(mapId, size, primaryMapOutput);
    conf = c;
    merger = m;
  }

  protected MergeManagerImpl<K, V> getMerger() {
    return merger;
  }

  protected abstract void doShuffle(
      MapHost host, IFileInputStream iFileInputStream,
      long compressedLength, long decompressedLength,
      ShuffleClientMetrics metrics, Reporter reporter) throws IOException;

  @Override
  public void shuffle(MapHost host, InputStream input,
                      long compressedLength, long decompressedLength,
                      ShuffleClientMetrics metrics,
                      Reporter reporter) throws IOException {
    IFileInputStream iFin =
        new IFileInputStream(input, compressedLength, conf);
    try {
      this.doShuffle(host, iFin, compressedLength,
                    decompressedLength, metrics, reporter);
    } finally {
      iFin.close();
    }
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/task/reduce/InMemoryMapOutput.java

@InterfaceAudience.Private
@InterfaceStability.Unstable
class InMemoryMapOutput<K, V> extends IFileWrappedMapOutput<K, V> {
  private static final Log LOG = LogFactory.getLog(InMemoryMapOutput.class);
  private final byte[] memory;
  private BoundedByteArrayOutputStream byteStream;
                           MergeManagerImpl<K, V> merger,
                           int size, CompressionCodec codec,
                           boolean primaryMapOutput) {
    super(conf, merger, mapId, (long)size, primaryMapOutput);
    this.codec = codec;
    byteStream = new BoundedByteArrayOutputStream(size);
    memory = byteStream.getBuffer();
  }

  @Override
  protected void doShuffle(MapHost host, IFileInputStream iFin,
                      long compressedLength, long decompressedLength,
                      ShuffleClientMetrics metrics,
                      Reporter reporter) throws IOException {
    InputStream input = iFin;

    if (codec != null) {
        throw new IOException("Unexpected extra bytes from input stream for " +
                               getMapId());
      }
    } finally {
      CodecPool.returnDecompressor(decompressor);
    }

  @Override
  public void commit() throws IOException {
    getMerger().closeInMemoryFile(this);
  }
  
  @Override
  public void abort() {
    getMerger().unreserve(memory.length);
  }

  @Override

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/task/reduce/MergeManagerImpl.java
      LOG.info(mapId + ": Shuffling to disk since " + requestedSize + 
               " is greater than maxSingleShuffleLimit (" + 
               maxSingleShuffleLimit + ")");
      return new OnDiskMapOutput<K,V>(mapId, this, requestedSize, jobConf,
         fetcher, true, FileSystem.getLocal(jobConf).getRaw(),
         mapOutputFile.getInputFileForWrite(mapId.getTaskID(), requestedSize));
    }
    

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/task/reduce/OnDiskMapOutput.java
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


@InterfaceAudience.Private
@InterfaceStability.Unstable
class OnDiskMapOutput<K, V> extends IFileWrappedMapOutput<K, V> {
  private static final Log LOG = LogFactory.getLog(OnDiskMapOutput.class);
  private final FileSystem fs;
  private final Path tmpOutputPath;
  private final Path outputPath;
  private final OutputStream disk; 
  private long compressedSize;

  @Deprecated
  public OnDiskMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId,
                         MergeManagerImpl<K,V> merger, long size,
                         JobConf conf,
                         MapOutputFile mapOutputFile,
                         int fetcher, boolean primaryMapOutput)
      throws IOException {
    this(mapId, merger, size, conf, fetcher,
        primaryMapOutput, FileSystem.getLocal(conf).getRaw(),
        mapOutputFile.getInputFileForWrite(mapId.getTaskID(), size));
  }

  @Deprecated
  OnDiskMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId,
                         MergeManagerImpl<K,V> merger, long size,
                         JobConf conf,
                         MapOutputFile mapOutputFile,
                         int fetcher, boolean primaryMapOutput,
                         FileSystem fs, Path outputPath) throws IOException {
    this(mapId, merger, size, conf, fetcher, primaryMapOutput, fs, outputPath);
  }

  OnDiskMapOutput(TaskAttemptID mapId,
                  MergeManagerImpl<K, V> merger, long size,
                  JobConf conf,
                  int fetcher, boolean primaryMapOutput,
                  FileSystem fs, Path outputPath) throws IOException {
    super(conf, merger, mapId, size, primaryMapOutput);
    this.fs = fs;
    this.outputPath = outputPath;
    tmpOutputPath = getTempPath(outputPath, fetcher);
    disk = CryptoUtils.wrapIfNecessary(conf, fs.create(tmpOutputPath));
  }

  @VisibleForTesting
  }

  @Override
  protected void doShuffle(MapHost host, IFileInputStream input,
                      long compressedLength, long decompressedLength,
                      ShuffleClientMetrics metrics,
                      Reporter reporter) throws IOException {
    long bytesLeft = compressedLength;
    try {
      final int BYTES_TO_READ = 64 * 1024;
      byte[] buf = new byte[BYTES_TO_READ];
      while (bytesLeft > 0) {
        int n = input.readWithChecksum(buf, 0,
                                      (int) Math.min(bytesLeft, BYTES_TO_READ));
        if (n < 0) {
          throw new IOException("read past end of stream reading " + 
                                getMapId());
      disk.close();
    } catch (IOException ioe) {
      IOUtils.cleanup(LOG, disk);

      throw ioe;
    fs.rename(tmpOutputPath, outputPath);
    CompressAwarePath compressAwarePath = new CompressAwarePath(outputPath,
        getSize(), this.compressedSize);
    getMerger().closeOnDiskFile(compressAwarePath);
  }
  
  @Override

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/test/java/org/apache/hadoop/mapreduce/task/reduce/TestFetcher.java
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.nimbusds.jose.util.StringUtils;

    ByteArrayInputStream in = new ByteArrayInputStream(bout.toByteArray());
    when(connection.getInputStream()).thenReturn(in);
    IFileWrappedMapOutput<Text,Text> mapOut = new InMemoryMapOutput<Text, Text>(
        job, map1ID, mm, 8, null, true );
    IFileWrappedMapOutput<Text,Text> mapOut2 = new InMemoryMapOutput<Text, Text>(
        job, map2ID, mm, 10, null, true );

    when(mm.reserve(eq(map1ID), anyLong(), anyInt())).thenReturn(mapOut);
    Path shuffledToDisk =
        OnDiskMapOutput.getTempPath(onDiskMapOutputPath, fetcher);
    fs = FileSystem.getLocal(job).getRaw();
    IFileWrappedMapOutput<Text,Text> odmo =
        new OnDiskMapOutput<Text,Text>(map1ID, mm, 100L, job, fetcher, true,
                                       fs, onDiskMapOutputPath);

    String mapData = "MAPDATA12345678901234567890";

  @Test(timeout=10000)
  public void testInterruptInMemory() throws Exception {
    final int FETCHER = 2;
    IFileWrappedMapOutput<Text,Text> immo = spy(new InMemoryMapOutput<Text,Text>(
          job, id, mm, 100, null, true));
    when(mm.reserve(any(TaskAttemptID.class), anyLong(), anyInt()))
        .thenReturn(immo);
    Path p = new Path("file:///tmp/foo");
    Path pTmp = OnDiskMapOutput.getTempPath(p, FETCHER);
    FileSystem mFs = mock(FileSystem.class, RETURNS_DEEP_STUBS);
    IFileWrappedMapOutput<Text,Text> odmo =
        spy(new OnDiskMapOutput<Text,Text>(map1ID, mm, 100L, job,
                                           FETCHER, true, mFs, p));
    when(mm.reserve(any(TaskAttemptID.class), anyLong(), anyInt()))
        .thenReturn(odmo);
    doNothing().when(mm).waitForResource();

