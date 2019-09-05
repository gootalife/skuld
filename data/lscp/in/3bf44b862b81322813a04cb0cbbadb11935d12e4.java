hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/LineReader.java
  public int readLine(Text str) throws IOException {
    return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
  }

  protected int getBufferPosn() {
    return bufferPosn;
  }

  protected int getBufferSize() {
    return bufferSize;
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/LineRecordReader.java
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.UncompressedSplitLineReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

      }
    } else {
      fileIn.seek(start);
      in = new UncompressedSplitLineReader(
          fileIn, job, recordDelimiter, split.getLength());
      filePosition = fileIn;
    }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/lib/input/LineRecordReader.java
      }
    } else {
      fileIn.seek(start);
      in = new UncompressedSplitLineReader(
          fileIn, job, this.recordDelimiterBytes, split.getLength());
      filePosition = fileIn;
    }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/lib/input/UncompressedSplitLineReader.java
++ b/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/lib/input/UncompressedSplitLineReader.java

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class UncompressedSplitLineReader extends SplitLineReader {
  private boolean needAdditionalRecord = false;
  private long splitLength;
  private long totalBytesRead = 0;
  private boolean finished = false;
  private boolean usingCRLF;
  private int unusedBytes = 0;
  private int lastBytesRead = 0;

  public UncompressedSplitLineReader(FSDataInputStream in, Configuration conf,
      byte[] recordDelimiterBytes, long splitLength) throws IOException {
    super(in, conf, recordDelimiterBytes);
    this.splitLength = splitLength;
    usingCRLF = (recordDelimiterBytes == null);
  }

  @Override
  protected int fillBuffer(InputStream in, byte[] buffer, boolean inDelimiter)
      throws IOException {
    int maxBytesToRead = buffer.length;
    if (totalBytesRead < splitLength) {
      maxBytesToRead = Math.min(maxBytesToRead,
                                (int)(splitLength - totalBytesRead));
    }
    int bytesRead = in.read(buffer, 0, maxBytesToRead);
    lastBytesRead = bytesRead;

    if (totalBytesRead == splitLength && inDelimiter && bytesRead > 0) {
      if (usingCRLF) {
        needAdditionalRecord = (buffer[0] != '\n');
      } else {
        needAdditionalRecord = true;
      }
    }
    if (bytesRead > 0) {
      totalBytesRead += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public int readLine(Text str, int maxLineLength, int maxBytesToConsume)
      throws IOException {
    long bytesRead = 0;
    if (!finished) {
      if (totalBytesRead > splitLength) {
        finished = true;
      }
      bytesRead = totalBytesRead;
      int bytesConsumed = super.readLine(str, maxLineLength, maxBytesToConsume);
      bytesRead = totalBytesRead - bytesRead;

      if (bytesConsumed == 0 && bytesRead == 0) {
        return 0;
      }

      int bufferSize = getBufferSize();

      if (lastBytesRead <= 0) {
        bytesRead += bufferSize;
      } else if (bytesRead > 0) {
        bytesRead += bufferSize - lastBytesRead;
      }

      bytesRead += unusedBytes;
      unusedBytes = bufferSize - getBufferPosn();
      bytesRead -= unusedBytes;
    }
    return (int) bytesRead;
  }

  @Override
  public boolean needAdditionalRecordAfterSplit() {
    return !finished && needAdditionalRecord;
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/test/java/org/apache/hadoop/mapred/TestLineRecordReader.java
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestLineRecordReader {
  private static Path workDir = new Path(new Path(System.getProperty(
      "test.build.data", "target"), "data"), "TestTextInputFormat");
  private static Path inputDir = new Path(workDir, "input");

  private void testSplitRecords(String testFileName, long firstSplitLength)
      throws IOException {
    long testFileSize = testFile.length();
    Path testFilePath = new Path(testFile.getAbsolutePath());
    Configuration conf = new Configuration();
    testSplitRecordsForFile(conf, firstSplitLength, testFileSize, testFilePath);
  }

  private void testSplitRecordsForFile(Configuration conf,
      long firstSplitLength, long testFileSize, Path testFilePath)
      throws IOException {
    conf.setInt(org.apache.hadoop.mapreduce.lib.input.
        LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
    assertTrue("unexpected test data at " + testFilePath,
        testFileSize > firstSplitLength);

    String delimiter = conf.get("textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    if (null != delimiter) {
      recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
    }
    FileSplit split = new FileSplit(testFilePath, 0, testFileSize,
        (String[])null);
    LineRecordReader reader = new LineRecordReader(conf, split,
        recordDelimiterBytes);
    LongWritable key = new LongWritable();
    Text value = new Text();
    int numRecordsNoSplits = 0;

    split = new FileSplit(testFilePath, 0, firstSplitLength, (String[])null);
    reader = new LineRecordReader(conf, split, recordDelimiterBytes);
    int numRecordsFirstSplit = 0;
    while (reader.next(key,  value)) {
      ++numRecordsFirstSplit;
    split = new FileSplit(testFilePath, firstSplitLength,
        testFileSize - firstSplitLength, (String[])null);
    reader = new LineRecordReader(conf, split, recordDelimiterBytes);
    int numRecordsRemainingSplits = 0;
    while (reader.next(key,  value)) {
      ++numRecordsRemainingSplits;
    }
    reader.close();

    assertEquals("Unexpected number of records in split",
        numRecordsNoSplits, numRecordsFirstSplit + numRecordsRemainingSplits);
  }

    }
    assertEquals(10, decompressors.size());
  }

  private Path createInputFile(Configuration conf, String data)
      throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Path file = new Path(inputDir, "test.txt");
    Writer writer = new OutputStreamWriter(localFs.create(file));
    try {
      writer.write(data);
    } finally {
      writer.close();
    }
    return file;
  }

  @Test
  public void testUncompressedInput() throws Exception {
    Configuration conf = new Configuration();
    String inputData = "abc+++def+++ghi+++"
        + "jkl+++mno+++pqr+++stu+++vw +++xyz";
    Path inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "+++");
    for(int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for(int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
  }

  @Test
  public void testUncompressedInputContainingCRLF() throws Exception {
    Configuration conf = new Configuration();
    String inputData = "a\r\nb\rc\nd\r\n";
    Path inputFile = createInputFile(conf, inputData);
    for(int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for(int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/test/java/org/apache/hadoop/mapreduce/lib/input/TestLineRecordReader.java
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CodecPool;
import org.junit.Test;

public class TestLineRecordReader {
  private static Path workDir = new Path(new Path(System.getProperty(
      "test.build.data", "target"), "data"), "TestTextInputFormat");
  private static Path inputDir = new Path(workDir, "input");

  private void testSplitRecords(String testFileName, long firstSplitLength)
      throws IOException {
    long testFileSize = testFile.length();
    Path testFilePath = new Path(testFile.getAbsolutePath());
    Configuration conf = new Configuration();
    testSplitRecordsForFile(conf, firstSplitLength, testFileSize, testFilePath);
  }

  private void testSplitRecordsForFile(Configuration conf,
      long firstSplitLength, long testFileSize, Path testFilePath)
      throws IOException {
    conf.setInt(org.apache.hadoop.mapreduce.lib.input.
        LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
    assertTrue("unexpected test data at " + testFilePath,
        testFileSize > firstSplitLength);

    String delimiter = conf.get("textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    if (null != delimiter) {
      recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
    }
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

    FileSplit split = new FileSplit(testFilePath, 0, testFileSize,
        (String[])null);
    LineRecordReader reader = new LineRecordReader(recordDelimiterBytes);
    reader.initialize(split, context);
    int numRecordsNoSplits = 0;
    while (reader.nextKeyValue()) {

    split = new FileSplit(testFilePath, 0, firstSplitLength, (String[])null);
    reader = new LineRecordReader(recordDelimiterBytes);
    reader.initialize(split, context);
    int numRecordsFirstSplit = 0;
    while (reader.nextKeyValue()) {
    split = new FileSplit(testFilePath, firstSplitLength,
        testFileSize - firstSplitLength, (String[])null);
    reader = new LineRecordReader(recordDelimiterBytes);
    reader.initialize(split, context);
    int numRecordsRemainingSplits = 0;
    while (reader.nextKeyValue()) {
      ++numRecordsRemainingSplits;
    }
    reader.close();
    assertEquals("Unexpected number of records in split ", numRecordsNoSplits,
        numRecordsFirstSplit + numRecordsRemainingSplits);
  }

  @Test
    }
    assertEquals(10, decompressors.size());
  }

  private Path createInputFile(Configuration conf, String data)
      throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Path file = new Path(inputDir, "test.txt");
    Writer writer = new OutputStreamWriter(localFs.create(file));
    try {
      writer.write(data);
    } finally {
      writer.close();
    }
    return file;
  }

  @Test
  public void testUncompressedInput() throws Exception {
    Configuration conf = new Configuration();
    String inputData = "abc+++def+++ghi+++"
        + "jkl+++mno+++pqr+++stu+++vw +++xyz";
    Path inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "+++");
    for(int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for(int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
  }

  @Test
  public void testUncompressedInputContainingCRLF() throws Exception {
    Configuration conf = new Configuration();
    String inputData = "a\r\nb\rc\nd\r\n";
    Path inputFile = createInputFile(conf, inputData);
    for(int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for(int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
  }
}

