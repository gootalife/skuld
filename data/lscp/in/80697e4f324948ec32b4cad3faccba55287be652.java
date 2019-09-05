hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/SequenceFile.java
    Metadata metadata = null;
    Compressor compressor = null;

    private boolean appendMode = false;

    protected Serializer keySerializer;
    protected Serializer uncompressedValSerializer;
    protected Serializer compressedValSerializer;
      }
    }

    static class AppendIfExistsOption extends Options.BooleanOption implements
        Option {
      AppendIfExistsOption(boolean value) {
        super(value);
      }
    }

    static class KeyClassOption extends Options.ClassOption implements Option {
      KeyClassOption(Class<?> value) {
        super(value);
      return new ReplicationOption(value);
    }
    
    public static Option appendIfExists(boolean value) {
      return new AppendIfExistsOption(value);
    }

    public static Option blockSize(long value) {
      return new BlockSizeOption(value);
    }
      ProgressableOption progressOption = 
        Options.getOption(ProgressableOption.class, opts);
      FileOption fileOption = Options.getOption(FileOption.class, opts);
      AppendIfExistsOption appendIfExistsOption = Options.getOption(
          AppendIfExistsOption.class, opts);
      FileSystemOption fsOption = Options.getOption(FileSystemOption.class, opts);
      StreamOption streamOption = Options.getOption(StreamOption.class, opts);
      KeyClassOption keyClassOption = 
          blockSizeOption.getValue();
        Progressable progress = progressOption == null ? null :
          progressOption.getValue();

        if (appendIfExistsOption != null && appendIfExistsOption.getValue()
            && fs.exists(p)) {

          SequenceFile.Reader reader = new SequenceFile.Reader(conf,
              SequenceFile.Reader.file(p), new Reader.OnlyHeaderOption());
          try {

            if (keyClassOption.getValue() != reader.getKeyClass()
                || valueClassOption.getValue() != reader.getValueClass()) {
              throw new IllegalArgumentException(
                  "Key/value class provided does not match the file");
            }

            if (reader.getVersion() != VERSION[3]) {
              throw new VersionMismatchException(VERSION[3],
                  reader.getVersion());
            }

            if (metadataOption != null) {
              LOG.info("MetaData Option is ignored during append");
            }
            metadataOption = (MetadataOption) SequenceFile.Writer
                .metadata(reader.getMetadata());

            CompressionOption readerCompressionOption = new CompressionOption(
                reader.getCompressionType(), reader.getCompressionCodec());

            if (readerCompressionOption.value != compressionTypeOption.value
                || !readerCompressionOption.codec.getClass().getName()
                    .equals(compressionTypeOption.codec.getClass().getName())) {
              throw new IllegalArgumentException(
                  "Compression option provided does not match the file");
            }

            sync = reader.getSync();

          } finally {
            reader.close();
          }

          out = fs.append(p, bufferSize, progress);
          this.appendMode = true;
        } else {
          out = fs
              .create(p, true, bufferSize, replication, blockSize, progress);
        }
      } else {
        out = streamOption.getValue();
      }
        }
        this.compressedValSerializer.open(deflateOut);
      }

      if (appendMode) {
        sync();
      } else {
        writeFileHeader();
      }
    }
    
    public Class getKeyClass() { return keyClass; }
    public CompressionCodec getCompressionCodec() { return codec; }
    
    private byte[] getSync() {
      return sync;
    }

    private byte getVersion() {
      return version;
    }


hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/io/TestSequenceFileAppend.java
++ b/hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/io/TestSequenceFileAppend.java

package org.apache.hadoop.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.serializer.JavaSerializationComparator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSequenceFileAppend {

  private static Configuration conf;
  private static FileSystem fs;
  private static Path ROOT_PATH = new Path(System.getProperty(
      "test.build.data", "build/test/data"));

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new Configuration();
    conf.set("io.serializations",
        "org.apache.hadoop.io.serializer.JavaSerialization");
    conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    fs = FileSystem.get(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    fs.close();
  }

  @Test(timeout = 30000)
  public void testAppend() throws Exception {

    Path file = new Path(ROOT_PATH, "testseqappend.seq");
    fs.delete(file, true);

    Text key1 = new Text("Key1");
    Text value1 = new Text("Value1");
    Text value2 = new Text("Updated");

    SequenceFile.Metadata metadata = new SequenceFile.Metadata();
    metadata.set(key1, value1);
    Writer.Option metadataOption = Writer.metadata(metadata);

    Writer writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class), metadataOption);

    writer.append(1L, "one");
    writer.append(2L, "two");
    writer.close();

    verify2Values(file);

    metadata.set(key1, value2);

    writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class),
        SequenceFile.Writer.appendIfExists(true), metadataOption);

    assertEquals(value1, writer.metadata.get(key1));

    writer.append(3L, "three");
    writer.append(4L, "four");

    writer.close();

    verifyAll4Values(file);

    Reader reader = new Reader(conf, Reader.file(file));
    assertEquals(value1, reader.getMetadata().get(key1));
    reader.close();

    try {
      Option wrongCompressOption = Writer.compression(CompressionType.RECORD,
          new GzipCodec());

      writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
          SequenceFile.Writer.keyClass(Long.class),
          SequenceFile.Writer.valueClass(String.class),
          SequenceFile.Writer.appendIfExists(true), wrongCompressOption);
      writer.close();
      fail("Expected IllegalArgumentException for compression options");
    } catch (IllegalArgumentException IAE) {
    }

    try {
      Option wrongCompressOption = Writer.compression(CompressionType.BLOCK,
          new DefaultCodec());

      writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
          SequenceFile.Writer.keyClass(Long.class),
          SequenceFile.Writer.valueClass(String.class),
          SequenceFile.Writer.appendIfExists(true), wrongCompressOption);
      writer.close();
      fail("Expected IllegalArgumentException for compression options");
    } catch (IllegalArgumentException IAE) {
    }

    fs.deleteOnExit(file);
  }

  @Test(timeout = 30000)
  public void testAppendRecordCompression() throws Exception {

    Path file = new Path(ROOT_PATH, "testseqappendblockcompr.seq");
    fs.delete(file, true);

    Option compressOption = Writer.compression(CompressionType.RECORD,
        new GzipCodec());
    Writer writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class), compressOption);

    writer.append(1L, "one");
    writer.append(2L, "two");
    writer.close();

    verify2Values(file);

    writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class),
        SequenceFile.Writer.appendIfExists(true), compressOption);

    writer.append(3L, "three");
    writer.append(4L, "four");
    writer.close();

    verifyAll4Values(file);

    fs.deleteOnExit(file);
  }

  @Test(timeout = 30000)
  public void testAppendBlockCompression() throws Exception {

    Path file = new Path(ROOT_PATH, "testseqappendblockcompr.seq");
    fs.delete(file, true);

    Option compressOption = Writer.compression(CompressionType.BLOCK,
        new GzipCodec());
    Writer writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class), compressOption);

    writer.append(1L, "one");
    writer.append(2L, "two");
    writer.close();

    verify2Values(file);

    writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class),
        SequenceFile.Writer.appendIfExists(true), compressOption);

    writer.append(3L, "three");
    writer.append(4L, "four");
    writer.close();

    verifyAll4Values(file);

    try {
      writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
          SequenceFile.Writer.keyClass(Long.class),
          SequenceFile.Writer.valueClass(String.class),
          SequenceFile.Writer.appendIfExists(true));
      writer.close();
      fail("Expected IllegalArgumentException for compression options");
    } catch (IllegalArgumentException IAE) {
    }

    try {
      Option wrongCompressOption = Writer.compression(CompressionType.RECORD,
          new GzipCodec());

      writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
          SequenceFile.Writer.keyClass(Long.class),
          SequenceFile.Writer.valueClass(String.class),
          SequenceFile.Writer.appendIfExists(true), wrongCompressOption);
      writer.close();
      fail("Expected IllegalArgumentException for compression options");
    } catch (IllegalArgumentException IAE) {
    }

    try {
      Option wrongCompressOption = Writer.compression(CompressionType.BLOCK,
          new DefaultCodec());

      writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
          SequenceFile.Writer.keyClass(Long.class),
          SequenceFile.Writer.valueClass(String.class),
          SequenceFile.Writer.appendIfExists(true), wrongCompressOption);
      writer.close();
      fail("Expected IllegalArgumentException for compression options");
    } catch (IllegalArgumentException IAE) {
    }

    fs.deleteOnExit(file);
  }

  @Test(timeout = 30000)
  public void testAppendSort() throws Exception {
    Path file = new Path(ROOT_PATH, "testseqappendSort.seq");
    fs.delete(file, true);

    Path sortedFile = new Path(ROOT_PATH, "testseqappendSort.seq.sort");
    fs.delete(sortedFile, true);

    SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs,
        new JavaSerializationComparator<Long>(), Long.class, String.class, conf);

    Option compressOption = Writer.compression(CompressionType.BLOCK,
        new GzipCodec());
    Writer writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class), compressOption);

    writer.append(2L, "two");
    writer.append(1L, "one");

    writer.close();

    writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class),
        SequenceFile.Writer.appendIfExists(true), compressOption);

    writer.append(4L, "four");
    writer.append(3L, "three");
    writer.close();

    sorter.sort(file, sortedFile);
    verifyAll4Values(sortedFile);

    fs.deleteOnExit(file);
    fs.deleteOnExit(sortedFile);
  }

  private void verify2Values(Path file) throws IOException {
    Reader reader = new Reader(conf, Reader.file(file));
    assertEquals(1L, reader.next((Object) null));
    assertEquals("one", reader.getCurrentValue((Object) null));
    assertEquals(2L, reader.next((Object) null));
    assertEquals("two", reader.getCurrentValue((Object) null));
    assertNull(reader.next((Object) null));
    reader.close();
  }

  private void verifyAll4Values(Path file) throws IOException {
    Reader reader = new Reader(conf, Reader.file(file));
    assertEquals(1L, reader.next((Object) null));
    assertEquals("one", reader.getCurrentValue((Object) null));
    assertEquals(2L, reader.next((Object) null));
    assertEquals("two", reader.getCurrentValue((Object) null));
    assertEquals(3L, reader.next((Object) null));
    assertEquals("three", reader.getCurrentValue((Object) null));
    assertEquals(4L, reader.next((Object) null));
    assertEquals("four", reader.getCurrentValue((Object) null));
    assertNull(reader.next((Object) null));
    reader.close();
  }
}

