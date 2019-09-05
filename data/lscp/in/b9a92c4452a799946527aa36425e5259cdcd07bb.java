hadoop-tools/hadoop-archives/src/main/java/org/apache/hadoop/tools/HadoopArchives.java
    Path outputPath = new Path(dest, archiveName);
    FileOutputFormat.setOutputPath(conf, outputPath);
    FileSystem outFs = outputPath.getFileSystem(conf);
    if (outFs.exists(outputPath)) {
      throw new IOException("Archive path: "
          + outputPath.toString() + " already exists");
    }
    if (outFs.isFile(dest)) {
      throw new IOException("Destination " + dest.toString()
          + " should be a directory but is a file");
    }
    conf.set(DST_DIR_LABEL, outputPath.toString());
    Path stagingArea;
          Path argPath = new Path(args[i]);
          if (argPath.isAbsolute()) {
            System.out.println(usage);
            throw new IOException("Source path " + argPath +
                " is not relative to "+ parentPath);
          }
          srcPaths.add(new Path(parentPath, argPath));

hadoop-tools/hadoop-archives/src/test/java/org/apache/hadoop/tools/TestHadoopArchives.java
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
    Assert.assertEquals(originalPaths, harPaths);
  }

  @Test
  public void testOutputPathValidity() throws Exception {
    final String inputPathStr = inputPath.toUri().getPath();
    final URI uri = fs.getUri();
    final String harName = "foo.har";
    System.setProperty(HadoopArchives.TEST_HADOOP_ARCHIVES_JAR_PATH,
        HADOOP_ARCHIVES_JAR);
    final HadoopArchives har = new HadoopArchives(conf);

    PrintStream stderr = System.err;
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    PrintStream newErr = new PrintStream(byteStream);
    System.setErr(newErr);

    createFile(archivePath, fs, harName);
    final String[] args = { "-archiveName", harName, "-p", inputPathStr, "*",
        archivePath.toString() };
    Assert.assertEquals(-1, ToolRunner.run(har, args));
    String output = byteStream.toString();
    final Path outputPath = new Path(archivePath, harName);
    Assert.assertTrue(output.indexOf("Archive path: " + outputPath.toString()
        + " already exists") != -1);

    byteStream.reset();

    createFile(archivePath, fs, "sub1");
    final Path archivePath2 = new Path(archivePath, "sub1");
    final String[] args2 = { "-archiveName", harName, "-p", inputPathStr, "*",
        archivePath2.toString() };
    Assert.assertEquals(-1, ToolRunner.run(har, args2));
    output = byteStream.toString();
    Assert.assertTrue(output.indexOf("Destination " + archivePath2.toString()
        + " should be a directory but is a file") != -1);

    System.setErr(stderr);
  }

  @Test
  public void testPathWithSpaces() throws Exception {

