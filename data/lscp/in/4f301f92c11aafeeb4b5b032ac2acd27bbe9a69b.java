hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/FileInputFormat.java
@InterfaceAudience.Public
@InterfaceStability.Stable
  }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/LineRecordReader.java
        end = cIn.getAdjustedEnd();
        filePosition = cIn; // take pos from compressed stream
      } else {
        if (start != 0) {
          throw new IOException("Cannot seek in " +
              codec.getClass().getSimpleName() + " compressed stream");
        }

        in = new SplitLineReader(codec.createInputStream(fileIn,
            decompressor), job, recordDelimiter);
        filePosition = fileIn;

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/lib/input/FileInputFormat.java
@InterfaceAudience.Public
@InterfaceStability.Stable
  }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/lib/input/LineRecordReader.java
        end = cIn.getAdjustedEnd();
        filePosition = cIn;
      } else {
        if (start != 0) {
          throw new IOException("Cannot seek in " +
              codec.getClass().getSimpleName() + " compressed stream");
        }

        in = new SplitLineReader(codec.createInputStream(fileIn,
            decompressor), job, this.recordDelimiterBytes);
        filePosition = fileIn;

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/test/java/org/apache/hadoop/mapred/TestLineRecordReader.java
    testSplitRecords("blockEndingInCR.txt.bz2", 136494);
  }

  @Test(expected=IOException.class)
  public void testSafeguardSplittingUnsplittableFiles() throws IOException {
    testSplitRecords("TestSafeguardSplittingUnsplittableFiles.txt.gz", 2);
  }

  public ArrayList<String> readRecords(URL testFileUrl, int splitSize)
      throws IOException {

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/test/java/org/apache/hadoop/mapreduce/lib/input/TestLineRecordReader.java
    testSplitRecords("blockEndingInCRThenLF.txt.bz2", 136498);
  }

  @Test(expected=IOException.class)
  public void testSafeguardSplittingUnsplittableFiles() throws IOException {
    testSplitRecords("TestSafeguardSplittingUnsplittableFiles.txt.gz", 2);
  }

  public ArrayList<String> readRecords(URL testFileUrl, int splitSize)
      throws IOException {

