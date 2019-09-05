hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/task/reduce/Fetcher.java
        try {
          failedTasks = copyMapOutput(host, input, remaining, fetchRetryEnabled);
        } catch (IOException e) {
          IOUtils.cleanup(LOG, input);
          connection.disconnect();

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/task/reduce/IFileWrappedMapOutput.java
                      long compressedLength, long decompressedLength,
                      ShuffleClientMetrics metrics,
                      Reporter reporter) throws IOException {
    doShuffle(host, new IFileInputStream(input, compressedLength, conf),
        compressedLength, decompressedLength, metrics, reporter);
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/task/reduce/LocalFetcher.java
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.IndexRecord;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
    FileSystem localFs = FileSystem.getLocal(job).getRaw();
    FSDataInputStream inStream = localFs.open(mapOutputFileName);
    try {
      inStream = CryptoUtils.wrapIfNecessary(job, inStream);
      inStream.seek(ir.startOffset + CryptoUtils.cryptoPadding(job));
      mapOutput.shuffle(LOCALHOST, inStream, compressedLength,
          decompressedLength, metrics, reporter);
    } finally {
      IOUtils.cleanup(LOG, inStream);
    }

    scheduler.copySucceeded(mapTaskId, LOCALHOST, compressedLength, 0, 0,

