hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/hdfs/NNBench.java
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;

      "This is not mandatory>\n" +
      "\t-replicationFactorPerFile <Replication factor for the files." +
        " default is 1. This is not mandatory>\n" +
      "\t-baseDir <base DFS path. default is /benchmarks/NNBench. " +
      "This is not mandatory>\n" +
      "\t-readFileAfterOpen <true or false. if true, it reads the file and " +
      "reports the average time to read. This is valid with the open_read " +

