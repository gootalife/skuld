hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/hdfs/NNBench.java
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;

      dataDirName = conf.get("test.nnbench.datadir.name");
      op = conf.get("test.nnbench.operation");
      readFile = conf.getBoolean("test.nnbench.readFileAfterOpen", false);
      int taskId =
          TaskAttemptID.forName(conf.get(MRJobConfig.TASK_ATTEMPT_ID))
              .getTaskID().getId();
      
      long totalTimeTPmS = 0l;
      long startTimeTPmS = 0l;
      successfulFileOps = 0l;
      
      if (barrier()) {
        String filePrefix = "file_" + taskId + "_";
        if (op.equals(OP_CREATE_WRITE)) {
          startTimeTPmS = System.currentTimeMillis();
          doCreateWriteOp(filePrefix, reporter);
        } else if (op.equals(OP_OPEN_READ)) {
          startTimeTPmS = System.currentTimeMillis();
          doOpenReadOp(filePrefix, reporter);
        } else if (op.equals(OP_RENAME)) {
          startTimeTPmS = System.currentTimeMillis();
          doRenameOp(filePrefix, reporter);
        } else if (op.equals(OP_DELETE)) {
          startTimeTPmS = System.currentTimeMillis();
          doDeleteOp(filePrefix, reporter);
        }
        
        endTimeTPms = System.currentTimeMillis();

