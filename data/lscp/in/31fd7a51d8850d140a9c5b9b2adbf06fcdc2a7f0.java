hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapred/TestLocalJobSubmission.java
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
    Path jarPath = makeJar(new Path(TEST_ROOT_DIR, "test.jar"));

    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost:9000");
    conf.set(MRConfig.FRAMEWORK_NAME, "local");
    final String[] args = {
        "-jt" , "local", "-libjars", jarPath.toString(),
        "-m", "1", "-r", "1", "-mt", "1", "-rt", "1"

