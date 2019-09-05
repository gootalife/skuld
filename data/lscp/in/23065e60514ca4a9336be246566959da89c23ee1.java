hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/test/MapredTestDriver.java
import org.apache.hadoop.util.ProgramDriver;

import org.apache.hadoop.hdfs.NNBench;
import org.apache.hadoop.hdfs.NNBenchWithoutMR;
import org.apache.hadoop.fs.TestFileSystem;
import org.apache.hadoop.fs.TestDFSIO;
import org.apache.hadoop.fs.DFSCIOTest;
      pgd.addClass("sleep", SleepJob.class, 
                   "A job that sleeps at each map and reduce task.");
      pgd.addClass("nnbench", NNBench.class, 
          "A benchmark that stresses the namenode w/ MR.");
      pgd.addClass("nnbenchWithoutMR", NNBenchWithoutMR.class,
          "A benchmark that stresses the namenode w/o MR.");
      pgd.addClass("testfilesystem", TestFileSystem.class, 
          "A test for FileSystem read/write.");
      pgd.addClass(TestDFSIO.class.getSimpleName(), TestDFSIO.class, 

