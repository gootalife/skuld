hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/JobClient.java
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
  public static enum TaskStatusFilter { NONE, KILLED, FAILED, SUCCEEDED, ALL }
  private TaskStatusFilter taskOutputFilter = TaskStatusFilter.FAILED; 
  
  private int maxRetry = MRJobConfig.DEFAULT_MR_CLIENT_JOB_MAX_RETRIES;
  private long retryInterval =
      MRJobConfig.DEFAULT_MR_CLIENT_JOB_RETRY_INTERVAL;

  static{
    ConfigUtil.loadResources();
  }
    setConf(conf);
    cluster = new Cluster(conf);
    clientUgi = UserGroupInformation.getCurrentUser();

    maxRetry = conf.getInt(MRJobConfig.MR_CLIENT_JOB_MAX_RETRIES,
      MRJobConfig.DEFAULT_MR_CLIENT_JOB_MAX_RETRIES);

    retryInterval =
      conf.getLong(MRJobConfig.MR_CLIENT_JOB_RETRY_INTERVAL,
        MRJobConfig.DEFAULT_MR_CLIENT_JOB_RETRY_INTERVAL);

  }

      }
    });
  }

  protected RunningJob getJobInner(final JobID jobid) throws IOException {
    try {
      
      Job job = getJobUsingCluster(jobid);
      if (job != null) {
        JobStatus status = JobStatus.downgrade(job.getStatus());
        if (status != null) {
          return new NetworkedJob(status, cluster,
              new JobConf(job.getConfiguration()));
        } 
      }
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
    return null;
  }

  public RunningJob getJob(final JobID jobid) throws IOException {
     for (int i = 0;i <= maxRetry;i++) {
       if (i > 0) {
         try {
           Thread.sleep(retryInterval);
         } catch (Exception e) { }
       }
       RunningJob job = getJobInner(jobid);
       if (job != null) {
         return job;
       }
     }
     return null;
  }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/MRJobConfig.java
    MR_PREFIX + "client.max-retries";
  public static final int DEFAULT_MR_CLIENT_MAX_RETRIES = 3;
  
  public static final String MR_CLIENT_JOB_MAX_RETRIES =
      MR_PREFIX + "client.job.max-retries";
  public static final int DEFAULT_MR_CLIENT_JOB_MAX_RETRIES = 0;

  public static final String MR_CLIENT_JOB_RETRY_INTERVAL =
      MR_PREFIX + "client.job.retry-interval";
  public static final long DEFAULT_MR_CLIENT_JOB_RETRY_INTERVAL =
      2000;

  public static final String MR_AM_STAGING_DIR = 
    MR_AM_PREFIX+"staging-dir";

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapred/JobClientUnitTest.java

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Assert;
    void setCluster(Cluster cluster) {
      this.cluster = cluster;
    }

  }

  public class TestJobClientGetJob extends TestJobClient {

    int lastGetJobRetriesCounter = 0;
    int getJobRetriesCounter = 0;
    int getJobRetries = 0;
    RunningJob runningJob;

    TestJobClientGetJob(JobConf jobConf) throws IOException {
      super(jobConf);
    }

    public int getLastGetJobRetriesCounter() {
      return lastGetJobRetriesCounter;
    }

    public void setGetJobRetries(int getJobRetries) {
      this.getJobRetries = getJobRetries;
    }

    public void setRunningJob(RunningJob runningJob) {
      this.runningJob = runningJob;
    }

    protected RunningJob getJobInner(final JobID jobid) throws IOException {
      if (getJobRetriesCounter >= getJobRetries) {
        lastGetJobRetriesCounter = getJobRetriesCounter;
        getJobRetriesCounter = 0;
        return runningJob;
      }
      getJobRetriesCounter++;
      return null;
    }

  }

  @Test

    JobStatus mockJobStatus = mock(JobStatus.class);
    when(mockJobStatus.getJobID()).thenReturn(jobID);
    when(mockJobStatus.getJobName()).thenReturn(jobID.toString());
    when(mockJobStatus.getState()).thenReturn(JobStatus.State.RUNNING);
    when(mockJobStatus.getStartTime()).thenReturn(startTime);
    when(mockJobStatus.getUsername()).thenReturn("mockuser");
    assertNull(client.getJob(id));
  }

  @Test
  public void testGetJobRetry() throws Exception {

    JobConf conf = new JobConf();
    conf.set(MRJobConfig.MR_CLIENT_JOB_MAX_RETRIES, "3");

    TestJobClientGetJob client = new TestJobClientGetJob(conf);
    JobID id = new JobID("ajob",1);
    RunningJob rj = mock(RunningJob.class);
    client.setRunningJob(rj);

    assertNotNull(client.getJob(id));
    assertEquals(client.getLastGetJobRetriesCounter(), 0);

    client.setGetJobRetries(3);
    assertNotNull(client.getJob(id));
    assertEquals(client.getLastGetJobRetriesCounter(), 3);

    client.setGetJobRetries(5);
    assertNull(client.getJob(id));
  }

}

