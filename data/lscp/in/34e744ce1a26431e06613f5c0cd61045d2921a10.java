hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/AttemptsPage.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRApps.TaskAttemptStateUI;
import org.apache.hadoop.yarn.webapp.SubView;
      return true;
    }

    @Override
    protected String getAttemptId(TaskId taskId, TaskAttemptInfo ta) {
      return "<a href='" + url("task", taskId.toString()) +
          "'>" + ta.getId() + "</a>";
    }

    @Override
    protected Collection<TaskAttempt> getTaskAttempts() {
      List<TaskAttempt> fewTaskAttemps = new ArrayList<TaskAttempt>();

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/TaskPage.java
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;

        String nodeHttpAddr = ta.getNode();
        String diag = ta.getNote() == null ? "" : ta.getNote();
        TaskId taskId = attempt.getID().getTaskId();
        attemptsTableData.append("[\"")
        .append(getAttemptId(taskId, ta)).append("\",\"")
        .append(progress).append("\",\"")
        .append(ta.getState().toString()).append("\",\"")
        .append(StringEscapeUtils.escapeJavaScript(

    }

    protected String getAttemptId(TaskId taskId, TaskAttemptInfo ta) {
      return ta.getId();
    }

    protected boolean isValidRequest() {
      return app.getTask() != null;
    }
    .append("\n{'aTargets': [ 5 ]")
    .append(", 'bSearchable': false }")

    .append("\n, {'sType':'string', 'aTargets': [ 0 ]")
    .append(", 'mRender': parseHadoopID }")

    .append("\n, {'sType':'numeric', 'aTargets': [ 6, 7")
    .append(" ], 'mRender': renderHadoopDate }")


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/webapp/TestBlocks.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskAttemptIdPBImpl;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskIdPBImpl;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.AttemptsPage.FewAttemptsBlock;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.webapp.view.BlockForTest;
    assertTrue(data.toString().contains("100011"));
    assertFalse(data.toString().contains("Dummy Status \n*"));
    assertTrue(data.toString().contains("Dummy Status \\n*"));
  }

  @Test
  public void testAttemptsBlock() {
    AppContext ctx = mock(AppContext.class);
    AppForTest app = new AppForTest(ctx);

    JobId jobId = new JobIdPBImpl();
    jobId.setId(0);
    jobId.setAppId(ApplicationIdPBImpl.newInstance(0,1));

    TaskId taskId = new TaskIdPBImpl();
    taskId.setId(0);
    taskId.setTaskType(TaskType.REDUCE);
    taskId.setJobId(jobId);
    Task task = mock(Task.class);
    when(task.getID()).thenReturn(taskId);
    TaskReport report = mock(TaskReport.class);

    when(task.getReport()).thenReturn(report);
    when(task.getType()).thenReturn(TaskType.REDUCE);

    Map<TaskId, Task> tasks =
        new HashMap<TaskId, Task>();
    Map<TaskAttemptId, TaskAttempt> attempts =
        new HashMap<TaskAttemptId, TaskAttempt>();
    TaskAttempt attempt = mock(TaskAttempt.class);
    TaskAttemptId taId = new TaskAttemptIdPBImpl();
    taId.setId(0);
    taId.setTaskId(task.getID());
    when(attempt.getID()).thenReturn(taId);

    final TaskAttemptState taState = TaskAttemptState.SUCCEEDED;
    when(attempt.getState()).thenReturn(taState);
    TaskAttemptReport taReport = mock(TaskAttemptReport.class);
    when(taReport.getTaskAttemptState()).thenReturn(taState);
    when(attempt.getReport()).thenReturn(taReport);
    attempts.put(taId, attempt);
    tasks.put(taskId, task);
    when(task.getAttempts()).thenReturn(attempts);

    app.setTask(task);
    Job job = mock(Job.class);
    when(job.getTasks(TaskType.REDUCE)).thenReturn(tasks);
    app.setJob(job);

    AttemptsBlockForTest block = new AttemptsBlockForTest(app,
        new Configuration());
    block.addParameter(AMParams.TASK_TYPE, "r");
    block.addParameter(AMParams.ATTEMPT_STATE, "SUCCESSFUL");

    PrintWriter pWriter = new PrintWriter(data);
    Block html = new BlockForTest(new HtmlBlockForTest(), pWriter, 0, false);

    block.render(html);
    pWriter.flush();
    assertTrue(data.toString().contains(
        "<a href='" + block.url("task",task.getID().toString()) +"'>"
        +"attempt_0_0001_r_000000_0</a>"));
  }

  private class ConfBlockForTest extends ConfBlock {
    }
  }

  private class AttemptsBlockForTest extends FewAttemptsBlock {
    private final Map<String, String> params = new HashMap<String, String>();

    public void addParameter(String name, String value) {
      params.put(name, value);
    }

    public String $(String key, String defaultValue) {
      String value = params.get(key);
      return value == null ? defaultValue : value;
    }

    public AttemptsBlockForTest(App ctx, Configuration conf) {
      super(ctx, conf);
    }

    @Override
    public String url(String... parts) {
      String result = "url://";
      for (String string : parts) {
        result += string + ":";
      }
      return result;
    }
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/webapp/HsAttemptsPage.java
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRApps.TaskAttemptStateUI;
import org.apache.hadoop.yarn.webapp.SubView;
      return app.getJob() != null;
    }

    @Override
    protected String getAttemptId(TaskId taskId, TaskAttemptInfo ta) {
      return "<a href='" + url("task", taskId.toString()) +
          "'>" + ta.getId() + "</a>";
    }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/webapp/HsTaskPage.java
        }
        long attemptElapsed =
            Times.elapsed(attemptStartTime, attemptFinishTime, false);
        TaskId taskId = attempt.getID().getTaskId();

        attemptsTableData.append("[\"")
        .append(getAttemptId(taskId, ta)).append("\",\"")
        .append(ta.getState()).append("\",\"")
        .append(StringEscapeUtils.escapeJavaScript(
              StringEscapeUtils.escapeHtml(ta.getStatus()))).append("\",\"")
      footRow._()._()._();
    }

    protected String getAttemptId(TaskId taskId, TaskAttemptInfo ta) {
      return ta.getId();
    }

      .append(", 'bSearchable': false }")

      .append("\n, {'sType':'numeric', 'aTargets': [ 0 ]")
      .append(", 'mRender': parseHadoopID }")

      .append("\n, {'sType':'numeric', 'aTargets': [ 5, 6")

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/test/java/org/apache/hadoop/mapreduce/v2/hs/webapp/TestBlocks.java
    block.render(html);
    pWriter.flush();
    assertTrue(data.toString().contains("attempt_0_0001_r_000000_0"));
    assertTrue(data.toString().contains("SUCCEEDED"));
    assertFalse(data.toString().contains("Processed 128/128 records <p> \n"));
    assertTrue(data.toString().contains("Processed 128\\/128 records &lt;p&gt; \\n"));

