hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/jobhistory/TestEvents.java

public class TestEvents {

  private static final String taskId = "task_1_2_r_3";

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_KILLED));
    assertEquals(taskId,
        ((TaskAttemptUnsuccessfulCompletion) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_STARTED));
    assertEquals(taskId,
        ((TaskAttemptStarted) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_FINISHED));
    assertEquals(taskId,
        ((TaskAttemptFinished) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_KILLED));
    assertEquals(taskId,
        ((TaskAttemptUnsuccessfulCompletion) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_KILLED));
    assertEquals(taskId,
        ((TaskAttemptUnsuccessfulCompletion) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_STARTED));
    assertEquals(taskId,
        ((TaskAttemptStarted) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_FINISHED));
    assertEquals(taskId,
        ((TaskAttemptFinished) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_KILLED));
    assertEquals(taskId,
        ((TaskAttemptUnsuccessfulCompletion) e.getDatum()).taskid.toString());

    e = reader.getNextEvent();
    assertTrue(e.getEventType().equals(EventType.REDUCE_ATTEMPT_KILLED));
    assertEquals(taskId,
        ((TaskAttemptUnsuccessfulCompletion) e.getDatum()).taskid.toString());

    reader.close();
    datum.hostname = "hostname";
    datum.rackname = "rackname";
    datum.physMemKbytes = Arrays.asList(1000, 2000, 3000);
    datum.taskid = taskId;
    datum.port = 1000;
    datum.taskType = "REDUCE";
    datum.status = "STATUS";
    datum.hostname = "hostname";
    datum.rackname = "rackName";
    datum.state = "state";
    datum.taskid = taskId;
    datum.taskStatus = "taskStatus";
    datum.taskType = "REDUCE";
    result.setDatum(datum);
    datum.locality = "locality";
    datum.shufflePort = 10001;
    datum.startTime = 1;
    datum.taskid = taskId;
    datum.taskType = "taskType";
    datum.trackerName = "trackerName";
    result.setDatum(datum);
    datum.hostname = "hostname";
    datum.rackname = "rackname";
    datum.state = "state";
    datum.taskid = taskId;
    datum.taskStatus = "taskStatus";
    datum.taskType = "REDUCE";
    result.setDatum(datum);
    datum.locality = "locality";
    datum.shufflePort = 10001;
    datum.startTime = 1;
    datum.taskid = taskId;
    datum.taskType = "taskType";
    datum.trackerName = "trackerName";
    result.setDatum(datum);

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/webapp/TestAMWebServicesTasks.java
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringEqual("exception message",
            "java.lang.Exception: TaskId string : "
                + "bogustaskid is not properly formed"
                + "\nReason: java.util.regex.Matcher[pattern=" +
                TaskID.TASK_ID_REGEX + " region=0,11 lastmatch=]", message);
        WebServicesTestUtils.checkStringMatch("exception type",
            "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringEqual("exception message",
            "java.lang.Exception: TaskId string : "
                + "task_0_0000_d_000000 is not properly formed"
                + "\nReason: java.util.regex.Matcher[pattern=" +
                TaskID.TASK_ID_REGEX + " region=0,20 lastmatch=]", message);
        WebServicesTestUtils.checkStringMatch("exception type",
            "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringEqual("exception message",
            "java.lang.Exception: TaskId string : "
                + "task_0_m_000000 is not properly formed"
                + "\nReason: java.util.regex.Matcher[pattern=" +
                TaskID.TASK_ID_REGEX + " region=0,15 lastmatch=]", message);
        WebServicesTestUtils.checkStringMatch("exception type",
            "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringEqual("exception message",
            "java.lang.Exception: TaskId string : "
                + "task_0_0000_m is not properly formed"
                + "\nReason: java.util.regex.Matcher[pattern=" +
                TaskID.TASK_ID_REGEX + " region=0,13 lastmatch=]", message);
        WebServicesTestUtils.checkStringMatch("exception type",
            "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/webapp/TestAppController.java
  private AppControllerForTest appController;
  private RequestContext ctx;
  private Job job;
  private static final String taskId = "task_01_01_m_01";

  @Before
  public void setUp() throws IOException {

    appController = new AppControllerForTest(app, configuration, ctx);
    appController.getProperty().put(AMParams.JOB_ID, "job_01_01");
    appController.getProperty().put(AMParams.TASK_ID, taskId);

  }

        "Access denied: User user does not have permission to view job job_01_01missing task ID",
        appController.getData());

    appController.getProperty().put(AMParams.TASK_ID, taskId);
    appController.taskCounters();
    assertEquals(CountersPage.class, appController.getClazz());
  }
  public void testTask() {
 
    appController.task();
    assertEquals("Attempts for " + taskId ,
        appController.getProperty().get("title"));

    assertEquals(TaskPage.class, appController.getClazz());
        "Access denied: User user does not have permission to view job job_01_01",
        appController.getData());

    appController.getProperty().put(AMParams.TASK_ID, taskId);
    appController.attempts();
    assertEquals("Bad request: missing task-type.", appController.getProperty()
        .get("title"));

