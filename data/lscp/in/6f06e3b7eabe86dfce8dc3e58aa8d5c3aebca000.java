hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/TaskID.java
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
public class TaskID extends org.apache.hadoop.mapred.ID {
  protected static final String TASK = "task";
  protected static final NumberFormat idFormat = NumberFormat.getInstance();
  public static final String TASK_ID_REGEX = TASK + "_(\\d+)_(\\d+)_" +
      CharTaskTypeMaps.allTaskTypes + "_(\\d+)";
  public static final Pattern taskIdPattern = Pattern.compile(TASK_ID_REGEX);
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(6);
    throws IllegalArgumentException {
    if(str == null)
      return null;
    Matcher m = taskIdPattern.matcher(str);
    if (m.matches()) {
      return new org.apache.hadoop.mapred.TaskID(m.group(1),
          Integer.parseInt(m.group(2)),
          CharTaskTypeMaps.getTaskType(m.group(3).charAt(0)),
          Integer.parseInt(m.group(4)));
    }
    String exceptionMsg = "TaskId string : " + str + " is not properly formed" +
        "\nReason: " + m.toString();
    throw new IllegalArgumentException(exceptionMsg);
  }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/webapp/HsJobBlock.java
import java.util.Date;
import java.util.List;

import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
    if(diagnostics != null && !diagnostics.isEmpty()) {
      StringBuffer b = new StringBuffer();
      for(String diag: diagnostics) {
        b.append(addTaskLinks(diag));
      }
      infoBlock._("Diagnostics:", b.toString());
    }
       _().
     _();
  }

  static String addTaskLinks(String text) {
    return TaskID.taskIdPattern.matcher(text).replaceAll(
        "<a href=\"/jobhistory/task/$0\">$0</a>");
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/test/java/org/apache/hadoop/mapreduce/v2/hs/webapp/TestBlocks.java
import org.apache.hadoop.yarn.webapp.view.BlockForTest;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;
public class TestBlocks {
  private ByteArrayOutputStream data = new ByteArrayOutputStream();

  @Test
  public void testPullTaskLink(){
    Task task = getTask(0);
    String taskId = task.getID().toString();

    Assert.assertEquals("pull links doesn't work correctly",
        "Task failed <a href=\"/jobhistory/task/" + taskId + "\">" +
        taskId + "</a>"
        , HsJobBlock.addTaskLinks("Task failed " + taskId));

    Assert.assertEquals("pull links doesn't work correctly",
        "Task failed <a href=\"/jobhistory/task/" + taskId + "\">" +
        taskId + "</a>\n Job failed as tasks failed. failedMaps:1 failedReduces:0"
        , HsJobBlock.addTaskLinks("Task failed " + taskId + "\n " +
        "Job failed as tasks failed. failedMaps:1 failedReduces:0"));
  }

    assertEquals(HsAttemptsPage.class, controller.attemptsPage());

    controller.set(AMParams.JOB_ID, "job_01_01");
    controller.set(AMParams.TASK_ID, "task_01_01_m_01");
    controller.set(AMParams.TASK_TYPE, "m");
    controller.set(AMParams.ATTEMPT_STATE, "State");


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/test/java/org/apache/hadoop/mapreduce/v2/hs/webapp/TestHsWebServicesTasks.java
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
                + "task_0_0000_d_000000 is not properly formed" +
                "\nReason: java.util.regex.Matcher[pattern=" +
                TaskID.TASK_ID_REGEX + " region=0,20 lastmatch=]", message);
        WebServicesTestUtils.checkStringMatch("exception type",
            "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringEqual("exception message",
            "java.lang.Exception: TaskId string : "
                + "task_0000_m_000000 is not properly formed" +
                "\nReason: java.util.regex.Matcher[pattern=" +
                TaskID.TASK_ID_REGEX + " region=0,18 lastmatch=]", message);
        WebServicesTestUtils.checkStringMatch("exception type",
            "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringEqual("exception message",
            "java.lang.Exception: TaskId string : "
                + "task_0_0000_m is not properly formed" +
                "\nReason: java.util.regex.Matcher[pattern=" +
                TaskID.TASK_ID_REGEX + " region=0,13 lastmatch=]", message);
        WebServicesTestUtils.checkStringMatch("exception type",
            "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",

