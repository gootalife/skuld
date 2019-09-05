hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/jobhistory/JobHistoryEventHandler.java
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.google.common.annotations.VisibleForTesting;
          if (!isTimerShutDown) {
            flushTimerTask = new FlushTimerTask(this);
            flushTimer.schedule(flushTimerTask, flushTimeout);
            isTimerActive = true;
          }
        }
      }
    }
    return JobState.KILLED.toString();
  }

  @VisibleForTesting
  boolean getFlushTimerStatus() {
    return isTimerActive;
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/jobhistory/TestJobHistoryEventHandler.java
      }

      handleNextNEvents(jheh, 9);
      Assert.assertTrue(jheh.getFlushTimerStatus());
      verify(mockWriter, times(0)).flush();

      Thread.sleep(2 * 4 * 1000l); // 4 seconds should be enough. Just be safe.
      verify(mockWriter).flush();
      Assert.assertFalse(jheh.getFlushTimerStatus());
    } finally {
      jheh.stop();
      verify(mockWriter).close();

