hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/event/AsyncDispatcher.java
  private volatile boolean drainEventsOnStop = false;

  private Object waitForDrained = new Object();

      @Override
      public void run() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          if (blockNewEvents) {
            synchronized (waitForDrained) {
              if (eventQueue.isEmpty()) {
                waitForDrained.notify();
              }
            }
      blockNewEvents = true;
      LOG.info("AsyncDispatcher is draining to stop, igonring any new events.");
      synchronized (waitForDrained) {
        while (!eventQueue.isEmpty() && eventHandlingThread.isAlive()) {
          waitForDrained.wait(1000);
          LOG.info("Waiting for AsyncDispatcher to drain. Thread state is :" +
              eventHandlingThread.getState());
    return handlerInstance;
  }

  @VisibleForTesting
  protected boolean hasPendingEvents() {
    return !eventQueue.isEmpty();
  }

  @VisibleForTesting
  protected boolean isEventThreadWaiting() {
    return eventHandlingThread.getState() == Thread.State.WAITING;
  }

  class GenericEventHandler implements EventHandler<Event> {
    public void handle(Event event) {
      if (blockNewEvents) {
        return;
      }

      int qSize = eventQueue.size();
      }
    };
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/event/DrainDispatcher.java
    this(new LinkedBlockingQueue<Event>());
  }

  public DrainDispatcher(BlockingQueue<Event> eventQueue) {
    super(eventQueue);
  }

  public void waitForEventThreadToWait() {
    while (!isEventThreadWaiting()) {
      Thread.yield();
    }
  }

  public void await() {
    while (hasPendingEvents()) {
      Thread.yield();
    }
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/event/TestAsyncDispatcher.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/event/TestAsyncDispatcher.java

package org.apache.hadoop.yarn.event;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.junit.Assert;
import org.junit.Test;

public class TestAsyncDispatcher {

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout=10000)
  public void testDispatcherOnCloseIfQueueEmpty() throws Exception {
    BlockingQueue<Event> eventQueue = spy(new LinkedBlockingQueue<Event>());
    Event event = mock(Event.class);
    doThrow(new InterruptedException()).when(eventQueue).put(event);
    DrainDispatcher disp = new DrainDispatcher(eventQueue);
    disp.init(new Configuration());
    disp.setDrainEventsOnStop();
    disp.start();
    disp.waitForEventThreadToWait();
    try {
      disp.getEventHandler().handle(event);
    } catch (YarnRuntimeException e) {
    }
    Assert.assertTrue("Event Queue should have been empty",
        eventQueue.isEmpty());
    disp.close();
  }
}

