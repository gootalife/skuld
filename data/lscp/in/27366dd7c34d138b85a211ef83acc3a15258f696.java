hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/event/AsyncDispatcher.java
  private volatile boolean drainEventsOnStop = false;

  private volatile boolean drained = true;
  private Object waitForDrained = new Object();

      @Override
      public void run() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          drained = eventQueue.isEmpty();
          if (blockNewEvents) {
            synchronized (waitForDrained) {
              if (drained) {
                waitForDrained.notify();
              }
            }
      blockNewEvents = true;
      LOG.info("AsyncDispatcher is draining to stop, igonring any new events.");
      synchronized (waitForDrained) {
        while (!drained && eventHandlingThread.isAlive()) {
          waitForDrained.wait(1000);
          LOG.info("Waiting for AsyncDispatcher to drain. Thread state is :" +
              eventHandlingThread.getState());
    return handlerInstance;
  }

  class GenericEventHandler implements EventHandler<Event> {
    public void handle(Event event) {
      if (blockNewEvents) {
        return;
      }
      drained = false;

      int qSize = eventQueue.size();
      }
    };
  }

  @VisibleForTesting
  protected boolean isDrained() {
    return this.drained;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/event/DrainDispatcher.java
    this(new LinkedBlockingQueue<Event>());
  }

  private DrainDispatcher(BlockingQueue<Event> eventQueue) {
    super(eventQueue);
  }

  public void await() {
    while (!isDrained()) {
      Thread.yield();
    }
  }

