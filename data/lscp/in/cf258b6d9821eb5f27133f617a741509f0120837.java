hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/CallQueueManager.java
public class CallQueueManager<E> {
  public static final Log LOG = LogFactory.getLog(CallQueueManager.class);
  private static final int CHECKPOINT_NUM = 20;
  private static final long CHECKPOINT_INTERVAL_MS = 10;

  @SuppressWarnings("unchecked")
  static <E> Class<? extends BlockingQueue<E>> convertQueueClass(
      Class<?> queueClass, Class<E> elementClass) {
    return (Class<? extends BlockingQueue<E>>)queueClass;
  }
  private final boolean clientBackOffEnabled;

  }

  private boolean queueIsReallyEmpty(BlockingQueue<?> q) {
    for (int i = 0; i < CHECKPOINT_NUM; i++) {
      try {
        Thread.sleep(CHECKPOINT_INTERVAL_MS);
      } catch (InterruptedException ie) {
        return false;
      }
      if (!q.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private String stringRepr(Object o) {

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/ipc/TestCallQueueManager.java
    HashMap<Runnable, Thread> threads = new HashMap<Runnable, Thread>();

    for (int i=0; i < 1000; i++) {
      Putter p = new Putter(manager, -1, -1);
      Thread pt = new Thread(p);
      producers.add(p);
      pt.start();
    }

    for (int i=0; i < 100; i++) {
      Taker t = new Taker(manager, -1, -1);
      Thread tt = new Thread(t);
      consumers.add(t);
      tt.start();
    }

    Thread.sleep(500);

    for (int i=0; i < 5; i++) {
      manager.swapQueue(queueClass, 5000, "", null);

