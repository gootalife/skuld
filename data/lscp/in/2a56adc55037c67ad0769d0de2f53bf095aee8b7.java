hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/net/unix/DomainSocketWatcher.java
              for (Iterator<Entry> iter = toAdd.iterator(); iter.hasNext(); ) {
                Entry entry = iter.next();
                iter.remove();
                DomainSocket sock = entry.getDomainSocket();
                Entry prevEntry = entries.put(sock.fd, entry);
                Preconditions.checkState(prevEntry == null,
                  LOG.trace(this + ": adding fd " + sock.fd);
                }
                fdSet.add(sock.fd);
              }
              while (true) {
          }
          entries.clear();
          fdSet.close();
          closed = true;
          if (!(toAdd.isEmpty() && toRemove.isEmpty())) {
            for (Iterator<Entry> iter = toAdd.iterator(); iter.hasNext();) {
              Entry entry = iter.next();
              entry.getDomainSocket().refCount.unreference();
              entry.getHandler().handle(entry.getDomainSocket());
              IOUtils.cleanup(LOG, entry.getDomainSocket());
              iter.remove();
            }
            while (true) {
              Map.Entry<Integer, DomainSocket> entry = toRemove.firstEntry();
              if (entry == null)
                break;
              sendCallback("close", entries, fdSet, entry.getValue().fd);
            }
          }
          processedCond.signalAll();
        } finally {
          lock.unlock();
        }

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/net/unix/TestDomainSocketWatcher.java
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

    watcher.close();
  }

  @Test(timeout = 300000)
  public void testStressInterruption() throws Exception {
    final int SOCKET_NUM = 250;
    final ReentrantLock lock = new ReentrantLock();
    final DomainSocketWatcher watcher = newDomainSocketWatcher(10);
    final ArrayList<DomainSocket[]> pairs = new ArrayList<DomainSocket[]>();
    final AtomicInteger handled = new AtomicInteger(0);

    final Thread adderThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          for (int i = 0; i < SOCKET_NUM; i++) {
            DomainSocket pair[] = DomainSocket.socketpair();
            watcher.add(pair[1], new DomainSocketWatcher.Handler() {
              @Override
              public boolean handle(DomainSocket sock) {
                handled.incrementAndGet();
                return true;
              }
            });
            lock.lock();
            try {
              pairs.add(pair);
            } finally {
              lock.unlock();
            }
            TimeUnit.MILLISECONDS.sleep(1);
          }
        } catch (Throwable e) {
          LOG.error(e);
          throw new RuntimeException(e);
        }
      }
    });

    final Thread removerThread = new Thread(new Runnable() {
      @Override
      public void run() {
        final Random random = new Random();
        try {
          while (handled.get() != SOCKET_NUM) {
            lock.lock();
            try {
              if (!pairs.isEmpty()) {
                int idx = random.nextInt(pairs.size());
                DomainSocket pair[] = pairs.remove(idx);
                if (random.nextBoolean()) {
                  pair[0].close();
                } else {
                  watcher.remove(pair[1]);
                }
                TimeUnit.MILLISECONDS.sleep(1);
              }
            } finally {
              lock.unlock();
            }
          }
        } catch (Throwable e) {
          LOG.error(e);
          throw new RuntimeException(e);
        }
      }
    });

    adderThread.start();
    removerThread.start();
    TimeUnit.MILLISECONDS.sleep(100);
    watcher.watcherThread.interrupt();
    Uninterruptibles.joinUninterruptibly(adderThread);
    Uninterruptibles.joinUninterruptibly(removerThread);
    Uninterruptibles.joinUninterruptibly(watcher.watcherThread);
  }


