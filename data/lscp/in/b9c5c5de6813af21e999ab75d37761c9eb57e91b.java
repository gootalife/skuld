hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FSLeafQueue.java
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.TreeSet;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
      return assigned;
    }

    TreeSet<FSAppAttempt> pendingForResourceApps =
        new TreeSet<FSAppAttempt>(policy.getComparator());
    readLock.lock();
    try {
      for (FSAppAttempt app : runnableApps) {
        Resource pending = app.getAppAttemptResourceUsage().getPending();
        if (!pending.equals(Resources.none())) {
          pendingForResourceApps.add(app);
        }
      }
    } finally {
      readLock.unlock();
    }
    for (FSAppAttempt sched : pendingForResourceApps) {
      if (SchedulerAppUtils.isBlacklisted(sched, node, LOG)) {
        continue;
      }
        break;
      }
    }
    return assigned;
  }


