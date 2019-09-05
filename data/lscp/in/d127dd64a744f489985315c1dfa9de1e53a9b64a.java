hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/ZKRMStateStore.java
      LOG.debug("Removing info for app: " + appId + " at: " + appIdRemovePath
          + " and its attempts.");
    }
    doDeleteMultiWithRetries(opList);
  }

  @Override
      throws Exception {
    ArrayList<Op> opList = new ArrayList<Op>();
    addStoreOrUpdateOps(opList, rmDTIdentifier, renewDate, false);
    doStoreMultiWithRetries(opList);
  }

  @Override
  protected synchronized void removeRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier) throws Exception {
    String nodeRemovePath =
        getNodePath(delegationTokensRootPath, DELEGATION_TOKEN_PREFIX
            + rmDTIdentifier.getSequenceNumber());
          + rmDTIdentifier.getSequenceNumber());
    }
    if (existsWithRetries(nodeRemovePath, false) != null) {
      ArrayList<Op> opList = new ArrayList<Op>();
      opList.add(Op.delete(nodeRemovePath, -1));
      doDeleteMultiWithRetries(opList);
    } else {
      LOG.debug("Attempted to delete a non-existing znode " + nodeRemovePath);
    }
  }

  @Override
      addStoreOrUpdateOps(opList, rmDTIdentifier, renewDate, true);
    }
    doStoreMultiWithRetries(opList);
  }

  private void addStoreOrUpdateOps(ArrayList<Op> opList,
      LOG.debug("Removing RMDelegationKey_" + delegationKey.getKeyId());
    }
    if (existsWithRetries(nodeRemovePath, false) != null) {
      doDeleteMultiWithRetries(Op.delete(nodeRemovePath, -1));
    } else {
      LOG.debug("Attempted to delete a non-existing znode " + nodeRemovePath);
    }
  private synchronized void doStoreMultiWithRetries(
      final List<Op> opList) throws Exception {
    final List<Op> execOpList = new ArrayList<Op>(opList.size() + 2);
    execOpList.add(createFencingNodePathOp);
  private void doStoreMultiWithRetries(final Op op) throws Exception {
    doStoreMultiWithRetries(Collections.singletonList(op));
  }

  private synchronized void doDeleteMultiWithRetries(
      final List<Op> opList) throws Exception {
    final List<Op> execOpList = new ArrayList<Op>(opList.size() + 2);
    execOpList.add(createFencingNodePathOp);
    execOpList.addAll(opList);
    execOpList.add(deleteFencingNodePathOp);
    new ZKAction<Void>() {
      @Override
      public Void run() throws KeeperException, InterruptedException {
        setHasDeleteNodeOp(true);
        zkClient.multi(execOpList);
        return null;
      }
    }.runWithRetries();
  }

  private void doDeleteMultiWithRetries(final Op op) throws Exception {
    doDeleteMultiWithRetries(Collections.singletonList(op));
  }

  @VisibleForTesting
  public void createWithRetries(
      final String path, final byte[] data, final List<ACL> acl,
      final CreateMode mode) throws Exception {
    doStoreMultiWithRetries(Op.create(path, data, acl, mode));
  }

  @VisibleForTesting
  @Unstable
  public void setDataWithRetries(final String path, final byte[] data,
                                 final int version) throws Exception {
    doStoreMultiWithRetries(Op.setData(path, data, version));
  }

  @VisibleForTesting
    for (String child : children) {
      recursiveDeleteWithRetriesHelper(path + "/" + child, false);
    }

    try {
      zkClient.delete(path, -1);
    } catch (KeeperException.NoNodeException nne) {
      LOG.info("Node " + path + " doesn't exist to delete");
    }
  }

          if(isFencedState()) { 
            break;
          }
          doStoreMultiWithRetries(emptyOpList);
          Thread.sleep(zkSessionTimeout);
        }
      } catch (InterruptedException ie) {
  }

  private abstract class ZKAction<T> {
    private boolean hasDeleteNodeOp = false;
    void setHasDeleteNodeOp(boolean hasDeleteOp) {
      this.hasDeleteNodeOp = hasDeleteOp;
    }
    abstract T run() throws KeeperException, InterruptedException;

            LOG.info("znode already exists!");
            return null;
          }
          if (hasDeleteNodeOp && ke.code() == Code.NONODE) {
            LOG.info("znode has already been deleted!");
            return null;
          }

          LOG.info("Exception while executing a ZK operation.", ke);
          if (shouldRetry(ke.code()) && ++retry < numRetries) {
            LOG.info("Retrying operation on ZK. Retry no. " + retry);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/TestZKRMStateStore.java
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import javax.crypto.SecretKey;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;

    store.close();
  }

  @Test
  public void testDuplicateRMAppDeletion() throws Exception {
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester();
    long submitTime = System.currentTimeMillis();
    long startTime = System.currentTimeMillis() + 1234;
    RMStateStore store = zkTester.getRMStateStore();
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);

    ApplicationAttemptId attemptIdRemoved = ConverterUtils
        .toApplicationAttemptId("appattempt_1352994193343_0002_000001");
    ApplicationId appIdRemoved = attemptIdRemoved.getApplicationId();
    storeApp(store, appIdRemoved, submitTime, startTime);
    storeAttempt(store, attemptIdRemoved,
        "container_1352994193343_0002_01_000001", null, null, dispatcher);

    ApplicationSubmissionContext context =
        new ApplicationSubmissionContextPBImpl();
    context.setApplicationId(appIdRemoved);
    ApplicationStateData appStateRemoved =
        ApplicationStateData.newInstance(
            submitTime, startTime, context, "user1");
    appStateRemoved.attempts.put(attemptIdRemoved, null);
    store.removeApplicationStateInternal(appStateRemoved);
    try {
      store.removeApplicationStateInternal(appStateRemoved);
    } catch (KeeperException.NoNodeException nne) {
      Assert.fail("NoNodeException should not happen.");
    }
    store.close();
  }
}

