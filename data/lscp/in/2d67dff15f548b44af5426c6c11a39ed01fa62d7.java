hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/retry/RetryPolicies.java
    return unwrapped instanceof StandbyException;
  }
  
  static RetriableException getWrappedRetriableException(Exception e) {
    if (!(e instanceof RemoteException)) {
      return null;
    }

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/retry/RetryUtils.java
import org.apache.hadoop.ipc.RemoteException;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.ipc.RetriableException;

public class RetryUtils {
  public static final Log LOG = LogFactory.getLog(RetryUtils.class);

          final RetryPolicy p;
          if (e instanceof RetriableException
              || RetryPolicies.getWrappedRetriableException(e) != null) {
            p = multipleLinearRandomRetry;
          } else if (e instanceof RemoteException) {
            final RemoteException re = (RemoteException)e;
            p = remoteExceptionToRetry.equals(re.getClassName())?
                multipleLinearRandomRetry: RetryPolicies.TRY_ONCE_THEN_FAIL;

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/io/retry/TestDefaultRetryPolicy.java
++ b/hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/io/retry/TestDefaultRetryPolicy.java

package org.apache.hadoop.io.retry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RetriableException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class TestDefaultRetryPolicy {
  @Rule
  public Timeout timeout = new Timeout(300000);

  @Test
  public void testWithRetriable() throws Exception {
    Configuration conf = new Configuration();
    RetryPolicy policy = RetryUtils.getDefaultRetryPolicy(
        conf, "Test.No.Such.Key",
        true,                     // defaultRetryPolicyEnabled = true
        "Test.No.Such.Key", "10000,6",
        null);
    RetryPolicy.RetryAction action = policy.shouldRetry(
        new RetriableException("Dummy exception"), 0, 0, true);
    assertThat(action.action,
        is(RetryPolicy.RetryAction.RetryDecision.RETRY));
  }

  @Test
  public void testWithWrappedRetriable() throws Exception {
    Configuration conf = new Configuration();
    RetryPolicy policy = RetryUtils.getDefaultRetryPolicy(
        conf, "Test.No.Such.Key",
        true,                     // defaultRetryPolicyEnabled = true
        "Test.No.Such.Key", "10000,6",
        null);
    RetryPolicy.RetryAction action = policy.shouldRetry(
        new RemoteException(RetriableException.class.getName(),
            "Dummy exception"), 0, 0, true);
    assertThat(action.action,
        is(RetryPolicy.RetryAction.RetryDecision.RETRY));
  }

  @Test
  public void testWithRetriableAndRetryDisabled() throws Exception {
    Configuration conf = new Configuration();
    RetryPolicy policy = RetryUtils.getDefaultRetryPolicy(
        conf, "Test.No.Such.Key",
        false,                     // defaultRetryPolicyEnabled = false
        "Test.No.Such.Key", "10000,6",
        null);
    RetryPolicy.RetryAction action = policy.shouldRetry(
        new RetriableException("Dummy exception"), 0, 0, true);
    assertThat(action.action,
        is(RetryPolicy.RetryAction.RetryDecision.FAIL));
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNodeRpcServer.java
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.RetryCache;
import org.apache.hadoop.ipc.RetryCache.CacheEntry;
import org.apache.hadoop.ipc.RetryCache.CacheEntryWithPayload;

  private void checkNNStartup() throws IOException {
    if (!this.nn.isStarted()) {
      throw new RetriableException(this.nn.getRole() + " still not started");
    }
  }


