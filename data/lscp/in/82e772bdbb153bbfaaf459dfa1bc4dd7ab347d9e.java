hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/retry/RetryPolicies.java
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;

        return new RetryAction(RetryAction.RetryDecision.RETRY,
              getFailoverOrRetrySleepTime(retries));
      } else if (e instanceof InvalidToken) {
        return new RetryAction(RetryAction.RetryDecision.FAIL, 0,
            "Invalid or Cancelled Token");
      } else if (e instanceof SocketException
          || (e instanceof IOException && !(e instanceof RemoteException))) {
        if (isIdempotentOrAtMostOnce) {

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/ipc/TestIPC.java
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.DefaultFailoverProxyProvider;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Assume;
      this.total = total;
    }

    protected Object returnValue(Object value) throws Exception {
      if (retry++ < total) {
        throw new IOException("Fake IOException");
      }
      return value;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      LongWritable param = new LongWritable(RANDOM.nextLong());
      LongWritable value = (LongWritable) client.call(param,
          NetUtils.getConnectAddress(server), null, null, 0, conf);
      return returnValue(value);
    }

    @Override
    }
  }

  private static class TestInvalidTokenHandler extends TestInvocationHandler {
    private int invocations = 0;
    TestInvalidTokenHandler(Client client, Server server) {
      super(client, server, 1);
    }

    @Override
    protected Object returnValue(Object value) throws Exception {
      throw new InvalidToken("Invalid Token");
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      invocations++;
      return super.invoke(proxy, method, args);
    }
  }

  @Test(timeout=60000)
  public void testSerial() throws IOException, InterruptedException {
    internalTestSerial(3, false, 2, 5, 100);
  
  private interface DummyProtocol {
    @Idempotent
    public void dummyRun() throws IOException;
  }
  
    }
  }

  @Test(expected = InvalidToken.class)
  public void testNoRetryOnInvalidToken() throws IOException {
    final Client client = new Client(LongWritable.class, conf);
    final TestServer server = new TestServer(1, false);
    TestInvalidTokenHandler handler =
        new TestInvalidTokenHandler(client, server);
    DummyProtocol proxy = (DummyProtocol) Proxy.newProxyInstance(
        DummyProtocol.class.getClassLoader(),
        new Class[] { DummyProtocol.class }, handler);
    FailoverProxyProvider<DummyProtocol> provider =
        new DefaultFailoverProxyProvider<DummyProtocol>(
            DummyProtocol.class, proxy);
    DummyProtocol retryProxy =
        (DummyProtocol) RetryProxy.create(DummyProtocol.class, provider,
        RetryPolicies.failoverOnNetworkException(
            RetryPolicies.TRY_ONCE_THEN_FAIL, 100, 100, 10000, 0));

    try {
      server.start();
      retryProxy.dummyRun();
    } finally {
      Assert.assertEquals(handler.invocations, 1);
      Client.setCallIdAndRetryCount(0, 0);
      client.stop();
      server.stop();
    }
  }


