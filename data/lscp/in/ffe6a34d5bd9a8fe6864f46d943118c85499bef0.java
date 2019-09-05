hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/retry/MultiException.java
++ b/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/retry/MultiException.java

package org.apache.hadoop.io.retry;

import java.io.IOException;
import java.util.Map;

public class MultiException extends IOException {

  private final Map<String, Exception> exes;

  public MultiException(Map<String, Exception> exes) {
    this.exes = exes;
  }

  public Map<String, Exception> getExceptions() {
    return exes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("{");
    for (Exception e : exes.values()) {
      sb.append(e.toString()).append(", ");
    }
    sb.append("}");
    return "MultiException[" + sb.toString() + "]";
  }
}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/retry/RetryInvocationHandler.java
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

        Object ret = invokeMethod(method, args);
        hasMadeASuccessfulCall = true;
        return ret;
      } catch (Exception ex) {
        boolean isIdempotentOrAtMostOnce = proxyProvider.getInterface()
            .getMethod(method.getName(), method.getParameterTypes())
            .isAnnotationPresent(Idempotent.class);
              .getMethod(method.getName(), method.getParameterTypes())
              .isAnnotationPresent(AtMostOnce.class);
        }
        List<RetryAction> actions = extractActions(policy, ex, retries++,
                invocationFailoverCount, isIdempotentOrAtMostOnce);
        RetryAction failAction = getFailAction(actions);
        if (failAction != null) {
          if (failAction.reason != null) {
            LOG.warn("Exception while invoking " + currentProxy.proxy.getClass()
                + "." + method.getName() + " over " + currentProxy.proxyInfo
                + ". Not retrying because " + failAction.reason, ex);
          }
          throw ex;
        } else { // retry or failover
          boolean worthLogging = 
            !(invocationFailoverCount == 0 && !hasMadeASuccessfulCall);
          worthLogging |= LOG.isDebugEnabled();
          RetryAction failOverAction = getFailOverAction(actions);
          long delay = getDelayMillis(actions);
          if (failOverAction != null && worthLogging) {
            String msg = "Exception while invoking " + method.getName()
                + " of class " + currentProxy.proxy.getClass().getSimpleName()
                + " over " + currentProxy.proxyInfo;
            if (invocationFailoverCount > 0) {
              msg += " after " + invocationFailoverCount + " fail over attempts"; 
            }
            msg += ". Trying to fail over " + formatSleepMessage(delay);
            LOG.info(msg, ex);
          } else {
            if(LOG.isDebugEnabled()) {
              LOG.debug("Exception while invoking " + method.getName()
                  + " of class " + currentProxy.proxy.getClass().getSimpleName()
                  + " over " + currentProxy.proxyInfo + ". Retrying "
                  + formatSleepMessage(delay), ex);
            }
          }

          if (delay > 0) {
            Thread.sleep(delay);
          }
          
          if (failOverAction != null) {
            synchronized (proxyProvider) {
    }
  }

  private long getDelayMillis(List<RetryAction> actions) {
    long retVal = 0;
    for (RetryAction action : actions) {
      if (action.action == RetryAction.RetryDecision.FAILOVER_AND_RETRY ||
              action.action == RetryAction.RetryDecision.RETRY) {
        if (action.delayMillis > retVal) {
          retVal = action.delayMillis;
        }
      }
    }
    return retVal;
  }

  private RetryAction getFailOverAction(List<RetryAction> actions) {
    for (RetryAction action : actions) {
      if (action.action == RetryAction.RetryDecision.FAILOVER_AND_RETRY) {
        return action;
      }
    }
    return null;
  }

  private RetryAction getFailAction(List<RetryAction> actions) {
    RetryAction fAction = null;
    for (RetryAction action : actions) {
      if (action.action == RetryAction.RetryDecision.FAIL) {
        fAction = action;
      } else {
        return null;
      }
    }
    return fAction;
  }

  private List<RetryAction> extractActions(RetryPolicy policy, Exception ex,
                                           int i, int invocationFailoverCount,
                                           boolean isIdempotentOrAtMostOnce)
          throws Exception {
    List<RetryAction> actions = new LinkedList<>();
    if (ex instanceof MultiException) {
      for (Exception th : ((MultiException) ex).getExceptions().values()) {
        actions.add(policy.shouldRetry(th, i, invocationFailoverCount,
                isIdempotentOrAtMostOnce));
      }
    } else {
      actions.add(policy.shouldRetry(ex, i,
              invocationFailoverCount, isIdempotentOrAtMostOnce));
    }
    return actions;
  }

  private static String formatSleepMessage(long millis) {
    if (millis > 0) {
      return "after sleeping for " + millis + "ms.";

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/ha/ConfiguredFailoverProxyProvider.java
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

  private static final Log LOG =
      LogFactory.getLog(ConfiguredFailoverProxyProvider.class);
  
  interface ProxyFactory<T> {
    T createProxy(Configuration conf, InetSocketAddress nnAddr, Class<T> xface,
        UserGroupInformation ugi, boolean withRetries,
        AtomicBoolean fallbackToSimpleAuth) throws IOException;
  }

  static class DefaultProxyFactory<T> implements ProxyFactory<T> {
    @Override
    public T createProxy(Configuration conf, InetSocketAddress nnAddr,
        Class<T> xface, UserGroupInformation ugi, boolean withRetries,
        AtomicBoolean fallbackToSimpleAuth) throws IOException {
      return NameNodeProxies.createNonHAProxy(conf,
          nnAddr, xface, ugi, false, fallbackToSimpleAuth).getProxy();
    }
  }

  protected final Configuration conf;
  protected final List<AddressRpcProxyPair<T>> proxies =
      new ArrayList<AddressRpcProxyPair<T>>();
  private final UserGroupInformation ugi;
  protected final Class<T> xface;

  private int currentProxyIndex = 0;
  private final ProxyFactory<T> factory;

  public ConfiguredFailoverProxyProvider(Configuration conf, URI uri,
      Class<T> xface) {
    this(conf, uri, xface, new DefaultProxyFactory<T>());
  }

  @VisibleForTesting
  ConfiguredFailoverProxyProvider(Configuration conf, URI uri,
      Class<T> xface, ProxyFactory<T> factory) {

    Preconditions.checkArgument(
        xface.isAssignableFrom(NamenodeProtocols.class),
        "Interface class %s is not a valid NameNode protocol!");
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
    this.conf.setInt(
            CommonConfigurationKeysPublic
                    .IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
            maxRetriesOnSocketTimeouts);

    try {
      HAUtil.cloneDelegationTokenForLogicalUri(ugi, uri, addressesOfNns);
      this.factory = factory;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    AddressRpcProxyPair<T> current = proxies.get(currentProxyIndex);
    if (current.namenode == null) {
      try {
        current.namenode = factory.createProxy(conf,
            current.address, xface, ugi, false, fallbackToSimpleAuth);
      } catch (IOException e) {
        LOG.error("Failed to create RPC proxy to NameNode", e);
        throw new RuntimeException(e);
  }

  @Override
  public  void performFailover(T currentProxy) {
    incrementProxyIndex();
  }

  synchronized void incrementProxyIndex() {
    currentProxyIndex = (currentProxyIndex + 1) % proxies.size();
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/ha/RequestHedgingProxyProvider.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/ha/RequestHedgingProxyProvider.java
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.io.retry.MultiException;

public class RequestHedgingProxyProvider<T> extends
        ConfiguredFailoverProxyProvider<T> {

  private static final Log LOG =
          LogFactory.getLog(RequestHedgingProxyProvider.class);

  class RequestHedgingInvocationHandler implements InvocationHandler {

    final Map<String, ProxyInfo<T>> targetProxies;

    public RequestHedgingInvocationHandler(
            Map<String, ProxyInfo<T>> targetProxies) {
      this.targetProxies = new HashMap<>(targetProxies);
    }

    @Override
    public Object
    invoke(Object proxy, final Method method, final Object[] args)
            throws Throwable {
      Map<Future<Object>, ProxyInfo<T>> proxyMap = new HashMap<>();
      int numAttempts = 0;

      ExecutorService executor = null;
      CompletionService<Object> completionService;
      try {
        targetProxies.remove(toIgnore);
        if (targetProxies.size() == 1) {
          ProxyInfo<T> proxyInfo = targetProxies.values().iterator().next();
          Object retVal = method.invoke(proxyInfo.proxy, args);
          successfulProxy = proxyInfo;
          return retVal;
        }
        executor = Executors.newFixedThreadPool(proxies.size());
        completionService = new ExecutorCompletionService<>(executor);
        for (final Map.Entry<String, ProxyInfo<T>> pEntry :
                targetProxies.entrySet()) {
          Callable<Object> c = new Callable<Object>() {
            @Override
            public Object call() throws Exception {
              return method.invoke(pEntry.getValue().proxy, args);
            }
          };
          proxyMap.put(completionService.submit(c), pEntry.getValue());
          numAttempts++;
        }

        Map<String, Exception> badResults = new HashMap<>();
        while (numAttempts > 0) {
          Future<Object> callResultFuture = completionService.take();
          Object retVal;
          try {
            retVal = callResultFuture.get();
            successfulProxy = proxyMap.get(callResultFuture);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Invocation successful on ["
                      + successfulProxy.proxyInfo + "]");
            }
            return retVal;
          } catch (Exception ex) {
            ProxyInfo<T> tProxyInfo = proxyMap.get(callResultFuture);
            LOG.warn("Invocation returned exception on "
                    + "[" + tProxyInfo.proxyInfo + "]");
            badResults.put(tProxyInfo.proxyInfo, ex);
            numAttempts--;
          }
        }

        if (badResults.size() == 1) {
          throw badResults.values().iterator().next();
        } else {
          throw new MultiException(badResults);
        }
      } finally {
        if (executor != null) {
          executor.shutdownNow();
        }
      }
    }
  }


  private volatile ProxyInfo<T> successfulProxy = null;
  private volatile String toIgnore = null;

  public RequestHedgingProxyProvider(
          Configuration conf, URI uri, Class<T> xface) {
    this(conf, uri, xface, new DefaultProxyFactory<T>());
  }

  @VisibleForTesting
  RequestHedgingProxyProvider(Configuration conf, URI uri,
                              Class<T> xface, ProxyFactory<T> factory) {
    super(conf, uri, xface, factory);
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized ProxyInfo<T> getProxy() {
    if (successfulProxy != null) {
      return successfulProxy;
    }
    Map<String, ProxyInfo<T>> targetProxyInfos = new HashMap<>();
    StringBuilder combinedInfo = new StringBuilder('[');
    for (int i = 0; i < proxies.size(); i++) {
      ProxyInfo<T> pInfo = super.getProxy();
      incrementProxyIndex();
      targetProxyInfos.put(pInfo.proxyInfo, pInfo);
      combinedInfo.append(pInfo.proxyInfo).append(',');
    }
    combinedInfo.append(']');
    T wrappedProxy = (T) Proxy.newProxyInstance(
            RequestHedgingInvocationHandler.class.getClassLoader(),
            new Class<?>[]{xface},
            new RequestHedgingInvocationHandler(targetProxyInfos));
    return new ProxyInfo<T>(wrappedProxy, combinedInfo.toString());
  }

  @Override
  public synchronized void performFailover(T currentProxy) {
    toIgnore = successfulProxy.proxyInfo;
    successfulProxy = null;
  }

}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestRequestHedgingProxyProvider.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestRequestHedgingProxyProvider.java
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider.ProxyFactory;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.retry.MultiException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;

public class TestRequestHedgingProxyProvider {

  private Configuration conf;
  private URI nnUri;
  private String ns;

  @Before
  public void setup() throws URISyntaxException {
    ns = "mycluster-" + Time.monotonicNow();
    nnUri = new URI("hdfs://" + ns);
    conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, ns);
    conf.set(
        DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + ns, "nn1,nn2");
    conf.set(
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns + ".nn1",
        "machine1.foo.bar:8020");
    conf.set(
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns + ".nn2",
        "machine2.foo.bar:8020");
  }

  @Test
  public void testHedgingWhenOneFails() throws Exception {
    final NamenodeProtocols goodMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(goodMock.getStats()).thenReturn(new long[] {1});
    final NamenodeProtocols badMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(badMock.getStats()).thenThrow(new IOException("Bad mock !!"));

    RequestHedgingProxyProvider<NamenodeProtocols> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri, NamenodeProtocols.class,
            createFactory(goodMock, badMock));
    long[] stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Mockito.verify(badMock).getStats();
    Mockito.verify(goodMock).getStats();
  }

  @Test
  public void testHedgingWhenOneIsSlow() throws Exception {
    final NamenodeProtocols goodMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(goodMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(1000);
        return new long[]{1};
      }
    });
    final NamenodeProtocols badMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(badMock.getStats()).thenThrow(new IOException("Bad mock !!"));

    RequestHedgingProxyProvider<NamenodeProtocols> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri, NamenodeProtocols.class,
            createFactory(goodMock, badMock));
    long[] stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(1, stats[0]);
    Mockito.verify(badMock).getStats();
    Mockito.verify(goodMock).getStats();
  }

  @Test
  public void testHedgingWhenBothFail() throws Exception {
    NamenodeProtocols badMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(badMock.getStats()).thenThrow(new IOException("Bad mock !!"));
    NamenodeProtocols worseMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(worseMock.getStats()).thenThrow(
            new IOException("Worse mock !!"));

    RequestHedgingProxyProvider<NamenodeProtocols> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri, NamenodeProtocols.class,
            createFactory(badMock, worseMock));
    try {
      provider.getProxy().proxy.getStats();
      Assert.fail("Should fail since both namenodes throw IOException !!");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof MultiException);
    }
    Mockito.verify(badMock).getStats();
    Mockito.verify(worseMock).getStats();
  }

  @Test
  public void testPerformFailover() throws Exception {
    final AtomicInteger counter = new AtomicInteger(0);
    final int[] isGood = {1};
    final NamenodeProtocols goodMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(goodMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        counter.incrementAndGet();
        if (isGood[0] == 1) {
          Thread.sleep(1000);
          return new long[]{1};
        }
        throw new IOException("Was Good mock !!");
      }
    });
    final NamenodeProtocols badMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(badMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        counter.incrementAndGet();
        if (isGood[0] == 2) {
          Thread.sleep(1000);
          return new long[]{2};
        }
        throw new IOException("Bad mock !!");
      }
    });
    RequestHedgingProxyProvider<NamenodeProtocols> provider =
            new RequestHedgingProxyProvider<>(conf, nnUri, NamenodeProtocols.class,
                    createFactory(goodMock, badMock));
    long[] stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(1, stats[0]);
    Assert.assertEquals(2, counter.get());
    Mockito.verify(badMock).getStats();
    Mockito.verify(goodMock).getStats();

    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(1, stats[0]);
    Mockito.verifyNoMoreInteractions(badMock);
    Assert.assertEquals(3, counter.get());

    isGood[0] = 2;
    try {
      provider.getProxy().proxy.getStats();
      Assert.fail("Should fail since previously successful proxy now fails ");
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof IOException);
    }

    Assert.assertEquals(4, counter.get());

    provider.performFailover(provider.getProxy().proxy);
    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(2, stats[0]);

    Assert.assertEquals(5, counter.get());

    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(2, stats[0]);

    Assert.assertEquals(6, counter.get());

    isGood[0] = 1;
    try {
      provider.getProxy().proxy.getStats();
      Assert.fail("Should fail since previously successful proxy now fails ");
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof IOException);
    }

    Assert.assertEquals(7, counter.get());

    provider.performFailover(provider.getProxy().proxy);
    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(1, stats[0]);
  }

  @Test
  public void testPerformFailoverWith3Proxies() throws Exception {
    conf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + ns,
            "nn1,nn2,nn3");
    conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns + ".nn3",
            "machine3.foo.bar:8020");

    final AtomicInteger counter = new AtomicInteger(0);
    final int[] isGood = {1};
    final NamenodeProtocols goodMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(goodMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        counter.incrementAndGet();
        if (isGood[0] == 1) {
          Thread.sleep(1000);
          return new long[]{1};
        }
        throw new IOException("Was Good mock !!");
      }
    });
    final NamenodeProtocols badMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(badMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        counter.incrementAndGet();
        if (isGood[0] == 2) {
          Thread.sleep(1000);
          return new long[]{2};
        }
        throw new IOException("Bad mock !!");
      }
    });
    final NamenodeProtocols worseMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(worseMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        counter.incrementAndGet();
        if (isGood[0] == 3) {
          Thread.sleep(1000);
          return new long[]{3};
        }
        throw new IOException("Worse mock !!");
      }
    });

    RequestHedgingProxyProvider<NamenodeProtocols> provider =
            new RequestHedgingProxyProvider<>(conf, nnUri, NamenodeProtocols.class,
                    createFactory(goodMock, badMock, worseMock));
    long[] stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(1, stats[0]);
    Assert.assertEquals(3, counter.get());
    Mockito.verify(badMock).getStats();
    Mockito.verify(goodMock).getStats();
    Mockito.verify(worseMock).getStats();

    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(1, stats[0]);
    Mockito.verifyNoMoreInteractions(badMock);
    Mockito.verifyNoMoreInteractions(worseMock);
    Assert.assertEquals(4, counter.get());

    isGood[0] = 2;
    try {
      provider.getProxy().proxy.getStats();
      Assert.fail("Should fail since previously successful proxy now fails ");
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof IOException);
    }

    Assert.assertEquals(5, counter.get());

    provider.performFailover(provider.getProxy().proxy);
    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(2, stats[0]);

    Assert.assertEquals(7, counter.get());

    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(2, stats[0]);

    Assert.assertEquals(8, counter.get());

    isGood[0] = 3;
    try {
      provider.getProxy().proxy.getStats();
      Assert.fail("Should fail since previously successful proxy now fails ");
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof IOException);
    }

    Assert.assertEquals(9, counter.get());

    provider.performFailover(provider.getProxy().proxy);
    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);

    Assert.assertEquals(3, stats[0]);

    Assert.assertEquals(11, counter.get());

    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(3, stats[0]);

    Assert.assertEquals(12, counter.get());
  }

  private ProxyFactory<NamenodeProtocols> createFactory(
      NamenodeProtocols... protos) {
    final Iterator<NamenodeProtocols> iterator =
        Lists.newArrayList(protos).iterator();
    return new ProxyFactory<NamenodeProtocols>() {
      @Override
      public NamenodeProtocols createProxy(Configuration conf,
          InetSocketAddress nnAddr, Class<NamenodeProtocols> xface,
          UserGroupInformation ugi, boolean withRetries,
          AtomicBoolean fallbackToSimpleAuth) throws IOException {
        return iterator.next();
      }
    };
  }
}

