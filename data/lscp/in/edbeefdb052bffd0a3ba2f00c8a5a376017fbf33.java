hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/CommonConfigurationKeys.java
  public static final String IPC_CALLQUEUE_NAMESPACE = "ipc";
  public static final String IPC_CALLQUEUE_IMPL_KEY = "callqueue.impl";
  public static final String IPC_CALLQUEUE_IDENTITY_PROVIDER_KEY = "identity-provider.impl";
  public static final String IPC_BACKOFF_ENABLE = "backoff.enable";
  public static final boolean IPC_BACKOFF_ENABLE_DEFAULT = false;


hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/CallQueueManager.java
      Class<?> queneClass, Class<E> elementClass) {
    return (Class<? extends BlockingQueue<E>>)queneClass;
  }
  private final boolean clientBackOffEnabled;

  private final AtomicReference<BlockingQueue<E>> takeRef;

  public CallQueueManager(Class<? extends BlockingQueue<E>> backingClass,
      boolean clientBackOffEnabled, int maxQueueSize, String namespace,
      Configuration conf) {
    BlockingQueue<E> bq = createCallQueueInstance(backingClass,
      maxQueueSize, namespace, conf);
    this.clientBackOffEnabled = clientBackOffEnabled;
    this.putRef = new AtomicReference<BlockingQueue<E>>(bq);
    this.takeRef = new AtomicReference<BlockingQueue<E>>(bq);
    LOG.info("Using callQueue " + backingClass);
      " could not be constructed.");
  }

  boolean isClientBackoffEnabled() {
    return clientBackOffEnabled;
  }

    putRef.get().put(e);
  }

  public boolean offer(E e) throws InterruptedException {
    return putRef.get().offer(e);
  }


hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/Server.java
    callQueue.swapQueue(getQueueClass(prefix, conf), maxQueueSize, prefix, conf);
  }

  static boolean getClientBackoffEnable(
      String prefix, Configuration conf) {
    String name = prefix + "." +
        CommonConfigurationKeys.IPC_BACKOFF_ENABLE;
    return conf.getBoolean(name,
        CommonConfigurationKeys.IPC_BACKOFF_ENABLE_DEFAULT);
  }

  public static class Call implements Schedulable {
    private final int callId;             // the client's call id
          rpcRequest, this, ProtoUtil.convert(header.getRpcKind()),
          header.getClientId().toByteArray(), traceSpan);

      if (callQueue.isClientBackoffEnabled()) {
        queueRequestOrAskClientToBackOff(call);
      } else {
        callQueue.put(call);              // queue the call; maybe blocked here
      }
      incRpcCount();  // Increment the rpc count
    }

    private void queueRequestOrAskClientToBackOff(Call call)
        throws WrappedRpcServerException, InterruptedException {
      boolean isCallQueued = callQueue.offer(call);
      if (!isCallQueued) {
        rpcMetrics.incrClientBackoff();
        RetriableException retriableException =
            new RetriableException("Server is too busy.");
        throw new WrappedRpcServerException(
            RpcErrorCodeProto.ERROR_RPC_SERVER, retriableException);
      }
    }

    final String prefix = getQueueClassPrefix();
    this.callQueue = new CallQueueManager<Call>(getQueueClass(prefix, conf),
        getClientBackoffEnable(prefix, conf), maxQueueSize, prefix, conf);

    this.secretManager = (SecretManager<TokenIdentifier>) secretManager;
    this.authorize = 

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/metrics/RpcMetrics.java
  MutableCounterLong rpcAuthorizationFailures;
  @Metric("Number of authorization sucesses")
  MutableCounterLong rpcAuthorizationSuccesses;
  @Metric("Number of client backoff requests")
  MutableCounterLong rpcClientBackoff;

  @Metric("Number of open connections") public int numOpenConnections() {
    return server.getNumOpenConnections();
      }
    }
  }

  public void incrClientBackoff() {
    rpcClientBackoff.incr();
  }
}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/ipc/TestCallQueueManager.java

  @Test
  public void testCallQueueCapacity() throws InterruptedException {
    manager = new CallQueueManager<FakeCall>(queueClass, false, 10, "", null);

    assertCanPut(manager, 10, 20); // Will stop at 10 due to capacity
  }

  @Test
  public void testEmptyConsume() throws InterruptedException {
    manager = new CallQueueManager<FakeCall>(queueClass, false, 10, "", null);

    assertCanTake(manager, 0, 1); // Fails since it's empty
  }

  @Test(timeout=60000)
  public void testSwapUnderContention() throws InterruptedException {
    manager = new CallQueueManager<FakeCall>(queueClass, false, 5000, "", null);

    ArrayList<Putter> producers = new ArrayList<Putter>();
    ArrayList<Taker> consumers = new ArrayList<Taker>();

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/ipc/TestRPC.java
    }
  }

  @Test (timeout=30000)
  public void testClientBackOff() throws Exception {
    boolean succeeded = false;
    final int numClients = 2;
    final List<Future<Void>> res = new ArrayList<Future<Void>>();
    final ExecutorService executorService =
        Executors.newFixedThreadPool(numClients);
    final Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    conf.setBoolean(CommonConfigurationKeys.IPC_CALLQUEUE_NAMESPACE +
        ".0." + CommonConfigurationKeys.IPC_BACKOFF_ENABLE, true);
    final Server server = new RPC.Builder(conf)
        .setProtocol(TestProtocol.class).setInstance(new TestImpl())
        .setBindAddress(ADDRESS).setPort(0)
        .setQueueSizePerHandler(1).setNumHandlers(1).setVerbose(true)
        .build();
    server.start();

    final TestProtocol proxy =
        RPC.getProxy(TestProtocol.class, TestProtocol.versionID,
            NetUtils.getConnectAddress(server), conf);
    try {
      for (int i = 0; i < numClients; i++) {
        res.add(executorService.submit(
            new Callable<Void>() {
              @Override
              public Void call() throws IOException, InterruptedException {
                proxy.sleep(100000);
                return null;
              }
            }));
      }
      while (server.getCallQueueLen() != 1
          && countThreads(CallQueueManager.class.getName()) != 1) {
        Thread.sleep(100);
      }
      try {
        proxy.sleep(100);
      } catch (RemoteException e) {
        IOException unwrapExeption = e.unwrapRemoteException();
        if (unwrapExeption instanceof RetriableException) {
            succeeded = true;
        }
      }
    } finally {
      server.stop();
      RPC.stopProxy(proxy);
      executorService.shutdown();
    }
    assertTrue("RetriableException not received", succeeded);
  }

  public static void main(String[] args) throws IOException {
    new TestRPC().testCallsInternal(conf);


