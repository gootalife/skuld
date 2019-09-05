hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapred/LocalContainerLauncher.java
  private final HashSet<File> localizedFiles;
  private final AppContext context;
  private final TaskUmbilicalProtocol umbilical;
  private final ClassLoader jobClassLoader;
  private ExecutorService taskRunner;
  private Thread eventHandler;
  private BlockingQueue<ContainerLauncherEvent> eventQueue =

  public LocalContainerLauncher(AppContext context,
                                TaskUmbilicalProtocol umbilical) {
    this(context, umbilical, null);
  }

  public LocalContainerLauncher(AppContext context,
                                TaskUmbilicalProtocol umbilical,
                                ClassLoader jobClassLoader) {
    super(LocalContainerLauncher.class.getName());
    this.context = context;
    this.umbilical = umbilical;
    this.jobClassLoader = jobClassLoader;

    try {
      curFC = FileContext.getFileContext(curDir.toURI());
            setDaemon(true).setNameFormat("uber-SubtaskRunner").build());
    eventHandler = new Thread(new EventHandler(), "uber-EventHandler");
    if (jobClassLoader != null) {
      LOG.info("Setting " + jobClassLoader +
          " as the context classloader of thread " + eventHandler.getName());
      eventHandler.setContextClassLoader(jobClassLoader);
    } else {
      LOG.info("Context classloader of thread " + eventHandler.getName() +
          ": " + eventHandler.getContextClassLoader());
    }
    eventHandler.start();
    super.serviceStart();
  }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/MRAppMaster.java
    protected void serviceStart() throws Exception {
      if (job.isUber()) {
        this.containerLauncher = new LocalContainerLauncher(context,
            (TaskUmbilicalProtocol) taskAttemptListener, jobClassLoader);
      } else {
        this.containerLauncher = new ContainerLauncherImpl(context);
      }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-common/src/main/java/org/apache/hadoop/mapreduce/v2/util/MRApps.java
  public static void setClassLoader(ClassLoader classLoader,
      Configuration conf) {
    if (classLoader != null) {
      LOG.info("Setting classloader " + classLoader +
          " on the configuration and as the thread context classloader");
      conf.setClassLoader(classLoader);
      Thread.currentThread().setContextClassLoader(classLoader);

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/v2/TestMRJobs.java
        throws IOException, InterruptedException {
      super.setup(context);
      final Configuration conf = context.getConfiguration();
      if (conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER, false)) {
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        if (!(tccl instanceof ApplicationClassLoader)) {
          throw new IOException("TCCL expected: " +
              ApplicationClassLoader.class.getName() + ", actual: " +
              tccl.getClass().getName());
        }
      }
      final String ioSortMb = conf.get(MRJobConfig.IO_SORT_MB);
      if (!TEST_IO_SORT_MB.equals(ioSortMb)) {
        throw new IOException("io.sort.mb expected: " + TEST_IO_SORT_MB

