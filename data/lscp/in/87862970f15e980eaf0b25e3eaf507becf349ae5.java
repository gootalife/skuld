hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapred/LocalContainerLauncher.java
  private final ClassLoader jobClassLoader;
  private ExecutorService taskRunner;
  private Thread eventHandler;
  private byte[] encryptedSpillKey = new byte[] {0};
  private BlockingQueue<ContainerLauncherEvent> eventQueue =
      new LinkedBlockingQueue<ContainerLauncherEvent>();

    }
  }

  public void setEncryptedSpillKey(byte[] encryptedSpillKey) {
    if (encryptedSpillKey != null) {
      this.encryptedSpillKey = encryptedSpillKey;
    }
  }

        conf.setBoolean("mapreduce.task.uberized", true);

        task.setEncryptedSpillKey(encryptedSpillKey);
        YarnChild.setEncryptedSpillKeyIfRequired(task);


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapred/TaskAttemptListenerImpl.java

  private JobTokenSecretManager jobTokenSecretManager = null;

  private byte[] encryptedSpillKey;

  public TaskAttemptListenerImpl(AppContext context,
      JobTokenSecretManager jobTokenSecretManager,
      RMHeartbeatHandler rmHeartbeatHandler,
      byte[] secretShuffleKey) {
    super(TaskAttemptListenerImpl.class.getName());
    this.context = context;
    this.jobTokenSecretManager = jobTokenSecretManager;
    this.rmHeartbeatHandler = rmHeartbeatHandler;
    this.encryptedSpillKey = secretShuffleKey;
  }

  @Override
            jvmIDToActiveAttemptMap.remove(wJvmID);
        launchedJVMs.remove(wJvmID);
        LOG.info("JVM with ID: " + jvmId + " given task: " + task.getTaskID());
        task.setEncryptedSpillKey(encryptedSpillKey);
        jvmTask = new JvmTask(task, false);
      }
    }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapred/YarnChild.java
        @Override
        public Object run() throws Exception {
          setEncryptedSpillKeyIfRequired(taskFinal);
          FileSystem.get(job).setWorkingDirectory(job.getWorkingDirectory());
          taskFinal.run(job, umbilical); // run the task
          return null;
    }
  }

  public static void setEncryptedSpillKeyIfRequired(Task task) throws
          Exception {
    if ((task != null) && (task.getEncryptedSpillKey() != null) && (task
            .getEncryptedSpillKey().length > 1)) {
      Credentials creds =
              UserGroupInformation.getCurrentUser().getCredentials();
      TokenCache.setEncryptedSpillKey(task.getEncryptedSpillKey(), creds);
      UserGroupInformation.getCurrentUser().addCredentials(creds);
    }
  }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/MRAppMaster.java
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.mapred.TaskAttemptListenerImpl;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;

import com.google.common.annotations.VisibleForTesting;

import javax.crypto.KeyGenerator;

  public static final int SHUTDOWN_HOOK_PRIORITY = 30;
  public static final String INTERMEDIATE_DATA_ENCRYPTION_ALGO = "HmacSHA1";

  private Clock clock;
  private final long startTime;
  private JobEventDispatcher jobEventDispatcher;
  private JobHistoryEventHandler jobHistoryEventHandler;
  private SpeculatorEventDispatcher speculatorEventDispatcher;
  private byte[] encryptedSpillKey;

    try {
      this.currentUser = UserGroupInformation.getCurrentUser();
      this.jobCredentials = ((JobConf)conf).getCredentials();
      if (CryptoUtils.isEncryptedSpillEnabled(conf)) {
        int keyLen = conf.getInt(
                MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS,
                MRJobConfig
                        .DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS);
        KeyGenerator keyGen =
                KeyGenerator.getInstance(INTERMEDIATE_DATA_ENCRYPTION_ALGO);
        keyGen.init(keyLen);
        encryptedSpillKey = keyGen.generateKey().getEncoded();
      } else {
        encryptedSpillKey = new byte[] {0};
      }
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    } catch (NoSuchAlgorithmException e) {
      throw new YarnRuntimeException(e);
    }
  }

  protected TaskAttemptListener createTaskAttemptListener(AppContext context) {
    TaskAttemptListener lis =
        new TaskAttemptListenerImpl(context, jobTokenSecretManager,
            getRMHeartbeatHandler(), encryptedSpillKey);
    return lis;
  }

      if (job.isUber()) {
        this.containerLauncher = new LocalContainerLauncher(context,
            (TaskUmbilicalProtocol) taskAttemptListener, jobClassLoader);
        ((LocalContainerLauncher) this.containerLauncher)
                .setEncryptedSpillKey(encryptedSpillKey);
      } else {
        this.containerLauncher = new ContainerLauncherImpl(context);
      }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapred/TestTaskAttemptFinishingMonitor.java
    when(appCtx.getClock()).thenReturn(clock);

    TaskAttemptListenerImpl listener =
        new TaskAttemptListenerImpl(appCtx, secret, rmHeartbeatHandler, null);

    listener.init(conf);
    listener.start();

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapred/TestTaskAttemptListenerImpl.java
    public MockTaskAttemptListenerImpl(AppContext context,
        JobTokenSecretManager jobTokenSecretManager,
        RMHeartbeatHandler rmHeartbeatHandler) {
      super(context, jobTokenSecretManager, rmHeartbeatHandler, null);
    }
    
    public MockTaskAttemptListenerImpl(AppContext context,
        JobTokenSecretManager jobTokenSecretManager,
        RMHeartbeatHandler rmHeartbeatHandler,
        TaskHeartbeatHandler hbHandler) {
      super(context, jobTokenSecretManager, rmHeartbeatHandler, null);
      this.taskHeartbeatHandler = hbHandler;
    }
    

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/TestFail.java
      return new TaskAttemptListenerImpl(getContext(), null, null, null) {
        @Override
        public void startRpcServer(){};
        @Override

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/Task.java
  private String user;                            // user running the job
  private TaskAttemptID taskId;                   // unique, includes job id
  private int partition;                          // id within job
  private byte[] encryptedSpillKey = new byte[] {0};  // Key Used to encrypt
  TaskStatus taskStatus;                          // current status of the task
  protected JobStatus.State jobRunStateForCleanup;
  protected boolean jobCleanup = false;
    this.tokenSecret = tokenSecret;
  }

  public byte[] getEncryptedSpillKey() {
    return encryptedSpillKey;
  }

  public void setEncryptedSpillKey(byte[] encryptedSpillKey) {
    if (encryptedSpillKey != null) {
      this.encryptedSpillKey = encryptedSpillKey;
    }
  }

    out.writeBoolean(writeSkipRecs);
    out.writeBoolean(taskCleanup);
    Text.writeString(out, user);
    out.writeInt(encryptedSpillKey.length);
    out.write(encryptedSpillKey);
    extraData.write(out);
  }
  
      setPhase(TaskStatus.Phase.CLEANUP);
    }
    user = StringInterner.weakIntern(Text.readString(in));
    int len = in.readInt();
    encryptedSpillKey = new byte[len];
    in.readFully(encryptedSpillKey);
    extraData.readFields(in);
  }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/CryptoUtils.java
import org.apache.hadoop.fs.crypto.CryptoFSDataInputStream;
import org.apache.hadoop.fs.crypto.CryptoFSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.LimitInputStream;

  private static final Log LOG = LogFactory.getLog(CryptoUtils.class);

  public static boolean isEncryptedSpillEnabled(Configuration conf) {
    return conf.getBoolean(MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA,
        MRJobConfig.DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA);
  }
  public static byte[] createIV(Configuration conf) throws IOException {
    CryptoCodec cryptoCodec = CryptoCodec.getInstance(conf);
    if (isEncryptedSpillEnabled(conf)) {
      byte[] iv = new byte[cryptoCodec.getCipherSuite().getAlgorithmBlockSize()];
      cryptoCodec.generateSecureRandom(iv);
      return iv;

  public static int cryptoPadding(Configuration conf) {
    return isEncryptedSpillEnabled(conf) ? CryptoCodec.getInstance(conf)
        .getCipherSuite().getAlgorithmBlockSize() + 8 : 0;
  }

  private static byte[] getEncryptionKey() throws IOException {
    return TokenCache.getEncryptedSpillKey(UserGroupInformation.getCurrentUser()
            .getCredentials());
  }

  public static FSDataOutputStream wrapIfNecessary(Configuration conf,
      FSDataOutputStream out) throws IOException {
    if (isEncryptedSpillEnabled(conf)) {
      out.write(ByteBuffer.allocate(8).putLong(out.getPos()).array());
      byte[] iv = createIV(conf);
      out.write(iv);
  public static InputStream wrapIfNecessary(Configuration conf, InputStream in,
      long length) throws IOException {
    if (isEncryptedSpillEnabled(conf)) {
      int bufferSize = getBufferSize(conf);
      if (length > -1) {
        in = new LimitInputStream(in, length);
  public static FSDataInputStream wrapIfNecessary(Configuration conf,
      FSDataInputStream in) throws IOException {
    if (isEncryptedSpillEnabled(conf)) {
      CryptoCodec cryptoCodec = CryptoCodec.getInstance(conf);
      int bufferSize = getBufferSize(conf);

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/JobSubmitter.java
package org.apache.hadoop.mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import static org.apache.hadoop.mapred.QueueManager.toFullPropertyName;

import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.TokenCache;
      if (TokenCache.getShuffleSecretKey(job.getCredentials()) == null) {
        KeyGenerator keyGen;
        try {
          keyGen = KeyGenerator.getInstance(SHUFFLE_KEYGEN_ALGORITHM);
          keyGen.init(SHUFFLE_KEY_LENGTH);
        } catch (NoSuchAlgorithmException e) {
          throw new IOException("Error generating shuffle secret key", e);
        }
        TokenCache.setShuffleSecretKey(shuffleKey.getEncoded(),
            job.getCredentials());
      }
      if (CryptoUtils.isEncryptedSpillEnabled(conf)) {
        conf.setInt(MRJobConfig.MR_AM_MAX_ATTEMPTS, 1);
        LOG.warn("Max job attempts set to 1 since encrypted intermediate" +
                "data spill is enabled");
      }

      copyAndConfigureFiles(job, submitJobDir);


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/security/TokenCache.java
  public static final String JOB_TOKENS_FILENAME = "mapreduce.job.jobTokenFile";
  private static final Text JOB_TOKEN = new Text("JobToken");
  private static final Text SHUFFLE_TOKEN = new Text("MapReduceShuffleToken");
  private static final Text ENC_SPILL_KEY = new Text("MapReduceEncryptedSpillKey");
  
    return getSecretKey(credentials, SHUFFLE_TOKEN);
  }

  @InterfaceAudience.Private
  public static void setEncryptedSpillKey(byte[] key, Credentials credentials) {
    credentials.addSecretKey(ENC_SPILL_KEY, key);
  }

  @InterfaceAudience.Private
  public static byte[] getEncryptedSpillKey(Credentials credentials) {
    return getSecretKey(credentials, ENC_SPILL_KEY);
  }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/task/reduce/LocalFetcher.java
    long compressedLength = ir.partLength;
    long decompressedLength = ir.rawLength;

    compressedLength -= CryptoUtils.cryptoPadding(job);
    decompressedLength -= CryptoUtils.cryptoPadding(job);

    MapOutput<K, V> mapOutput = merger.reserve(mapTaskId, decompressedLength,
        id);
    inStream = CryptoUtils.wrapIfNecessary(job, inStream);

    try {
      inStream.seek(ir.startOffset + CryptoUtils.cryptoPadding(job));
      mapOutput.shuffle(LOCALHOST, inStream, compressedLength, decompressedLength, metrics, reporter);
    } finally {
      try {

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/test/java/org/apache/hadoop/mapreduce/task/reduce/TestMerger.java
    jobConf.setBoolean(MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA, true);
    conf.setBoolean(MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA, true);
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    TokenCache.setEncryptedSpillKey(new byte[16], credentials);
    UserGroupInformation.getCurrentUser().addCredentials(credentials);
    testInMemoryAndOnDiskMerger();
  }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapred/TestMRIntermediateDataEncryption.java

  @Test
  public void testSingleReducer() throws Exception {
    doEncryptionTest(3, 1, 2, false);
  }

  @Test
  public void testUberMode() throws Exception {
    doEncryptionTest(3, 1, 2, true);
  }

  @Test
  public void testMultipleMapsPerNode() throws Exception {
    doEncryptionTest(8, 1, 2, false);
  }

  @Test
  public void testMultipleReducers() throws Exception {
    doEncryptionTest(2, 4, 2, false);
  }

  public void doEncryptionTest(int numMappers, int numReducers, int numNodes,
                               boolean isUber) throws Exception {
    doEncryptionTest(numMappers, numReducers, numNodes, 1000, isUber);
  }

  public void doEncryptionTest(int numMappers, int numReducers, int numNodes,
                               int numLines, boolean isUber) throws Exception {
    MiniDFSCluster dfsCluster = null;
    MiniMRClientCluster mrCluster = null;
    FileSystem fileSystem = null;
      createInput(fileSystem, numMappers, numLines);
      runMergeTest(new JobConf(mrCluster.getConfig()), fileSystem,
              numMappers, numReducers, numLines, isUber);
    } finally {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
    }
  }

  private void runMergeTest(JobConf job, FileSystem fileSystem, int
          numMappers, int numReducers, int numLines, boolean isUber)
          throws Exception {
    fileSystem.delete(OUTPUT, true);
    job.setJobName("Test");
    job.setInt("mapreduce.map.maxattempts", 1);
    job.setInt("mapreduce.reduce.maxattempts", 1);
    job.setInt("mapred.test.num_lines", numLines);
    if (isUber) {
      job.setBoolean("mapreduce.job.ubertask.enable", true);
    }
    job.setBoolean(MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA, true);
    try {
      submittedJob = client.submitJob(job);

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapred/TestMapProgress.java
    throws IOException, InterruptedException {
      StringBuffer buf = new StringBuffer("Task ");
      buf.append(taskId);
      if (taskStatus != null) {
        buf.append(" making progress to ");
        buf.append(taskStatus.getProgress());
        String state = taskStatus.getStateString();
          buf.append(" and state of ");
          buf.append(state);
        }
      }
      LOG.info(buf.toString());

