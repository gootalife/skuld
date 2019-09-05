hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/DistCpConstants.java
public class DistCpConstants {

  public static final int DEFAULT_LISTSTATUS_THREADS = 1;

  public static final int DEFAULT_MAPS = 20;

  public static final String CONF_LABEL_SYNC_FOLDERS = "distcp.sync.folders";
  public static final String CONF_LABEL_DELETE_MISSING = "distcp.delete.missing.source";
  public static final String CONF_LABEL_SSL_CONF = "distcp.keystore.resource";
  public static final String CONF_LABEL_LISTSTATUS_THREADS = "distcp.liststatus.threads";
  public static final String CONF_LABEL_MAX_MAPS = "distcp.max.maps";
  public static final String CONF_LABEL_SOURCE_LISTING = "distcp.source.listing";
  public static final String CONF_LABEL_COPY_STRATEGY = "distcp.copy.strategy";

hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/DistCpOptionSwitch.java
  SSL_CONF(DistCpConstants.CONF_LABEL_SSL_CONF,
      new Option("mapredSslConf", true, "Configuration for ssl config file" +
          ", to use with hftps://")),
  NUM_LISTSTATUS_THREADS(DistCpConstants.CONF_LABEL_LISTSTATUS_THREADS,
      new Option("numListstatusThreads", true, "Number of threads to " +
          "use for building file listing (max " +
          DistCpOptions.maxNumListstatusThreads + ").")),

hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/DistCpOptions.java
  private boolean blocking = true;
  private boolean useDiff = false;

  public static final int maxNumListstatusThreads = 40;
  private int numListstatusThreads = 0;  // Indicates that flag is not set.
  private int maxMaps = DistCpConstants.DEFAULT_MAPS;
  private int mapBandwidth = DistCpConstants.DEFAULT_BANDWIDTH_MB;

      this.overwrite = that.overwrite;
      this.skipCRC = that.skipCRC;
      this.blocking = that.blocking;
      this.numListstatusThreads = that.numListstatusThreads;
      this.maxMaps = that.maxMaps;
      this.mapBandwidth = that.mapBandwidth;
      this.sslConfigurationFile = that.getSslConfigurationFile();
    this.skipCRC = skipCRC;
  }

  public int getNumListstatusThreads() {
    return numListstatusThreads;
  }

  public void setNumListstatusThreads(int numThreads) {
    if (numThreads > maxNumListstatusThreads) {
      this.numListstatusThreads = maxNumListstatusThreads;
    } else if (numThreads > 0) {
      this.numListstatusThreads = numThreads;
    } else {
      this.numListstatusThreads = 0;
    }
  }


hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/OptionsParser.java
          getOptionValue(DistCpOptionSwitch.SSL_CONF.getSwitch()));
    }

    if (command.hasOption(DistCpOptionSwitch.NUM_LISTSTATUS_THREADS.getSwitch())) {
      try {
        Integer numThreads = Integer.parseInt(getVal(command,
              DistCpOptionSwitch.NUM_LISTSTATUS_THREADS.getSwitch()).trim());
        option.setNumListstatusThreads(numThreads);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Number of liststatus threads is invalid: " + getVal(command,
                DistCpOptionSwitch.NUM_LISTSTATUS_THREADS.getSwitch()), e);
      }
    }

    if (command.hasOption(DistCpOptionSwitch.MAX_MAPS.getSwitch())) {
      try {
        Integer maps = Integer.parseInt(

hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/SimpleCopyListing.java
import org.apache.hadoop.io.Text;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.tools.util.ProducerConsumer;
import org.apache.hadoop.tools.util.WorkReport;
import org.apache.hadoop.tools.util.WorkRequest;
import org.apache.hadoop.tools.util.WorkRequestProcessor;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;

import com.google.common.annotations.VisibleForTesting;

import java.io.*;
import java.util.ArrayList;

import static org.apache.hadoop.tools.DistCpConstants
        .HDFS_RESERVED_RAW_DIRECTORY_NAME;
  private static final Log LOG = LogFactory.getLog(SimpleCopyListing.class);

  private long totalPaths = 0;
  private long totalDirs = 0;
  private long totalBytesToCopy = 0;
  private int numListstatusThreads = 1;
  private final int maxRetries = 3;

  protected SimpleCopyListing(Configuration configuration, Credentials credentials) {
    super(configuration, credentials);
    numListstatusThreads = getConf().getInt(
        DistCpConstants.CONF_LABEL_LISTSTATUS_THREADS,
        DistCpConstants.DEFAULT_LISTSTATUS_THREADS);
  }

  @VisibleForTesting
  protected SimpleCopyListing(Configuration configuration, Credentials credentials,
                              int numListstatusThreads) {
    super(configuration, credentials);
    this.numListstatusThreads = numListstatusThreads;
  }

  @Override
  @VisibleForTesting
  public void doBuildListing(SequenceFile.Writer fileListWriter,
      DistCpOptions options) throws IOException {
    if (options.getNumListstatusThreads() > 0) {
      numListstatusThreads = options.getNumListstatusThreads();
    }

    try {
      for (Path path: options.getSourcePaths()) {
        FileSystem sourceFS = path.getFileSystem(getConf());
              sourcePathRoot, options);
        }
        if (explore) {
          ArrayList<FileStatus> sourceDirs = new ArrayList<FileStatus>();
          for (FileStatus sourceStatus: sourceFiles) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Recording source-path: " + sourceStatus.getPath() + " for copy.");

            if (sourceStatus.isDirectory()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Adding source dir for traverse: " + sourceStatus.getPath());
              }
              sourceDirs.add(sourceStatus);
            }
          }
          traverseDirectory(fileListWriter, sourceFS, sourceDirs,
                            sourcePathRoot, options);
        }
      }
      fileListWriter.close();
      printStats();
      LOG.info("Build file listing completed.");
      fileListWriter = null;
    } finally {
      IOUtils.cleanup(LOG, fileListWriter);
            SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
  }

  private static class FileStatusProcessor
      implements WorkRequestProcessor<FileStatus, FileStatus[]> {
    private FileSystem fileSystem;

    public FileStatusProcessor(FileSystem fileSystem) {
      this.fileSystem = fileSystem;
    }

    public WorkReport<FileStatus[]> processItem(
        WorkRequest<FileStatus> workRequest) {
      FileStatus parent = workRequest.getItem();
      int retry = workRequest.getRetry();
      WorkReport<FileStatus[]> result = null;
      try {
        if (retry > 0) {
          int sleepSeconds = 2;
          for (int i = 1; i < retry; i++) {
            sleepSeconds *= 2;
          }
          try {
            Thread.sleep(1000 * sleepSeconds);
          } catch (InterruptedException ie) {
            LOG.debug("Interrupted while sleeping in exponential backoff.");
          }
        }
        result = new WorkReport<FileStatus[]>(
            fileSystem.listStatus(parent.getPath()), 0, true);
      } catch (FileNotFoundException fnf) {
        LOG.error("FileNotFoundException exception in listStatus: " +
                  fnf.getMessage());
        result = new WorkReport<FileStatus[]>(new FileStatus[0], 0, true, fnf);
      } catch (Exception e) {
        LOG.error("Exception in listStatus. Will send for retry.");
        FileStatus[] parentList = new FileStatus[1];
        parentList[0] = parent;
        result = new WorkReport<FileStatus[]>(parentList, retry + 1, false, e);
      }
      return result;
    }
  }

  private void printStats() {
    LOG.info("Paths (files+dirs) cnt = " + totalPaths +
             "; dirCnt = " + totalDirs);
  }

  private void maybePrintStats() {
    if (totalPaths % 100000 == 0) {
      printStats();
    }
  }

  private void traverseDirectory(SequenceFile.Writer fileListWriter,
                                 FileSystem sourceFS,
                                 ArrayList<FileStatus> sourceDirs,
                                 Path sourcePathRoot,
                                 DistCpOptions options)
                                 throws IOException {
    final boolean preserveAcls = options.shouldPreserve(FileAttribute.ACL);
    final boolean preserveXAttrs = options.shouldPreserve(FileAttribute.XATTR);
    final boolean preserveRawXattrs = options.shouldPreserveRawXattrs();

    assert numListstatusThreads > 0;
    LOG.debug("Starting thread pool of " + numListstatusThreads +
              " listStatus workers.");
    ProducerConsumer<FileStatus, FileStatus[]> workers =
        new ProducerConsumer<FileStatus, FileStatus[]>(numListstatusThreads);
    for (int i = 0; i < numListstatusThreads; i++) {
      workers.addWorker(
          new FileStatusProcessor(sourcePathRoot.getFileSystem(getConf())));
    }

    for (FileStatus status : sourceDirs) {
      workers.put(new WorkRequest<FileStatus>(status, 0));
      maybePrintStats();
    }

    while (workers.hasWork()) {
      try {
        WorkReport<FileStatus[]> workResult = workers.take();
        int retry = workResult.getRetry();
        for (FileStatus child: workResult.getItem()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Recording source-path: " + child.getPath() + " for copy.");
          }
          if (retry == 0) {
            CopyListingFileStatus childCopyListingStatus =
              DistCpUtils.toCopyListingFileStatus(sourceFS, child,
                preserveAcls && child.isDirectory(),
                preserveRawXattrs && child.isDirectory());
            writeToFileListing(fileListWriter, childCopyListingStatus,
                 sourcePathRoot, options);
          }
          if (retry < maxRetries) {
            if (child.isDirectory()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Traversing into source dir: " + child.getPath());
              }
              workers.put(new WorkRequest<FileStatus>(child, retry));
              maybePrintStats();
            }
          } else {
            LOG.error("Giving up on " + child.getPath() +
                      " after " + retry + " retries.");
          }
        }
      } catch (InterruptedException ie) {
        LOG.error("Could not get item from childQueue. Retrying...");
      }
    }
    workers.shutdown();
  }

  private void writeToFileListingRoot(SequenceFile.Writer fileListWriter,
      CopyListingFileStatus fileStatus, Path sourcePathRoot,

    if (!fileStatus.isDirectory()) {
      totalBytesToCopy += fileStatus.getLen();
    } else {
      totalDirs++;
    }
    totalPaths++;
  }

hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/util/ProducerConsumer.java
++ b/hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/util/ProducerConsumer.java

package org.apache.hadoop.tools.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.util.WorkReport;
import org.apache.hadoop.tools.util.WorkRequest;
import org.apache.hadoop.tools.util.WorkRequestProcessor;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class ProducerConsumer<T, R> {
  private Log LOG = LogFactory.getLog(ProducerConsumer.class);
  private LinkedBlockingQueue<WorkRequest<T>> inputQueue;
  private LinkedBlockingQueue<WorkReport<R>> outputQueue;
  private ExecutorService executor;
  private AtomicInteger workCnt;

  public ProducerConsumer(int numThreads) {
    this.inputQueue = new LinkedBlockingQueue<WorkRequest<T>>();
    this.outputQueue = new LinkedBlockingQueue<WorkReport<R>>();
    executor = Executors.newFixedThreadPool(numThreads);
    workCnt = new AtomicInteger(0);
  }

  public void addWorker(WorkRequestProcessor<T, R> processor) {
    executor.execute(new Worker(processor));
  }

  public void shutdown() {
    executor.shutdown();
  }

  public int getWorkCnt() {
    return workCnt.get();
  }

  public boolean hasWork() {
    return workCnt.get() > 0;
  }

  public void put(WorkRequest<T> workRequest) {
    boolean isDone = false;
    while (!isDone) {
      try {
        inputQueue.put(workRequest);
        workCnt.incrementAndGet();
        isDone = true;
      } catch (InterruptedException ie) {
        LOG.error("Could not put workRequest into inputQueue. Retrying...");
      }
    }
  }

  public WorkReport<R> take() throws InterruptedException {
    WorkReport<R> report = outputQueue.take();
    workCnt.decrementAndGet();
    return report;
  }

  public WorkReport<R> blockingTake() {
    while (true) {
      try {
        WorkReport<R> report = outputQueue.take();
        workCnt.decrementAndGet();
        return report;
      } catch (InterruptedException ie) {
        LOG.debug("Retrying in blockingTake...");
      }
    }
  }

  private class Worker implements Runnable {
    private WorkRequestProcessor<T, R> processor;

    public Worker(WorkRequestProcessor<T, R> processor) {
      this.processor = processor;
    }

    public void run() {
      while (true) {
        try {
          WorkRequest<T> work = inputQueue.take();
          WorkReport<R> result = processor.processItem(work);

          boolean isDone = false;
          while (!isDone) {
            try {
              outputQueue.put(result);
              isDone = true;
            } catch (InterruptedException ie) {
              LOG.debug("Could not put report into outputQueue. Retrying...");
            }
          }
        } catch (InterruptedException ie) {
          LOG.debug("Interrupted while waiting for request from inputQueue.");
        }
      }
    }
  }
}

hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/util/WorkReport.java
++ b/hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/util/WorkReport.java

package org.apache.hadoop.tools.util;

public class WorkReport<T> {
  private T item;
  private final boolean success;
  private final int retry;
  private final Exception exception;

  public WorkReport(T item, int retry, boolean success) {
    this(item, retry, success, null);
  }

  public WorkReport(T item, int retry, boolean success, Exception exception) {
    this.item = item;
    this.retry = retry;
    this.success = success;
    this.exception = exception;
  }

  public T getItem() {
    return item;
  }

  public boolean getSuccess() {
    return success;
  }

  public int getRetry() {
    return retry;
  }

  public Exception getException() {
    return exception;
  }
}

hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/util/WorkRequest.java
++ b/hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/util/WorkRequest.java

package org.apache.hadoop.tools.util;

public class WorkRequest<T> {
  private int retry;
  private T item;

  public WorkRequest(T item) {
    this(item, 0);
  }

  public WorkRequest(T item, int retry) {
    this.item = item;
    this.retry = retry;
  }

  public T getItem() {
    return item;
  }

  public int getRetry() {
    return retry;
  }
}

hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/util/WorkRequestProcessor.java
++ b/hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/util/WorkRequestProcessor.java

package org.apache.hadoop.tools.util;

import org.apache.hadoop.tools.util.WorkReport;
import org.apache.hadoop.tools.util.WorkRequest;

public interface WorkRequestProcessor<T, R> {

  public WorkReport<R> processItem(WorkRequest<T> workRequest);
}

hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/TestCopyListing.java
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.Test;
import org.junit.Assert;
import org.junit.BeforeClass;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(value = Parameterized.class)
public class TestCopyListing extends SimpleCopyListing {
  private static final Log LOG = LogFactory.getLog(TestCopyListing.class);

    }
  }

  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] { { 1 }, { 2 }, { 10 }, { 20} };
    return Arrays.asList(data);
  }

  public TestCopyListing(int numListstatusThreads) {
    super(config, CREDENTIALS, numListstatusThreads);
  }

  protected TestCopyListing(Configuration configuration) {

hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/TestIntegration.java
import org.apache.hadoop.tools.util.TestDistCpUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(value = Parameterized.class)
public class TestIntegration {
  private static final Log LOG = LogFactory.getLog(TestIntegration.class);

  private static Path listFile;
  private static Path target;
  private static String root;
  private int numListstatusThreads;

  public TestIntegration(int numListstatusThreads) {
    this.numListstatusThreads = numListstatusThreads;
  }

  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] { { 1 }, { 2 }, { 10 } };
    return Arrays.asList(data);
  }

  private static Configuration getConf() {
    Configuration conf = new Configuration();
    options.setDeleteMissing(delete);
    options.setOverwrite(overwrite);
    options.setTargetPathExists(targetExists);
    options.setNumListstatusThreads(numListstatusThreads);
    try {
      new DistCp(getConf(), options).execute();
    } catch (Exception e) {

hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/TestOptionsParser.java
    } catch (IllegalArgumentException ignore) { }
  }

  @Test
  public void testParseNumListstatusThreads() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertEquals(0, options.getNumListstatusThreads());

    options = OptionsParser.parse(new String[] {
        "--numListstatusThreads",
        "12",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertEquals(12, options.getNumListstatusThreads());

    options = OptionsParser.parse(new String[] {
        "--numListstatusThreads",
        "0",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertEquals(0, options.getNumListstatusThreads());

    try {
      OptionsParser.parse(new String[] {
          "--numListstatusThreads",
          "hello",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target/"});
      Assert.fail("Non numberic numListstatusThreads parsed");
    } catch (IllegalArgumentException ignore) { }

    options = OptionsParser.parse(new String[] {
        "--numListstatusThreads",
        "100",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertEquals(DistCpOptions.maxNumListstatusThreads,
                        options.getNumListstatusThreads());
  }

  @Test
  public void testSourceListing() {
    DistCpOptions options = OptionsParser.parse(new String[] {

hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/util/TestProducerConsumer.java
++ b/hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/util/TestProducerConsumer.java

package org.apache.hadoop.tools.util;

import org.apache.hadoop.tools.util.ProducerConsumer;
import org.apache.hadoop.tools.util.WorkReport;
import org.apache.hadoop.tools.util.WorkRequest;
import org.apache.hadoop.tools.util.WorkRequestProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.lang.Exception;
import java.lang.Integer;

public class TestProducerConsumer {
  public class CopyProcessor implements WorkRequestProcessor<Integer, Integer> {
    public WorkReport<Integer> processItem(WorkRequest<Integer> workRequest) {
      Integer item = new Integer(workRequest.getItem());
      return new WorkReport<Integer>(item, 0, true);
    }
  }

  public class ExceptionProcessor implements WorkRequestProcessor<Integer, Integer> {
    @SuppressWarnings("null")
    public WorkReport<Integer> processItem(WorkRequest<Integer> workRequest) {
      try {
        Integer item = null;
        item.intValue(); // Throw NULL pointer exception.

        return new WorkReport<Integer>(item, 0, true);
      } catch (Exception e) {
        Integer item = new Integer(workRequest.getItem());
        return new WorkReport<Integer>(item, 1, false, e);
      }
    }
  }

  @Test
  public void testSimpleProducerConsumer() {
    ProducerConsumer<Integer, Integer> worker =
        new ProducerConsumer<Integer, Integer>(1);
    worker.addWorker(new CopyProcessor());
    worker.put(new WorkRequest<Integer>(42));
    try {
      WorkReport<Integer> report = worker.take();
      Assert.assertEquals(42, report.getItem().intValue());
    } catch (InterruptedException ie) {
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testMultipleProducerConsumer() {
    ProducerConsumer<Integer, Integer> workers =
        new ProducerConsumer<Integer, Integer>(10);
    for (int i = 0; i < 10; i++) {
      workers.addWorker(new CopyProcessor());
    }

    int sum = 0;
    int numRequests = 2000;
    for (int i = 0; i < numRequests; i++) {
      workers.put(new WorkRequest<Integer>(i + 42));
      sum += i + 42;
    }

    int numReports = 0;
    while (workers.getWorkCnt() > 0) {
      WorkReport<Integer> report = workers.blockingTake();
      sum -= report.getItem().intValue();
      numReports++;
    }
    Assert.assertEquals(0, sum);
    Assert.assertEquals(numRequests, numReports);
  }

  @Test
  public void testExceptionProducerConsumer() {
    ProducerConsumer<Integer, Integer> worker =
        new ProducerConsumer<Integer, Integer>(1);
    worker.addWorker(new ExceptionProcessor());
    worker.put(new WorkRequest<Integer>(42));
    try {
      WorkReport<Integer> report = worker.take();
      Assert.assertEquals(42, report.getItem().intValue());
      Assert.assertFalse(report.getSuccess());
      Assert.assertNotNull(report.getException());
    } catch (InterruptedException ie) {
      Assert.assertTrue(false);
    }
  }
}

