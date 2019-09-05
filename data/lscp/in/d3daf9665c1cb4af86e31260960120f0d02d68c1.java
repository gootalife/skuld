hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/main/java/org/apache/hadoop/mapred/ResourceMgrDelegate.java
    return client.getApplications(applicationTypes, applicationStates);
  }

  @Override
  public List<ApplicationReport> getApplications(Set<String> queues,
      Set<String> users, Set<String> applicationTypes,
      EnumSet<YarnApplicationState> applicationStates) throws YarnException,
      IOException {
    return client.getApplications(queues, users, applicationTypes,
      applicationStates);
  }

  @Override
  public YarnClusterMetrics getYarnClusterMetrics() throws YarnException,
      IOException {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/QueueInfo.java
      float maximumCapacity, float currentCapacity,
      List<QueueInfo> childQueues, List<ApplicationReport> applications,
      QueueState queueState, Set<String> accessibleNodeLabels,
      String defaultNodeLabelExpression, QueueStatistics queueStatistics) {
    QueueInfo queueInfo = Records.newRecord(QueueInfo.class);
    queueInfo.setQueueName(queueName);
    queueInfo.setCapacity(capacity);
    queueInfo.setQueueState(queueState);
    queueInfo.setAccessibleNodeLabels(accessibleNodeLabels);
    queueInfo.setDefaultNodeLabelExpression(defaultNodeLabelExpression);
    queueInfo.setQueueStatistics(queueStatistics);
    return queueInfo;
  }

  @Stable
  public abstract void setDefaultNodeLabelExpression(
      String defaultLabelExpression);

  @Public
  @Unstable
  public abstract QueueStatistics getQueueStatistics();

  @Public
  @Unstable
  public abstract void setQueueStatistics(QueueStatistics queueStatistics);

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/QueueStatistics.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/QueueStatistics.java

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.util.Records;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class QueueStatistics {

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static QueueStatistics newInstance(long submitted, long running,
      long pending, long completed, long killed, long failed, long activeUsers,
      long availableMemoryMB, long allocatedMemoryMB, long pendingMemoryMB,
      long reservedMemoryMB, long availableVCores, long allocatedVCores,
      long pendingVCores, long reservedVCores) {
    QueueStatistics statistics = Records.newRecord(QueueStatistics.class);
    statistics.setNumAppsSubmitted(submitted);
    statistics.setNumAppsRunning(running);
    statistics.setNumAppsPending(pending);
    statistics.setNumAppsCompleted(completed);
    statistics.setNumAppsKilled(killed);
    statistics.setNumAppsFailed(failed);
    statistics.setNumActiveUsers(activeUsers);
    statistics.setAvailableMemoryMB(availableMemoryMB);
    statistics.setAllocatedMemoryMB(allocatedMemoryMB);
    statistics.setPendingMemoryMB(pendingMemoryMB);
    statistics.setReservedMemoryMB(reservedMemoryMB);
    statistics.setAvailableVCores(availableVCores);
    statistics.setAllocatedVCores(allocatedVCores);
    statistics.setPendingVCores(pendingVCores);
    statistics.setReservedVCores(reservedVCores);
    return statistics;
  }

  public abstract long getNumAppsSubmitted();

  public abstract void setNumAppsSubmitted(long numAppsSubmitted);

  public abstract long getNumAppsRunning();

  public abstract void setNumAppsRunning(long numAppsRunning);

  public abstract long getNumAppsPending();

  public abstract void setNumAppsPending(long numAppsPending);

  public abstract long getNumAppsCompleted();

  public abstract void setNumAppsCompleted(long numAppsCompleted);

  public abstract long getNumAppsKilled();

  public abstract void setNumAppsKilled(long numAppsKilled);

  public abstract long getNumAppsFailed();

  public abstract void setNumAppsFailed(long numAppsFailed);

  public abstract long getNumActiveUsers();

  public abstract void setNumActiveUsers(long numActiveUsers);

  public abstract long getAvailableMemoryMB();

  public abstract void setAvailableMemoryMB(long availableMemoryMB);

  public abstract long getAllocatedMemoryMB();

  public abstract void setAllocatedMemoryMB(long allocatedMemoryMB);

  public abstract long getPendingMemoryMB();

  public abstract void setPendingMemoryMB(long pendingMemoryMB);

  public abstract long getReservedMemoryMB();

  public abstract void setReservedMemoryMB(long reservedMemoryMB);

  public abstract long getAvailableVCores();

  public abstract void setAvailableVCores(long availableVCores);

  public abstract long getAllocatedVCores();

  public abstract void setAllocatedVCores(long allocatedVCores);

  public abstract long getPendingVCores();

  public abstract void setPendingVCores(long pendingVCores);

  public abstract long getReservedVCores();

  public abstract void setReservedVCores(long reservedVCores);
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/YarnClusterMetrics.java
  @Unstable
  public abstract void setNumNodeManagers(int numNodeManagers);

  @Public
  @Unstable
  public abstract int getNumDecommissionedNodeManagers();

  @Private
  @Unstable
  public abstract void setNumDecommissionedNodeManagers(
      int numDecommissionedNodeManagers);

  @Public
  @Unstable
  public abstract int getNumActiveNodeManagers();

  @Private
  @Unstable
  public abstract void setNumActiveNodeManagers(int numActiveNodeManagers);

  @Public
  @Unstable
  public abstract int getNumLostNodeManagers();

  @Private
  @Unstable
  public abstract void setNumLostNodeManagers(int numLostNodeManagers);

  @Public
  @Unstable
  public abstract int getNumUnhealthyNodeManagers();

  @Private
  @Unstable
  public abstract void setNumUnhealthyNodeManagers(int numUnhealthNodeManagers);

  @Public
  @Unstable
  public abstract int getNumRebootedNodeManagers();

  @Private
  @Unstable
  public abstract void setNumRebootedNodeManagers(int numRebootedNodeManagers);

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/api/YarnClient.java
      EnumSet<YarnApplicationState> applicationStates) throws YarnException,
      IOException;

  public abstract List<ApplicationReport> getApplications(Set<String> queues,
      Set<String> users, Set<String> applicationTypes,
      EnumSet<YarnApplicationState> applicationStates) throws YarnException,
      IOException;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/api/impl/YarnClientImpl.java
    return response.getApplicationList();
  }

  @Override
  public List<ApplicationReport> getApplications(Set<String> queues,
      Set<String> users, Set<String> applicationTypes,
      EnumSet<YarnApplicationState> applicationStates) throws YarnException,
      IOException {
    GetApplicationsRequest request =
        GetApplicationsRequest.newInstance(applicationTypes, applicationStates);
    request.setQueues(queues);
    request.setUsers(users);
    GetApplicationsResponse response = rmClient.getApplications(request);
    return response.getApplicationList();
  }

  @Override
  public YarnClusterMetrics getYarnClusterMetrics() throws YarnException,
      IOException {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/cli/TopCLI.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/cli/TopCLI.java

package org.apache.hadoop.yarn.client.cli;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.codehaus.jettison.json.JSONObject;

public class TopCLI extends YarnCLI {

  private static final Log LOG = LogFactory.getLog(TopCLI.class);
  private String CLEAR = "\u001b[2J";
  private String CLEAR_LINE = "\u001b[2K";
  private String SET_CURSOR_HOME = "\u001b[H";
  private String CHANGE_BACKGROUND = "\u001b[7m";
  private String RESET_BACKGROUND = "\u001b[0m";
  private String SET_CURSOR_LINE_6_COLUMN_0 = "\u001b[6;0f";

  protected Cache<GetApplicationsRequest, List<ApplicationReport>>
      applicationReportsCache = CacheBuilder.newBuilder().maximumSize(1000)
        .expireAfterWrite(5, TimeUnit.SECONDS).build();

  enum DisplayScreen {
    TOP, HELP, SORT, FIELDS
  }

  enum Columns { // in the order in which they should be displayed
    APPID, USER, TYPE, QUEUE, PRIORITY, CONT, RCONT, VCORES, RVCORES, MEM,
    RMEM, VCORESECS, MEMSECS, PROGRESS, TIME, NAME
  }

  static class ColumnInformation {
    String header;
    String format;
    boolean display; // should we show this field or not
    String description;
    String key; // key to press for sorting/toggling field

    public ColumnInformation(String header, String format, boolean display,
        String description, String key) {
      this.header = header;
      this.format = format;
      this.display = display;
      this.description = description;
      this.key = key;
    }
  }

  private static class ApplicationInformation {
    final String appid;
    final String user;
    final String type;
    final int priority;
    final int usedContainers;
    final int reservedContainers;
    final long usedMemory;
    final long reservedMemory;
    final int usedVirtualCores;
    final int reservedVirtualCores;
    final int attempts;
    final float progress;
    final String state;
    long runningTime;
    final String time;
    final String name;
    final int nodes;
    final String queue;
    final long memorySeconds;
    final long vcoreSeconds;

    final EnumMap<Columns, String> displayStringsMap;

    ApplicationInformation(ApplicationReport appReport) {
      displayStringsMap = new EnumMap<>(Columns.class);
      appid = appReport.getApplicationId().toString();
      displayStringsMap.put(Columns.APPID, appid);
      user = appReport.getUser();
      displayStringsMap.put(Columns.USER, user);
      type = appReport.getApplicationType().toLowerCase();
      displayStringsMap.put(Columns.TYPE, type);
      state = appReport.getYarnApplicationState().toString().toLowerCase();
      name = appReport.getName();
      displayStringsMap.put(Columns.NAME, name);
      queue = appReport.getQueue();
      displayStringsMap.put(Columns.QUEUE, queue);
      priority = 0;
      usedContainers =
          appReport.getApplicationResourceUsageReport().getNumUsedContainers();
      displayStringsMap.put(Columns.CONT, String.valueOf(usedContainers));
      reservedContainers =
          appReport.getApplicationResourceUsageReport()
            .getNumReservedContainers();
      displayStringsMap.put(Columns.RCONT, String.valueOf(reservedContainers));
      usedVirtualCores =
          appReport.getApplicationResourceUsageReport().getUsedResources()
            .getVirtualCores();
      displayStringsMap.put(Columns.VCORES, String.valueOf(usedVirtualCores));
      usedMemory =
          appReport.getApplicationResourceUsageReport().getUsedResources()
            .getMemory() / 1024;
      displayStringsMap.put(Columns.MEM, String.valueOf(usedMemory) + "G");
      reservedVirtualCores =
          appReport.getApplicationResourceUsageReport().getReservedResources()
            .getVirtualCores();
      displayStringsMap.put(Columns.RVCORES,
          String.valueOf(reservedVirtualCores));
      reservedMemory =
          appReport.getApplicationResourceUsageReport().getReservedResources()
            .getMemory() / 1024;
      displayStringsMap.put(Columns.RMEM, String.valueOf(reservedMemory) + "G");
      attempts = appReport.getCurrentApplicationAttemptId().getAttemptId();
      nodes = 0;
      runningTime = Time.now() - appReport.getStartTime();
      time = DurationFormatUtils.formatDuration(runningTime, "dd:HH:mm");
      displayStringsMap.put(Columns.TIME, String.valueOf(time));
      progress = appReport.getProgress() * 100;
      displayStringsMap.put(Columns.PROGRESS, String.format("%.2f", progress));
      memorySeconds =
          appReport.getApplicationResourceUsageReport().getMemorySeconds() / 1024;
      displayStringsMap.put(Columns.MEMSECS, String.valueOf(memorySeconds));
      vcoreSeconds =
          appReport.getApplicationResourceUsageReport().getVcoreSeconds();
      displayStringsMap.put(Columns.VCORESECS, String.valueOf(vcoreSeconds));
    }
  }


  public static final Comparator<ApplicationInformation> AppIDComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.appid.compareTo(a2.appid);
        }
      };
  public static final Comparator<ApplicationInformation> UserComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.user.compareTo(a2.user);
        }
      };
  public static final Comparator<ApplicationInformation> AppTypeComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.type.compareTo(a2.type);
        }
      };
  public static final Comparator<ApplicationInformation> QueueNameComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.queue.compareTo(a2.queue);
        }
      };
  public static final Comparator<ApplicationInformation> UsedContainersComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.usedContainers - a2.usedContainers;
        }
      };
  public static final Comparator<ApplicationInformation> ReservedContainersComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.reservedContainers - a2.reservedContainers;
        }
      };
  public static final Comparator<ApplicationInformation> UsedMemoryComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return Long.valueOf(a1.usedMemory).compareTo(a2.usedMemory);
        }
      };
  public static final Comparator<ApplicationInformation> ReservedMemoryComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return Long.valueOf(a1.reservedMemory).compareTo(a2.reservedMemory);
        }
      };
  public static final Comparator<ApplicationInformation> UsedVCoresComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.usedVirtualCores - a2.usedVirtualCores;
        }
      };
  public static final Comparator<ApplicationInformation> ReservedVCoresComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.reservedVirtualCores - a2.reservedVirtualCores;
        }
      };
  public static final Comparator<ApplicationInformation> VCoreSecondsComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return Long.valueOf(a1.vcoreSeconds).compareTo(a2.vcoreSeconds);
        }
      };
  public static final Comparator<ApplicationInformation> MemorySecondsComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return Long.valueOf(a1.memorySeconds).compareTo(a2.memorySeconds);
        }
      };
  public static final Comparator<ApplicationInformation> ProgressComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return Float.compare(a1.progress, a2.progress);
        }
      };
  public static final Comparator<ApplicationInformation> RunningTimeComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return Long.valueOf(a1.runningTime).compareTo(a2.runningTime);
        }
      };
  public static final Comparator<ApplicationInformation> AppNameComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.name.compareTo(a2.name);
        }
      };

  private static class NodesInformation {
    int totalNodes;
    int runningNodes;
    int unhealthyNodes;
    int decommissionedNodes;
    int lostNodes;
    int rebootedNodes;
  }

  private static class QueueMetrics {
    long appsSubmitted;
    long appsRunning;
    long appsPending;
    long appsCompleted;
    long appsKilled;
    long appsFailed;
    long activeUsers;
    long availableMemoryGB;
    long allocatedMemoryGB;
    long pendingMemoryGB;
    long reservedMemoryGB;
    long availableVCores;
    long allocatedVCores;
    long pendingVCores;
    long reservedVCores;
  }

  private class KeyboardMonitor extends Thread {

    public void run() {
      Scanner keyboard = new Scanner(System.in, "UTF-8");
      while (runKeyboardMonitor.get()) {
        String in = keyboard.next();
        try {
          if (displayScreen == DisplayScreen.SORT) {
            handleSortScreenKeyPress(in);
          } else if (displayScreen == DisplayScreen.TOP) {
            handleTopScreenKeyPress(in);
          } else if (displayScreen == DisplayScreen.FIELDS) {
            handleFieldsScreenKeyPress(in);
          } else {
            handleHelpScreenKeyPress();
          }
        } catch (Exception e) {
          LOG.error("Caught exception", e);
        }
      }
    }
  }

  long refreshPeriod = 3 * 1000;
  int terminalWidth = -1;
  int terminalHeight = -1;
  String appsHeader;
  boolean ascendingSort;
  long rmStartTime;
  Comparator<ApplicationInformation> comparator;
  Options opts;
  CommandLine cliParser;

  Set<String> queues;
  Set<String> users;
  Set<String> types;

  DisplayScreen displayScreen;
  AtomicBoolean showingTopScreen;
  AtomicBoolean runMainLoop;
  AtomicBoolean runKeyboardMonitor;
  final Object lock = new Object();

  String currentSortField;

  Map<String, Columns> keyFieldsMap;
  List<String> sortedKeys;

  Thread displayThread;

  final EnumMap<Columns, ColumnInformation> columnInformationEnumMap;

  public TopCLI() throws IOException, InterruptedException {
    super();
    queues = new HashSet<>();
    users = new HashSet<>();
    types = new HashSet<>();
    comparator = UsedContainersComparator;
    ascendingSort = false;
    displayScreen = DisplayScreen.TOP;
    showingTopScreen = new AtomicBoolean();
    showingTopScreen.set(true);
    currentSortField = "c";
    keyFieldsMap = new HashMap<>();
    runKeyboardMonitor = new AtomicBoolean();
    runMainLoop = new AtomicBoolean();
    runKeyboardMonitor.set(true);
    runMainLoop.set(true);
    displayThread = Thread.currentThread();
    columnInformationEnumMap = new EnumMap<>(Columns.class);
    generateColumnInformationMap();
    generateKeyFieldsMap();
    sortedKeys = new ArrayList<>(keyFieldsMap.keySet());
    Collections.sort(sortedKeys);
    setTerminalSequences();
  }

  public static void main(String[] args) throws Exception {
    TopCLI topImp = new TopCLI();
    topImp.setSysOutPrintStream(System.out);
    topImp.setSysErrPrintStream(System.err);
    int res = ToolRunner.run(topImp, args);
    topImp.stop();
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    try {
      parseOptions(args);
      if (cliParser.hasOption("help")) {
        printUsage();
        return 0;
      }
    } catch (Exception e) {
      LOG.error("Unable to parse options", e);
      return 1;
    }
    setAppsHeader();

    Thread keyboardMonitor = new KeyboardMonitor();
    keyboardMonitor.start();

    rmStartTime = getRMStartTime();
    clearScreen();

    while (runMainLoop.get()) {
      if (displayScreen == DisplayScreen.TOP) {
        showTopScreen();
        try {
          Thread.sleep(refreshPeriod);
        } catch (InterruptedException ie) {
          break;
        }
      } else if (displayScreen == DisplayScreen.SORT) {
        showSortScreen();
        Thread.sleep(100);
      } else if (displayScreen == DisplayScreen.FIELDS) {
        showFieldsScreen();
        Thread.sleep(100);
      }
      if (rmStartTime == -1) {
        rmStartTime = getRMStartTime();
      }
    }
    clearScreen();
    return 0;
  }

  private void parseOptions(String[] args) throws ParseException, IOException,
      InterruptedException {

    opts = new Options();
    opts.addOption("queues", true,
      "Comma separated list of queues to restrict applications");
    opts.addOption("users", true,
      "Comma separated list of users to restrict applications");
    opts.addOption("types", true, "Comma separated list of types to restrict"
        + " applications, case sensitive(though the display is lower case)");
    opts.addOption("cols", true, "Number of columns on the terminal");
    opts.addOption("rows", true, "Number of rows on the terminal");
    opts.addOption("help", false,
      "Print usage; for help while the tool is running press 'h' + Enter");
    opts.addOption("delay", true,
      "The refresh delay(in seconds), default is 3 seconds");

    cliParser = new GnuParser().parse(opts, args);
    if (cliParser.hasOption("queues")) {
      String clqueues = cliParser.getOptionValue("queues");
      String[] queuesArray = clqueues.split(",");
      queues.addAll(Arrays.asList(queuesArray));
    }

    if (cliParser.hasOption("users")) {
      String clusers = cliParser.getOptionValue("users");
      users.addAll(Arrays.asList(clusers.split(",")));
    }

    if (cliParser.hasOption("types")) {
      String cltypes = cliParser.getOptionValue("types");
      types.addAll(Arrays.asList(cltypes.split(",")));
    }

    if (cliParser.hasOption("cols")) {
      terminalWidth = Integer.parseInt(cliParser.getOptionValue("cols"));
    } else {
      setTerminalWidth();
    }

    if (cliParser.hasOption("rows")) {
      terminalHeight = Integer.parseInt(cliParser.getOptionValue("rows"));
    } else {
      setTerminalHeight();
    }

    if (cliParser.hasOption("delay")) {
      int delay = Integer.parseInt(cliParser.getOptionValue("delay"));
      if (delay < 1) {
        LOG.warn("Delay set too low, using default");
      } else {
        refreshPeriod = delay * 1000;
      }
    }
  }

  private void printUsage() {
    new HelpFormatter().printHelp("yarn top", opts);
    System.out.println("");
    System.out.println("'yarn top' is a tool to help cluster administrators"
        + " understand cluster usage better.");
    System.out.println("Some notes about the implementation:");
    System.out.println("  1. Fetching information for all the apps is an"
        + " expensive call for the RM.");
    System.out.println("     To prevent a performance degradation, the results"
        + " are cached for 5 seconds,");
    System.out.println("     irrespective of the delay value. Information about"
        + " the NodeManager(s) and queue");
    System.out.println("     utilization stats are fetched at the specified"
        + " delay interval. Once we have a");
    System.out.println("     better understanding of the performance impact,"
        + " this might change.");
    System.out.println("  2. Since the tool is implemented in Java, you must"
        + " hit Enter for key presses to");
    System.out.println("     be processed.");
  }

  private void setAppsHeader() {
    List<String> formattedStrings = new ArrayList<>();
    for (EnumMap.Entry<Columns, ColumnInformation> entry :
        columnInformationEnumMap.entrySet()) {
      if (entry.getValue().display) {
        formattedStrings.add(String.format(entry.getValue().format,
          entry.getValue().header));
      }
    }
    appsHeader = StringUtils.join(formattedStrings.toArray(), " ");
    if (appsHeader.length() > terminalWidth) {
      appsHeader =
          appsHeader.substring(0, terminalWidth
              - System.lineSeparator().length());
    } else {
      appsHeader +=
          StringUtils.repeat(" ", terminalWidth - appsHeader.length()
              - System.lineSeparator().length());
    }
    appsHeader += System.lineSeparator();
  }

  private void setTerminalWidth() throws IOException, InterruptedException {
    if (terminalWidth != -1) {
      return;
    }
    String[] command = { "tput", "cols" };
    String op = getCommandOutput(command).trim();
    try {
      terminalWidth = Integer.parseInt(op);
    } catch (NumberFormatException ne) {
      LOG.warn("Couldn't determine terminal width, setting to 80", ne);
      terminalWidth = 80;
    }
  }

  private void setTerminalHeight() throws IOException, InterruptedException {
    if (terminalHeight != -1) {
      return;
    }
    String[] command = { "tput", "lines" };
    String op = getCommandOutput(command).trim();
    try {
      terminalHeight = Integer.parseInt(op);
    } catch (NumberFormatException ne) {
      LOG.warn("Couldn't determine terminal height, setting to 24", ne);
      terminalHeight = 24;
    }
  }

  protected void setTerminalSequences() throws IOException,
      InterruptedException {
    String[] tput_cursor_home = { "tput", "cup", "0", "0" };
    String[] tput_clear = { "tput", "clear" };
    String[] tput_clear_line = { "tput", "el" };
    String[] tput_set_cursor_line_6_column_0 = { "tput", "cup", "5", "0" };
    String[] tput_change_background = { "tput", "smso" };
    String[] tput_reset_background = { "tput", "rmso" };
    SET_CURSOR_HOME = getCommandOutput(tput_cursor_home);
    CLEAR = getCommandOutput(tput_clear);
    CLEAR_LINE = getCommandOutput(tput_clear_line);
    SET_CURSOR_LINE_6_COLUMN_0 =
        getCommandOutput(tput_set_cursor_line_6_column_0);
    CHANGE_BACKGROUND = getCommandOutput(tput_change_background);
    RESET_BACKGROUND = getCommandOutput(tput_reset_background);
  }

  private void generateColumnInformationMap() {
    columnInformationEnumMap.put(Columns.APPID, new ColumnInformation(
      "APPLICATIONID", "%31s", true, "Application Id", "a"));
    columnInformationEnumMap.put(Columns.USER, new ColumnInformation("USER",
      "%-10s", true, "Username", "u"));
    columnInformationEnumMap.put(Columns.TYPE, new ColumnInformation("TYPE",
      "%10s", true, "Application type", "t"));
    columnInformationEnumMap.put(Columns.QUEUE, new ColumnInformation("QUEUE",
      "%10s", true, "Application queue", "q"));
    columnInformationEnumMap.put(Columns.CONT, new ColumnInformation("#CONT",
      "%7s", true, "Number of containers", "c"));
    columnInformationEnumMap.put(Columns.RCONT, new ColumnInformation("#RCONT",
      "%7s", true, "Number of reserved containers", "r"));
    columnInformationEnumMap.put(Columns.VCORES, new ColumnInformation(
      "VCORES", "%7s", true, "Allocated vcores", "v"));
    columnInformationEnumMap.put(Columns.RVCORES, new ColumnInformation(
      "RVCORES", "%7s", true, "Reserved vcores", "o"));
    columnInformationEnumMap.put(Columns.MEM, new ColumnInformation("MEM",
      "%7s", true, "Allocated memory", "m"));
    columnInformationEnumMap.put(Columns.RMEM, new ColumnInformation("RMEM",
      "%7s", true, "Reserved memory", "w"));
    columnInformationEnumMap.put(Columns.VCORESECS, new ColumnInformation(
      "VCORESECS", "%10s", true, "Vcore seconds", "s"));
    columnInformationEnumMap.put(Columns.MEMSECS, new ColumnInformation(
      "MEMSECS", "%10s", true, "Memory seconds(in GBseconds)", "y"));
    columnInformationEnumMap.put(Columns.PROGRESS, new ColumnInformation(
      "%PROGR", "%6s", true, "Progress(percentage)", "p"));
    columnInformationEnumMap.put(Columns.TIME, new ColumnInformation("TIME",
      "%10s", true, "Running time", "i"));
    columnInformationEnumMap.put(Columns.NAME, new ColumnInformation("NAME",
      "%s", true, "Application name", "n"));
  }

  private void generateKeyFieldsMap() {
    for (EnumMap.Entry<Columns, ColumnInformation> entry :
        columnInformationEnumMap.entrySet()) {
      keyFieldsMap.put(entry.getValue().key, entry.getKey());
    }
  }

  protected NodesInformation getNodesInfo() {
    NodesInformation nodeInfo = new NodesInformation();
    YarnClusterMetrics yarnClusterMetrics;
    try {
      yarnClusterMetrics = client.getYarnClusterMetrics();
    } catch (IOException ie) {
      LOG.error("Unable to fetch cluster metrics", ie);
      return nodeInfo;
    } catch (YarnException ye) {
      LOG.error("Unable to fetch cluster metrics", ye);
      return nodeInfo;
    }

    nodeInfo.decommissionedNodes =
        yarnClusterMetrics.getNumDecommissionedNodeManagers();
    nodeInfo.totalNodes = yarnClusterMetrics.getNumNodeManagers();
    nodeInfo.runningNodes = yarnClusterMetrics.getNumActiveNodeManagers();
    nodeInfo.lostNodes = yarnClusterMetrics.getNumLostNodeManagers();
    nodeInfo.unhealthyNodes = yarnClusterMetrics.getNumUnhealthyNodeManagers();
    nodeInfo.rebootedNodes = yarnClusterMetrics.getNumRebootedNodeManagers();
    return nodeInfo;
  }

  protected QueueMetrics getQueueMetrics() {
    QueueMetrics queueMetrics = new QueueMetrics();
    List<QueueInfo> queuesInfo;
    if (queues.isEmpty()) {
      try {
        queuesInfo = client.getRootQueueInfos();
      } catch (Exception ie) {
        LOG.error("Unable to get queue information", ie);
        return queueMetrics;
      }
    } else {
      queuesInfo = new ArrayList<>();
      for (String queueName : queues) {
        try {
          QueueInfo qInfo = client.getQueueInfo(queueName);
          queuesInfo.add(qInfo);
        } catch (Exception ie) {
          LOG.error("Unable to get queue information", ie);
          return queueMetrics;
        }
      }
    }

    for (QueueInfo childInfo : queuesInfo) {
      QueueStatistics stats = childInfo.getQueueStatistics();
      if (stats != null) {
        queueMetrics.appsSubmitted += stats.getNumAppsSubmitted();
        queueMetrics.appsRunning += stats.getNumAppsRunning();
        queueMetrics.appsPending += stats.getNumAppsPending();
        queueMetrics.appsCompleted += stats.getNumAppsCompleted();
        queueMetrics.appsKilled += stats.getNumAppsKilled();
        queueMetrics.appsFailed += stats.getNumAppsFailed();
        queueMetrics.activeUsers += stats.getNumActiveUsers();
        queueMetrics.availableMemoryGB += stats.getAvailableMemoryMB();
        queueMetrics.allocatedMemoryGB += stats.getAllocatedMemoryMB();
        queueMetrics.pendingMemoryGB += stats.getPendingMemoryMB();
        queueMetrics.reservedMemoryGB += stats.getReservedMemoryMB();
        queueMetrics.availableVCores += stats.getAvailableVCores();
        queueMetrics.allocatedVCores += stats.getAllocatedVCores();
        queueMetrics.pendingVCores += stats.getPendingVCores();
        queueMetrics.reservedVCores += stats.getReservedVCores();
      }
    }
    queueMetrics.availableMemoryGB = queueMetrics.availableMemoryGB / 1024;
    queueMetrics.allocatedMemoryGB = queueMetrics.allocatedMemoryGB / 1024;
    queueMetrics.pendingMemoryGB = queueMetrics.pendingMemoryGB / 1024;
    queueMetrics.reservedMemoryGB = queueMetrics.reservedMemoryGB / 1024;
    return queueMetrics;
  }

  long getRMStartTime() {
    try {
      URL url =
          new URL("http://"
              + client.getConfig().get(YarnConfiguration.RM_WEBAPP_ADDRESS)
              + "/ws/v1/cluster/info");
      URLConnection conn = url.openConnection();
      conn.connect();
      InputStream in = conn.getInputStream();
      String encoding = conn.getContentEncoding();
      encoding = encoding == null ? "UTF-8" : encoding;
      String body = IOUtils.toString(in, encoding);
      JSONObject obj = new JSONObject(body);
      JSONObject clusterInfo = obj.getJSONObject("clusterInfo");
      return clusterInfo.getLong("startedOn");
    } catch (Exception e) {
      LOG.error("Could not fetch RM start time", e);
    }
    return -1;
  }

  String getHeader(QueueMetrics queueMetrics, NodesInformation nodes) {
    StringBuilder ret = new StringBuilder();
    String queue = "root";
    if (!queues.isEmpty()) {
      queue = StringUtils.join(queues, ",");
    }
    long now = Time.now();
    long uptime = now - rmStartTime;
    long days = TimeUnit.MILLISECONDS.toDays(uptime);
    long hours =
        TimeUnit.MILLISECONDS.toHours(uptime)
            - TimeUnit.DAYS.toHours(TimeUnit.MILLISECONDS.toDays(uptime));
    long minutes =
        TimeUnit.MILLISECONDS.toMinutes(uptime)
            - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(uptime));
    String uptimeStr = String.format("%dd, %d:%d", days, hours, minutes);
    String currentTime = DateFormatUtils.ISO_TIME_NO_T_FORMAT.format(now);

    ret.append(CLEAR_LINE);
    ret.append(limitLineLength(String.format(
      "YARN top - %s, up %s, %d active users, queue(s): %s%n", currentTime,
      uptimeStr, queueMetrics.activeUsers, queue), terminalWidth, true));

    ret.append(CLEAR_LINE);
    ret.append(limitLineLength(String.format(
      "NodeManager(s): %d total, %d active, %d unhealthy, %d decommissioned,"
          + " %d lost, %d rebooted%n", nodes.totalNodes, nodes.runningNodes,
      nodes.unhealthyNodes, nodes.decommissionedNodes, nodes.lostNodes,
      nodes.rebootedNodes), terminalWidth, true));

    ret.append(CLEAR_LINE);
    ret.append(limitLineLength(String.format(
        "Queue(s) Applications: %d running, %d submitted, %d pending,"
            + " %d completed, %d killed, %d failed%n", queueMetrics.appsRunning,
        queueMetrics.appsSubmitted, queueMetrics.appsPending,
        queueMetrics.appsCompleted, queueMetrics.appsKilled,
        queueMetrics.appsFailed), terminalWidth, true));

    ret.append(CLEAR_LINE);
    ret.append(limitLineLength(String.format("Queue(s) Mem(GB): %d available,"
        + " %d allocated, %d pending, %d reserved%n",
      queueMetrics.availableMemoryGB, queueMetrics.allocatedMemoryGB,
      queueMetrics.pendingMemoryGB, queueMetrics.reservedMemoryGB),
      terminalWidth, true));

    ret.append(CLEAR_LINE);
    ret.append(limitLineLength(String.format("Queue(s) VCores: %d available,"
        + " %d allocated, %d pending, %d reserved%n",
      queueMetrics.availableVCores, queueMetrics.allocatedVCores,
      queueMetrics.pendingVCores, queueMetrics.reservedVCores), terminalWidth,
      true));
    return ret.toString();
  }

  String getPrintableAppInformation(List<ApplicationInformation> appsInfo) {
    StringBuilder ret = new StringBuilder();
    int limit = terminalHeight - 8;
    List<String> columns = new ArrayList<>();
    for (int i = 0; i < limit; ++i) {
      ret.append(CLEAR_LINE);
      if(i < appsInfo.size()) {
        ApplicationInformation appInfo = appsInfo.get(i);
        columns.clear();
        for (EnumMap.Entry<Columns, ColumnInformation> entry :
            columnInformationEnumMap.entrySet()) {
          if (entry.getValue().display) {
            String value = "";
            if (appInfo.displayStringsMap.containsKey(entry.getKey())) {
              value = appInfo.displayStringsMap.get(entry.getKey());
            }
            columns.add(String.format(entry.getValue().format, value));
          }
        }
        ret.append(limitLineLength(
            (StringUtils.join(columns.toArray(), " ") + System.lineSeparator()),
            terminalWidth, true));
      }
      else {
        ret.append(System.lineSeparator());
      }
    }
    return ret.toString();
  }

  protected void clearScreen() {
    System.out.print(CLEAR);
    System.out.flush();
  }

  protected void clearScreenWithoutScroll() {
    System.out.print(SET_CURSOR_HOME);
    for(int i = 0; i < terminalHeight; ++i) {
      System.out.println(CLEAR_LINE);
    }
  }

  protected void printHeader(String header) {
    System.out.print(SET_CURSOR_HOME);
    System.out.print(header);
    System.out.println("");
  }

  protected void printApps(String appInfo) {
    System.out.print(CLEAR_LINE);
    System.out.print(CHANGE_BACKGROUND + appsHeader + RESET_BACKGROUND);
    System.out.print(appInfo);
  }

  private void showHelpScreen() {
    synchronized (lock) {
      if (!showingTopScreen.get()) {
        return;
      }
      showingTopScreen.set(false);
      clearScreenWithoutScroll();
      System.out.print(SET_CURSOR_HOME);
      System.out.println("Help for yarn top.");
      System.out.println("Delay: " + (refreshPeriod / 1000)
          + " secs; Secure mode: " + UserGroupInformation.isSecurityEnabled());
      System.out.println("");
      System.out.println("  s + Enter : Select sort field");
      System.out.println("  f + Enter : Select fields to display");
      System.out.println("  R + Enter: Reverse current sort order");
      System.out.println("  h + Enter: Display this screen");
      System.out.println("  q + Enter: Quit");
      System.out.println("");
      System.out.println("Press any key followed by Enter to continue");
    }
  }

  private void showSortScreen() {
    synchronized (lock) {
      showingTopScreen.set(false);
      System.out.print(SET_CURSOR_HOME);
      System.out.println(CLEAR_LINE + "Current Sort Field: " + currentSortField);
      System.out.println(CLEAR_LINE + "Select sort field via letter followed by"
          + " Enter, type any other key followed by Enter to return");
      System.out.println(CLEAR_LINE);
      for (String key : sortedKeys) {
        String prefix = " ";
        if (key.equals(currentSortField)) {
          prefix = "*";
        }
        ColumnInformation value =
            columnInformationEnumMap.get(keyFieldsMap.get(key));
        System.out.print(CLEAR_LINE);
        System.out.println(String.format("%s %s: %-15s = %s", prefix, key,
          value.header, value.description));
      }
    }
  }

  protected void showFieldsScreen() {
    synchronized (lock) {
      showingTopScreen.set(false);
      System.out.print(SET_CURSOR_HOME);
      System.out.println(CLEAR_LINE + "Current Fields: ");
      System.out.println(CLEAR_LINE + "Toggle fields via field letter followed"
          + " by Enter, type any other key followed by Enter to return");
      for (String key : sortedKeys) {
        ColumnInformation info =
            columnInformationEnumMap.get(keyFieldsMap.get(key));
        String prefix = " ";
        String letter = key;
        if (info.display) {
          prefix = "*";
          letter = key.toUpperCase();
        }
        System.out.print(CLEAR_LINE);
        System.out.println(String.format("%s %s: %-15s = %s", prefix, letter,
          info.header, info.description));
      }
    }
  }

  protected void showTopScreen() {
    List<ApplicationInformation> appsInfo = new ArrayList<>();
    List<ApplicationReport> apps;
    try {
      apps = fetchAppReports();
    } catch (Exception e) {
      LOG.error("Unable to get application information", e);
      return;
    }

    for (ApplicationReport appReport : apps) {
      ApplicationInformation appInfo = new ApplicationInformation(appReport);
      appsInfo.add(appInfo);
    }
    if (ascendingSort) {
      Collections.sort(appsInfo, comparator);
    } else {
      Collections.sort(appsInfo, Collections.reverseOrder(comparator));
    }
    NodesInformation nodesInfo = getNodesInfo();
    QueueMetrics queueMetrics = getQueueMetrics();
    String header = getHeader(queueMetrics, nodesInfo);
    String appsStr = getPrintableAppInformation(appsInfo);
    synchronized (lock) {
      printHeader(header);
      printApps(appsStr);
      System.out.print(SET_CURSOR_LINE_6_COLUMN_0);
      System.out.print(CLEAR_LINE);
    }
  }

  private void handleSortScreenKeyPress(String input) {
    String f = currentSortField;
    currentSortField = input.toLowerCase();
    switch (input.toLowerCase()) {
    case "a":
      comparator = AppIDComparator;
      break;
    case "u":
      comparator = UserComparator;
      break;
    case "t":
      comparator = AppTypeComparator;
      break;
    case "q":
      comparator = QueueNameComparator;
      break;
    case "c":
      comparator = UsedContainersComparator;
      break;
    case "r":
      comparator = ReservedContainersComparator;
      break;
    case "v":
      comparator = UsedVCoresComparator;
      break;
    case "o":
      comparator = ReservedVCoresComparator;
      break;
    case "m":
      comparator = UsedMemoryComparator;
      break;
    case "w":
      comparator = ReservedMemoryComparator;
      break;
    case "s":
      comparator = VCoreSecondsComparator;
      break;
    case "y":
      comparator = MemorySecondsComparator;
      break;
    case "p":
      comparator = ProgressComparator;
      break;
    case "i":
      comparator = RunningTimeComparator;
      break;
    case "n":
      comparator = AppNameComparator;
      break;
    default:
      currentSortField = f;
      showTopScreen();
      showingTopScreen.set(true);
      displayScreen = DisplayScreen.TOP;
    }
  }

  private void handleFieldsScreenKeyPress(String input) {
    if (keyFieldsMap.containsKey(input.toLowerCase())) {
      toggleColumn(keyFieldsMap.get(input.toLowerCase()));
      setAppsHeader();
    } else {
      showTopScreen();
      showingTopScreen.set(true);
      displayScreen = DisplayScreen.TOP;
    }
  }

  private void handleTopScreenKeyPress(String input) {
    switch (input.toLowerCase()) {
    case "q":
      runMainLoop.set(false);
      runKeyboardMonitor.set(false);
      displayThread.interrupt();
      break;
    case "s":
      displayScreen = DisplayScreen.SORT;
      showSortScreen();
      break;
    case "f":
      displayScreen = DisplayScreen.FIELDS;
      showFieldsScreen();
      break;
    case "r":
      ascendingSort = !ascendingSort;
      break;
    case "h":
      displayScreen = DisplayScreen.HELP;
      showHelpScreen();
      break;
    default:
      break;
    }
  }

  private void handleHelpScreenKeyPress() {
    showTopScreen();
    showingTopScreen.set(true);
    displayScreen = DisplayScreen.TOP;
  }

  String limitLineLength(String line, int length, boolean addNewline) {
    if (line.length() > length) {
      String tmp;
      if (addNewline) {
        tmp = line.substring(0, length - System.lineSeparator().length());
        tmp += System.lineSeparator();
      } else {
        tmp = line.substring(0, length);
      }
      return tmp;
    }
    return line;
  }

  void toggleColumn(Columns col) {
    columnInformationEnumMap.get(col).display =
        !columnInformationEnumMap.get(col).display;
  }

  protected List<ApplicationReport> fetchAppReports() throws YarnException,
      IOException {
    List<ApplicationReport> ret;
    EnumSet<YarnApplicationState> states =
        EnumSet.of(YarnApplicationState.ACCEPTED, YarnApplicationState.RUNNING);
    GetApplicationsRequest req =
        GetApplicationsRequest.newInstance(types, states);
    req.setQueues(queues);
    req.setUsers(users);
    ret = applicationReportsCache.getIfPresent(req);
    if (ret != null) {
      return ret;
    }
    ret = client.getApplications(queues, users, types, states);
    applicationReportsCache.put(req, ret);
    return ret;
  }

  private String getCommandOutput(String[] command) throws IOException,
      InterruptedException {
    Process p = Runtime.getRuntime().exec(command);
    p.waitFor();
    byte[] output = IOUtils.toByteArray(p.getInputStream());
    return new String(output, "ASCII");
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/ProtocolHATestBase.java

    public QueueInfo createFakeQueueInfo() {
      return QueueInfo.newInstance("root", 100f, 100f, 50f, null,
          createFakeAppReports(), QueueState.RUNNING, null, null, null);
    }

    public List<QueueUserACLInfo> createFakeQueueUserACLInfoList() {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/cli/TestYarnCLI.java
    nodeLabels.add("GPU");
    nodeLabels.add("JDK_7");
    QueueInfo queueInfo = QueueInfo.newInstance("queueA", 0.4f, 0.8f, 0.5f,
        null, null, QueueState.RUNNING, nodeLabels, "GPU", null);
    when(client.getQueueInfo(any(String.class))).thenReturn(queueInfo);
    int result = cli.run(new String[] { "-status", "queueA" });
    assertEquals(0, result);
  public void testGetQueueInfoWithEmptyNodeLabel() throws Exception {
    QueueCLI cli = createAndGetQueueCLI();
    QueueInfo queueInfo = QueueInfo.newInstance("queueA", 0.4f, 0.8f, 0.5f,
        null, null, QueueState.RUNNING, null, null, null);
    when(client.getQueueInfo(any(String.class))).thenReturn(queueInfo);
    int result = cli.run(new String[] { "-status", "queueA" });
    assertEquals(0, result);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/QueueInfoPBImpl.java
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueInfoProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueInfoProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueStatisticsProto;

import com.google.protobuf.TextFormat;

    }
    builder.setDefaultNodeLabelExpression(defaultNodeLabelExpression);
  }

  private QueueStatistics convertFromProtoFormat(QueueStatisticsProto q) {
    return new QueueStatisticsPBImpl(q);
  }

  private QueueStatisticsProto convertToProtoFormat(QueueStatistics q) {
    return ((QueueStatisticsPBImpl) q).getProto();
  }

  @Override
  public QueueStatistics getQueueStatistics() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasQueueStatistics()) ? convertFromProtoFormat(p
      .getQueueStatistics()) : null;
  }

  @Override
  public void setQueueStatistics(QueueStatistics queueStatistics) {
    maybeInitBuilder();
    if (queueStatistics == null) {
      builder.clearQueueStatistics();
      return;
    }
    builder.setQueueStatistics(convertToProtoFormat(queueStatistics));
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/QueueStatisticsPBImpl.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/QueueStatisticsPBImpl.java

package org.apache.hadoop.yarn.api.records.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueStatisticsProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueStatisticsProtoOrBuilder;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class QueueStatisticsPBImpl extends QueueStatistics {

  QueueStatisticsProto proto = QueueStatisticsProto.getDefaultInstance();
  QueueStatisticsProto.Builder builder = null;
  boolean viaProto = false;

  public QueueStatisticsPBImpl() {
    builder = QueueStatisticsProto.newBuilder();
  }

  public QueueStatisticsPBImpl(QueueStatisticsProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public QueueStatisticsProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = QueueStatisticsProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public long getNumAppsSubmitted() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumAppsSubmitted()) ? p.getNumAppsSubmitted() : -1;
  }

  @Override
  public void setNumAppsSubmitted(long numAppsSubmitted) {
    maybeInitBuilder();
    builder.setNumAppsSubmitted(numAppsSubmitted);
  }

  @Override
  public long getNumAppsRunning() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumAppsRunning()) ? p.getNumAppsRunning() : -1;
  }

  @Override
  public void setNumAppsRunning(long numAppsRunning) {
    maybeInitBuilder();
    builder.setNumAppsRunning(numAppsRunning);
  }

  @Override
  public long getNumAppsPending() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumAppsPending()) ? p.getNumAppsPending() : -1;
  }

  @Override
  public void setNumAppsPending(long numAppsPending) {
    maybeInitBuilder();
    builder.setNumAppsPending(numAppsPending);
  }

  @Override
  public long getNumAppsCompleted() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumAppsCompleted()) ? p.getNumAppsCompleted() : -1;
  }

  @Override
  public void setNumAppsCompleted(long numAppsCompleted) {
    maybeInitBuilder();
    builder.setNumAppsCompleted(numAppsCompleted);
  }

  @Override
  public long getNumAppsKilled() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumAppsKilled()) ? p.getNumAppsKilled() : -1;
  }

  @Override
  public void setNumAppsKilled(long numAppsKilled) {
    maybeInitBuilder();
    builder.setNumAppsKilled(numAppsKilled);
  }

  @Override
  public long getNumAppsFailed() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumAppsFailed()) ? p.getNumAppsFailed() : -1;
  }

  @Override
  public void setNumAppsFailed(long numAppsFailed) {
    maybeInitBuilder();
    builder.setNumAppsFailed(numAppsFailed);
  }

  @Override
  public long getNumActiveUsers() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumActiveUsers()) ? p.getNumActiveUsers() : -1;
  }

  @Override
  public void setNumActiveUsers(long numActiveUsers) {
    maybeInitBuilder();
    builder.setNumActiveUsers(numActiveUsers);
  }

  @Override
  public long getAvailableMemoryMB() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasAvailableMemoryMB()) ? p.getAvailableMemoryMB() : -1;
  }

  @Override
  public void setAvailableMemoryMB(long availableMemoryMB) {
    maybeInitBuilder();
    builder.setAvailableMemoryMB(availableMemoryMB);
  }

  @Override
  public long getAllocatedMemoryMB() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasAllocatedMemoryMB()) ? p.getAllocatedMemoryMB() : -1;
  }

  @Override
  public void setAllocatedMemoryMB(long allocatedMemoryMB) {
    maybeInitBuilder();
    builder.setAllocatedMemoryMB(allocatedMemoryMB);
  }

  @Override
  public long getPendingMemoryMB() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasPendingMemoryMB()) ? p.getPendingMemoryMB() : -1;
  }

  @Override
  public void setPendingMemoryMB(long pendingMemoryMB) {
    maybeInitBuilder();
    builder.setPendingMemoryMB(pendingMemoryMB);
  }

  @Override
  public long getReservedMemoryMB() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasReservedMemoryMB()) ? p.getReservedMemoryMB() : -1;
  }

  @Override
  public void setReservedMemoryMB(long reservedMemoryMB) {
    maybeInitBuilder();
    builder.setReservedMemoryMB(reservedMemoryMB);
  }

  @Override
  public long getAvailableVCores() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasAvailableVCores()) ? p.getAvailableVCores() : -1;
  }

  @Override
  public void setAvailableVCores(long availableVCores) {
    maybeInitBuilder();
    builder.setAvailableVCores(availableVCores);
  }

  @Override
  public long getAllocatedVCores() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasAllocatedVCores()) ? p.getAllocatedVCores() : -1;
  }

  @Override
  public void setAllocatedVCores(long allocatedVCores) {
    maybeInitBuilder();
    builder.setAllocatedVCores(allocatedVCores);
  }

  @Override
  public long getPendingVCores() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasPendingVCores()) ? p.getPendingVCores() : -1;
  }

  @Override
  public void setPendingVCores(long pendingVCores) {
    maybeInitBuilder();
    builder.setPendingVCores(pendingVCores);
  }

  @Override
  public long getReservedVCores() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasReservedVCores()) ? p.getReservedVCores() : -1;
  }

  @Override
  public void setReservedVCores(long reservedVCores) {
    maybeInitBuilder();
    builder.setReservedVCores(reservedVCores);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/YarnClusterMetricsPBImpl.java
    builder.setNumNodeManagers((numNodeManagers));
  }

  @Override
  public int getNumDecommissionedNodeManagers() {
    YarnClusterMetricsProtoOrBuilder p = viaProto ? proto : builder;
    if (p.hasNumDecommissionedNms()) {
      return (p.getNumDecommissionedNms());
    }
    return 0;
  }

  @Override
  public void
      setNumDecommissionedNodeManagers(int numDecommissionedNodeManagers) {
    maybeInitBuilder();
    builder.setNumDecommissionedNms((numDecommissionedNodeManagers));
  }

  @Override
  public int getNumActiveNodeManagers() {
    YarnClusterMetricsProtoOrBuilder p = viaProto ? proto : builder;
    if (p.hasNumActiveNms()) {
      return (p.getNumActiveNms());
    }
    return 0;
  }

  @Override
  public void setNumActiveNodeManagers(int numActiveNodeManagers) {
    maybeInitBuilder();
    builder.setNumActiveNms((numActiveNodeManagers));
  }

  @Override
  public int getNumLostNodeManagers() {
    YarnClusterMetricsProtoOrBuilder p = viaProto ? proto : builder;
    if (p.hasNumLostNms()) {
      return (p.getNumLostNms());
    }
    return 0;
  }

  @Override
  public void setNumLostNodeManagers(int numLostNodeManagers) {
    maybeInitBuilder();
    builder.setNumLostNms((numLostNodeManagers));
  }

  @Override
  public int getNumUnhealthyNodeManagers() {
    YarnClusterMetricsProtoOrBuilder p = viaProto ? proto : builder;
    if (p.hasNumUnhealthyNms()) {
      return (p.getNumUnhealthyNms());
    }
    return 0;

  }

  @Override
  public void setNumUnhealthyNodeManagers(int numUnhealthyNodeManagers) {
    maybeInitBuilder();
    builder.setNumUnhealthyNms((numUnhealthyNodeManagers));
  }

  @Override
  public int getNumRebootedNodeManagers() {
    YarnClusterMetricsProtoOrBuilder p = viaProto ? proto : builder;
    if (p.hasNumRebootedNms()) {
      return (p.getNumRebootedNms());
    }
    return 0;
  }

  @Override
  public void setNumRebootedNodeManagers(int numRebootedNodeManagers) {
    maybeInitBuilder();
    builder.setNumRebootedNms((numRebootedNodeManagers));
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/api/TestPBImplRecords.java
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
    typeValueCache.put(QueueInfo.class, QueueInfo.newInstance("root", 1.0f,
        1.0f, 0.1f, null, null, QueueState.RUNNING, ImmutableSet.of("x", "y"),
        "x && y", null));
    generateByNewInstance(QueueStatistics.class);
    generateByNewInstance(QueueUserACLInfo.class);
    generateByNewInstance(YarnClusterMetrics.class);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ClientRMService.java
    YarnClusterMetrics ymetrics = recordFactory
        .newRecordInstance(YarnClusterMetrics.class);
    ymetrics.setNumNodeManagers(this.rmContext.getRMNodes().size());
    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();
    ymetrics.setNumDecommissionedNodeManagers(clusterMetrics
      .getNumDecommisionedNMs());
    ymetrics.setNumActiveNodeManagers(clusterMetrics.getNumActiveNMs());
    ymetrics.setNumLostNodeManagers(clusterMetrics.getNumLostNMs());
    ymetrics.setNumUnhealthyNodeManagers(clusterMetrics.getUnhealthyNMs());
    ymetrics.setNumRebootedNodeManagers(clusterMetrics.getNumRebootedNMs());
    response.setClusterMetrics(ymetrics);
    return response;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/AbstractCSQueue.java
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
    queueInfo.setQueueState(state);
    queueInfo.setDefaultNodeLabelExpression(defaultLabelExpression);
    queueInfo.setCurrentCapacity(getUsedCapacity());
    queueInfo.setQueueStatistics(getQueueStatistics());
    return queueInfo;
  }

  public QueueStatistics getQueueStatistics() {
    QueueStatistics stats =
        recordFactory.newRecordInstance(QueueStatistics.class);
    stats.setNumAppsSubmitted(getMetrics().getAppsSubmitted());
    stats.setNumAppsRunning(getMetrics().getAppsRunning());
    stats.setNumAppsPending(getMetrics().getAppsPending());
    stats.setNumAppsCompleted(getMetrics().getAppsCompleted());
    stats.setNumAppsKilled(getMetrics().getAppsKilled());
    stats.setNumAppsFailed(getMetrics().getAppsFailed());
    stats.setNumActiveUsers(getMetrics().getActiveUsers());
    stats.setAvailableMemoryMB(getMetrics().getAvailableMB());
    stats.setAllocatedMemoryMB(getMetrics().getAllocatedMB());
    stats.setPendingMemoryMB(getMetrics().getPendingMB());
    stats.setReservedMemoryMB(getMetrics().getReservedMB());
    stats.setAvailableVCores(getMetrics().getAvailableVirtualCores());
    stats.setAllocatedVCores(getMetrics().getAllocatedVirtualCores());
    stats.setPendingVCores(getMetrics().getPendingVirtualCores());
    stats.setReservedVCores(getMetrics().getReservedVirtualCores());
    return stats;
  }
  
  @Private
  public synchronized Resource getMaximumAllocation() {
    return maximumAllocation;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FSQueue.java
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
    }
    queueInfo.setChildQueues(childQueueInfos);
    queueInfo.setQueueState(QueueState.RUNNING);
    queueInfo.setQueueStatistics(getQueueStatistics());
    return queueInfo;
  }

  public QueueStatistics getQueueStatistics() {
    QueueStatistics stats =
        recordFactory.newRecordInstance(QueueStatistics.class);
    stats.setNumAppsSubmitted(getMetrics().getAppsSubmitted());
    stats.setNumAppsRunning(getMetrics().getAppsRunning());
    stats.setNumAppsPending(getMetrics().getAppsPending());
    stats.setNumAppsCompleted(getMetrics().getAppsCompleted());
    stats.setNumAppsKilled(getMetrics().getAppsKilled());
    stats.setNumAppsFailed(getMetrics().getAppsFailed());
    stats.setNumActiveUsers(getMetrics().getActiveUsers());
    stats.setAvailableMemoryMB(getMetrics().getAvailableMB());
    stats.setAllocatedMemoryMB(getMetrics().getAllocatedMB());
    stats.setPendingMemoryMB(getMetrics().getPendingMB());
    stats.setReservedMemoryMB(getMetrics().getReservedMB());
    stats.setAvailableVCores(getMetrics().getAvailableVirtualCores());
    stats.setAllocatedVCores(getMetrics().getAllocatedVirtualCores());
    stats.setPendingVCores(getMetrics().getPendingVirtualCores());
    stats.setReservedVCores(getMetrics().getReservedVirtualCores());
    return stats;
  }
  
  @Override
  public FSQueueMetrics getMetrics() {
    return metrics;

