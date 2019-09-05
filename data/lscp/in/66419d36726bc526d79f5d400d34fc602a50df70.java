hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/SysInfo.java
  public abstract float getCpuUsage();

  public abstract long getNetworkBytesRead();

  public abstract long getNetworkBytesWritten();

}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/SysInfoLinux.java
                      "[ \t]*([0-9]*)[ \t]*([0-9]*)[ \t].*");
  private CpuTimeTracker cpuTimeTracker;

  private static final String PROCFS_NETFILE = "/proc/net/dev";
  private static final Pattern PROCFS_NETFILE_FORMAT =
      Pattern .compile("^[ \t]*([a-zA-Z]+[0-9]*):" +
               "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)" +
               "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)" +
               "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)" +
               "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+).*");


  private String procfsMemFile;
  private String procfsCpuFile;
  private String procfsStatFile;
  private String procfsNetFile;
  private long jiffyLengthInMillis;

  private long ramSize = 0;
  private int numCores = 0;
  private long cpuFrequency = 0L; // CPU frequency on the system (kHz)
  private long numNetBytesRead = 0L; // aggregated bytes read from network
  private long numNetBytesWritten = 0L; // aggregated bytes written to network

  private boolean readMemInfoFile = false;
  private boolean readCpuInfoFile = false;

  public SysInfoLinux() {
    this(PROCFS_MEMFILE, PROCFS_CPUINFO, PROCFS_STAT,
         PROCFS_NETFILE, JIFFY_LENGTH_IN_MILLIS);
  }

  @VisibleForTesting
  public SysInfoLinux(String procfsMemFile,
                                       String procfsCpuFile,
                                       String procfsStatFile,
                                       String procfsNetFile,
                                       long jiffyLengthInMillis) {
    this.procfsMemFile = procfsMemFile;
    this.procfsCpuFile = procfsCpuFile;
    this.procfsStatFile = procfsStatFile;
    this.procfsNetFile = procfsNetFile;
    this.jiffyLengthInMillis = jiffyLengthInMillis;
    this.cpuTimeTracker = new CpuTimeTracker(jiffyLengthInMillis);
  }
    }
  }

  private void readProcNetInfoFile() {

    numNetBytesRead = 0L;
    numNetBytesWritten = 0L;

    BufferedReader in;
    InputStreamReader fReader;
    try {
      fReader = new InputStreamReader(
          new FileInputStream(procfsNetFile), Charset.forName("UTF-8"));
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      return;
    }

    Matcher mat;
    try {
      String str = in.readLine();
      while (str != null) {
        mat = PROCFS_NETFILE_FORMAT.matcher(str);
        if (mat.find()) {
          assert mat.groupCount() >= 16;

          if (mat.group(1).equals("lo")) {
            str = in.readLine();
            continue;
          }
          numNetBytesRead += Long.parseLong(mat.group(2));
          numNetBytesWritten += Long.parseLong(mat.group(10));
        }
        str = in.readLine();
      }
    } catch (IOException io) {
      LOG.warn("Error reading the stream " + io);
    } finally {
      try {
        fReader.close();
        try {
          in.close();
        } catch (IOException i) {
          LOG.warn("Error closing the stream " + in);
        }
      } catch (IOException i) {
        LOG.warn("Error closing the stream " + fReader);
      }
    }
  }

  @Override
  public long getPhysicalMemorySize() {
    return overallCpuUsage;
  }

  @Override
  public long getNetworkBytesRead() {
    readProcNetInfoFile();
    return numNetBytesRead;
  }

  @Override
  public long getNetworkBytesWritten() {
    readProcNetInfoFile();
    return numNetBytesWritten;
  }

    System.out.println("CPU frequency (kHz) : " + plugin.getCpuFrequency());
    System.out.println("Cumulative CPU time (ms) : " +
            plugin.getCumulativeCpuTime());
    System.out.println("Total network read (bytes) : "
            + plugin.getNetworkBytesRead());
    System.out.println("Total network written (bytes) : "
            + plugin.getNetworkBytesWritten());
    try {
      Thread.sleep(500L);

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/SysInfoWindows.java
    refreshIfNeeded();
    return cpuUsage;
  }

  @Override
  public long getNetworkBytesRead() {
    return 0L;
  }

  @Override
  public long getNetworkBytesWritten() {
    return 0L;
  }

}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/util/TestSysInfoLinux.java
    public FakeLinuxResourceCalculatorPlugin(String procfsMemFile,
                                             String procfsCpuFile,
                                             String procfsStatFile,
			                                       String procfsNetFile,
                                             long jiffyLengthInMillis) {
      super(procfsMemFile, procfsCpuFile, procfsStatFile, procfsNetFile,
          jiffyLengthInMillis);
    }
    @Override
    long getCurrentTime() {
  private static final String FAKE_MEMFILE;
  private static final String FAKE_CPUFILE;
  private static final String FAKE_STATFILE;
  private static final String FAKE_NETFILE;
  private static final long FAKE_JIFFY_LENGTH = 10L;
  static {
    int randomNum = (new Random()).nextInt(1000000000);
    FAKE_MEMFILE = TEST_ROOT_DIR + File.separator + "MEMINFO_" + randomNum;
    FAKE_CPUFILE = TEST_ROOT_DIR + File.separator + "CPUINFO_" + randomNum;
    FAKE_STATFILE = TEST_ROOT_DIR + File.separator + "STATINFO_" + randomNum;
    FAKE_NETFILE = TEST_ROOT_DIR + File.separator + "NETINFO_" + randomNum;
    plugin = new FakeLinuxResourceCalculatorPlugin(FAKE_MEMFILE, FAKE_CPUFILE,
                                                   FAKE_STATFILE,
                                                   FAKE_NETFILE,
                                                   FAKE_JIFFY_LENGTH);
  }
  static final String MEMINFO_FORMAT =
    "procs_running 1\n" +
    "procs_blocked 0\n";

  static final String NETINFO_FORMAT =
    "Inter-|   Receive                                                |  Transmit\n"+
    "face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets"+
    "errs drop fifo colls carrier compressed\n"+
    "   lo: 42236310  563003    0    0    0     0          0         0 42236310  563003    " +
    "0    0    0     0       0          0\n"+
    " eth0: %d 3452527    0    0    0     0          0    299787 %d 1866280    0    0    " +
    "0     0       0          0\n"+
    " eth1: %d 3152521    0    0    0     0          0    219781 %d 1866290    0    0    " +
    "0     0       0          0\n";

      IOUtils.closeQuietly(fWriter);
    }
  }

  @Test
  public void parsingProcNetFile() throws IOException {
    long numBytesReadIntf1 = 2097172468L;
    long numBytesWrittenIntf1 = 1355620114L;
    long numBytesReadIntf2 = 1097172460L;
    long numBytesWrittenIntf2 = 1055620110L;
    File tempFile = new File(FAKE_NETFILE);
    tempFile.deleteOnExit();
    FileWriter fWriter = new FileWriter(FAKE_NETFILE);
    fWriter.write(String.format(NETINFO_FORMAT,
                            numBytesReadIntf1, numBytesWrittenIntf1,
                            numBytesReadIntf2, numBytesWrittenIntf2));
    fWriter.close();
    assertEquals(plugin.getNetworkBytesRead(), numBytesReadIntf1 + numBytesReadIntf2);
    assertEquals(plugin.getNetworkBytesWritten(), numBytesWrittenIntf1 + numBytesWrittenIntf2);
  }

}

hadoop-tools/hadoop-gridmix/src/test/java/org/apache/hadoop/mapred/gridmix/DummyResourceCalculatorPlugin.java
      "mapred.tasktracker.cumulativecputime.testing";
  public static final String CPU_USAGE = "mapred.tasktracker.cpuusage.testing";
  public static final String NETWORK_BYTES_READ =
      "mapred.tasktracker.networkread.testing";
  public static final String NETWORK_BYTES_WRITTEN =
      "mapred.tasktracker.networkwritten.testing";
  public static final String PROC_CUMULATIVE_CPU_TIME =
      "mapred.tasktracker.proccumulativecputime.testing";
  public float getCpuUsage() {
    return getConf().getFloat(CPU_USAGE, -1);
  }

  @Override
  public long getNetworkBytesRead() {
    return getConf().getLong(NETWORK_BYTES_READ, -1);
  }

  @Override
  public long getNetworkBytesWritten() {
    return getConf().getLong(NETWORK_BYTES_WRITTEN, -1);
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/ResourceCalculatorPlugin.java
    return sys.getCpuUsage();
  }

  public long getNetworkBytesRead() {
    return sys.getNetworkBytesRead();
  }

  public long getNetworkBytesWritten() {
    return sys.getNetworkBytesWritten();
  }


