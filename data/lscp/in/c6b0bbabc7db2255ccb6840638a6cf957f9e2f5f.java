hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/SysInfo.java
  public abstract long getNetworkBytesWritten();

  public abstract long getStorageBytesRead();

  public abstract long getStorageBytesWritten();

}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/SysInfoLinux.java
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
               "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)" +
               "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+).*");

  private static final String PROCFS_DISKSFILE = "/proc/diskstats";
  private static final Pattern PROCFS_DISKSFILE_FORMAT =
      Pattern.compile("^[ \t]*([0-9]+)[ \t]*([0-9 ]+)" +
              "(?!([a-zA-Z]+[0-9]+))([a-zA-Z]+)" +
              "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)" +
              "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)" +
              "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)");
  private static final Pattern PROCFS_DISKSECTORFILE_FORMAT =
      Pattern.compile("^([0-9]+)");

  private String procfsMemFile;
  private String procfsCpuFile;
  private String procfsStatFile;
  private String procfsNetFile;
  private String procfsDisksFile;
  private long jiffyLengthInMillis;

  private long ramSize = 0;
  private long cpuFrequency = 0L; // CPU frequency on the system (kHz)
  private long numNetBytesRead = 0L; // aggregated bytes read from network
  private long numNetBytesWritten = 0L; // aggregated bytes written to network
  private long numDisksBytesRead = 0L; // aggregated bytes read from disks
  private long numDisksBytesWritten = 0L; // aggregated bytes written to disks

  private boolean readMemInfoFile = false;
  private boolean readCpuInfoFile = false;

  private HashMap<String, Integer> perDiskSectorSize = null;

  public static final long PAGE_SIZE = getConf("PAGESIZE");
  public static final long JIFFY_LENGTH_IN_MILLIS =
      Math.max(Math.round(1000D / getConf("CLK_TCK")), -1);

  public SysInfoLinux() {
    this(PROCFS_MEMFILE, PROCFS_CPUINFO, PROCFS_STAT,
         PROCFS_NETFILE, PROCFS_DISKSFILE, JIFFY_LENGTH_IN_MILLIS);
  }

  @VisibleForTesting
                                       String procfsCpuFile,
                                       String procfsStatFile,
                                       String procfsNetFile,
                                       String procfsDisksFile,
                                       long jiffyLengthInMillis) {
    this.procfsMemFile = procfsMemFile;
    this.procfsCpuFile = procfsCpuFile;
    this.procfsStatFile = procfsStatFile;
    this.procfsNetFile = procfsNetFile;
    this.procfsDisksFile = procfsDisksFile;
    this.jiffyLengthInMillis = jiffyLengthInMillis;
    this.cpuTimeTracker = new CpuTimeTracker(jiffyLengthInMillis);
    this.perDiskSectorSize = new HashMap<String, Integer>();
  }

    }
  }

  private void readProcDisksInfoFile() {

    numDisksBytesRead = 0L;
    numDisksBytesWritten = 0L;

    BufferedReader in;
    try {
      in = new BufferedReader(new InputStreamReader(
            new FileInputStream(procfsDisksFile), Charset.forName("UTF-8")));
    } catch (FileNotFoundException f) {
      return;
    }

    Matcher mat;
    try {
      String str = in.readLine();
      while (str != null) {
        mat = PROCFS_DISKSFILE_FORMAT.matcher(str);
        if (mat.find()) {
          String diskName = mat.group(4);
          assert diskName != null;
          if (diskName.contains("loop") || diskName.contains("ram")) {
            str = in.readLine();
            continue;
          }

          Integer sectorSize;
          synchronized (perDiskSectorSize) {
            sectorSize = perDiskSectorSize.get(diskName);
            if (null == sectorSize) {
              sectorSize = readDiskBlockInformation(diskName, 512);
              perDiskSectorSize.put(diskName, sectorSize);
            }
          }

          String sectorsRead = mat.group(7);
          String sectorsWritten = mat.group(11);
          if (null == sectorsRead || null == sectorsWritten) {
            return;
          }
          numDisksBytesRead += Long.parseLong(sectorsRead) * sectorSize;
          numDisksBytesWritten += Long.parseLong(sectorsWritten) * sectorSize;
        }
        str = in.readLine();
      }
    } catch (IOException e) {
      LOG.warn("Error reading the stream " + procfsDisksFile, e);
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        LOG.warn("Error closing the stream " + procfsDisksFile, e);
      }
    }
  }

  int readDiskBlockInformation(String diskName, int defSector) {

    assert perDiskSectorSize != null && diskName != null;

    String procfsDiskSectorFile =
            "/sys/block/" + diskName + "/queue/hw_sector_size";

    BufferedReader in;
    try {
      in = new BufferedReader(new InputStreamReader(
            new FileInputStream(procfsDiskSectorFile),
              Charset.forName("UTF-8")));
    } catch (FileNotFoundException f) {
      return defSector;
    }

    Matcher mat;
    try {
      String str = in.readLine();
      while (str != null) {
        mat = PROCFS_DISKSECTORFILE_FORMAT.matcher(str);
        if (mat.find()) {
          String secSize = mat.group(1);
          if (secSize != null) {
            return Integer.parseInt(secSize);
          }
        }
        str = in.readLine();
      }
      return defSector;
    } catch (IOException|NumberFormatException e) {
      LOG.warn("Error reading the stream " + procfsDiskSectorFile, e);
      return defSector;
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        LOG.warn("Error closing the stream " + procfsDiskSectorFile, e);
      }
    }
  }

  @Override
  public long getPhysicalMemorySize() {
    return numNetBytesWritten;
  }

  @Override
  public long getStorageBytesRead() {
    readProcDisksInfoFile();
    return numDisksBytesRead;
  }

  @Override
  public long getStorageBytesWritten() {
    readProcDisksInfoFile();
    return numDisksBytesWritten;
  }

            + plugin.getNetworkBytesRead());
    System.out.println("Total network written (bytes) : "
            + plugin.getNetworkBytesWritten());
    System.out.println("Total storage read (bytes) : "
            + plugin.getStorageBytesRead());
    System.out.println("Total storage written (bytes) : "
            + plugin.getStorageBytesWritten());
    try {
      Thread.sleep(500L);

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/SysInfoWindows.java
    return 0L;
  }

  @Override
  public long getStorageBytesRead() {
    return 0L;
  }

  @Override
  public long getStorageBytesWritten() {
    return 0L;
  }

}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/util/TestSysInfoLinux.java
  static class FakeLinuxResourceCalculatorPlugin extends SysInfoLinux {
    static final int SECTORSIZE = 4096;

    long currentTime = 0;
    public FakeLinuxResourceCalculatorPlugin(String procfsMemFile,
                                             String procfsCpuFile,
                                             String procfsStatFile,
			                                       String procfsNetFile,
                                             String procfsDisksFile,
                                             long jiffyLengthInMillis) {
      super(procfsMemFile, procfsCpuFile, procfsStatFile, procfsNetFile,
          procfsDisksFile, jiffyLengthInMillis);
    }
    @Override
    long getCurrentTime() {
    public void advanceTime(long adv) {
      currentTime += adv * this.getJiffyLengthInMillis();
    }
    @Override
    int readDiskBlockInformation(String diskName, int defSector) {
      return SECTORSIZE;
    }
  }
  private static final FakeLinuxResourceCalculatorPlugin plugin;
  private static String TEST_ROOT_DIR = new Path(System.getProperty(
  private static final String FAKE_CPUFILE;
  private static final String FAKE_STATFILE;
  private static final String FAKE_NETFILE;
  private static final String FAKE_DISKSFILE;
  private static final long FAKE_JIFFY_LENGTH = 10L;
  static {
    int randomNum = (new Random()).nextInt(1000000000);
    FAKE_CPUFILE = TEST_ROOT_DIR + File.separator + "CPUINFO_" + randomNum;
    FAKE_STATFILE = TEST_ROOT_DIR + File.separator + "STATINFO_" + randomNum;
    FAKE_NETFILE = TEST_ROOT_DIR + File.separator + "NETINFO_" + randomNum;
    FAKE_DISKSFILE = TEST_ROOT_DIR + File.separator + "DISKSINFO_" + randomNum;
    plugin = new FakeLinuxResourceCalculatorPlugin(FAKE_MEMFILE, FAKE_CPUFILE,
                                                   FAKE_STATFILE,
                                                   FAKE_NETFILE,
                                                   FAKE_DISKSFILE,
                                                   FAKE_JIFFY_LENGTH);
  }
  static final String MEMINFO_FORMAT =
    " eth1: %d 3152521    0    0    0     0          0    219781 %d 1866290    0    0    " +
    "0     0       0          0\n";

  static final String DISKSINFO_FORMAT =
      "1       0 ram0 0 0 0 0 0 0 0 0 0 0 0\n"+
      "1       1 ram1 0 0 0 0 0 0 0 0 0 0 0\n"+
      "1       2 ram2 0 0 0 0 0 0 0 0 0 0 0\n"+
      "1       3 ram3 0 0 0 0 0 0 0 0 0 0 0\n"+
      "1       4 ram4 0 0 0 0 0 0 0 0 0 0 0\n"+
      "1       5 ram5 0 0 0 0 0 0 0 0 0 0 0\n"+
      "1       6 ram6 0 0 0 0 0 0 0 0 0 0 0\n"+
      "7       0 loop0 0 0 0 0 0 0 0 0 0 0 0\n"+
      "7       1 loop1 0 0 0 0 0 0 0 0 0 0 0\n"+
      "8       0 sda 82575678 2486518 %d 59876600 3225402 19761924 %d " +
      "6407705 4 48803346 66227952\n"+
      "8       1 sda1 732 289 21354 787 7 3 32 4 0 769 791"+
      "8       2 sda2 744272 2206315 23605200 6742762 336830 2979630 " +
      "26539520 1424776 4 1820130 8165444\n"+
      "8       3 sda3 81830497 279914 17881852954 53132969 2888558 16782291 " +
      "157367552 4982925 0 47077660 58061635\n"+
      "8      32 sdc 10148118 693255 %d 122125461 6090515 401630172 %d 2696685590 " +
      "0 26848216 2818793840\n"+
      "8      33 sdc1 10147917 693230 2054138426 122125426 6090506 401630172 " +
      "3261765880 2696685589 0 26848181 2818793804\n"+
      "8      64 sde 9989771 553047 %d 93407551 5978572 391997273 %d 2388274325 " +
      "0 24396646 2481664818\n"+
      "8      65 sde1 9989570 553022 1943973346 93407489 5978563 391997273 3183807264 " +
      "2388274325 0 24396584 2481666274\n"+
      "8      80 sdf 10197163 693995 %d 144374395 6216644 408395438 %d 2669389056 0 " +
      "26164759 2813746348\n"+
      "8      81 sdf1 10196962 693970 2033452794 144374355 6216635 408395438 3316897064 " +
      "2669389056 0 26164719 2813746308\n"+
      "8     129 sdi1 10078602 657936 2056552626 108362198 6134036 403851153 3279882064 " +
      "2639256086 0 26260432 2747601085\n";

    assertEquals(plugin.getNetworkBytesWritten(), numBytesWrittenIntf1 + numBytesWrittenIntf2);
  }

  @Test
  public void parsingProcDisksFile() throws IOException {
    long numSectorsReadsda = 1790549L; long numSectorsWrittensda = 1839071L;
    long numSectorsReadsdc = 20541402L; long numSectorsWrittensdc = 32617658L;
    long numSectorsReadsde = 19439751L; long numSectorsWrittensde = 31838072L;
    long numSectorsReadsdf = 20334546L; long numSectorsWrittensdf = 33168970L;
    File tempFile = new File(FAKE_DISKSFILE);
    tempFile.deleteOnExit();
    FileWriter fWriter = new FileWriter(FAKE_DISKSFILE);
    fWriter.write(String.format(DISKSINFO_FORMAT,
             numSectorsReadsda, numSectorsWrittensda,
             numSectorsReadsdc, numSectorsWrittensdc,
             numSectorsReadsde, numSectorsWrittensde,
             numSectorsReadsdf, numSectorsWrittensdf));

    fWriter.close();
    long expectedNumSectorsRead = numSectorsReadsda + numSectorsReadsdc +
                                  numSectorsReadsde + numSectorsReadsdf;
    long expectedNumSectorsWritten = numSectorsWrittensda + numSectorsWrittensdc +
                                     numSectorsWrittensde + numSectorsWrittensdf;
    int diskSectorSize = FakeLinuxResourceCalculatorPlugin.SECTORSIZE;
    assertEquals(expectedNumSectorsRead * diskSectorSize,
        plugin.getStorageBytesRead());
    assertEquals(expectedNumSectorsWritten * diskSectorSize,
        plugin.getStorageBytesWritten());
  }
}

hadoop-tools/hadoop-gridmix/src/test/java/org/apache/hadoop/mapred/gridmix/DummyResourceCalculatorPlugin.java
  public static final String NETWORK_BYTES_WRITTEN =
      "mapred.tasktracker.networkwritten.testing";
  public static final String STORAGE_BYTES_READ =
      "mapred.tasktracker.storageread.testing";
  public static final String STORAGE_BYTES_WRITTEN =
      "mapred.tasktracker.storagewritten.testing";
  public static final String PROC_CUMULATIVE_CPU_TIME =
      "mapred.tasktracker.proccumulativecputime.testing";
    return getConf().getLong(NETWORK_BYTES_WRITTEN, -1);
  }

  @Override
  public long getStorageBytesRead() {
    return getConf().getLong(STORAGE_BYTES_READ, -1);
  }

  @Override
  public long getStorageBytesWritten() {
    return getConf().getLong(STORAGE_BYTES_WRITTEN, -1);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/ResourceCalculatorPlugin.java
    return sys.getNetworkBytesWritten();
  }

  public long getStorageBytesRead() {
    return sys.getStorageBytesRead();
  }

  public long getStorageBytesWritten() {
    return sys.getStorageBytesWritten();
  }


