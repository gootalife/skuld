hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/CpuTimeTracker.java
++ b/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/CpuTimeTracker.java

package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.math.BigInteger;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CpuTimeTracker {
  public static final int UNAVAILABLE = -1;
  private final long minimumTimeInterval;

  private BigInteger cumulativeCpuTime = BigInteger.ZERO;

  private BigInteger lastCumulativeCpuTime = BigInteger.ZERO;

  private long sampleTime;
  private long lastSampleTime;
  private float cpuUsage;
  private BigInteger jiffyLengthInMillis;

  public CpuTimeTracker(long jiffyLengthInMillis) {
    this.jiffyLengthInMillis = BigInteger.valueOf(jiffyLengthInMillis);
    this.cpuUsage = UNAVAILABLE;
    this.sampleTime = UNAVAILABLE;
    this.lastSampleTime = UNAVAILABLE;
    minimumTimeInterval =  10 * jiffyLengthInMillis;
  }

  public float getCpuTrackerUsagePercent() {
    if (lastSampleTime == UNAVAILABLE ||
        lastSampleTime > sampleTime) {
      lastSampleTime = sampleTime;
      lastCumulativeCpuTime = cumulativeCpuTime;
      return cpuUsage;
    }
    if (sampleTime > lastSampleTime + minimumTimeInterval) {
      cpuUsage =
          ((cumulativeCpuTime.subtract(lastCumulativeCpuTime)).floatValue())
      lastSampleTime = sampleTime;
      lastCumulativeCpuTime = cumulativeCpuTime;
    }
    return cpuUsage;
  }

  public long getCumulativeCpuTime() {
    return cumulativeCpuTime.longValue();
  }

  public void updateElapsedJiffies(BigInteger elapsedJiffies, long newTime) {
    cumulativeCpuTime = elapsedJiffies.multiply(jiffyLengthInMillis);
    sampleTime = newTime;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SampleTime " + this.sampleTime);
    sb.append(" CummulativeCpuTime " + this.cumulativeCpuTime);
    sb.append(" LastSampleTime " + this.lastSampleTime);
    sb.append(" LastCummulativeCpuTime " + this.lastCumulativeCpuTime);
    sb.append(" CpuUsage " + this.cpuUsage);
    sb.append(" JiffyLengthMillisec " + this.jiffyLengthInMillis);
    return sb.toString();
  }
}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/SysInfo.java
++ b/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/SysInfo.java

package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class SysInfo {

  public static SysInfo newInstance() {
    if (Shell.LINUX) {
      return new SysInfoLinux();
    }
    if (Shell.WINDOWS) {
      return new SysInfoWindows();
    }
    throw new UnsupportedOperationException("Could not determine OS");
  }

  public abstract long getVirtualMemorySize();

  public abstract long getPhysicalMemorySize();

  public abstract long getAvailableVirtualMemorySize();

  public abstract long getAvailablePhysicalMemorySize();

  public abstract int getNumProcessors();

  public abstract int getNumCores();

  public abstract long getCpuFrequency();

  public abstract long getCumulativeCpuTime();

  public abstract float getCpuUsage();

}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/SysInfoLinux.java
++ b/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/SysInfoLinux.java

package org.apache.hadoop.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SysInfoLinux extends SysInfo {
  private static final Log LOG =
      LogFactory.getLog(SysInfoLinux.class);

  private static final String PROCFS_MEMFILE = "/proc/meminfo";
  private static final Pattern PROCFS_MEMFILE_FORMAT =
      Pattern.compile("^([a-zA-Z]*):[ \t]*([0-9]*)[ \t]kB");

  private static final String MEMTOTAL_STRING = "MemTotal";
  private static final String SWAPTOTAL_STRING = "SwapTotal";
  private static final String MEMFREE_STRING = "MemFree";
  private static final String SWAPFREE_STRING = "SwapFree";
  private static final String INACTIVE_STRING = "Inactive";

  private static final String PROCFS_CPUINFO = "/proc/cpuinfo";
  private static final Pattern PROCESSOR_FORMAT =
      Pattern.compile("^processor[ \t]:[ \t]*([0-9]*)");
  private static final Pattern FREQUENCY_FORMAT =
      Pattern.compile("^cpu MHz[ \t]*:[ \t]*([0-9.]*)");
  private static final Pattern PHYSICAL_ID_FORMAT =
      Pattern.compile("^physical id[ \t]*:[ \t]*([0-9]*)");
  private static final Pattern CORE_ID_FORMAT =
      Pattern.compile("^core id[ \t]*:[ \t]*([0-9]*)");

  private static final String PROCFS_STAT = "/proc/stat";
  private static final Pattern CPU_TIME_FORMAT =
      Pattern.compile("^cpu[ \t]*([0-9]*)" +
                      "[ \t]*([0-9]*)[ \t]*([0-9]*)[ \t].*");
  private CpuTimeTracker cpuTimeTracker;

  private String procfsMemFile;
  private String procfsCpuFile;
  private String procfsStatFile;
  private long jiffyLengthInMillis;

  private long ramSize = 0;
  private long swapSize = 0;
  private long ramSizeFree = 0;  // free ram space on the machine (kB)
  private long swapSizeFree = 0; // free swap space on the machine (kB)
  private long inactiveSize = 0; // inactive cache memory (kB)
  private int numProcessors = 0;
  private int numCores = 0;
  private long cpuFrequency = 0L; // CPU frequency on the system (kHz)

  private boolean readMemInfoFile = false;
  private boolean readCpuInfoFile = false;

  public static final long PAGE_SIZE = getConf("PAGESIZE");
  public static final long JIFFY_LENGTH_IN_MILLIS =
      Math.max(Math.round(1000D / getConf("CLK_TCK")), -1);

  private static long getConf(String attr) {
    if(Shell.LINUX) {
      try {
        ShellCommandExecutor shellExecutorClk = new ShellCommandExecutor(
            new String[] {"getconf", attr });
        shellExecutorClk.execute();
        return Long.parseLong(shellExecutorClk.getOutput().replace("\n", ""));
      } catch (IOException|NumberFormatException e) {
        return -1;
      }
    }
    return -1;
  }

  long getCurrentTime() {
    return System.currentTimeMillis();
  }

  public SysInfoLinux() {
    this(PROCFS_MEMFILE, PROCFS_CPUINFO, PROCFS_STAT,
        JIFFY_LENGTH_IN_MILLIS);
  }

  @VisibleForTesting
  public SysInfoLinux(String procfsMemFile,
                                       String procfsCpuFile,
                                       String procfsStatFile,
                                       long jiffyLengthInMillis) {
    this.procfsMemFile = procfsMemFile;
    this.procfsCpuFile = procfsCpuFile;
    this.procfsStatFile = procfsStatFile;
    this.jiffyLengthInMillis = jiffyLengthInMillis;
    this.cpuTimeTracker = new CpuTimeTracker(jiffyLengthInMillis);
  }

  private void readProcMemInfoFile() {
    readProcMemInfoFile(false);
  }

  private void readProcMemInfoFile(boolean readAgain) {

    if (readMemInfoFile && !readAgain) {
      return;
    }

    BufferedReader in;
    InputStreamReader fReader;
    try {
      fReader = new InputStreamReader(
          new FileInputStream(procfsMemFile), Charset.forName("UTF-8"));
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      LOG.warn("Couldn't read " + procfsMemFile
          + "; can't determine memory settings");
      return;
    }

    Matcher mat;

    try {
      String str = in.readLine();
      while (str != null) {
        mat = PROCFS_MEMFILE_FORMAT.matcher(str);
        if (mat.find()) {
          if (mat.group(1).equals(MEMTOTAL_STRING)) {
            ramSize = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(SWAPTOTAL_STRING)) {
            swapSize = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(MEMFREE_STRING)) {
            ramSizeFree = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(SWAPFREE_STRING)) {
            swapSizeFree = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(INACTIVE_STRING)) {
            inactiveSize = Long.parseLong(mat.group(2));
          }
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

    readMemInfoFile = true;
  }

  private void readProcCpuInfoFile() {
    if (readCpuInfoFile) {
      return;
    }
    HashSet<String> coreIdSet = new HashSet<>();
    BufferedReader in;
    InputStreamReader fReader;
    try {
      fReader = new InputStreamReader(
          new FileInputStream(procfsCpuFile), Charset.forName("UTF-8"));
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      LOG.warn("Couldn't read " + procfsCpuFile + "; can't determine cpu info");
      return;
    }
    Matcher mat;
    try {
      numProcessors = 0;
      numCores = 1;
      String currentPhysicalId = "";
      String str = in.readLine();
      while (str != null) {
        mat = PROCESSOR_FORMAT.matcher(str);
        if (mat.find()) {
          numProcessors++;
        }
        mat = FREQUENCY_FORMAT.matcher(str);
        if (mat.find()) {
          cpuFrequency = (long)(Double.parseDouble(mat.group(1)) * 1000); // kHz
        }
        mat = PHYSICAL_ID_FORMAT.matcher(str);
        if (mat.find()) {
          currentPhysicalId = str;
        }
        mat = CORE_ID_FORMAT.matcher(str);
        if (mat.find()) {
          coreIdSet.add(currentPhysicalId + " " + str);
          numCores = coreIdSet.size();
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
    readCpuInfoFile = true;
  }

  private void readProcStatFile() {
    BufferedReader in;
    InputStreamReader fReader;
    try {
      fReader = new InputStreamReader(
          new FileInputStream(procfsStatFile), Charset.forName("UTF-8"));
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      return;
    }

    Matcher mat;
    try {
      String str = in.readLine();
      while (str != null) {
        mat = CPU_TIME_FORMAT.matcher(str);
        if (mat.find()) {
          long uTime = Long.parseLong(mat.group(1));
          long nTime = Long.parseLong(mat.group(2));
          long sTime = Long.parseLong(mat.group(3));
          cpuTimeTracker.updateElapsedJiffies(
              BigInteger.valueOf(uTime + nTime + sTime),
              getCurrentTime());
          break;
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
    readProcMemInfoFile();
    return ramSize * 1024;
  }

  @Override
  public long getVirtualMemorySize() {
    readProcMemInfoFile();
    return (ramSize + swapSize) * 1024;
  }

  @Override
  public long getAvailablePhysicalMemorySize() {
    readProcMemInfoFile(true);
    return (ramSizeFree + inactiveSize) * 1024;
  }

  @Override
  public long getAvailableVirtualMemorySize() {
    readProcMemInfoFile(true);
    return (ramSizeFree + swapSizeFree + inactiveSize) * 1024;
  }

  @Override
  public int getNumProcessors() {
    readProcCpuInfoFile();
    return numProcessors;
  }

  @Override
  public int getNumCores() {
    readProcCpuInfoFile();
    return numCores;
  }

  @Override
  public long getCpuFrequency() {
    readProcCpuInfoFile();
    return cpuFrequency;
  }

  @Override
  public long getCumulativeCpuTime() {
    readProcStatFile();
    return cpuTimeTracker.getCumulativeCpuTime();
  }

  @Override
  public float getCpuUsage() {
    readProcStatFile();
    float overallCpuUsage = cpuTimeTracker.getCpuTrackerUsagePercent();
    if (overallCpuUsage != CpuTimeTracker.UNAVAILABLE) {
      overallCpuUsage = overallCpuUsage / getNumProcessors();
    }
    return overallCpuUsage;
  }

  public static void main(String[] args) {
    SysInfoLinux plugin = new SysInfoLinux();
    System.out.println("Physical memory Size (bytes) : "
        + plugin.getPhysicalMemorySize());
    System.out.println("Total Virtual memory Size (bytes) : "
        + plugin.getVirtualMemorySize());
    System.out.println("Available Physical memory Size (bytes) : "
        + plugin.getAvailablePhysicalMemorySize());
    System.out.println("Total Available Virtual memory Size (bytes) : "
        + plugin.getAvailableVirtualMemorySize());
    System.out.println("Number of Processors : " + plugin.getNumProcessors());
    System.out.println("CPU frequency (kHz) : " + plugin.getCpuFrequency());
    System.out.println("Cumulative CPU time (ms) : " +
            plugin.getCumulativeCpuTime());
    try {
      Thread.sleep(500L);
    } catch (InterruptedException e) {
    }
    System.out.println("CPU usage % : " + plugin.getCpuUsage());
  }

  @VisibleForTesting
  void setReadCpuInfoFile(boolean readCpuInfoFileValue) {
    this.readCpuInfoFile = readCpuInfoFileValue;
  }

  public long getJiffyLengthInMillis() {
    return this.jiffyLengthInMillis;
  }
}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/SysInfoWindows.java
++ b/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/SysInfoWindows.java
package org.apache.hadoop.util;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SysInfoWindows extends SysInfo {

  private static final Log LOG = LogFactory.getLog(SysInfoWindows.class);

  private long vmemSize;
  private long memSize;
  private long vmemAvailable;
  private long memAvailable;
  private int numProcessors;
  private long cpuFrequencyKhz;
  private long cumulativeCpuTimeMs;
  private float cpuUsage;

  private long lastRefreshTime;
  static final int REFRESH_INTERVAL_MS = 1000;

  public SysInfoWindows() {
    lastRefreshTime = 0;
    reset();
  }

  @VisibleForTesting
  long now() {
    return System.nanoTime();
  }

  void reset() {
    vmemSize = -1;
    memSize = -1;
    vmemAvailable = -1;
    memAvailable = -1;
    numProcessors = -1;
    cpuFrequencyKhz = -1;
    cumulativeCpuTimeMs = -1;
    cpuUsage = -1;
  }

  String getSystemInfoInfoFromShell() {
    ShellCommandExecutor shellExecutor = new ShellCommandExecutor(
        new String[] {Shell.WINUTILS, "systeminfo" });
    try {
      shellExecutor.execute();
      return shellExecutor.getOutput();
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    return null;
  }

  void refreshIfNeeded() {
    long now = now();
    if (now - lastRefreshTime > REFRESH_INTERVAL_MS) {
      long refreshInterval = now - lastRefreshTime;
      lastRefreshTime = now;
      long lastCumCpuTimeMs = cumulativeCpuTimeMs;
      reset();
      String sysInfoStr = getSystemInfoInfoFromShell();
      if (sysInfoStr != null) {
        final int sysInfoSplitCount = 7;
        String[] sysInfo = sysInfoStr.substring(0, sysInfoStr.indexOf("\r\n"))
            .split(",");
        if (sysInfo.length == sysInfoSplitCount) {
          try {
            vmemSize = Long.parseLong(sysInfo[0]);
            memSize = Long.parseLong(sysInfo[1]);
            vmemAvailable = Long.parseLong(sysInfo[2]);
            memAvailable = Long.parseLong(sysInfo[3]);
            numProcessors = Integer.parseInt(sysInfo[4]);
            cpuFrequencyKhz = Long.parseLong(sysInfo[5]);
            cumulativeCpuTimeMs = Long.parseLong(sysInfo[6]);
            if (lastCumCpuTimeMs != -1) {
              cpuUsage = (cumulativeCpuTimeMs - lastCumCpuTimeMs)
                  / (refreshInterval * 1.0f);
            }
          } catch (NumberFormatException nfe) {
            LOG.warn("Error parsing sysInfo", nfe);
          }
        } else {
          LOG.warn("Expected split length of sysInfo to be "
              + sysInfoSplitCount + ". Got " + sysInfo.length);
        }
      }
    }
  }

  @Override
  public long getVirtualMemorySize() {
    refreshIfNeeded();
    return vmemSize;
  }

  @Override
  public long getPhysicalMemorySize() {
    refreshIfNeeded();
    return memSize;
  }

  @Override
  public long getAvailableVirtualMemorySize() {
    refreshIfNeeded();
    return vmemAvailable;
  }

  @Override
  public long getAvailablePhysicalMemorySize() {
    refreshIfNeeded();
    return memAvailable;
  }

  @Override
  public int getNumProcessors() {
    refreshIfNeeded();
    return numProcessors;
  }

  @Override
  public int getNumCores() {
    return getNumProcessors();
  }

  @Override
  public long getCpuFrequency() {
    refreshIfNeeded();
    return cpuFrequencyKhz;
  }

  @Override
  public long getCumulativeCpuTime() {
    refreshIfNeeded();
    return cumulativeCpuTimeMs;
  }

  @Override
  public float getCpuUsage() {
    refreshIfNeeded();
    return cpuUsage;
  }
}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/util/TestSysInfoLinux.java
++ b/hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/util/TestSysInfoLinux.java

package org.apache.hadoop.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestSysInfoLinux {
  static class FakeLinuxResourceCalculatorPlugin extends
      SysInfoLinux {

    long currentTime = 0;
    public FakeLinuxResourceCalculatorPlugin(String procfsMemFile,
                                             String procfsCpuFile,
                                             String procfsStatFile,
                                             long jiffyLengthInMillis) {
      super(procfsMemFile, procfsCpuFile, procfsStatFile, jiffyLengthInMillis);
    }
    @Override
    long getCurrentTime() {
      return currentTime;
    }
    public void advanceTime(long adv) {
      currentTime += adv * this.getJiffyLengthInMillis();
    }
  }
  private static final FakeLinuxResourceCalculatorPlugin plugin;
  private static String TEST_ROOT_DIR = new Path(System.getProperty(
         "test.build.data", "/tmp")).toString().replace(' ', '+');
  private static final String FAKE_MEMFILE;
  private static final String FAKE_CPUFILE;
  private static final String FAKE_STATFILE;
  private static final long FAKE_JIFFY_LENGTH = 10L;
  static {
    int randomNum = (new Random()).nextInt(1000000000);
    FAKE_MEMFILE = TEST_ROOT_DIR + File.separator + "MEMINFO_" + randomNum;
    FAKE_CPUFILE = TEST_ROOT_DIR + File.separator + "CPUINFO_" + randomNum;
    FAKE_STATFILE = TEST_ROOT_DIR + File.separator + "STATINFO_" + randomNum;
    plugin = new FakeLinuxResourceCalculatorPlugin(FAKE_MEMFILE, FAKE_CPUFILE,
                                                   FAKE_STATFILE,
                                                   FAKE_JIFFY_LENGTH);
  }
  static final String MEMINFO_FORMAT =
    "MemTotal:      %d kB\n" +
    "MemFree:         %d kB\n" +
    "Buffers:        138244 kB\n" +
    "Cached:         947780 kB\n" +
    "SwapCached:     142880 kB\n" +
    "Active:        3229888 kB\n" +
    "Inactive:       %d kB\n" +
    "SwapTotal:     %d kB\n" +
    "SwapFree:      %d kB\n" +
    "Dirty:          122012 kB\n" +
    "Writeback:           0 kB\n" +
    "AnonPages:     2710792 kB\n" +
    "Mapped:          24740 kB\n" +
    "Slab:           132528 kB\n" +
    "SReclaimable:   105096 kB\n" +
    "SUnreclaim:      27432 kB\n" +
    "PageTables:      11448 kB\n" +
    "NFS_Unstable:        0 kB\n" +
    "Bounce:              0 kB\n" +
    "CommitLimit:   4125904 kB\n" +
    "Committed_AS:  4143556 kB\n" +
    "VmallocTotal: 34359738367 kB\n" +
    "VmallocUsed:      1632 kB\n" +
    "VmallocChunk: 34359736375 kB\n" +
    "HugePages_Total:     0\n" +
    "HugePages_Free:      0\n" +
    "HugePages_Rsvd:      0\n" +
    "Hugepagesize:     2048 kB";

  static final String CPUINFO_FORMAT =
    "processor : %s\n" +
    "vendor_id : AuthenticAMD\n" +
    "cpu family  : 15\n" +
    "model   : 33\n" +
    "model name  : Dual Core AMD Opteron(tm) Processor 280\n" +
    "stepping  : 2\n" +
    "cpu MHz   : %f\n" +
    "cache size  : 1024 KB\n" +
    "physical id : %s\n" +
    "siblings  : 2\n" +
    "core id   : %s\n" +
    "cpu cores : 2\n" +
    "fpu   : yes\n" +
    "fpu_exception : yes\n" +
    "cpuid level : 1\n" +
    "wp    : yes\n" +
    "flags   : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov " +
    "pat pse36 clflush mmx fxsr sse sse2 ht syscall nx mmxext fxsr_opt lm " +
    "3dnowext 3dnow pni lahf_lm cmp_legacy\n" +
    "bogomips  : 4792.41\n" +
    "TLB size  : 1024 4K pages\n" +
    "clflush size  : 64\n" +
    "cache_alignment : 64\n" +
    "address sizes : 40 bits physical, 48 bits virtual\n" +
    "power management: ts fid vid ttp";

  static final String STAT_FILE_FORMAT =
    "cpu  %d %d %d 1646495089 831319 48713 164346 0\n" +
    "cpu0 15096055 30805 3823005 411456015 206027 13 14269 0\n" +
    "cpu1 14760561 89890 6432036 408707910 456857 48074 130857 0\n" +
    "cpu2 12761169 20842 3758639 413976772 98028 411 10288 0\n" +
    "cpu3 12355207 47322 5789691 412354390 70406 213 8931 0\n" +
    "intr 114648668 20010764 2 0 945665 2 0 0 0 0 0 0 0 4 0 0 0 0 0 0\n" +
    "ctxt 242017731764\n" +
    "btime 1257808753\n" +
    "processes 26414943\n" +
    "procs_running 1\n" +
    "procs_blocked 0\n";

  @Test
  public void parsingProcStatAndCpuFile() throws IOException {
    long numProcessors = 8;
    long cpuFrequencyKHz = 2392781;
    String fileContent = "";
    for (int i = 0; i < numProcessors; i++) {
      fileContent +=
          String.format(CPUINFO_FORMAT, i, cpuFrequencyKHz / 1000D, 0, 0)
              + "\n";
    }
    File tempFile = new File(FAKE_CPUFILE);
    tempFile.deleteOnExit();
    FileWriter fWriter = new FileWriter(FAKE_CPUFILE);
    fWriter.write(fileContent);
    fWriter.close();
    assertEquals(plugin.getNumProcessors(), numProcessors);
    assertEquals(plugin.getCpuFrequency(), cpuFrequencyKHz);

    long uTime = 54972994;
    long nTime = 188860;
    long sTime = 19803373;
    tempFile = new File(FAKE_STATFILE);
    tempFile.deleteOnExit();
    updateStatFile(uTime, nTime, sTime);
    assertEquals(plugin.getCumulativeCpuTime(),
                 FAKE_JIFFY_LENGTH * (uTime + nTime + sTime));
    assertEquals(plugin.getCpuUsage(), (float)(CpuTimeTracker.UNAVAILABLE),0.0);

    uTime += 100L;
    plugin.advanceTime(200L);
    updateStatFile(uTime, nTime, sTime);
    assertEquals(plugin.getCumulativeCpuTime(),
                 FAKE_JIFFY_LENGTH * (uTime + nTime + sTime));
    assertEquals(plugin.getCpuUsage(), 6.25F, 0.0);

    uTime += 600L;
    plugin.advanceTime(300L);
    updateStatFile(uTime, nTime, sTime);
    assertEquals(plugin.getCpuUsage(), 25F, 0.0);

    uTime += 1L;
    plugin.advanceTime(1L);
    updateStatFile(uTime, nTime, sTime);
    assertEquals(plugin.getCumulativeCpuTime(),
                 FAKE_JIFFY_LENGTH * (uTime + nTime + sTime));
    assertEquals(plugin.getCpuUsage(), 25F, 0.0); // CPU usage is not updated.
  }

  private void updateStatFile(long uTime, long nTime, long sTime)
    throws IOException {
    FileWriter fWriter = new FileWriter(FAKE_STATFILE);
    fWriter.write(String.format(STAT_FILE_FORMAT, uTime, nTime, sTime));
    fWriter.close();
  }

  @Test
  public void parsingProcMemFile() throws IOException {
    long memTotal = 4058864L;
    long memFree = 99632L;
    long inactive = 567732L;
    long swapTotal = 2096472L;
    long swapFree = 1818480L;
    File tempFile = new File(FAKE_MEMFILE);
    tempFile.deleteOnExit();
    FileWriter fWriter = new FileWriter(FAKE_MEMFILE);
    fWriter.write(String.format(MEMINFO_FORMAT,
      memTotal, memFree, inactive, swapTotal, swapFree));

    fWriter.close();
    assertEquals(plugin.getAvailablePhysicalMemorySize(),
                 1024L * (memFree + inactive));
    assertEquals(plugin.getAvailableVirtualMemorySize(),
                 1024L * (memFree + inactive + swapFree));
    assertEquals(plugin.getPhysicalMemorySize(), 1024L * memTotal);
    assertEquals(plugin.getVirtualMemorySize(), 1024L * (memTotal + swapTotal));
  }

  @Test
  public void testCoreCounts() throws IOException {

    String fileContent = "";
    long numProcessors = 2;
    long cpuFrequencyKHz = 2392781;
    for (int i = 0; i < numProcessors; i++) {
      fileContent =
          fileContent.concat(String.format(CPUINFO_FORMAT, i,
            cpuFrequencyKHz / 1000D, 0, 0));
      fileContent = fileContent.concat("\n");
    }
    writeFakeCPUInfoFile(fileContent);
    plugin.setReadCpuInfoFile(false);
    assertEquals(numProcessors, plugin.getNumProcessors());
    assertEquals(1, plugin.getNumCores());

    fileContent = "";
    numProcessors = 4;
    for (int i = 0; i < numProcessors; i++) {
      fileContent =
          fileContent.concat(String.format(CPUINFO_FORMAT, i,
            cpuFrequencyKHz / 1000D, 0, i));
      fileContent = fileContent.concat("\n");
    }
    writeFakeCPUInfoFile(fileContent);
    plugin.setReadCpuInfoFile(false);
    assertEquals(numProcessors, plugin.getNumProcessors());
    assertEquals(4, plugin.getNumCores());

    fileContent = "";
    numProcessors = 4;
    for (int i = 0; i < numProcessors; i++) {
      fileContent =
          fileContent.concat(String.format(CPUINFO_FORMAT, i,
            cpuFrequencyKHz / 1000D, i / 2, 0));
      fileContent = fileContent.concat("\n");
    }
    writeFakeCPUInfoFile(fileContent);
    plugin.setReadCpuInfoFile(false);
    assertEquals(numProcessors, plugin.getNumProcessors());
    assertEquals(2, plugin.getNumCores());

    fileContent = "";
    numProcessors = 4;
    for (int i = 0; i < numProcessors; i++) {
      fileContent =
          fileContent.concat(String.format(CPUINFO_FORMAT, i,
            cpuFrequencyKHz / 1000D, i / 2, i % 2));
      fileContent = fileContent.concat("\n");
    }
    writeFakeCPUInfoFile(fileContent);
    plugin.setReadCpuInfoFile(false);
    assertEquals(numProcessors, plugin.getNumProcessors());
    assertEquals(4, plugin.getNumCores());

    fileContent = "";
    numProcessors = 8;
    for (int i = 0; i < numProcessors; i++) {
      fileContent =
          fileContent.concat(String.format(CPUINFO_FORMAT, i,
            cpuFrequencyKHz / 1000D, i / 4, (i % 4) / 2));
      fileContent = fileContent.concat("\n");
    }
    writeFakeCPUInfoFile(fileContent);
    plugin.setReadCpuInfoFile(false);
    assertEquals(numProcessors, plugin.getNumProcessors());
    assertEquals(4, plugin.getNumCores());
  }

  private void writeFakeCPUInfoFile(String content) throws IOException {
    File tempFile = new File(FAKE_CPUFILE);
    FileWriter fWriter = new FileWriter(FAKE_CPUFILE);
    tempFile.deleteOnExit();
    try {
      fWriter.write(content);
    } finally {
      IOUtils.closeQuietly(fWriter);
    }
  }
}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/util/TestSysInfoWindows.java
++ b/hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/util/TestSysInfoWindows.java

package org.apache.hadoop.util;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestSysInfoWindows {


  static class SysInfoWindowsMock extends SysInfoWindows {
    private long time = SysInfoWindows.REFRESH_INTERVAL_MS + 1;
    private String infoStr = null;
    void setSysinfoString(String infoStr) {
      this.infoStr = infoStr;
    }
    void advance(long dur) {
      time += dur;
    }
    @Override
    String getSystemInfoInfoFromShell() {
      return infoStr;
    }
    @Override
    long now() {
      return time;
    }
  }

  @Test(timeout = 10000)
  public void parseSystemInfoString() {
    SysInfoWindowsMock tester = new SysInfoWindowsMock();
    tester.setSysinfoString(
        "17177038848,8589467648,15232745472,6400417792,1,2805000,6261812\r\n");
    assertEquals(17177038848L, tester.getVirtualMemorySize());
    assertEquals(8589467648L, tester.getPhysicalMemorySize());
    assertEquals(15232745472L, tester.getAvailableVirtualMemorySize());
    assertEquals(6400417792L, tester.getAvailablePhysicalMemorySize());
    assertEquals(1, tester.getNumProcessors());
    assertEquals(1, tester.getNumCores());
    assertEquals(2805000L, tester.getCpuFrequency());
    assertEquals(6261812L, tester.getCumulativeCpuTime());
    assertEquals(-1.0, tester.getCpuUsage(), 0.0);
  }

  @Test(timeout = 10000)
  public void refreshAndCpuUsage() throws InterruptedException {
    SysInfoWindowsMock tester = new SysInfoWindowsMock();
    tester.setSysinfoString(
        "17177038848,8589467648,15232745472,6400417792,1,2805000,6261812\r\n");
    tester.getAvailablePhysicalMemorySize();
    assertEquals(6400417792L, tester.getAvailablePhysicalMemorySize());
    assertEquals(-1.0, tester.getCpuUsage(), 0.0);

    tester.setSysinfoString(
        "17177038848,8589467648,15232745472,5400417792,1,2805000,6263012\r\n");
    tester.getAvailablePhysicalMemorySize();
    assertEquals(6400417792L, tester.getAvailablePhysicalMemorySize());
    assertEquals(-1.0, tester.getCpuUsage(), 0.0);

    tester.advance(SysInfoWindows.REFRESH_INTERVAL_MS + 1);

    assertEquals(5400417792L, tester.getAvailablePhysicalMemorySize());
    assertEquals((6263012 - 6261812) / (SysInfoWindows.REFRESH_INTERVAL_MS + 1f),
        tester.getCpuUsage(), 0.0);
  }

  @Test(timeout = 10000)
  public void errorInGetSystemInfo() {
    SysInfoWindowsMock tester = new SysInfoWindowsMock();
    tester.setSysinfoString(null);
    tester.getAvailablePhysicalMemorySize();
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/LinuxResourceCalculatorPlugin.java
package org.apache.hadoop.yarn.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.SysInfoLinux;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class LinuxResourceCalculatorPlugin extends ResourceCalculatorPlugin {

  public LinuxResourceCalculatorPlugin() {
    super(new SysInfoLinux());
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/ProcfsBasedProcessTree.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.CpuTimeTracker;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.SysInfoLinux;
import org.apache.hadoop.yarn.conf.YarnConfiguration;


  public static final String PROCFS_STAT_FILE = "stat";
  public static final String PROCFS_CMDLINE_FILE = "cmdline";
  public static final long PAGE_SIZE = SysInfoLinux.PAGE_SIZE;
  public static final long JIFFY_LENGTH_IN_MILLIS =
      SysInfoLinux.JIFFY_LENGTH_IN_MILLIS; // in millisecond
  private final CpuTimeTracker cpuTimeTracker;
  private Clock clock;

  protected Map<String, ProcessTreeSmapMemInfo> processSMAPTree =
      new HashMap<String, ProcessTreeSmapMemInfo>();

  private String procfsDir;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/ResourceCalculatorPlugin.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.SysInfo;

@InterfaceAudience.LimitedPrivate({"YARN", "MAPREDUCE"})
@InterfaceStability.Unstable
public class ResourceCalculatorPlugin extends Configured {

  private final SysInfo sys;

  protected ResourceCalculatorPlugin() {
    this(SysInfo.newInstance());
  }

  public ResourceCalculatorPlugin(SysInfo sys) {
    this.sys = sys;
  }

  public long getVirtualMemorySize() {
    return sys.getVirtualMemorySize();
  }

  public long getPhysicalMemorySize() {
    return sys.getPhysicalMemorySize();
  }

  public long getAvailableVirtualMemorySize() {
    return sys.getAvailableVirtualMemorySize();
  }

  public long getAvailablePhysicalMemorySize() {
    return sys.getAvailablePhysicalMemorySize();
  }

  public int getNumProcessors() {
    return sys.getNumProcessors();
  }

  public int getNumCores() {
    return sys.getNumCores();
  }

  public long getCpuFrequency() {
    return sys.getCpuFrequency();
  }

  public long getCumulativeCpuTime() {
    return sys.getCumulativeCpuTime();
  }

  public float getCpuUsage() {
    return sys.getCpuUsage();
  }

    if (clazz != null) {
      return ReflectionUtils.newInstance(clazz, conf);
    }
    try {
      return new ResourceCalculatorPlugin();
    } catch (SecurityException e) {
      return null;
    }
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/WindowsBasedProcessTree.java

  @Override
  public float getCpuUsagePercent() {
    return UNAVAILABLE;
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/WindowsResourceCalculatorPlugin.java
package org.apache.hadoop.yarn.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.SysInfoWindows;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class WindowsResourceCalculatorPlugin extends ResourceCalculatorPlugin {

  public WindowsResourceCalculatorPlugin() {
    super(new SysInfoWindows());
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/util/TestResourceCalculatorProcessTree.java

    @Override
    public float getCpuUsagePercent() {
      return UNAVAILABLE;
    }

    public boolean checkPidPgrpidForMatch() {

