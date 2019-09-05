hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/DirectoryCollection.java
  private float diskUtilizationPercentageCutoff;
  private long diskUtilizationSpaceCutoff;

  private int goodDirsDiskUtilizationPercentage;

            + dirsFailedCheck.get(dir).message);
      }
    }
    setGoodDirsDiskUtilizationPercentage();
    return setChanged;
  }

        diskUtilizationSpaceCutoff < 0 ? 0 : diskUtilizationSpaceCutoff;
    this.diskUtilizationSpaceCutoff = diskUtilizationSpaceCutoff;
  }

  private void setGoodDirsDiskUtilizationPercentage() {

    long totalSpace = 0;
    long usableSpace = 0;

    for (String dir : localDirs) {
      File f = new File(dir);
      if (!f.isDirectory()) {
        continue;
      }
      totalSpace += f.getTotalSpace();
      usableSpace += f.getUsableSpace();
    }
    if (totalSpace != 0) {
      long tmp = ((totalSpace - usableSpace) * 100) / totalSpace;
      if (Integer.MIN_VALUE < tmp && Integer.MAX_VALUE > tmp) {
        goodDirsDiskUtilizationPercentage = (int) tmp;
      }
    } else {
      goodDirsDiskUtilizationPercentage = 0;
    }
  }

  public int getGoodDirsDiskUtilizationPercentage() {
    return goodDirsDiskUtilizationPercentage;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/LocalDirsHandlerService.java
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;

  
  private static String FILE_SCHEME = "file";

  private NodeManagerMetrics nodeManagerMetrics = null;

  }

  public LocalDirsHandlerService() {
    this(null);
  }

  public LocalDirsHandlerService(NodeManagerMetrics nodeManagerMetrics) {
    super(LocalDirsHandlerService.class.getName());
    this.nodeManagerMetrics = nodeManagerMetrics;
  }

      updateDirsAfterTest();
    }

    updateMetrics();

    lastDisksCheckTime = System.currentTimeMillis();
  }

    validPaths.toArray(arrValidPaths);
    return arrValidPaths;
  }

  protected void updateMetrics() {
    if (nodeManagerMetrics != null) {
      nodeManagerMetrics.setBadLocalDirs(localDirs.getFailedDirs().size());
      nodeManagerMetrics.setBadLogDirs(logDirs.getFailedDirs().size());
      nodeManagerMetrics.setGoodLocalDirsDiskUtilizationPerc(
          localDirs.getGoodDirsDiskUtilizationPercentage());
      nodeManagerMetrics.setGoodLogDirsDiskUtilizationPerc(
          logDirs.getGoodDirsDiskUtilizationPercentage());
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeManager.java
    this.dispatcher = new AsyncDispatcher();

    nodeHealthChecker = new NodeHealthCheckerService();
    dirsHandler = new LocalDirsHandlerService(metrics);

    addService(nodeHealthChecker);
    dirsHandler = nodeHealthChecker.getDiskHandler();


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/metrics/NodeManagerMetrics.java
  @Metric MutableGaugeInt availableVCores;
  @Metric("Container launch duration")
      MutableRate containerLaunchDuration;
  @Metric("# of bad local dirs")
      MutableGaugeInt badLocalDirs;
  @Metric("# of bad log dirs")
      MutableGaugeInt badLogDirs;
  @Metric("Disk utilization % on good local dirs")
      MutableGaugeInt goodLocalDirsDiskUtilizationPerc;
  @Metric("Disk utilization % on good log dirs")
      MutableGaugeInt goodLogDirsDiskUtilizationPerc;


  private long allocatedMB;
  private long availableMB;
    containerLaunchDuration.add(value);
  }

  public void setBadLocalDirs(int badLocalDirs) {
    this.badLocalDirs.set(badLocalDirs);
  }

  public void setBadLogDirs(int badLogDirs) {
    this.badLogDirs.set(badLogDirs);
  }

  public void setGoodLocalDirsDiskUtilizationPerc(
      int goodLocalDirsDiskUtilizationPerc) {
    this.goodLocalDirsDiskUtilizationPerc.set(goodLocalDirsDiskUtilizationPerc);
  }

  public void setGoodLogDirsDiskUtilizationPerc(
      int goodLogDirsDiskUtilizationPerc) {
    this.goodLogDirsDiskUtilizationPerc.set(goodLogDirsDiskUtilizationPerc);
  }

  public int getRunningContainers() {
    return containersRunning.value();
  }
  public int getCompletedContainers() {
    return containersCompleted.value();
  }

  @VisibleForTesting
  public int getBadLogDirs() {
    return badLogDirs.value();
  }

  @VisibleForTesting
  public int getBadLocalDirs() {
    return badLocalDirs.value();
  }

  @VisibleForTesting
  public int getGoodLogDirsDiskUtilizationPerc() {
    return goodLogDirsDiskUtilizationPerc.value();
  }

  @VisibleForTesting
  public int getGoodLocalDirsDiskUtilizationPerc() {
    return goodLocalDirsDiskUtilizationPerc.value();
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestDirectoryCollection.java
    Assert.assertEquals(0, dc.getGoodDirs().size());
    Assert.assertEquals(1, dc.getFailedDirs().size());
    Assert.assertEquals(1, dc.getFullDirs().size());
    Assert.assertEquals(0, dc.getGoodDirsDiskUtilizationPercentage());

    dc = new DirectoryCollection(dirs, 100.0F);
    int utilizedSpacePerc =
        (int) ((testDir.getTotalSpace() - testDir.getUsableSpace()) * 100 /
            testDir.getTotalSpace());
    dc.checkDirs();
    Assert.assertEquals(1, dc.getGoodDirs().size());
    Assert.assertEquals(0, dc.getFailedDirs().size());
    Assert.assertEquals(0, dc.getFullDirs().size());
    Assert.assertEquals(utilizedSpacePerc,
      dc.getGoodDirsDiskUtilizationPercentage());

    dc = new DirectoryCollection(dirs, testDir.getTotalSpace() / (1024 * 1024));
    dc.checkDirs();
    Assert.assertEquals(0, dc.getGoodDirs().size());
    Assert.assertEquals(1, dc.getFailedDirs().size());
    Assert.assertEquals(1, dc.getFullDirs().size());
    Assert.assertEquals(0, dc.getGoodDirsDiskUtilizationPercentage());

    dc = new DirectoryCollection(dirs, 100.0F, 0);
    utilizedSpacePerc =
        (int)((testDir.getTotalSpace() - testDir.getUsableSpace()) * 100 /
            testDir.getTotalSpace());
    dc.checkDirs();
    Assert.assertEquals(1, dc.getGoodDirs().size());
    Assert.assertEquals(0, dc.getFailedDirs().size());
    Assert.assertEquals(0, dc.getFullDirs().size());
    Assert.assertEquals(utilizedSpacePerc,
      dc.getGoodDirsDiskUtilizationPercentage());
  }

  @Test

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestLocalDirsHandlerService.java
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
    conf.set(YarnConfiguration.NM_LOG_DIRS, logDir1 + "," + logDir2);
    conf.setFloat(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE,
      0.0f);
    NodeManagerMetrics nm = NodeManagerMetrics.create();
    LocalDirsHandlerService dirSvc = new LocalDirsHandlerService(nm);
    dirSvc.init(conf);
    Assert.assertEquals(0, dirSvc.getLocalDirs().size());
    Assert.assertEquals(0, dirSvc.getLogDirs().size());
    Assert.assertEquals(1, dirSvc.getDiskFullLocalDirs().size());
    Assert.assertEquals(1, dirSvc.getDiskFullLogDirs().size());
    Assert.assertEquals(2, nm.getBadLocalDirs());
    Assert.assertEquals(2, nm.getBadLogDirs());
    Assert.assertEquals(0, nm.getGoodLocalDirsDiskUtilizationPerc());
    Assert.assertEquals(0, nm.getGoodLogDirsDiskUtilizationPerc());

    conf.setFloat(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE,
      100.0f);
    nm = NodeManagerMetrics.create();
    dirSvc = new LocalDirsHandlerService(nm);
    dirSvc.init(conf);
    Assert.assertEquals(1, dirSvc.getLocalDirs().size());
    Assert.assertEquals(1, dirSvc.getLogDirs().size());
    Assert.assertEquals(0, dirSvc.getDiskFullLocalDirs().size());
    Assert.assertEquals(0, dirSvc.getDiskFullLogDirs().size());
    File dir = new File(localDir1);
    int utilizationPerc =
        (int) ((dir.getTotalSpace() - dir.getUsableSpace()) * 100 /
            dir.getTotalSpace());
    Assert.assertEquals(1, nm.getBadLocalDirs());
    Assert.assertEquals(1, nm.getBadLogDirs());
    Assert.assertEquals(utilizationPerc,
      nm.getGoodLocalDirsDiskUtilizationPerc());
    Assert
      .assertEquals(utilizationPerc, nm.getGoodLogDirsDiskUtilizationPerc());

    FileUtils.deleteDirectory(new File(localDir1));
    FileUtils.deleteDirectory(new File(localDir2));
    FileUtils.deleteDirectory(new File(logDir1));

