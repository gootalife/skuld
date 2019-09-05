hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java

  @Override  // NameNodeMXBean
  public RollingUpgradeInfo.Bean getRollingUpgradeStatus() {
    if (!isRollingUpgrade()) {
      return null;
    }
    RollingUpgradeInfo upgradeInfo = getRollingUpgradeInfo();
    if (upgradeInfo.createdRollbackImages()) {
      return new RollingUpgradeInfo.Bean(upgradeInfo);
    }
    readLock();
    try {
      upgradeInfo = getRollingUpgradeInfo();
      if (upgradeInfo == null) {
        return null;
      }
      if (!upgradeInfo.createdRollbackImages()) {
        boolean hasRollbackImage = this.getFSImage().hasRollbackFSImage();
        upgradeInfo.setCreatedRollbackImages(hasRollbackImage);
      }
    } catch (IOException ioe) {
      LOG.warn("Encountered exception setting Rollback Image", ioe);
    } finally {
      readUnlock();
    }
    return new RollingUpgradeInfo.Bean(upgradeInfo);
  }

  public boolean isRollingUpgrade() {

