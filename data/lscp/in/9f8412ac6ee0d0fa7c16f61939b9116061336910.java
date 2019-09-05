hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/SecondaryNameNode.java

  private final long starttime = Time.now();
  private volatile long lastCheckpointTime = 0;
  private volatile long lastCheckpointWallclockTime = 0;

  private URL fsName;
  private CheckpointStorage checkpointImage;
      + "\nName Node Address      : " + nameNodeAddr
      + "\nStart Time             : " + new Date(starttime)
      + "\nLast Checkpoint        : " + (lastCheckpointTime == 0? "--":
        new Date(lastCheckpointWallclockTime))
      + " (" + ((Time.monotonicNow() - lastCheckpointTime) / 1000)
      + " seconds ago)"
      + "\nCheckpoint Period      : " + checkpointConf.getPeriod() + " seconds"
      + "\nCheckpoint Transactions: " + checkpointConf.getTxnCount()
      + "\nCheckpoint Dirs        : " + checkpointDirs
        if(UserGroupInformation.isSecurityEnabled())
          UserGroupInformation.getCurrentUser().checkTGTAndReloginFromKeytab();
        
        final long monotonicNow = Time.monotonicNow();
        final long now = Time.now();

        if (shouldCheckpointBasedOnCount() ||
            monotonicNow >= lastCheckpointTime + 1000 * checkpointConf.getPeriod()) {
          doCheckpoint();
          lastCheckpointTime = monotonicNow;
          lastCheckpointWallclockTime = now;
        }
      } catch (IOException e) {
        LOG.error("Exception in doCheckpoint", e);
    checkpointThread.start();
  }

  @Override // SecondaryNameNodeInfoMXBean
  public String getHostAndPort() {
    return NetUtils.getHostPortString(nameNodeAddr);
  }

  @Override // SecondaryNameNodeInfoMXBean
  public long getStartTime() {
    return starttime;
  }

  @Override // SecondaryNameNodeInfoMXBean
  public long getLastCheckpointTime() {
    return lastCheckpointWallclockTime;
  }

  @Override // SecondaryNameNodeInfoMXBean
  public long getLastCheckpointDeltaMs() {
    if (lastCheckpointTime == 0) {
      return -1;
    } else {
      return (Time.monotonicNow() - lastCheckpointTime);
    }
  }

  @Override // SecondaryNameNodeInfoMXBean
  public String[] getCheckpointDirectories() {
    ArrayList<String> r = Lists.newArrayListWithCapacity(checkpointDirs.size());
    for (URI d : checkpointDirs) {
    return r.toArray(new String[r.size()]);
  }

  @Override // SecondaryNameNodeInfoMXBean
  public String[] getCheckpointEditlogDirectories() {
    ArrayList<String> r = Lists.newArrayListWithCapacity(checkpointEditsDirs.size());
    for (URI d : checkpointEditsDirs) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/SecondaryNameNodeInfoMXBean.java
  public long getLastCheckpointTime();

  public long getLastCheckpointDeltaMs();


