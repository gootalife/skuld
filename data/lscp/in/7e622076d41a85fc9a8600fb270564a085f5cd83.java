hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
  public static final String  DFS_LIST_LIMIT = "dfs.ls.limit";
  public static final int     DFS_LIST_LIMIT_DEFAULT = 1000;
  public static final String  DFS_CONTENT_SUMMARY_LIMIT_KEY = "dfs.content-summary.limit";
  public static final int     DFS_CONTENT_SUMMARY_LIMIT_DEFAULT = 5000;
  public static final String  DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_KEY = "dfs.content-summary.sleep-microsec";
  public static final long    DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_DEFAULT = 500;
  public static final String  DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY = "dfs.datanode.failed.volumes.tolerated";
  public static final int     DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT = 0;
  public static final String  DFS_DATANODE_SYNCONCLOSE_KEY = "dfs.datanode.synconclose";

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/ContentSummaryComputationContext.java
  private long nextCountLimit = 0;
  private long limitPerRun = 0;
  private long yieldCount = 0;
  private long sleepMilliSec = 0;
  private int sleepNanoSec = 0;

  public ContentSummaryComputationContext(FSDirectory dir,
      FSNamesystem fsn, long limitPerRun, long sleepMicroSec) {
    this.dir = dir;
    this.fsn = fsn;
    this.limitPerRun = limitPerRun;
    this.nextCountLimit = limitPerRun;
    this.counts = new ContentCounts.Builder().build();
    this.sleepMilliSec = sleepMicroSec/1000;
    this.sleepNanoSec = (int)((sleepMicroSec%1000)*1000);
  }

  public ContentSummaryComputationContext(BlockStoragePolicySuite bsps) {
    this(null, null, 0, 1000);
    this.bsps = bsps;
  }

    fsn.readUnlock();

    try {
      Thread.sleep(sleepMilliSec, sleepNanoSec);
    } catch (InterruptedException ie) {
    } finally {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirStatAndListingOp.java
        ContentSummaryComputationContext cscc =
            new ContentSummaryComputationContext(fsd, fsd.getFSNamesystem(),
                fsd.getContentCountLimit(), fsd.getContentSleepMicroSec());
        ContentSummary cs = targetNode.computeAndConvertContentSummary(cscc);
        fsd.addYieldCount(cscc.getYieldCount());
        return cs;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirectory.java
  private final int maxDirItems;
  private final int lsLimit;  // max list limit
  private final int contentCountLimit; // max content summary counts per run
  private final long contentSleepMicroSec;
  private final INodeMap inodeMap; // Synchronized by dirLock
  private long yieldCount = 0; // keep track of lock yield count.

    this.contentCountLimit = conf.getInt(
        DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_KEY,
        DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_DEFAULT);
    this.contentSleepMicroSec = conf.getLong(
        DFSConfigKeys.DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_KEY,
        DFSConfigKeys.DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_DEFAULT);
    
    this.maxComponentLength = conf.getInt(
    return contentCountLimit;
  }

  long getContentSleepMicroSec() {
    return contentSleepMicroSec;
  }

  int getInodeXAttrsLimit() {
    return inodeXAttrsLimit;
  }

